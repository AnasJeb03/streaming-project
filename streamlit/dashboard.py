import streamlit as st
import pymongo
import pandas as pd
import os  # ← FIXED: Added this import
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

# Configuration de la page
st.set_page_config(
    page_title="Invoice Dashboard",
    page_icon="📊",
    layout="wide"
)

# Connexion à MongoDB
@st.cache_resource
def get_mongodb_client():
    """Connexion à MongoDB"""
    try:
        # FIXED: Better environment variable handling
        mongo_url = os.getenv(
            'MONGODB_URL', 
            'mongodb://admin:admin123@localhost:27017/'
        )
        client = pymongo.MongoClient(mongo_url)
        # Test connection
        client.server_info()
        return client
    except Exception as e:
        st.error(f"Erreur de connexion à MongoDB: {str(e)}")
        return None

# Récupérer les données depuis MongoDB
@st.cache_data(ttl=5)
def get_invoices_data():
    """Récupère toutes les invoices depuis MongoDB"""
    client = get_mongodb_client()
    if client is None:
        return []
    
    try:
        db = client['invoices_db']
        collection = db['invoices']
        invoices = list(collection.find())
        return invoices
    except Exception as e:
        st.error(f"Erreur lors de la récupération des données: {str(e)}")
        return []

def get_invoice_by_number(invoice_no):
    """Récupère une invoice spécifique par son numéro"""
    client = get_mongodb_client()
    if client is None:
        return None
    
    try:
        db = client['invoices_db']
        collection = db['invoices']
        invoice = collection.find_one({"InvoiceNo": invoice_no})
        return invoice
    except Exception as e:
        st.error(f"Erreur: {str(e)}")
        return None

def main():
    st.title("📊 Invoice Streaming Dashboard")
    st.markdown("---")
    
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Choose a page:",
        ["Overview", "Invoice Details", "Analytics", "Raw Data"]
    )
    
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    invoices = get_invoices_data()
    
    if not invoices:
        st.warning("⚠️ No invoices found in MongoDB. Please send some invoices first!")
        st.info("Start the API and send invoices using the client script.")
        return
    
    if page == "Overview":
        show_overview(invoices)
    elif page == "Invoice Details":
        show_invoice_details(invoices)
    elif page == "Analytics":
        show_analytics(invoices)
    elif page == "Raw Data":
        show_raw_data(invoices)

def show_overview(invoices):
    st.header("📈 Overview")
    
    total_invoices = len(invoices)
    total_revenue = sum(inv.get('InvoiceTotal', 0) for inv in invoices)
    total_items = sum(len(inv.get('Items', [])) for inv in invoices)
    unique_customers = len(set(inv.get('CustomerID') for inv in invoices))
    unique_countries = len(set(inv.get('Country') for inv in invoices))
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Invoices", f"{total_invoices:,}")
    with col2:
        st.metric("Total Revenue", f"${total_revenue:,.2f}")
    with col3:
        st.metric("Total Items", f"{total_items:,}")
    with col4:
        st.metric("Unique Customers", f"{unique_customers:,}")
    with col5:
        st.metric("Countries", f"{unique_countries}")
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Top 10 Countries by Revenue")
        country_revenue = {}
        for inv in invoices:
            country = inv.get('Country', 'Unknown')
            country_revenue[country] = country_revenue.get(country, 0) + inv.get('InvoiceTotal', 0)
        
        top_countries = sorted(country_revenue.items(), key=lambda x: x[1], reverse=True)[:10]
        
        if top_countries:
            df_countries = pd.DataFrame(top_countries, columns=['Country', 'Revenue'])
            fig = px.bar(df_countries, x='Country', y='Revenue', 
                        title='Revenue by Country',
                        color='Revenue',
                        color_continuous_scale='Blues')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Top 10 Customers by Spending")
        customer_spending = {}
        for inv in invoices:
            customer_id = inv.get('CustomerID')
            customer_spending[customer_id] = customer_spending.get(customer_id, 0) + inv.get('InvoiceTotal', 0)
        
        top_customers = sorted(customer_spending.items(), key=lambda x: x[1], reverse=True)[:10]
        
        if top_customers:
            df_customers = pd.DataFrame(top_customers, columns=['CustomerID', 'Total Spent'])
            fig = px.bar(df_customers, x='CustomerID', y='Total Spent',
                        title='Top Customers by Spending',
                        color='Total Spent',
                        color_continuous_scale='Greens')
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    st.subheader("📋 Recent Invoices")
    
    recent_invoices = sorted(invoices, 
                    key=lambda x: str(x.get('_id', '')), 
                    reverse=True)[:10]
    
    df_recent = pd.DataFrame([
        {
            'Invoice No': inv.get('InvoiceNo'),
            'Customer ID': inv.get('CustomerID'),
            'Country': inv.get('Country'),
            'Items': len(inv.get('Items', [])),
            'Total': f"${inv.get('InvoiceTotal', 0):.2f}",
            'Date': inv.get('InvoiceDate')
        }
        for inv in recent_invoices
    ])
    
    st.dataframe(df_recent, use_container_width=True)

def show_invoice_details(invoices):
    st.header("🔍 Invoice Details")
    
    invoice_numbers = [inv.get('InvoiceNo') for inv in invoices]
    selected_invoice = st.selectbox("Select an Invoice:", invoice_numbers)
    
    if selected_invoice:
        invoice = get_invoice_by_number(selected_invoice)
        
        if invoice:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Invoice Number", invoice.get('InvoiceNo'))
                st.metric("Customer ID", invoice.get('CustomerID'))
            
            with col2:
                st.metric("Country", invoice.get('Country'))
                st.metric("Invoice Date", invoice.get('InvoiceDate'))
            
            with col3:
                st.metric("Total Items", len(invoice.get('Items', [])))
                st.metric("Total Amount", f"${invoice.get('InvoiceTotal', 0):.2f}")
            
            st.markdown("---")
            
            st.subheader("📦 Items")
            items = invoice.get('Items', [])
            
            if items:
                df_items = pd.DataFrame(items)
                df_items['TotalPrice'] = df_items['TotalPrice'].apply(lambda x: f"${x:.2f}")
                df_items['UnitPrice'] = df_items['UnitPrice'].apply(lambda x: f"${x:.2f}")
                
                st.dataframe(df_items, use_container_width=True)
            else:
                st.info("No items found for this invoice")

def show_analytics(invoices):
    st.header("📊 Advanced Analytics")
    
    df = pd.DataFrame([
        {
            'InvoiceNo': inv.get('InvoiceNo'),
            'CustomerID': inv.get('CustomerID'),
            'Country': inv.get('Country'),
            'InvoiceTotal': inv.get('InvoiceTotal', 0),
            'ItemCount': len(inv.get('Items', [])),
            'InvoiceDate': inv.get('InvoiceDate')
        }
        for inv in invoices
    ])
    
    st.subheader("💰 Invoice Amount Distribution")
    fig = px.histogram(df, x='InvoiceTotal', nbins=50,
                      title='Distribution of Invoice Amounts',
                      labels={'InvoiceTotal': 'Invoice Total ($)'})
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("🌍 Revenue by Country")
    country_stats = df.groupby('Country').agg({
        'InvoiceTotal': 'sum',
        'InvoiceNo': 'count'
    }).reset_index()
    country_stats.columns = ['Country', 'Total Revenue', 'Number of Invoices']
    country_stats = country_stats.sort_values('Total Revenue', ascending=False)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.pie(country_stats.head(10), values='Total Revenue', names='Country',
                    title='Top 10 Countries by Revenue')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.pie(country_stats.head(10), values='Number of Invoices', names='Country',
                    title='Top 10 Countries by Invoice Count')
        st.plotly_chart(fig, use_container_width=True)

def show_raw_data(invoices):
    st.header("📄 Raw Data")
    
    df = pd.DataFrame([
        {
            'InvoiceNo': inv.get('InvoiceNo'),
            'CustomerID': inv.get('CustomerID'),
            'Country': inv.get('Country'),
            'InvoiceTotal': inv.get('InvoiceTotal', 0),
            'ItemCount': len(inv.get('Items', [])),
            'InvoiceDate': inv.get('InvoiceDate'),
            'ReceivedAt': inv.get('received_at')
        }
        for inv in invoices
    ])
    
    st.sidebar.subheader("Filters")
    
    countries = ['All'] + sorted(df['Country'].unique().tolist())
    selected_country = st.sidebar.selectbox("Filter by Country:", countries)
    
    if selected_country != 'All':
        df = df[df['Country'] == selected_country]
    
    min_total = st.sidebar.number_input("Min Invoice Total:", value=0.0)
    max_total = st.sidebar.number_input("Max Invoice Total:", value=float(df['InvoiceTotal'].max()))
    
    df = df[(df['InvoiceTotal'] >= min_total) & (df['InvoiceTotal'] <= max_total)]
    
    st.write(f"Showing {len(df)} invoices")
    st.dataframe(df, use_container_width=True)
    
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download as CSV",
        data=csv,
        file_name="invoices_data.csv",
        mime="text/csv"
    )

if __name__ == "__main__":
    main()