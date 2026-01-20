import pandas as pd
import json
import os
from datetime import datetime

def csv_to_json_invoices(csv_path, output_dir):
    """
    Convert CSV e-commerce data into individual JSON files per invoice.
    
    Args:
        csv_path: Path to the input CSV file
        output_dir: Directory where JSON files will be saved
    """
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Read the CSV file
    print(f"Reading CSV from: {csv_path}")
    df = pd.read_csv(csv_path, encoding='ISO-8859-1')
    
    # Display basic info
    print(f"Total rows: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    print("\nFirst few rows:")
    print(df.head())
    
    # Clean the data
    # Remove rows with missing CustomerID or InvoiceNo
    df = df.dropna(subset=['CustomerID', 'InvoiceNo'])
    
    # Convert CustomerID to integer
    df['CustomerID'] = df['CustomerID'].astype(int)
    
    # Group by InvoiceNo to create one JSON per invoice
    grouped = df.groupby('InvoiceNo')
    
    print(f"\nProcessing {len(grouped)} unique invoices...")
    
    invoice_count = 0
    
    for invoice_no, invoice_data in grouped:
        # Create invoice structure
        invoice_json = {
            "InvoiceNo": str(invoice_no),
            "CustomerID": int(invoice_data['CustomerID'].iloc[0]),
            "InvoiceDate": str(invoice_data['InvoiceDate'].iloc[0]),
            "Country": str(invoice_data['Country'].iloc[0]),
            "Items": []
        }
        
        # Add items to the invoice
        for _, row in invoice_data.iterrows():
            item = {
                "StockCode": str(row['StockCode']),
                "Description": str(row['Description']),
                "Quantity": int(row['Quantity']),
                "UnitPrice": float(row['UnitPrice']),
                "TotalPrice": float(row['Quantity'] * row['UnitPrice'])
            }
            invoice_json["Items"].append(item)
        
        # Calculate invoice total
        invoice_json["InvoiceTotal"] = sum(item["TotalPrice"] for item in invoice_json["Items"])
        
        # Save as JSON file
        filename = f"invoice_{invoice_no}.json"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(invoice_json, f, indent=2, ensure_ascii=False)
        
        invoice_count += 1
        
        # Print progress every 100 invoices
        if invoice_count % 100 == 0:
            print(f"Processed {invoice_count} invoices...")
    
    print(f"\n✅ Successfully created {invoice_count} JSON files in {output_dir}")
    
    # Show sample JSON
    if invoice_count > 0:
        sample_file = os.path.join(output_dir, os.listdir(output_dir)[0])
        print(f"\nSample JSON file content ({sample_file}):")
        with open(sample_file, 'r') as f:
            print(json.dumps(json.load(f), indent=2))

if __name__ == "__main__":
    # Configure paths
    CSV_PATH = "data/raw/data.csv"  # Update this to your CSV path
    OUTPUT_DIR = "data/json/invoices"
    
    # Run conversion
    csv_to_json_invoices(CSV_PATH, OUTPUT_DIR)