from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from datetime import datetime
import logging
import os
from dotenv import load_dotenv
from kafka_producer import get_kafka_producer

# Charger les variables d'environnement
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Invoice Streaming API", version="2.0.0")

# Pydantic models for data validation
class InvoiceItem(BaseModel):
    StockCode: str
    Description: str
    Quantity: int
    UnitPrice: float
    TotalPrice: float

class Invoice(BaseModel):
    InvoiceNo: str
    CustomerID: int
    InvoiceDate: str
    Country: str
    Items: List[InvoiceItem]
    InvoiceTotal: float

# Kafka configuration from environment
KAFKA_TOPIC = "invoices-input"

# Statistics storage (for tracking)
stats = {
    "total_invoices_sent": 0,
    "total_revenue": 0.0,
    "kafka_errors": 0
}

@app.on_event("startup")
async def startup_event():
    """Initialize Kafka producer on startup"""
    try:
        get_kafka_producer()
        logger.info("Kafka Producer initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka Producer: {str(e)}")

@app.get("/")
def read_root():
    """Health check endpoint"""
    return {
        "message": "Invoice Streaming API with Kafka",
        "version": "2.0.0",
        "status": "healthy",
        "kafka_topic": KAFKA_TOPIC,
        "stats": stats
    }

@app.post("/invoices")
async def receive_invoice(invoice: Invoice):
    """
    Receive an invoice and send it to Kafka
    """
    try:
        logger.info(f"Received invoice: {invoice.InvoiceNo} for customer {invoice.CustomerID}")
        
        # Get Kafka producer
        producer = get_kafka_producer()
        
        # Prepare invoice data
        invoice_dict = invoice.model_dump()
        invoice_dict['received_at'] = datetime.now().isoformat()
        
        # Send to Kafka (using InvoiceNo as key)
        success = producer.send_message(
            topic=KAFKA_TOPIC,
            key=invoice.InvoiceNo,
            value=invoice_dict
        )
        
        if success:
            # Update statistics
            stats["total_invoices_sent"] += 1
            stats["total_revenue"] += invoice.InvoiceTotal
            
            return {
                "status": "success",
                "message": f"Invoice {invoice.InvoiceNo} sent to Kafka successfully",
                "invoice_no": invoice.InvoiceNo,
                "customer_id": invoice.CustomerID,
                "total": invoice.InvoiceTotal,
                "items_count": len(invoice.Items),
                "kafka_topic": KAFKA_TOPIC
            }
        else:
            stats["kafka_errors"] += 1
            raise HTTPException(status_code=500, detail="Failed to send invoice to Kafka")
    
    except Exception as e:
        logger.error(f"Error processing invoice: {str(e)}")
        stats["kafka_errors"] += 1
        raise HTTPException(status_code=500, detail=f"Error processing invoice: {str(e)}")

@app.get("/stats")
def get_stats():
    """Get statistics about sent invoices"""
    return {
        "total_invoices_sent": stats["total_invoices_sent"],
        "total_revenue": round(stats["total_revenue"], 2),
        "kafka_errors": stats["kafka_errors"],
        "kafka_topic": KAFKA_TOPIC
    }

@app.post("/reset-stats")
def reset_stats():
    """Reset statistics (for testing)"""
    stats["total_invoices_sent"] = 0
    stats["total_revenue"] = 0.0
    stats["kafka_errors"] = 0
    return {"status": "success", "message": "Statistics reset"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("API_PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)