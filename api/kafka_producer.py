from kafka import KafkaProducer
import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class KafkaProducerClient:
    def __init__(self, bootstrap_servers=None):
        """
        Initialize Kafka Producer
        """
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')
            
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda v: v.encode('utf-8') if v else None
            )
            logger.info(f"Kafka Producer connected to {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {str(e)}")
            raise
    
    def send_message(self, topic, key, value):
        """
        Send a message to Kafka topic
        
        Args:
            topic: Kafka topic name
            key: Message key (usually invoice number)
            value: Message value (invoice data as dict)
        """
        try:
            future = self.producer.send(topic, key=key, value=value)
            # Wait for the message to be sent (synchronous)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Message sent to topic '{topic}' - Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            return True
        
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {str(e)}")
            return False
    
    def close(self):
        """Close the Kafka producer"""
        if self.producer:
            self.producer.close()
            logger.info("Kafka Producer closed")

# Global Kafka producer instance
kafka_producer = None

def get_kafka_producer():
    """Get or create Kafka producer instance"""
    global kafka_producer
    if kafka_producer is None:
        kafka_producer = KafkaProducerClient()
    return kafka_producer