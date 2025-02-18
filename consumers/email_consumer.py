import json
from kafka import KafkaConsumer
from utils.utils_config import get_kafka_broker_address
from utils.utils_logger import logger

def create_kafka_consumer():
    """Create a Kafka consumer that reads email messages."""
    kafka_broker = get_kafka_broker_address()
    
    consumer = KafkaConsumer(
        "email_stream",
        bootstrap_servers=kafka_broker,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    return consumer

def process_email_messages():
    """Consume and process email messages."""
    consumer = create_kafka_consumer()
    logger.info("Starting Email Consumer...")

    try:
        for message in consumer:
            email = message.value
            logger.info(f"Received Email: {email}")
            # Process email (e.g., store in DB, trigger notifications)
    except KeyboardInterrupt:
        logger.info("Stopping Email Consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    process_email_messages()
