import json
import time
import random
from kafka import KafkaProducer
from faker import Faker
from utils.utils_config import get_kafka_broker_address
from utils.utils_logger import logger

# Initialize Faker for generating fake email data
fake = Faker()

def create_kafka_producer():
    """Create a Kafka producer with JSON serializer."""
    kafka_broker = get_kafka_broker_address()
    
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    return producer

def generate_fake_email():
    """Generate a fake email message."""
    return {
        "sender": fake.email(),
        "recipient": fake.email(),
        "subject": fake.sentence(),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

def send_email_messages():
    """Send fake email messages to Kafka."""
    producer = create_kafka_producer()
    topic = "email_stream"

    logger.info("Starting Email Producer...")
    try:
        while True:
            email_data = generate_fake_email()
            producer.send(topic, email_data)
            logger.info(f"Sent: {email_data}")
            time.sleep(random.randint(1, 3))  # Simulate email arrival
    except KeyboardInterrupt:
        logger.info("Stopping Email Producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    send_email_messages()
