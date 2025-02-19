from kafka import KafkaProducer
from faker import Faker
import json
import time

# Initialize Faker
fake = Faker()

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Function to generate a fake email event
def generate_email():
    return {
        "timestamp": time.time(),
        "sender": fake.email(),
        "recipient": fake.email(),
        "subject": fake.sentence(),
        "body": fake.paragraph(),
    }

# Sending email events to Kafka
if __name__ == "__main__":
    while True:
        email_data = generate_email()
        producer.send("email_stream", email_data)
        print(f"Produced: {email_data}")
        time.sleep(2)  # Simulate real-time streaming
