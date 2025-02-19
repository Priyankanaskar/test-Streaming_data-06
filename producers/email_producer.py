from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def send_chat_message(user, message):
    data = {"user": user, "message": message}
    producer.send("chat_stream", data)
    print(f"Sent: {data}")

if __name__ == "__main__":
    while True:
        user = input("Enter username: ")
        message = input("Enter message: ")
        send_chat_message(user, message)
