import os
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka configurations
kafka_host = os.getenv("KAFKA_HOST")
kafka_port = "29092"
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=f"{kafka_host}:{kafka_port}",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def process_event(event):
    # Example processing: Add a simple calculation
    amount = event.get('amount', 0)
    processed_value = amount * 1.1  # Just an example of processing
    print(f"Processed event: {event['event_id']} with processed value: {processed_value}")

if __name__ == "__main__":
    print(f"Consuming events from topic: {kafka_topic}")
    for message in consumer:
        event = message.value
        print(f"Received event: {event}")
        process_event(event)
