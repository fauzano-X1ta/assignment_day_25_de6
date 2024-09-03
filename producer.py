import json
import uuid
import os
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from faker import Faker
from time import sleep

# Load environment variables from .env file
dotenv_path = Path(".env")
load_dotenv(dotenv_path=dotenv_path)

# Get Kafka configurations from environment variables
kafka_host = os.getenv("KAFKA_HOST")
kafka_port = "29092"  
kafka_topic = os.getenv("KAFKA_TOPIC_NAME", "purchase_topic")  
# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=f"{kafka_host}:{kafka_port}",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Initialize Faker for generating fake data
faker = Faker()

class PurchaseEventProducer:
    @staticmethod
    def generate_purchase_event():
        products = ['Product A', 'Product B', 'Product C']
        return {
            'transaction_id': str(uuid.uuid4()),
            'timestamp': faker.date_time_this_year().isoformat(),
            'product': faker.random_element(elements=products),
            'amount': faker.random_int(min=1, max=100),
            'customer_id': faker.random_number(digits=6)
        }

# Send purchase events to Kafka topic
for _ in range(1, 400):
    event_data = PurchaseEventProducer.generate_purchase_event()
    producer.send(topic=kafka_topic, value=event_data)
    print(f"Sent message: {event_data['transaction_id']}")
    sleep(5)  # Sleep for 5 seconds between messages

# Flush and close the producer
producer.flush()
producer.close()
