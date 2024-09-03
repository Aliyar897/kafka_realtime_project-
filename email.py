import json
from confluent_kafka import Consumer

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'email-consumer-group',  # Assign a consumer group ID
    'auto.offset.reset': 'earliest'  # Start reading from the earliest offset
}

# Initialize the consumer
consumer = Consumer(consumer_conf)

# Subscribe to the topic
consumer.subscribe([ORDER_CONFIRMED_KAFKA_TOPIC])

emails_sent_so_far = set()
print("Gonna start listening")
while True:
    msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second

    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue

    # Process the message
    consumed_message = json.loads(msg.value().decode('utf-8'))
    customer_email = consumed_message["customer_email"]
    print(f"Sending email to {customer_email}")
    emails_sent_so_far.add(customer_email)
    print(f"So far emails sent to {len(emails_sent_so_far)} unique emails")

# Close the consumer
consumer.close()
