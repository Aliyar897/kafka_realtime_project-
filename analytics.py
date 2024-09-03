import json
from confluent_kafka import Consumer, KafkaError

ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'analytics-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Initialize the consumer
consumer = Consumer(consumer_conf)

# Subscribe to the topic
consumer.subscribe([ORDER_CONFIRMED_KAFKA_TOPIC])

total_orders_count = 0
total_revenue = 0
print("Gonna start listening")

while True:
    msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(f"Error: {msg.error()}")
            break

    # Process the message
    consumed_message = json.loads(msg.value().decode('utf-8'))
    total_cost = float(consumed_message["total_cost"])
    total_orders_count += 1
    total_revenue += total_cost
    print(f"Orders so far today: {total_orders_count}")
    print(f"Revenue so far today: {total_revenue}")

# Close the consumer
consumer.close()
