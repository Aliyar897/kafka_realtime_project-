import json
from confluent_kafka import Consumer, Producer, KafkaError

ORDER_KAFKA_TOPIC = "order_details"
ORDER_CONFIRMED_KAFKA_TOPIC = "order_confirmed"

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:29092'
}

# Initialize consumer and producer
consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)

# Subscribe to the topic
consumer.subscribe([ORDER_KAFKA_TOPIC])

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
    print("Ongoing transaction..")
    consumed_message = json.loads(msg.value().decode('utf-8'))
    print(consumed_message)
    user_id = consumed_message["user_id"]
    total_cost = consumed_message["total_cost"]
    
    data = {
        "customer_id": user_id,
        "customer_email": f"{user_id}@gmail.com",
        "total_cost": total_cost
    }
    
    print("Successful transaction..")
    producer.produce(ORDER_CONFIRMED_KAFKA_TOPIC, json.dumps(data).encode('utf-8'))
    producer.flush()  # Ensure the message is sent

# Close the consumer and producer
consumer.close()
