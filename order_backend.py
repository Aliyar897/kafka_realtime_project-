from confluent_kafka import Producer
import json
import time

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 20000

producer = Producer({'bootstrap.servers': 'localhost:29092'})

print("Going to be generating order after 5 seconds")
print("Will generate one unique order every 5 seconds")
# time.sleep(10)

for i in range(1, ORDER_LIMIT):
    data = {
        "order_id": i,
        "user_id": f"tom_{i}",
        "total_cost": i * 5,
        "items": "burger,sandwich",
    }

    producer.produce(ORDER_KAFKA_TOPIC, json.dumps(data).encode('utf-8'))
    producer.flush()
    print(f"Done Sending..{i}")
    # time.sleep(5)

