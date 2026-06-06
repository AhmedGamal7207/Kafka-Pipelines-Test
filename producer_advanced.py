import json
import random
import time
from confluent_kafka import Producer


BOOTSTRAP_SERVERS = "localhost:9092,localhost:9095,localhost:9096"
TOPIC = "topic_raw"


producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all"
})


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"✅ Sent to {msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def get_partition(location):
    if location == "Cairo":
        return 0
    elif location == "Alexandria":
        return 1
    else:
        raise ValueError(f"Unsupported location: {location}")


locations = ["Cairo", "Alexandria"]

for i in range(20):
    location = random.choice(locations)

    transaction = {
        "transaction_id": f"tx_{i + 1}",
        "user_id": random.randint(1, 100),
        "amount": round(random.uniform(50, 150000), 2),
        "location": location
    }

    partition = get_partition(location)

    producer.produce(
        topic=TOPIC,
        value=json.dumps(transaction).encode("utf-8"),
        partition=partition,
        callback=delivery_report
    )

    producer.poll(0)
    time.sleep(1)

producer.flush()
print("✅ Producer finished.")