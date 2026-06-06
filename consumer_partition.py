import json
from confluent_kafka import Consumer, Producer, TopicPartition


BOOTSTRAP_SERVERS = "localhost:9092,localhost:9095,localhost:9096"

SOURCE_TOPIC = "topic_raw"
FRAUD_TOPIC = "topic_fraud"


consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "cairo-fraud-scoring-engine",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
})

producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all"
})


def delivery_report(err, msg):
    if err:
        print(f"❌ Failed to forward fraud record: {err}")
    else:
        print(
            f"🚨 Fraud forwarded to {msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


# Manually assign ONLY partition 0
consumer.assign([
    TopicPartition(SOURCE_TOPIC, 0)
])

print("✅ Consumer pinned to topic_raw partition 0 only")
print("📍 Consuming Cairo transactions only...")
print("🛑 Press CTRL+C to stop")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        record = json.loads(msg.value().decode("utf-8"))

        print(
            f"📥 Received from partition={msg.partition()} "
            f"offset={msg.offset()} | {record}"
        )

        if record["amount"] > 100000:
            producer.produce(
                topic=FRAUD_TOPIC,
                value=json.dumps(record).encode("utf-8"),
                callback=delivery_report
            )
            producer.poll(0)

except KeyboardInterrupt:
    print("\n🛑 Stopping consumer...")

finally:
    producer.flush()
    consumer.close()
    print("✅ Consumer closed")