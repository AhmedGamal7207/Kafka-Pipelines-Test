import sys
import json
from confluent_kafka import Consumer


BOOTSTRAP_SERVERS = "localhost:9092,localhost:9095,localhost:9096"
TOPIC = "topic_raw"


if len(sys.argv) != 2 or sys.argv[1] not in ["earliest", "latest"]:
    print("Usage:")
    print("  python test_offsets.py earliest")
    print("  python test_offsets.py latest")
    sys.exit(1)


offset_strategy = sys.argv[1]

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,

    # Change group.id each run so Kafka applies auto.offset.reset again
    "group.id": f"offset-test-{offset_strategy}",

    "auto.offset.reset": offset_strategy,
    "enable.auto.commit": False
})

consumer.subscribe([TOPIC])

print(f"✅ Consumer started with auto.offset.reset={offset_strategy}")
print("🛑 Press CTRL+C to stop")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"❌ Error: {msg.error()}")
            continue

        record = json.loads(msg.value().decode("utf-8"))

        print(
            f"📥 topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"value={record}"
        )

except KeyboardInterrupt:
    print("\n🛑 Stopping...")

finally:
    consumer.close()
    print("✅ Consumer closed")