import json
import os
import time
from datetime import datetime

import pandas as pd
from confluent_kafka import Consumer


BOOTSTRAP_SERVERS = "localhost:9092,localhost:9095,localhost:9096"
TOPIC = "topic_fraud"

OUTPUT_DIR = "data_lake"
BATCH_SIZE = 5


os.makedirs(OUTPUT_DIR, exist_ok=True)

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "fraud-parquet-sink",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
})

consumer.subscribe([TOPIC])

buffer = []

print("✅ Consuming fraud alerts from topic_fraud")
print(f"📁 Writing Parquet files to: {OUTPUT_DIR}")
print("🛑 Press CTRL+C to stop")


def write_parquet(records):
    if not records:
        return

    df = pd.DataFrame(records)

    filename = f"fraud_alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    path = os.path.join(OUTPUT_DIR, filename)

    df.to_parquet(path, engine="pyarrow", index=False)

    print(f"💾 Wrote {len(records)} records to {path}")


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        record = json.loads(msg.value().decode("utf-8"))
        record["kafka_partition"] = msg.partition()
        record["kafka_offset"] = msg.offset()
        record["ingested_at"] = datetime.now().isoformat()

        buffer.append(record)

        print(f"📥 Buffered fraud alert: {record}")

        if len(buffer) >= BATCH_SIZE:
            write_parquet(buffer)
            buffer.clear()

except KeyboardInterrupt:
    print("\n🛑 Stopping sink...")

finally:
    write_parquet(buffer)
    consumer.close()
    print("✅ Sink closed")