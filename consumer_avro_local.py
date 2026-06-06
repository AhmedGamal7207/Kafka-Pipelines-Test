import io
from confluent_kafka import Consumer
from fastavro import parse_schema, schemaless_reader


BOOTSTRAP_SERVERS = "localhost:9092,localhost:9095,localhost:9096"
TOPIC = "sales_topic"


sales_schema = {
    "type": "record",
    "name": "Sale",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "item_name", "type": "string"},
        {"name": "price", "type": "float"}
    ]
}

parsed_schema = parse_schema(sales_schema)


consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "local-avro-consumer",
    "auto.offset.reset": "earliest"
})

consumer.subscribe([TOPIC])

print("✅ Local Avro consumer started")
print("🛑 Press CTRL+C to stop")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"❌ Consumer error: {msg.error()}")
            continue

        raw_bytes = msg.value()

        buffer = io.BytesIO(raw_bytes)
        record = schemaless_reader(buffer, parsed_schema)

        print(
            f"📥 Decoded from partition={msg.partition()} "
            f"offset={msg.offset()} | {record}"
        )

except KeyboardInterrupt:
    print("\n🛑 Stopping consumer...")

finally:
    consumer.close()
    print("✅ Consumer closed")