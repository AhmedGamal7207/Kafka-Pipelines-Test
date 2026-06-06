import io
from confluent_kafka import Producer
from fastavro import parse_schema, schemaless_writer
from fastavro.validation import validate


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


producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all"
})


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"✅ Delivered Avro bytes to {msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}"
        )


def serialize_avro(record):
    validate(record, parsed_schema)

    buffer = io.BytesIO()
    schemaless_writer(buffer, parsed_schema, record)

    return buffer.getvalue()


# Test Case A: Valid
record = {
    "order_id": 101,
    "item_name": "Laptop",
    "price": 1200.50
}

# Test Case B: Broken schema
# Uncomment this to test validation failure
# record = {
#     "order_id": "ABC_NOT_AN_INT",
#     "item_name": "Laptop",
#     "price": 1200.50
# }

try:
    avro_bytes = serialize_avro(record)

    print(f"✅ Local Avro validation passed: {record}")
    print(f"📦 Serialized binary size: {len(avro_bytes)} bytes")

    producer.produce(
        topic=TOPIC,
        value=avro_bytes,
        callback=delivery_report
    )

    producer.flush()

except Exception as e:
    print("❌ Local Avro validation/serialization failed")
    print(e)