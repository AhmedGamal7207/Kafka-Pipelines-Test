import os
import duckdb


PARQUET_PATH = "data_lake/*.parquet"


if not os.path.exists("data_lake"):
    print("❌ data_lake folder does not exist yet.")
    print("Run consumer_to_parquet.py first.")
    exit()


con = duckdb.connect()

print("✅ Reading Parquet files directly with DuckDB\n")


print("1️⃣ All fraud records:")
print(con.execute(f"""
    SELECT *
    FROM read_parquet('{PARQUET_PATH}')
    ORDER BY ingested_at DESC
""").df())


print("\n2️⃣ Fraud count by location:")
print(con.execute(f"""
    SELECT
        location,
        COUNT(*) AS fraud_count,
        AVG(amount) AS avg_amount,
        MAX(amount) AS max_amount
    FROM read_parquet('{PARQUET_PATH}')
    GROUP BY location
    ORDER BY fraud_count DESC
""").df())


print("\n3️⃣ Top 5 highest fraud amounts:")
print(con.execute(f"""
    SELECT
        transaction_id,
        user_id,
        amount,
        location,
        kafka_partition,
        kafka_offset
    FROM read_parquet('{PARQUET_PATH}')
    ORDER BY amount DESC
    LIMIT 5
""").df())

con.close()