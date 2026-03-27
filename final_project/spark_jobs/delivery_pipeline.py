import sys
import psycopg2
from datetime import timedelta
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

PIPELINE_NAME = "delivery_pipeline"
OVERLAP_HOURS = 24

RAW_PATH = sys.argv[1]

JDBC_URL = "jdbc:postgresql://postgres:5432/airflow"
DB_HOST = "postgres"
DB_PORT = 5432
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASSWORD = "airflow"

JAR_PATH = "/opt/airflow/postgresql-42.7.3.jar"


def get_spark():
    return (
        SparkSession.builder
        .appName("delivery_pipeline_staging")
        .config("spark.jars", JAR_PATH)
        .config("spark.driver.extraClassPath", JAR_PATH)
        .config("spark.executor.extraClassPath", JAR_PATH)
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def write_jdbc(df, table):
    (
        df.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 1000)
        .mode("append")
        .save()
    )


def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def upsert_from_staging(
    staging_table,
    target_table,
    columns,
    conflict_keys,
    dedup_key="created_at",
    batch_size=10000
):
    conn = get_conn()
    cur = conn.cursor()
    write_cur = conn.cursor()

    dedup_query = f"""
        SELECT {",".join(columns)}
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY {",".join(conflict_keys)}
                       ORDER BY {dedup_key} DESC
                   ) as rn
            FROM {staging_table}
        ) t
        WHERE rn = 1
    """

    cur.execute(dedup_query)

    update_cols = [c for c in columns if c not in conflict_keys]

    set_clause = ", ".join(
        [f"{c}=EXCLUDED.{c}" for c in update_cols]
    )

    conflict_clause = ", ".join(conflict_keys)

    insert_query = f"""
        INSERT INTO {target_table} ({",".join(columns)})
        VALUES %s
        ON CONFLICT ({conflict_clause})
        DO UPDATE SET {set_clause}
    """

    while True:
        try:
            rows = cur.fetchmany(batch_size)
        except Exception as e:
            print("❌ ERROR during fetchmany:", e)
            conn.rollback()
            raise

        if not rows:
            break

        try:
            execute_values(write_cur, insert_query, rows)
        except Exception as e:
            print("❌ ERROR during insert:")
            print(e)
            print("Sample rows:", rows[:3])

            conn.rollback()
            raise

    conn.commit()
    cur.close()
    write_cur.close()
    conn.close()


def get_last_loaded_ts(spark):
    try:
        df = spark.read.format("jdbc").options(
            url=JDBC_URL,
            dbtable="etl_state",
            user=DB_USER,
            password=DB_PASSWORD,
            driver="org.postgresql.Driver"
        ).load()

        row = df.filter(col("pipeline_name") == PIPELINE_NAME).collect()
        if not row:
            return None
        return row[0]["last_loaded_at"]

    except Exception as e:
        print("STATE ERROR:", e)
        return None


def update_state(new_ts):
    if new_ts is None:
        return

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO etl_state (pipeline_name, last_loaded_at)
        VALUES (%s, %s)
        ON CONFLICT (pipeline_name)
        DO UPDATE SET last_loaded_at = EXCLUDED.last_loaded_at
    """, (PIPELINE_NAME, new_ts))

    conn.commit()
    cur.close()
    conn.close()


def main():
    spark = get_spark()

    df = spark.read.parquet(RAW_PATH)

    last_ts = get_last_loaded_ts(spark)
    print("LAST_TS:", last_ts)

    if last_ts:
        df = df.filter(
            col("created_at") > (last_ts - timedelta(hours=OVERLAP_HOURS))
        )

    if len(df.take(1)) == 0:
        print("No data")
        return

    df = df.repartition(8)

    users = df.select(
        col("user_id"),
        col("user_phone").alias("phone")
    ).dropDuplicates(["user_id"])

    write_jdbc(users, "users_staging")

    stores = df.select(
        col("store_id"),
        col("store_address").alias("address")
    ).dropDuplicates(["store_id"])

    write_jdbc(stores, "stores_staging")

    drivers = df.select(
        col("driver_id"),
        col("driver_phone").alias("phone")
    ).dropDuplicates(["driver_id"])

    write_jdbc(drivers, "drivers_staging")

    items = df.select(
        col("item_id"),
        col("item_title").alias("title"),
        col("item_category").alias("category")
    ).dropDuplicates(["item_id"])

    write_jdbc(items, "items_staging")

    orders = df.select(
        col("order_id"),
        col("user_id"),
        col("store_id"),
        col("created_at"),
        col("paid_at"),
        col("payment_type"),
        col("order_discount"),
        col("delivery_cost"),
        col("address_text").alias("delivery_address")
    ).dropDuplicates(["order_id"])

    write_jdbc(orders, "orders_staging")

    order_status = df.select(
        col("order_id"),
        col("canceled_at"),
        col("order_cancellation_reason").alias("cancellation_reason")
    ).dropDuplicates(["order_id"])

    write_jdbc(order_status, "order_status_staging")

    order_items = df.select(
        col("order_id"),
        col("item_id"),
        col("item_quantity").alias("quantity"),
        col("item_price").alias("price"),
        col("item_discount").alias("discount"),
        col("item_canceled_quantity").alias("canceled_quantity"),
        col("item_replaced_id").alias("replaced_item_id")
    ).dropDuplicates(["order_id", "item_id"])

    write_jdbc(order_items, "order_items_staging")

    deliveries = df.select(
        col("order_id"),
        col("driver_id"),
        col("delivery_started_at"),
        col("delivered_at")
    ).dropDuplicates(["order_id", "driver_id"])

    write_jdbc(deliveries, "order_deliveries_staging")

    upsert_from_staging(
        "users_staging",
        "users",
        ["user_id", "phone"],
        ["user_id"]
    )

    upsert_from_staging(
        "stores_staging",
        "stores",
        ["store_id", "address"],
        ["store_id"]
    )

    upsert_from_staging(
        "drivers_staging",
        "drivers",
        ["driver_id", "phone"],
        ["driver_id"]
    )

    upsert_from_staging(
        "items_staging",
        "items",
        ["item_id", "title", "category"],
        ["item_id"]
    )

    upsert_from_staging(
        "orders_staging",
        "orders",
        [
            "order_id", "user_id", "store_id", "created_at",
            "paid_at", "payment_type", "order_discount",
            "delivery_cost", "delivery_address"
        ],
        ["order_id"]
    )

    upsert_from_staging(
        "order_status_staging",
        "order_status",
        ["order_id", "canceled_at", "cancellation_reason"],
        ["order_id"]
    )

    upsert_from_staging(
        "order_items_staging",
        "order_items",
        [
            "order_id", "item_id", "quantity", "price",
            "discount", "canceled_quantity", "replaced_item_id",
        ],
        ["order_id", "item_id"]
    )

    upsert_from_staging(
        "order_deliveries_staging",
        "order_deliveries",
        ["order_id", "driver_id", "delivery_started_at", "delivered_at"],
        ["order_id", "driver_id"]
    )

    max_ts = df.select(spark_max("created_at")).collect()[0][0]
    print("NEW WATERMARK:", max_ts)

    update_state(max_ts)

    spark.stop()


if __name__ == "__main__":
    main()
