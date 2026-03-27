from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, countDistinct, when, avg, broadcast
)
from pyspark.storagelevel import StorageLevel

JDBC_URL = "jdbc:postgresql://postgres:5432/airflow"
DB_USER = "airflow"
DB_PASSWORD = "airflow"
JAR_PATH = "/opt/airflow/postgresql-42.7.3.jar"


def get_spark():
    return (
        SparkSession.builder
        .appName("build_marts_optimized")
        .config("spark.jars", JAR_PATH)
        .config("spark.driver.extraClassPath", JAR_PATH)
        .config("spark.executor.extraClassPath", JAR_PATH)
        .config("spark.driver.memory", "3g")
        .config("spark.executor.memory", "3g")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "200m")
        .getOrCreate()
    )


def read_table(spark, table):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", 5000)
        .load()
    )


def write_table(df, table):
    (
        df.coalesce(4)
        .write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 5000)
        .mode("overwrite")
        .save()
    )


def main():
    spark = get_spark()

    orders = read_table(spark, "orders")
    items = read_table(spark, "order_items")
    status = read_table(spark, "order_status")
    deliveries = read_table(spark, "order_deliveries")

    items.persist(StorageLevel.MEMORY_AND_DISK)

    base = (
        orders
        .join(broadcast(status), "order_id", "left")
        .join(broadcast(deliveries), "order_id", "left")
    )

    items_agg_per_order = (
        items
        .groupBy("order_id")
        .agg(
            _sum(col("price") * col("quantity") - col("discount")).alias("item_turnover"),
            _sum(col("price") * col("quantity")).alias("gross_amount"),
            _sum(
                (col("quantity") - col("canceled_quantity")) * col("price") - col("discount")
            ).alias("order_revenue"),
            _sum("quantity").alias("qty"),
            _sum("canceled_quantity").alias("cancel_qty")
        )
    )

    orders_joined = (
        base
        .join(items_agg_per_order, "order_id", "left")
    )

    orders_mart = (
        orders_joined
        .groupBy(col("created_at").cast("date").alias("dt"))
        .agg(
            _sum("item_turnover").alias("turnover"),
            _sum("order_revenue").alias("revenue"),

            countDistinct("order_id").alias("created_orders"),
            _sum(when(col("delivered_at").isNotNull(), 1).otherwise(0)).alias("delivered_orders"),
            _sum(when(col("canceled_at").isNotNull(), 1).otherwise(0)).alias("canceled_orders"),
            _sum(
                when(
                    col("delivered_at").isNotNull() & col("canceled_at").isNotNull(), 1
                ).otherwise(0)
            ).alias("cancel_after_delivery"),
            _sum(
                when(
                    col("cancellation_reason").isin("Ошибка приложения", "Проблемы с оплатой"), 1
                ).otherwise(0)
            ).alias("service_errors"),
            countDistinct("user_id").alias("users"),
            avg("gross_amount").alias("avg_check"),
            countDistinct("driver_id").alias("active_drivers"),
        )
        .withColumn("profit", col("revenue") - col("turnover") * 0.1)
    )

    orders_dates = orders.select("order_id", col("created_at").cast("date").alias("dt"))
    items_mart = (
        items
        .join(broadcast(orders_dates), "order_id", "left")
        .groupBy("dt", "item_id")
        .agg(
            _sum(col("price") * col("quantity") - col("discount")).alias("turnover"),
            _sum("quantity").alias("ordered_qty"),
            _sum("canceled_quantity").alias("canceled_qty"),
            countDistinct("order_id").alias("orders_count"),
            countDistinct(
                when(col("canceled_quantity") > 0, col("order_id"))
            ).alias("orders_with_cancel")
        )
    )

    write_table(orders_mart, "orders_mart")
    write_table(items_mart, "items_mart")

    items.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
