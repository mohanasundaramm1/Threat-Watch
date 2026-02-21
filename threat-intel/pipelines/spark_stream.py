import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp, lit, expr, percentile_approx
from pyspark.sql.types import StructType, StructField, StringType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "domain-events")
BRONZE_DIR = os.getenv("BRONZE_DIR", "./bronze/domain_events")
DLQ_DIR = os.getenv("DLQ_DIR", "./dlq/bad_json")
CHK_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "./chk/domain_events")
TRIGGER = os.getenv("SPARK_TRIGGER_SECS", "5")  # micro-batch cadence (seconds)

spark = (
    SparkSession.builder.appName("KafkaToBronze+Latency")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Expected JSON schema (from the producer)
schema = StructType([
    StructField("id", StringType(), False),
    StructField("domain", StringType(), False),
    StructField("tld", StringType(), True),
    StructField("event_ts", StringType(), False),
    StructField("source", StringType(), False),
    StructField("producer_ts", StringType(), False),
])

# Read from Kafka
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

df = raw.selectExpr("CAST(value AS STRING) as value", "timestamp as kafka_ts")
parsed = df.withColumn("json", from_json(col("value"), schema))

# Good rows with latency
good = (
    parsed.where(col("json").isNotNull())
    .withColumn("id", col("json.id"))
    .withColumn("domain", col("json.domain"))
    .withColumn("tld", col("json.tld"))
    .withColumn("event_ts", to_timestamp(col("json.event_ts")))
    .withColumn("producer_ts", to_timestamp(col("json.producer_ts")))
    .withColumn("source", col("json.source"))
    .withColumn("ingest_ts", current_timestamp())
    .withColumn("latency_sec", expr("ROUND(unix_timestamp(ingest_ts) - unix_timestamp(event_ts), 3)"))
    .drop("json")
)

# Bad rows → DLQ
bad = (
    parsed.where(col("json").isNull())
    .withColumn("reason", lit("json_parse_failed"))
    .withColumn("ingest_ts", current_timestamp())
    .select("value", "ingest_ts", "reason")
)

# Console sink
good_writer = (
    good.writeStream.format("console")
    .outputMode("append").option("truncate", "false")
    .trigger(processingTime=f"{TRIGGER} seconds")
    .start()
)

# Bronze sink
bronze_writer = (
    good.writeStream.format("parquet")
    .option("path", BRONZE_DIR)
    .option("checkpointLocation", CHK_DIR)
    .outputMode("append")
    .trigger(processingTime=f"{TRIGGER} seconds")
    .start()
)

# DLQ sink
dlq_writer = (
    bad.writeStream.format("parquet")
    .option("path", DLQ_DIR)
    .option("checkpointLocation", CHK_DIR + "_dlq")
    .outputMode("append")
    .trigger(processingTime=f"{TRIGGER} seconds")
    .start()
)

# Per-batch latency metrics
def log_latency(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"[metrics] batch_id={batch_id} empty")
        return
    q = batch_df.selectExpr("CAST(latency_sec AS DOUBLE) AS latency_sec") \
                .agg(percentile_approx("latency_sec", 0.5).alias("p50"),
                     percentile_approx("latency_sec", 0.95).alias("p95"))
    row = q.collect()[0]
    cnt = batch_df.count()
    print(f"[metrics] batch_id={batch_id} rows={cnt} p50={row['p50']}s p95={row['p95']}s")

metrics_writer = (
    good.writeStream.foreachBatch(log_latency)
    .option("checkpointLocation", CHK_DIR + "_metrics")
    .trigger(processingTime=f"{TRIGGER} seconds")
    .start()
)

spark.streams.awaitAnyTermination()
