# ct/ingest/stream_ct.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
    expr,
    lit,
    to_date,
)
from pyspark.sql.types import StructType, StructField, StringType

# ---------- path setup (anchored in repo) ----------

THIS_DIR  = os.path.dirname(__file__)                      # .../ct/ingest
CT_DIR    = os.path.abspath(os.path.join(THIS_DIR, ".."))  # .../ct
REPO_ROOT = os.path.abspath(os.path.join(CT_DIR, ".."))    # .../threat-intel

DATA_DIR = os.path.join(CT_DIR, "data")

RAW_DIR_DEFAULT  = os.path.join(DATA_DIR, "raw")
DLQ_DIR_DEFAULT  = os.path.join(DATA_DIR, "dlq", "bad_json")
CHK_DIR_DEFAULT  = os.path.join(DATA_DIR, "chk", "stream_ct")

os.makedirs(RAW_DIR_DEFAULT, exist_ok=True)
os.makedirs(os.path.dirname(DLQ_DIR_DEFAULT), exist_ok=True)
os.makedirs(os.path.dirname(CHK_DIR_DEFAULT), exist_ok=True)

# Kafka -> Spark config
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC     = os.getenv("KAFKA_TOPIC", "ct-events")

# Output locations (can be overridden via env)
OUT_DIR = os.getenv("CT_RAW_DIR", RAW_DIR_DEFAULT)
CHK_DIR = os.getenv("CT_CHK_DIR", CHK_DIR_DEFAULT)
DLQ_DIR = os.getenv("CT_DLQ_DIR", DLQ_DIR_DEFAULT)

# Schema (matches forwarder)
schema = StructType([
    StructField("id",          StringType(), False),
    StructField("domain",      StringType(), False),
    StructField("tld",         StringType(), True),
    StructField("event_ts",    StringType(), False),   # ISO8601
    StructField("producer_ts", StringType(), False),   # ISO8601
    StructField("source",      StringType(), False),
])

spark = (
    SparkSession.builder.appName("CT->Raw")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
    .config("spark.sql.streaming.minBatchesToRetain", "10")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------- Kafka source ----------

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    # stability on local ARM/docker
    .option("kafka.group.id", "ct-stream")
    .option("fetchOffset.retryIntervalMs", "1000")
    .option("fetchOffset.numRetries", "60")
    .option("kafkaConsumer.pollTimeoutMs", "120000")
    .option("maxOffsetsPerTrigger", "5000")
    .load()
)

df = raw.selectExpr("CAST(value AS STRING) AS value", "timestamp AS kafka_ts")
parsed = df.withColumn("json", from_json(col("value"), schema))

# ---------- good / bad split ----------

good = (
    parsed.where(col("json").isNotNull())
    .select(
        col("json.id").alias("id"),
        col("json.domain").alias("domain"),
        col("json.tld").alias("tld"),
        # Let Spark parse ISO8601 automatically (handles microseconds, offsets, etc.)
        to_timestamp(col("json.event_ts")).alias("event_ts"),
        to_timestamp(col("json.producer_ts")).alias("producer_ts"),
        col("json.source").alias("source"),
        current_timestamp().alias("ingest_ts"),
    )
    .withColumn(
        "latency_sec",
        expr("ROUND(unix_timestamp(ingest_ts) - unix_timestamp(event_ts), 3)"),
    )
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["id"])
    .withColumn("ds", to_date(col("event_ts")))  # partition by day
)

bad = (
    parsed.where(col("json").isNull())
    .select(
        col("value"),
        lit("json_parse_failed").alias("reason"),
        current_timestamp().alias("ingest_ts"),
    )
)

# ---------- sinks ----------

# Console (for quick visibility; feel free to comment out later)
(
    good.writeStream.format("console")
    .outputMode("append")
    .option("truncate", "false")
    .trigger(processingTime="5 seconds")
    .start()
)

# Raw sink (ct/data/raw)
(
    good.writeStream.format("parquet")
    .option("path", OUT_DIR)
    .option("checkpointLocation", CHK_DIR)
    .option("compression", "snappy")
    .partitionBy("ds")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

# DLQ (ct/data/dlq/bad_json)
(
    bad.writeStream.format("parquet")
    .option("path", DLQ_DIR)
    .option("checkpointLocation", CHK_DIR + "_dlq")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

spark.streams.awaitAnyTermination()
