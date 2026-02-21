import os, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp, lit, expr, udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from jsonschema import Draft202012Validator, exceptions as js_ex

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "domain-events")
BRONZE_DIR = os.getenv("BRONZE_DIR", "./bronze/domain_events")
DLQ_DIR = os.getenv("DLQ_DIR", "./dlq/bad_records")
CHK_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "./chk/domain_events_contract")
SCHEMA_PATH = os.getenv("EVENT_SCHEMA_PATH", "./config/event.schema.json")

spark = (
    SparkSession.builder.appName("KafkaToBronze+Contract")
    .config("spark.sql.shuffle.partitions","4")
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1) Loose schema to parse JSON structure
loose = StructType([
    StructField("id", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("tld", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("source", StringType(), True),
    StructField("producer_ts", StringType(), True),
])

# 2) Load strict JSON Schema and broadcast
with open(SCHEMA_PATH) as f:
    _schema = json.load(f)
_validator_bc = spark.sparkContext.broadcast(_schema)

def validate_contract(row_json: str):
    """Return None if OK, else reason string."""
    try:
        obj = json.loads(row_json)
    except Exception as e:
        return f"json_decode_error:{str(e)[:80]}"
    try:
        Draft202012Validator(_validator_bc.value).validate(obj)
    except js_ex.ValidationError as ve:
        # point to the first broken path for clarity
        path = ".".join([str(p) for p in ve.path]) or "<root>"
        return f"contract_violation:{path}:{ve.message[:120]}"
    return None

validate_udf = udf(validate_contract, StringType())

# Read from Kafka
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .option("failOnDataLoss", "false")
       .load())

df = raw.selectExpr("CAST(value AS STRING) as value", "timestamp as kafka_ts")

parsed = df.withColumn("reason", validate_udf(col("value"))) \
           .withColumn("json", from_json(col("value"), loose))

# GOOD = passes contract AND parsed
good = (parsed.where(col("reason").isNull() & col("json").isNotNull())
        .select(
            col("json.id").alias("id"),
            col("json.domain").alias("domain"),
            col("json.tld").alias("tld"),
            to_timestamp(col("json.event_ts")).alias("event_ts"),
            to_timestamp(col("json.producer_ts")).alias("producer_ts"),
            col("json.source").alias("source"),
            col("kafka_ts"),
            current_timestamp().alias("ingest_ts")
        )
        .withColumn("latency_sec", expr("ROUND(unix_timestamp(ingest_ts)-unix_timestamp(event_ts),3)")))

# BAD = json parse failed or contract failed
bad = (parsed.where(col("reason").isNotNull() | col("json").isNull())
       .select("value","reason","kafka_ts")
       .withColumn("ingest_ts", current_timestamp()))

# Console preview
(good.writeStream
 .format("console").outputMode("append").option("truncate","false")
 .start())

# Bronze sink (same as before)
(good.writeStream
 .format("parquet")
 .option("path", BRONZE_DIR)
 .option("checkpointLocation", CHK_DIR + "_good")
 .outputMode("append")
 .start())

# DLQ with reason
(bad.writeStream
 .format("parquet")
 .option("path", DLQ_DIR)
 .option("checkpointLocation", CHK_DIR + "_dlq")
 .outputMode("append")
 .start())

spark.streams.awaitAnyTermination()
