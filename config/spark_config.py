import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

def load_env(path: str = ".env") -> None:

    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key   = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:   # don't override Docker env vars
                os.environ[key] = value

def create_spark_session(app_name: str) -> SparkSession:
    load_env()
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access   = os.getenv("MINIO_ACCESS_KEY")
    minio_secret   = os.getenv("MINIO_SECRET_KEY")
    spark_master   = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

    # Fail fast if critical env vars are missing
    missing = [k for k, v in {
        "MINIO_ENDPOINT":   minio_endpoint,
        "MINIO_ACCESS_KEY": minio_access,
        "MINIO_SECRET_KEY": minio_secret,
    }.items() if not v]

    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Check your .env file."
        )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        # ── Jars (Kafka + S3/MinIO + PostgreSQL) ──────────────────────
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                "org.postgresql:postgresql:42.7.3",
            ])
        )
        # ── MinIO / S3 config ─────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",                f"http://{minio_endpoint}")
        .config("spark.hadoop.fs.s3a.access.key",              minio_access)
        .config("spark.hadoop.fs.s3a.secret.key",              minio_secret)
        .config("spark.hadoop.fs.s3a.path.style.access",       "true")
        .config("spark.hadoop.fs.s3a.impl",                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # ── Streaming tuning ──────────────────────────────────────────
        .config("spark.sql.shuffle.partitions",                "4")
        .config("spark.default.parallelism",                   "4")
        .config("spark.eventLog.enabled",                      "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark