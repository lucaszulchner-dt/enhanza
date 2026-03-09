

import random
import string
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import Row


CATALOG_NAME = "iceberg_bucket_enhanz"          
BUCKET = "gs://iceberg_bucket_enhanz"
PROJECT_ID = "pj-enhan-terraform-main"
NAMESPACE = "concurrency_partition_test77"
TABLE_NAME = "test_concurrent"

spark = (
    SparkSession.builder
    .appName("concurrency_partition_test")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG_NAME}",
            "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.type",
            "rest")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.uri",
            "https://biglake.googleapis.com/iceberg/v1/restcatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse",
            BUCKET)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.header.x-goog-user-project",
            PROJECT_ID)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.type",
            "org.apache.iceberg.gcp.auth.GoogleAuthManager")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl",
            "org.apache.iceberg.gcp.gcs.GCSFileIO")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.rest-metrics-reporting-enabled",
            "false")
    .config("spark.sql.defaultCatalog", CATALOG_NAME)
    .getOrCreate()
)


spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
spark.sql(f"USE {NAMESPACE}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id          STRING,
        org_id      STRING,
        value       DOUBLE,
        source      STRING,
        ts          STRING
    )
    USING iceberg
    PARTITIONED BY (org_id)
""")

def generate_batch(n: int = 10) -> list[Row]:
    rows = []
    for _ in range(n):
        rows.append(Row(
            id="".join(random.choices(string.ascii_lowercase, k=8)),
            org_id="org_A",
            value=round(random.uniform(0, 1000), 2),
            source="writer_A",
            ts=datetime.now(timezone.utc).isoformat(),
        ))
    return rows



for run in range(20):
    rows = generate_batch(10)
    df = spark.createDataFrame(rows)

    df.writeTo(f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}").append()

    print(f"[WRITER B] Run {run + 1}: appended {len(rows)} rows")


count = spark.sql(f"SELECT count(*) AS cnt FROM {TABLE_NAME}").collect()[0]["cnt"]
print(f"\nTotal rows in {TABLE_NAME}: {count}")

spark.stop()