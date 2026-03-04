"""
PySpark equivalent of the DLT Iceberg pipeline.

Prerequisites:
  - Iceberg 1.10+ with GCP bundle
  - Set GOOGLE_APPLICATION_CREDENTIALS env var to your SA key JSON path
  - The BigLake catalog must already exist (catalog name = bucket name)

Run locally with:
  spark-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.iceberg:iceberg-gcp-bundle:1.10.0 \
    spark_iceberg_biglake.py

Or on Dataproc Serverless:
  gcloud dataproc batches submit pyspark gs://<BUCKET>/spark_iceberg_biglake.py \
    --project=pj-enhan-terraform-main \
    --region=<REGION> \
    --version=2.2 \
    --properties="spark.sql.defaultCatalog=iceberg_bucket_enhanz,..."
"""

import random
import string
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import Row

# ---------------------------------------------------------------------------
# Configuration — adjust these to match your environment
# ---------------------------------------------------------------------------
CATALOG_NAME = "iceberg_bucket_enhanz"  # must match GCS bucket name
BUCKET = "gs://iceberg_bucket_enhanz"
PROJECT_ID = "pj-enhan-terraform-main"
NAMESPACE = "concurrency_partition_test77"
TABLE_NAME = "test_concurrent"

# ---------------------------------------------------------------------------
# Spark session with BigLake Iceberg REST catalog
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("concurrency_partition_test")
    # ── Iceberg extensions ──
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # ── Catalog configuration ──
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest")
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.uri",
        "https://biglake.googleapis.com/iceberg/v1/restcatalog",
    )
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", BUCKET)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.header.x-goog-user-project", PROJECT_ID)
    # ── Auth via GoogleAuthManager (uses GOOGLE_APPLICATION_CREDENTIALS) ──
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.type",
        "org.apache.iceberg.gcp.auth.GoogleAuthManager",
    )
    # ── GCS file I/O ──
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.io-impl",
        "org.apache.iceberg.gcp.gcs.GCSFileIO",
    )
    # ── Disable REST metrics (optional, reduces noise) ──
    .config(f"spark.sql.catalog.{CATALOG_NAME}.rest-metrics-reporting-enabled", "false")
    # ── If your catalog uses credential vending, uncomment the next line ──
    # .config(f"spark.sql.catalog.{CATALOG_NAME}.header.X-Iceberg-Access-Delegation",
    #         "vended-credentials")
    # ── Default catalog so SQL doesn't need fully-qualified names ──
    .config("spark.sql.defaultCatalog", CATALOG_NAME)
    .getOrCreate()
)

# ---------------------------------------------------------------------------
# Create namespace + table (idempotent)
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Generate fake data — mirrors the DLT resource
# ---------------------------------------------------------------------------
def generate_batch(n: int = 10) -> list[Row]:
    rows = []
    for _ in range(n):
        rows.append(
            Row(
                id="".join(random.choices(string.ascii_lowercase, k=8)),
                org_id="org_A",
                value=round(random.uniform(0, 1000), 2),
                source="writer_A",
                ts=datetime.now(timezone.utc).isoformat(),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Append loop — 20 runs, same as the DLT pipeline
# ---------------------------------------------------------------------------
for run in range(20):
    rows = generate_batch(10)
    df = spark.createDataFrame(rows)

    # writeTo + append mirrors DLT's write_disposition="append"
    df.writeTo(f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}").append()

    print(f"[WRITER B] Run {run + 1}: appended {len(rows)} rows")

# ---------------------------------------------------------------------------
# Quick verification
# ---------------------------------------------------------------------------
count = spark.sql(f"SELECT count(*) AS cnt FROM {TABLE_NAME}").collect()[0]["cnt"]
print(f"\nTotal rows in {TABLE_NAME}: {count}")

spark.stop()
