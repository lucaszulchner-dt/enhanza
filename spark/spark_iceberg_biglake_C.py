# writer_conflict_test.py
import random
import string
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession, Row

CATALOG_NAME = "iceberg_bucket_enhanz"
BUCKET = "gs://iceberg_bucket_enhanz"
PROJECT_ID = "pj-enhan-terraform-main"
NAMESPACE = "concurrency_conflict_test"
TABLE_NAME = "test_concurrent"

spark = (
    SparkSession.builder.appName("conflict_test")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
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
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.rest.auth.type",
        "org.apache.iceberg.gcp.auth.GoogleAuthManager",
    )
    .config(
        f"spark.sql.catalog.{CATALOG_NAME}.io-impl",
        "org.apache.iceberg.gcp.gcs.GCSFileIO",
    )
    .config("spark.sql.defaultCatalog", CATALOG_NAME)
    .getOrCreate()
)

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
spark.sql(f"USE {NAMESPACE}")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id STRING, org_id STRING, value DOUBLE, source STRING, ts STRING
    ) USING iceberg PARTITIONED BY (org_id)
""")


def write_batch(writer_name, org_id):
    rows = [
        Row(
            id="".join(random.choices(string.ascii_lowercase, k=8)),
            org_id=org_id,
            value=round(random.uniform(0, 1000), 2),
            source=writer_name,
            ts=datetime.now(timezone.utc).isoformat(),
        )
        for _ in range(10)
    ]

    df = spark.createDataFrame(rows)
    df.writeTo(f"{CATALOG_NAME}.{NAMESPACE}.{TABLE_NAME}").append()
    return f"{writer_name} committed"


for round_num in range(10):
    print(f"\n{'=' * 60}")
    print(f"ROUND {round_num + 1}: launching 2 concurrent writers")
    print(f"{'=' * 60}")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(write_batch, "writer_A", "org_A"),
            pool.submit(write_batch, "writer_B", "org_B"),
        ]
        for f in as_completed(futures):
            try:
                print(f"  OK: {f.result()}")
            except Exception as e:
                print(f"  FAILED: {e}")

count = spark.sql(f"SELECT count(*) AS cnt FROM {TABLE_NAME}").collect()[0]["cnt"]
print(f"\nTotal rows: {count}")
print(f"Expected:   {10 * 2 * 10} = 200")

spark.stop()
