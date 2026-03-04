import dlt
import random
import string
from datetime import datetime, timezone
from dlt.destinations.adapters import iceberg_adapter, iceberg_partition


@dlt.resource(write_disposition="append", table_format="iceberg")
def fake_data():
    for i in range(10):
        yield {
            "id": "".join(random.choices(string.ascii_lowercase, k=8)),
            "org_id": "org_B",  # <--- WRITER B always writes to org_B partition
            "value": round(random.uniform(0, 1000), 2),
            "source": "colab",
            "ts": datetime.now(timezone.utc).isoformat(),
        }


# Partition the table by org_id
iceberg_adapter(fake_data, partition=["org_id"])

pipeline = dlt.pipeline(
    pipeline_name="concurrency_partition_test",
    destination="filesystem",
    dataset_name="concurrency_partition_test01",
)

for run in range(20):
    load_info = pipeline.run(
        fake_data(),
        table_name="test_concurrent",
        loader_file_format="parquet",
    )
    print(f"[WRITER B] Run {run + 1}: {load_info}")
