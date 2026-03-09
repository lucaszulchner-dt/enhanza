import dlt
import random
from datetime import datetime, timezone

@dlt.resource(write_disposition="append")
def fake_data(org_id):
    for i in range(10):
        yield {
            "id": f"id_{i}",
            "org_id": org_id,
            "value": round(random.uniform(0, 1000), 2),
            "ts": datetime.now(timezone.utc).isoformat(),
        }

for i in range(10):
    print (f"starting {i}")

    pipeline = dlt.pipeline(
        pipeline_name="simple_writer",
        destination="filesystem",
        dataset_name="raw_writes",
    )

    load_info = pipeline.run(
        fake_data("org_A"),
        table_name="test_concurrent",
        loader_file_format="parquet",
    )

    print(f"Load info: {load_info}")