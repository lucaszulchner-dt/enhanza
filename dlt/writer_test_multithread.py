import dlt
import random
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

@dlt.resource(write_disposition="append")
def fake_data(org_id):
    for i in range(10):
        yield {
            "id": f"id_{i}",
            "org_id": org_id,
            "value": round(random.uniform(0, 1000), 2),
            "ts": datetime.now(timezone.utc).isoformat(),
        }

def run_pipeline(index):
    print(f"starting {index}")

    pipeline = dlt.pipeline(
        pipeline_name=f"simple_writer_{index}",  # unique name per worker
        destination="filesystem",
        dataset_name="raw_writes",
    )

    load_info = pipeline.run(
        fake_data("org_A"),
        table_name="test_concurrent",
        loader_file_format="parquet",
    )

    print(f"Load info ({index}): {load_info}")
    return load_info

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(run_pipeline, i): i for i in range(10)}
    for future in as_completed(futures):
        idx = futures[future]
        try:
            future.result()
        except Exception as e:
            print(f"Pipeline {idx} failed: {e}")