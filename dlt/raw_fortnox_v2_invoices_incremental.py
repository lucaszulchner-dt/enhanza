# export GOOGLE_APPLICATION_CREDENTIALS="/mnt/c/Users/lucas.rios/Documents/aaacode/aaa_enhanza/terraform/sa-key.json"
# works with json key or SA impersonation

import dlt
import json
from dlt.sources.filesystem import filesystem


@dlt.resource(write_disposition="append")
def read_fortnox_table(
    table_folder: str,
    last_sync=dlt.sources.incremental("_sync_ts", initial_value="2001-01-01"),
):

    source = filesystem(
        bucket_url=f"gs://sample_data_bucket_enhanz/{table_folder}",
        file_glob="**/*.jsonl",
    )

    for file_item in source:
        relative_path = file_item["relative_path"]

        # extract metadata
        org_id = sync_ts = None
        for part in relative_path.split("/"):
            if part.startswith("OrgId="):
                org_id = part.split("=", 1)[1]
            elif part.startswith("SyncTs="):
                sync_ts = part.split("=", 1)[1]

        # skip small
        if file_item["size_in_bytes"] <= 2:
            continue

        if sync_ts and sync_ts < last_sync.last_value:
            # skip already processed
            print(f"Skipping old file: {sync_ts}")
            continue

        # add metadata
        with file_item.open(mode="rt", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                    yield {
                        **record,
                        "_org_id": org_id,
                        "_sync_ts": sync_ts,
                        "_source_file_name": file_item["file_name"],
                        "_source_full": relative_path,
                    }
                except json.JSONDecodeError as e:
                    print(f"Parse error in {relative_path}: {e}")


pipeline = dlt.pipeline(
    pipeline_name="fortnox_raw",
    destination="filesystem",
    dataset_name="raw12",
)


load_info = pipeline.run(
    read_fortnox_table("FORTNOX_V2_INVOICES").with_name("test"),
    loader_file_format="parquet",
    table_format="iceberg",
)

print(load_info)
