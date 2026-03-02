#export GOOGLE_APPLICATION_CREDENTIALS="/mnt/c/Users/lucas.rios/Documents/aaacode/aaa_enhanza/terraform/sa-key.json"
#works with json key or SA impersonation

import dlt
import json
from dlt.sources.filesystem import filesystem


def read_fortnox_table(table_folder: str):
    """Get files from GCS, read each one, yield enriched records."""
    
    source = filesystem(
        bucket_url=f"gs://sample_data_bucket_enhanz/{table_folder}",
        file_glob="**/*.jsonl",
    )

    for file_item in source:
        # filesystem() might yield lists or single items
        # let's handle both
        if isinstance(file_item, list):
            items = file_item
        else:
            items = [file_item]

        for item in items:
            relative_path = item["relative_path"]
            print(f"Processing: {relative_path} ({item['size_in_bytes']} bytes)")

            # Skip empty files
            if item["size_in_bytes"] <= 2:
                print(f"  Skipping (too small)")
                continue

            # Extract metadata from path
            org_id = None
            sync_ts = None
            for part in relative_path.split("/"):
                if part.startswith("OrgId="):
                    org_id = part.split("=", 1)[1]
                elif part.startswith("SyncTs="):
                    sync_ts = part.split("=", 1)[1]

            # Read the JSONL file
            with item.open() as f:
                for line in f:
                    if isinstance(line, bytes):
                        line = line.decode("utf-8")
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        record["_org_id"] = org_id
                        record["_sync_ts"] = sync_ts
                        record["_source_file"] = item["file_name"]
                        yield record
                    except json.JSONDecodeError as e:
                        print(f"  Parse error: {e}")
                        yield {
                            "_org_id": org_id,
                            "_sync_ts": sync_ts,
                            "_source_file": item["file_name"],
                            "_parse_error": str(e),
                        }


pipeline = dlt.pipeline(
    pipeline_name="fortnox_raw",
    destination="filesystem",
    dataset_name="raw",
)

resource = dlt.resource(
    read_fortnox_table("FORTNOX_V2_INVOICES"),
    name="fortnox_v2_invoices",
    write_disposition="replace",
)

load_info = pipeline.run(
    resource,
    loader_file_format="parquet",
    table_format="iceberg",
)
print(load_info)