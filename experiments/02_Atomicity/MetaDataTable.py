import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


TABLE = Path("data/generated/02_table")


def write_data_file(file_name, start_id, count):
    TABLE.mkdir(parents=True, exist_ok=True)

    rows = {
        "id": list(range(start_id, start_id + count)),
        "value": [x * 10 for x in range(start_id, start_id + count)],
    }

    pq.write_table(
        pa.table(rows),
        TABLE / file_name,
    )


def write_metadata(version, files):
    metadata = {
        "version": version,
        "files": files,
    }

    with open(TABLE / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)


def read_table():
    with open(TABLE / "metadata.json") as f:
        metadata = json.load(f)

    print(f"Table version: {metadata['version']}")

    total_rows = 0

    for file_name in metadata["files"]:
        table = pq.read_table(TABLE / file_name)
        total_rows += table.num_rows

    print(f"Rows visible through table: {total_rows}")


def main():
    if TABLE.exists():
        import shutil
        shutil.rmtree(TABLE)

    # Initial table state
    write_data_file("part-001.parquet", 0, 100)
    write_data_file("part-002.parquet", 100, 100)

    write_metadata(
        version=1,
        files=[
            "part-001.parquet",
            "part-002.parquet",
        ],
    )

    read_table()


if __name__ == "__main__":
    main()