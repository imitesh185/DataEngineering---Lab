import json
import threading
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


TABLE_DIR = Path("data/generated/02_table/orders")
DATA_DIR = TABLE_DIR / "data"
METADATA_FILE = TABLE_DIR / "metadata.json"

barrier = threading.Barrier(2)


def write_parquet(filename: str, start: int, end: int):
    table = pa.table({
        "id": list(range(start, end)),
        "amount": [100] * (end - start),
    })

    pq.write_table(table, DATA_DIR / filename)


def read_metadata():
    with open(METADATA_FILE) as f:
        return json.load(f)


def write_metadata(metadata):
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2)


def writer(name, filename, start, end):
    # 1. Both writers read the same table version
    metadata = read_metadata()
    observed_version = metadata["version"]

    print(
        f"{name}: read version {observed_version}"
    )

    # 2. Each writer creates its own data file
    write_parquet(filename, start, end)

    print(
        f"{name}: wrote {filename}"
    )

    # 3. FORCE both writers to reach this point
    #    before either continues to commit.
    barrier.wait()

    # 4. Both writers now believe they can create version 2
    new_metadata = {
        "version": observed_version + 1,
        "files": metadata["files"] + [filename],
    }

    print(
        f"{name}: attempting to publish version "
        f"{new_metadata['version']}"
    )

    # 5. Last writer to write wins
    write_metadata(new_metadata)

    print(
        f"{name}: published version "
        f"{new_metadata['version']}"
    )


def setup():
    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Clean previous files
    for file in DATA_DIR.glob("*.parquet"):
        file.unlink()

    write_parquet("part-001.parquet", 0, 100)
    write_parquet("part-002.parquet", 100, 200)

    write_metadata({
        "version": 1,
        "files": [
            "part-001.parquet",
            "part-002.parquet",
        ],
    })


def main():
    setup()

    print("Initial metadata:")
    print(json.dumps(read_metadata(), indent=2))

    thread_a = threading.Thread(
        target=writer,
        args=("Writer A", "part-003.parquet", 200, 300),
    )

    thread_b = threading.Thread(
        target=writer,
        args=("Writer B", "part-004.parquet", 300, 400),
    )

    thread_a.start()
    thread_b.start()

    thread_a.join()
    thread_b.join()

    print("\nFinal metadata:")
    print(json.dumps(read_metadata(), indent=2))

    print("\nPhysical files:")
    for file in sorted(DATA_DIR.glob("*.parquet")):
        print(f"  {file.name}")


if __name__ == "__main__":
    main()