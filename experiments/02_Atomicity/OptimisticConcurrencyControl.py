import json
import threading
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


TABLE_DIR = Path("data/generated/02_table/OCCorders")
DATA_DIR = TABLE_DIR / "data"
METADATA_FILE = TABLE_DIR / "metadata.json"

commit_lock = threading.Lock()


def write_parquet(filename, start, end):
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
    # -----------------------------
    # 1. READ
    # -----------------------------
    metadata = read_metadata()

    observed_version = metadata["version"]

    print(
        f"{name}: read version {observed_version}"
    )

    # -----------------------------
    # 2. WRITE DATA
    # -----------------------------
    write_parquet(filename, start, end)

    print(
        f"{name}: wrote {filename}"
    )

    # -----------------------------
    # 3. PREPARE COMMIT
    # -----------------------------
    new_metadata = {
        "version": observed_version + 1,
        "files": metadata["files"] + [filename],
    }

    # -----------------------------
    # 4 + 5. ATOMIC OCC COMMIT
    # -----------------------------
    with commit_lock:

        current_metadata = read_metadata()
        current_version = current_metadata["version"]

        if current_version != observed_version:
            print(
                f"{name}: CONFLICT! "
                f"expected V{observed_version}, "
                f"found V{current_version}"
            )
            return

        write_metadata(new_metadata)

        print(
            f"{name}: COMMITTED V{new_metadata['version']}"
        )

def setup():
    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

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