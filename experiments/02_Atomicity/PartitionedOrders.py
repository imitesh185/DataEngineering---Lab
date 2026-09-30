import json
import os
import shutil
import threading
import time
from pathlib import Path

BASE_DIR = Path("data/generated/02_table/")
ROOT = os.path.join(BASE_DIR, "partitioned_orders")

WORKERS = 6
ORDERS_PER_WORKER = 5

regions = ["west", "east", "north", "south", "central", "south"]


def reset():
    if os.path.exists(ROOT):
        shutil.rmtree(ROOT)

    os.makedirs(ROOT)


def write_metadata(path, metadata):
    with open(path, "w") as f:
        json.dump(metadata, f, indent=2)


def read_metadata(path):
    with open(path, "r") as f:
        return json.load(f)


def write_order_file(path, worker_id, batch_id):
    """
    We don't need real Parquet contents for this experiment.
    The important thing is that every writer creates its own
    physical order file before attempting metadata publication.
    """

    with open(path, "w") as f:
        f.write(
            f"worker={worker_id}\n"
            f"batch={batch_id}\n"
            f"orders={ORDERS_PER_WORKER}\n"
        )


# ============================================================
# CASE A
# All writers modify ONE Orders table
# ============================================================

def single_orders_table():

    print("\n========================================")
    print("CASE A: ONE SHARED ORDERS TABLE")
    print("========================================")

    table_dir = os.path.join(ROOT, "single")
    data_dir = os.path.join(table_dir, "data")

    os.makedirs(data_dir)

    metadata_path = os.path.join(table_dir, "metadata.json")

    write_metadata(
        metadata_path,
        {
            "version": 1,
            "files": []
        }
    )

    commit_lock = threading.Lock()

    conflicts = 0
    commits = 0

    stats_lock = threading.Lock()

    def writer(worker_id):

        nonlocal conflicts, commits

        for batch_id in range(ORDERS_PER_WORKER):

            # ----------------------------------------
            # 1. Read current table state
            # ----------------------------------------

            metadata = read_metadata(metadata_path)

            observed_version = metadata["version"]
            observed_files = metadata["files"].copy()

            # ----------------------------------------
            # 2. Create physical order data
            # ----------------------------------------

            filename = (
                f"worker-{worker_id}-"
                f"batch-{batch_id}.parquet"
            )

            filepath = os.path.join(data_dir, filename)

            write_order_file(
                filepath,
                worker_id,
                batch_id
            )

            # Simulate work between read and commit
            time.sleep(0.002)

            # ----------------------------------------
            # 3. Attempt OCC commit
            # ----------------------------------------

            new_metadata = {
                "version": observed_version + 1,
                "files": observed_files + [filename]
            }

            with commit_lock:

                current = read_metadata(metadata_path)

                if current["version"] != observed_version:

                    with stats_lock:
                        conflicts += 1

                    continue

                write_metadata(
                    metadata_path,
                    new_metadata
                )

                with stats_lock:
                    commits += 1

    threads = [
        threading.Thread(
            target=writer,
            args=(worker_id,)
        )
        for worker_id in range(WORKERS)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    final_metadata = read_metadata(metadata_path)

    physical_files = os.listdir(data_dir)

    print("\nFinal table version:")
    print(final_metadata["version"])

    print("Files visible through metadata:")
    print(len(final_metadata["files"]))

    print("Physical files:")
    print(len(physical_files))

    print("Successful commits:")
    print(commits)

    print("OCC conflicts:")
    print(conflicts)

    print("\nMetadata:")
    print(json.dumps(final_metadata, indent=2))


# ============================================================
# CASE B
# Orders are partitioned by region.
#
# Each writer owns a different region.
# ============================================================

def partitioned_orders():

    print("\n========================================")
    print("CASE B: PARTITIONED ORDERS TABLE")
    print("========================================")

    table_dir = os.path.join(ROOT, "partitioned")

    os.makedirs(table_dir)

    # One independent table state per region
    for region in set(regions):

        region_dir = os.path.join(
            table_dir,
            f"region={region}"
        )

        data_dir = os.path.join(
            region_dir,
            "data"
        )

        os.makedirs(data_dir)

        metadata_path = os.path.join(
            region_dir,
            "metadata.json"
        )

        write_metadata(
            metadata_path,
            {
                "version": 1,
                "files": []
            }
        )

    # Each region has its own commit lock
    locks = {
        region: threading.Lock()
        for region in set(regions)
    }

    conflicts = 0
    commits = 0

    stats_lock = threading.Lock()

    def writer(worker_id):

        nonlocal conflicts, commits

        region = regions[worker_id]

        region_dir = os.path.join(
            table_dir,
            f"region={region}"
        )

        data_dir = os.path.join(
            region_dir,
            "data"
        )

        metadata_path = os.path.join(
            region_dir,
            "metadata.json"
        )

        for batch_id in range(ORDERS_PER_WORKER):

            # ----------------------------------------
            # 1. Read THIS REGION's state
            # ----------------------------------------

            metadata = read_metadata(metadata_path)

            observed_version = metadata["version"]
            observed_files = metadata["files"].copy()

            # ----------------------------------------
            # 2. Write physical order file
            # ----------------------------------------

            filename = (
                f"worker-{worker_id}-"
                f"batch-{batch_id}.parquet"
            )

            filepath = os.path.join(
                data_dir,
                filename
            )

            write_order_file(
                filepath,
                worker_id,
                batch_id
            )

            time.sleep(0.002)

            # ----------------------------------------
            # 3. OCC commit
            # ----------------------------------------

            new_metadata = {
                "version": observed_version + 1,
                "files": observed_files + [filename]
            }

            with locks[region]:

                current = read_metadata(metadata_path)

                if current["version"] != observed_version:

                    with stats_lock:
                        conflicts += 1

                    continue

                write_metadata(
                    metadata_path,
                    new_metadata
                )

                with stats_lock:
                    commits += 1

    threads = [
        threading.Thread(
            target=writer,
            args=(worker_id,)
        )
        for worker_id in range(WORKERS)
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    # ----------------------------------------
    # Inspect every partition
    # ----------------------------------------

    total_visible_files = 0
    total_physical_files = 0

    for region in set(regions):

        region_dir = os.path.join(
            table_dir,
            f"region={region}"
        )

        metadata_path = os.path.join(
            region_dir,
            "metadata.json"
        )

        data_dir = os.path.join(
            region_dir,
            "data"
        )

        metadata = read_metadata(metadata_path)

        visible = len(metadata["files"])
        physical = len(os.listdir(data_dir))

        total_visible_files += visible
        total_physical_files += physical

        print(
            f"{region:8} "
            f"version={metadata['version']} "
            f"visible={visible} "
            f"physical={physical}"
        )

    print("\nTotal files visible through metadata:")
    print(total_visible_files)

    print("Total physical files:")
    print(total_physical_files)

    print("Successful commits:")
    print(commits)

    print("OCC conflicts:")
    print(conflicts)


# ============================================================

reset()

single_orders_table()
partitioned_orders()