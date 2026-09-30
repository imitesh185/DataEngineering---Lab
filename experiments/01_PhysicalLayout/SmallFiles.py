from pathlib import Path
import random
import time

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


N = 1_000_000
SEED = 42

OUT = Path("data/generated/02_small_files")
OUT.mkdir(parents=True, exist_ok=True)


def generate_rows():
    rng = random.Random(SEED)

    for event_id in range(N):
        yield {
            "event_id": event_id,
            "account_id": rng.randrange(100_000),
            "amount": event_id % 1000,
        }


def write_layout(name, num_files):
    layout = OUT / name
    layout.mkdir(parents=True, exist_ok=True)

    rows_per_file = N // num_files
    rows = list(generate_rows())

    for file_id in range(num_files):
        start = file_id * rows_per_file

        if file_id == num_files - 1:
            end = N
        else:
            end = start + rows_per_file

        table = pa.Table.from_pylist(rows[start:end])

        pq.write_table(
            table,
            layout / f"part-{file_id:05d}.parquet",
            compression="snappy",
        )

    print(f"{name}: {num_files} files")


def benchmark():
    con = duckdb.connect()

    for name in ["1_file", "10_files", "100_files", "1000_files"]:
        path = OUT / name

        # Warm-up
        con.execute(
            f"""
            SELECT COUNT(*)
            FROM read_parquet('{path}/*.parquet')
            """
        ).fetchone()

        start = time.perf_counter()

        result = con.execute(
            f"""
            SELECT
                COUNT(*),
                SUM(amount)
            FROM read_parquet('{path}/*.parquet')
            WHERE account_id BETWEEN 100 AND 199
            """
        ).fetchone()

        elapsed = time.perf_counter() - start

        print(
            f"{name:12} "
            f"rows={result[0]:8} "
            f"time={elapsed:.4f}s"
        )


def main():
    write_layout("1_file", 1)
    write_layout("10_files", 10)
    write_layout("100_files", 100)
    write_layout("1000_files", 1000)

    print("\nBenchmark:\n")
    benchmark()


if __name__ == "__main__":
    main()