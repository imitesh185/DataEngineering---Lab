import random
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

N = 1_000_000
Rows_Per_Group = 100_000
Seed = 42

OUT = Path("data/generated/01_PhysicalLayout")
OUT.mkdir(parents=True, exist_ok=True)

def generate_rows():
    rng = random.Random(Seed)
    rows = []
    for event_id in range(N):
        account_id = rng.randrange(10_000)

        rows.append(
            {
                "event_id": event_id,
                "account_id": account_id,
                "amount": event_id%1000,
            }
        )

    return rows

def write_parquet(rows, name):
    table = pa.Table.from_pylist(rows)
    path = OUT / f"{name}.parquet"

    pq.write_table(
        table,
        path,
        row_group_size=Rows_Per_Group,
    )

    print(f"Wrote {Path}")

def main():
    rows = generate_rows()

    #1. Random
    random_rows = rows.copy()
    random.Random(Seed).shuffle(random_rows)

    #2. Sorted by account_id
    sorted_rows = sorted(
        rows,
        key=lambda r:r["account_id"],
    )

    #3. Clustered
    #Divide accounts into contiguous range
    clustered_rows = sorted(
        rows,
        key= lambda r: (r["account_id"] // 1000, r["account_id"]),
    )

    #4. Adversarial
    #Intentionally spread every account range across row groups.
    adversarial_rows = []

    for group_start in range(0, N, Rows_Per_Group):
        group = rows[group_start : group_start + Rows_Per_Group]
        #Spread Account_ID values throughout the group
        group = sorted(
            group,
            key= lambda r:r["account_id"] % 1000,
        )
        adversarial_rows.extend(group)


    write_parquet(random_rows, "random")
    write_parquet(sorted_rows, "sorted")
    write_parquet(clustered_rows, "clustered")
    write_parquet(adversarial_rows, "adversarial")

    print("\nRunning Query..\n")

    con = duckdb.connect()

    for name in [
        "random",
        "sorted",
        "clustered",
        "adversarial",
    ] :
        path = OUT / f"{name}.parquet"

        result = con.execute(
            f"""
            SELECT 
                COUNT(*) as rows,
                SUM(amount) as total
                FROM read_parquet('{path}')
                WHERE account_id BETWEEN 100 and 199 
            """
        ).fetchone()

        print(f"{name:12} -> rows={result[0]}, total={result[1]}")

if __name__ == "__main__" :
    main()

    

