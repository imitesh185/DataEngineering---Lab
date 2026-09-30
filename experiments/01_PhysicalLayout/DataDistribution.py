import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

N = 1_000_000
Row_Group_Size = 100_000
Seed = 42

OUT = Path("data/generated/01_DataDistribution")
OUT.mkdir(parents=True, exist_ok=True)

def generate_account_ids(cardinality : int, skewed : bool = False) :
    rng = random.Random(Seed)

    if not skewed :
        for _ in range(N) :
            yield rng.randrange(cardinality)
        return

    #A small number of accounts receive most events
    weights = [ 1/ (rank ** 1.2) for rank in range(1, cardinality + 1)]

    total = sum(weights)
    probabilities = [w / total for w in weights]

    for _ in range(N) :
        yield rng.choices(
            range(cardinality),
            weights=probabilities,
            k = 1
        )[0]

def write_dataset(name, account_ids) :
    event_ids = range(N)
    table = pa.table(
        {
            "event_id" : pa.array(event_ids, type=pa.int64()),
            "account_id" : pa.array(account_ids, type=pa.int64()),
            "amount" : pa.array(
                (i % 1000 for i in range(N)),
                type=pa.int64(),
            ),
        }
    )

    path = OUT / f"{name}.parquet"

    pq.write_table(
        table,
        path,
        row_group_size= Row_Group_Size,
        compression= "snappy",
    )

    print(f"{name:20} {path.stat().st_size / 1024 / 1024:.2f} MB")

def main():
    write_dataset(
        "low_cardinality",
        generate_account_ids(10),
    )

    write_dataset(
        "high_cardinality",
        generate_account_ids(100_000),
    )
    print("Writing skewed")
    write_dataset(
        "skewed",
        generate_account_ids(100_000, skewed=True,),
    )

if __name__ == "__main__" :
    main()