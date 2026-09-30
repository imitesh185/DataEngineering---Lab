from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake


BASE = Path("data/generated/03_DeltaLake&Iceberg/MetadataReconstruct")
TABLE = BASE / "delta_orders"

if BASE.exists():
    shutil.rmtree(BASE)

TABLE.mkdir(parents=True)


# ---------------------------------------------------------
# 1. Create two physical Parquet files
# ---------------------------------------------------------

schema = pa.schema([
    ("order_id", pa.int64()),
    ("customer", pa.string()),
])

part_001 = pa.table(
    {
        "order_id": [1, 2],
        "customer": ["A", "B"],
    },
    schema=schema,
)

part_002 = pa.table(
    {
        "order_id": [3, 4],
        "customer": ["C", "D"],
    },
    schema=schema,
)


# Write the first file through Delta.
# This creates the Delta table and its transaction log.
write_deltalake(
    str(TABLE),
    part_001,
    mode="overwrite",
)

# Append the second file.
write_deltalake(
    str(TABLE),
    part_002,
    mode="append",
)


# ---------------------------------------------------------
# 2. Create a completely valid Parquet file that Delta
#    does NOT know about.
# ---------------------------------------------------------

orphan = pa.table(
    {
        "order_id": [999],
        "customer": ["ORPHAN"],
    },
    schema=schema,
)

orphan_path = TABLE / "orphan.parquet"
pq.write_table(orphan, orphan_path)


# ---------------------------------------------------------
# 3. What physically exists?
# ---------------------------------------------------------

print("\n=== PHYSICAL FILES ===")

for path in sorted(TABLE.rglob("*")):
    if path.is_file():
        print(path.relative_to(TABLE))


# ---------------------------------------------------------
# 4. What does Delta say belongs to the table?
# ---------------------------------------------------------

dt = DeltaTable(str(TABLE))

print("\n=== DELTA ACTIVE FILES ===")

for file in sorted(dt.file_uris()):
    print(Path(file).name)


# ---------------------------------------------------------
# 5. What does the table actually contain?
# ---------------------------------------------------------

print("\n=== DELTA TABLE VERSION ===")
print(dt.version())

print("\n=== DELTA TABLE DATA ===")
print(dt.to_pyarrow_table().to_pandas())


# ---------------------------------------------------------
# 6. Inspect transaction log
# ---------------------------------------------------------

print("\n=== DELTA LOG ===")

log_dir = TABLE / "_delta_log"

for path in sorted(log_dir.iterdir()):
    print(path.name)

print("\nTable location:")
print(TABLE)

from deltalake import DeltaTable

dt = DeltaTable(str(TABLE))

files_before = sorted(Path(f).name for f in dt.file_uris())

print("\n=== BEFORE REMOVE ===")
print(files_before)

# Remove the first Delta data file
target = files_before[0]

dt.delete(f"order_id <= 2")

print("\n=== AFTER REMOVE ===")

dt = DeltaTable(str(TABLE))

print("Delta active files:")
for file in sorted(dt.file_uris()):
    print(Path(file).name)

print("\nPhysical files:")
for path in sorted(TABLE.glob("*.parquet")):
    print(path.name)

print("\nCurrent table:")
print(dt.to_pyarrow_table().to_pandas())

