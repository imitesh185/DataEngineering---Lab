# Engineering Lab Notes

## Project

**Lakehouse Under Attack**

Learning loop:

```text
BREAK → OBSERVE → EXPLAIN → FIX → REPRODUCE → GENERALIZE
```

The purpose of this lab is not to learn systems by seeing only their intended behavior.

The approach is to deliberately break assumptions, observe what actually happens, understand why the system was designed that way, and then connect the observation to production systems.

---

# Phase 1 — Parquet Physical Reality

## Goal

Understand what actually happens inside a Parquet file instead of treating Parquet as a black-box file format.

The main question:

> If Parquet is just a file containing data, how does it make large-scale analytical reads efficient?

---

## Row Groups

A Parquet file is divided into **row groups**.

Conceptually:

```text
Parquet file
│
├── Row Group 1
├── Row Group 2
├── Row Group 3
└── ...
```

A row group contains a horizontal subset of rows.

For example:

```text
Rows 0–999
Rows 1000–1999
Rows 2000–2999
```

Each row group independently contains the columns.

---

## Column Chunks

Inside a row group, each column is stored separately.

```text
Row Group
│
├── id column chunk
├── customer_id column chunk
├── timestamp column chunk
└── amount column chunk
```

This is why analytical queries can avoid reading columns that are not required.

If the query only needs:

```sql
SELECT customer_id, amount
```

there is no reason to read the physical data for unrelated columns.

This is **column pruning**.

---

## Pages

Column chunks are further divided into pages.

```text
Column Chunk
│
├── Page
├── Page
├── Page
└── ...
```

Pages are the smaller physical units used for encoding/compression and reading.

The hierarchy observed was:

```text
Parquet
   ↓
Row Groups
   ↓
Column Chunks
   ↓
Pages
```

---

# Statistics

Parquet stores statistics for physical regions of data, including values such as:

* minimum
* maximum
* null count

For example:

```text
Row Group 1
timestamp:
min = Jan 1
max = Jan 10

Row Group 2
timestamp:
min = Jan 11
max = Jan 20
```

A query such as:

```sql
WHERE timestamp = Jan 15
```

does not need to read Row Group 1 because its statistics prove that the requested value cannot exist there.

This is **predicate pushdown / data skipping** at the physical-layout level.

The important idea:

> The query engine can reason about whether a physical region can contain relevant data before reading that region.

---

# Ordering / Physical Layout

The experiments compared different physical arrangements of the same logical data.

The important discovery was:

> Physical ordering changes the usefulness of statistics.

If values are clustered:

```text
Row Group 1 → 1–100
Row Group 2 → 101–200
Row Group 3 → 201–300
```

statistics become highly selective.

If values are randomly distributed:

```text
Row Group 1 → 3, 500, 81, 920, ...
Row Group 2 → 17, 600, 42, 850, ...
```

the min/max ranges become broad.

For a query targeting a particular value, many row groups may appear capable of containing it.

Therefore:

```text
logical data
    +
physical arrangement
    ↓
query efficiency
```

The physical layout is part of the performance characteristics of the dataset.

---

# Adversarial / Poor Physical Layout

The attack was to deliberately make the physical layout bad.

The point was not that Parquet stops working.

It still produces valid Parquet.

The problem is that the metadata becomes less useful for skipping data.

This established an important distinction:

> A file can be structurally valid while being physically inefficient for the workload.

---

# Data Distribution Attack

The next attack focused on the distribution of values rather than simply ordering.

Two important cases were considered:

```text
low cardinality
```

versus:

```text
high cardinality
```

Cardinality describes the number of distinct values in a column.

Examples:

```text
country:
India
India
India
USA
USA
```

has relatively low cardinality.

Whereas:

```text
transaction_id:
a1
a2
a3
a4
...
```

has high cardinality.

---

## Why Cardinality Matters

Encoding strategies behave differently depending on data distribution.

Low-cardinality data can often benefit substantially from dictionary encoding.

Conceptually:

```text
"India" → 1
"USA"   → 2
"UK"    → 3
```

Instead of repeatedly storing the full strings.

High-cardinality data can make dictionary encoding less attractive because the dictionary itself becomes large.

An encoding strategy may therefore fall back to another representation.

The broader lesson:

> Encoding efficiency depends on the characteristics of the data, not just the column's data type.

---

# Compression

Data distribution also affects compression.

Repeated values provide redundancy that compression can exploit.

For example:

```text
India
India
India
India
India
```

is highly compressible.

Random/high-entropy data is harder to compress.

Therefore:

```text
data distribution
      ↓
encoding efficiency
      ↓
compression ratio
      ↓
physical file size
      ↓
I/O required
```

---

# Statistics and Distribution

Distribution also influences statistics.

If a column is physically clustered, min/max statistics can be narrow.

If values are scattered throughout a row group, statistics can become broad.

Therefore:

```text
data distribution
      ↓
physical layout
      ↓
statistics quality
      ↓
data skipping
```

This connected the encoding/compression attack back to the earlier physical-layout experiment.

---

# Small Files Attack

The final Phase 1 attack was the **small-files problem**.

Instead of:

```text
1 large Parquet file
```

we deliberately created many small Parquet files:

```text
file-001.parquet
file-002.parquet
file-003.parquet
...
file-N.parquet
```

Each file is individually valid.

The problem occurs at the table/system level.

Every file creates additional overhead:

```text
discover file
open file
read metadata/footer
plan work
schedule work
read data
close file
```

So a workload with many tiny files can spend significant effort managing files rather than processing useful data.

The important distinction:

> Small files are not a Parquet correctness problem. They are a system-level planning, metadata, scheduling and I/O overhead problem.

This is why lakehouse systems eventually need mechanisms such as compaction and file-size management.

---

# Phase 1 Mental Model

The final physical model became:

```text
                    Parquet
                       │
                  Row Groups
                       │
                 Column Chunks
                       │
                     Pages
                       │
          ┌────────────┴────────────┐
          ↓                         ↓
      Statistics                Encoding
          ↓                         ↓
    Data skipping             Compression
          │                         │
          └────────────┬────────────┘
                       ↓
                Physical efficiency
```

And physical layout matters:

```text
same logical data
        +
different physical layout
        ↓
different read behavior
```

---

# Phase 2 — Break the "Parquet Files = Table" Assumption

## Starting Question

After understanding Parquet, the next question was:

> If I have perfectly valid Parquet files, what is still missing for them to behave like a reliable table?

The answer was:

```text
Parquet file
    ↓
contains DATA

metadata
    ↓
defines TABLE STATE
```

A directory full of Parquet files is therefore not automatically a reliable table.

---

# Toy Orders Table

We built the smallest possible table abstraction:

```text
orders/
├── data/
│   ├── part-001.parquet
│   ├── part-002.parquet
│   └── ...
└── metadata.json
```

Example metadata:

```json
{
  "version": 1,
  "files": [
    "part-001.parquet",
    "part-002.parquet"
  ]
}
```

The metadata is the authority for table membership.

Therefore:

```text
file exists
    ≠
file belongs to table
```

DuckDB can act as the query engine while Parquet remains the physical storage.

Conceptually:

```text
              OUR TOY TABLE
                    │
        ┌───────────┴───────────┐
        │                       │
   metadata.json          Parquet files
   "what is visible?"     "where is data?"
        │                       │
        └───────────┬───────────┘
                    ↓
                 DuckDB
                    ↓
                  SELECT
```

---

# Attack — Data Written, Metadata Not Updated

Scenario:

```text
WRITE part-003.parquet
        ↓
      CRASH
        ↓
metadata never updated
```

Physical storage:

```text
part-001
part-002
part-003
```

Metadata:

```text
part-001
part-002
```

The reader sees only 001 and 002.

`part-003` physically exists but is not part of the logical table.

This is an **orphan/uncommitted file**.

The key lesson:

> A file existing in storage does not make its data part of the table. The committed metadata/snapshot determines table membership.

---

# Attack — Metadata Updated, Data Absent

Reverse the ordering:

```text
1. Update metadata
2. Metadata references part-003
3. CRASH
4. part-003 never successfully appears
```

Now the logical table references something that cannot be read.

The reader trusts the metadata and attempts to access the referenced file.

This exposes the need for atomic publication:

> Readers should see either the old complete table state or the new complete table state — never a half-published state.

---

# Why Directory Listing Is Not the Table

A question arose around how we know a particular Parquet file is an orphan.

The answer is:

```text
Physical storage                 Authoritative metadata

part-001.parquet      ✓          part-001.parquet ✓
part-002.parquet      ✓          part-002.parquet ✓
part-003.parquet      ✓          part-003.parquet ✗
```

The file is an orphan because it physically exists but is not referenced by the authoritative table state.

Readers do not need to treat the raw directory listing as the table definition.

---

# Attack — Concurrent Writers

Two writers can read the same table version.

```text
Initial:
V1 = [001, 002]

Writer A reads V1
Writer B reads V1
```

A creates:

```text
V2 = [001, 002, 003]
```

B creates:

```text
V2 = [001, 002, 004]
```

If both blindly write `metadata.json`, the second writer can overwrite the first.

Observed experiment:

```text
Writer A: published version 2
Writer B: published version 2
```

Final metadata contained:

```text
001
002
004
```

while physical storage contained:

```text
001
002
003
004
```

Therefore `003` became an orphan.

---

# Important Discovery — Version Numbers Alone Are Not Enough

Both writers had:

```text
observed_version = 1
```

Both calculated:

```text
new_version = 2
```

Simply writing:

```text
version = 2
```

does not establish concurrency control.

The system must ensure that:

> A writer can only commit if the state it originally observed is still the current state.

---

# Optimistic Concurrency Control

OCC was then introduced.

The mental model:

```text
Writer A:
observed = V1

Writer B:
observed = V1
```

A reaches commit first:

```text
expected = V1
current  = V1

→ SUCCESS

new version = V2
```

B reaches commit:

```text
expected = V1
current  = V2

→ CONFLICT
```

B cannot overwrite A.

The important operation is effectively:

```text
if current_version == expected_version:
    publish new version
else:
    conflict
```

The validation and publication need to be atomic.

The Python `threading.Lock` in the toy experiment was used only to demonstrate this atomic commit point locally.

A Python mutex is not the production implementation of distributed lakehouse concurrency control.

---

# Retry Is Not Always "Publish Again"

A deeper issue appeared with state-dependent operations.

Example:

```text
V1:
value = 1
```

A reads V1:

```text
1 + 1 = 2
```

B reads V1:

```text
1 + 1 = 2
```

A commits:

```text
V2:
value = 2
```

B conflicts.

B cannot blindly publish its previously calculated result:

```text
value = 2
```

because that calculation was based on stale state.

B must:

```text
READ V2
   ↓
value = 2
   ↓
RE-COMPUTE
2 + 1 = 3
   ↓
VALIDATE
   ↓
COMMIT V3
```

Therefore:

> Retry = read new state → re-evaluate operation → validate → publish.

For an append-only operation such as adding a new independent file, rebasing can be simpler.

For deletes/updates/state-dependent operations, retry may require actual re-execution against the new state.

---

# Multiple Workers and Contention

The next question was:

> What happens if many workers repeatedly modify the same shared state?

The important variable is not simply the number of threads.

The important question is:

> How many workers are modifying the same mutable state, and how much do their read-to-commit windows overlap?

Conceptually:

```text
many workers
      ↓
same mutable state
      ↓
overlapping transactions
      ↓
contention
      ↓
OCC conflicts
      ↓
retries
```

More workers do not automatically mean more conflicts.

The conflict rate depends on:

* number of workers
* shared state
* transaction duration
* overlap between transactions
* commit frequency
* retry behavior
* whether work can be partitioned

---

# What Happens Without Retry?

If an OCC conflict occurs and no retry/failure policy exists:

```text
transaction
    ↓
conflict
    ↓
operation fails
```

The system does not magically make the operation succeed.

Retry behavior must therefore be part of the system design where appropriate.

---

# Reducing Contention

Synchronization is not the only way to deal with contention.

Possible approaches include:

```text
1. Locks
2. Optimistic concurrency control
3. Partition/shard the mutable state
```

A more fundamental strategy is often:

> Reduce how much state has to be shared.

Instead of:

```text
one shared state
      ↑
many workers
```

partition it:

```text
state A     state B     state C
   ↑           ↑           ↑
worker(s)   worker(s)   worker(s)
```

This reduces the number of workers competing for each individual state.

---

# Final Attack — Partitioned Orders

We tested this using the actual Orders model rather than an artificial counter.

## Case A — One Shared Orders State

```text
Worker 1 ─┐
Worker 2 ─┤
Worker 3 ─┤
Worker 4 ─┼──→ one metadata.json
Worker 5 ─┤
Worker 6 ─┘
```

Every writer was competing for the same table state.

Observed:

```text
Physical files:       30
Visible files:         7
Successful commits:    7
OCC conflicts:        23
```

Therefore:

```text
30 physical files
       ↓
7 committed files
       ↓
23 uncommitted/orphan files
```

---

# Case B — Partitioned Orders

The table was divided into independent regions.

Conceptually:

```text
orders/
├── region=west/
├── region=east/
├── region=north/
├── region=central/
└── region=south/
```

Different workers could now operate on different metadata states.

Observed:

```text
Physical files:       30
Visible files:        25
Successful commits:   25
OCC conflicts:         5
```

The exact conflict count is timing-dependent, but the structural result demonstrated the effect of reducing shared mutable state.

---

# The South Partition

Two workers were deliberately assigned to South.

```text
Worker 3 ─┐
          ├──→ south metadata
Worker 5 ─┘
```

Observed:

```text
south
version = 6
visible = 5
physical = 10
```

This was important because it showed:

> Partitioning does not eliminate concurrency conflicts.

It reduces the **contention domain**.

If multiple writers target the same partition, they can still conflict.

---

# Partitioning Is Not Free

Partitioning can make cross-partition operations harder.

Example:

```text
Move Order #123

West
 ↓
remove

South
 ↓
add
```

Now one logical business operation touches two independent states.

This creates an additional coordination problem.

Therefore:

```text
partitioning
    ↓
less contention
    +
more independent state
    ↓
cross-partition operations become harder
```

---

# Final Phase 1 → Phase 2 Mental Model

Phase 1 showed:

```text
Parquet
   ↓
physical data organization
   ↓
row groups
   ↓
column chunks
   ↓
pages
   ↓
statistics / encoding / compression
   ↓
efficient or inefficient physical reads
```

Phase 2 added the missing table abstraction:

```text
                  TABLE
                    │
          ┌─────────┴─────────┐
          ↓                   ↓
    Physical data        Logical state
    Parquet files          Metadata
                                │
                                ↓
                         Version / snapshot
                                │
                                ↓
                         Atomic publication
                                │
                                ↓
                              OCC
                                │
                    ┌───────────┴───────────┐
                    ↓                       ↓
                 success                 conflict
                                            ↓
                                         retry
                                            ↓
                                  re-read + recompute
```

The combined mental model is:

> **Parquet solves physical analytical storage. It does not by itself solve table state, atomic publication, concurrency, snapshots, retries, or lifecycle management.**

That is the problem space that leads to lakehouse table formats.

---

# Phase 2 Completion

Phase 2 is complete.

The next step is no longer to build another toy table.

The next step is to attack the real implementations:

```text
                    Phase 3
                       │
              ┌────────┴────────┐
              ↓                 ↓
           Delta Lake       Apache Iceberg
              │                 │
          transaction        snapshots
             log              manifests
              │                 │
              └────────┬────────┘
                       ↓
                real table state
                + concurrency
                + recovery
                + evolution
```

The goal is to map what we just built manually to how production lakehouse formats actually implement it.