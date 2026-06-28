# 📚 Apache Spark & PySpark — Revision Notes

> Theory notes from Module 6 of the DataTalksClub Data Engineering Zoomcamp.  
> These notes cover the **concepts behind the code** — ideal for quick revision.

---

## Table of Contents

1. [What is Apache Spark?](#1-what-is-apache-spark)
2. [Spark Architecture](#2-spark-architecture)
3. [SparkSession & SparkContext](#3-sparksession--sparkcontext)
4. [DataFrames](#4-dataframes)
5. [Schemas & Data Types](#5-schemas--data-types)
6. [Partitions & Parallelism](#6-partitions--parallelism)
7. [Lazy Evaluation & the DAG](#7-lazy-evaluation--the-dag)
8. [Transformations vs Actions](#8-transformations-vs-actions)
9. [Parquet Format](#9-parquet-format)
10. [Spark SQL & Temp Tables](#10-spark-sql--temp-tables)
11. [User Defined Functions (UDFs)](#11-user-defined-functions-udfs)
12. [GroupBy — How Spark Executes It](#12-groupby--how-spark-executes-it)
13. [Joins — How Spark Executes Them](#13-joins--how-spark-executes-them)
14. [RDDs — Resilient Distributed Datasets](#14-rdds--resilient-distributed-datasets)
15. [Spark + Google Cloud Storage](#15-spark--google-cloud-storage)
16. [Quick Reference Cheatsheet](#16-quick-reference-cheatsheet)

---

## 1. What is Apache Spark?

Apache Spark is an **open-source, distributed computing engine** designed for large-scale data processing. It processes data across a **cluster of machines** in parallel, making it possible to transform datasets that are far too large for a single machine.

**Key characteristics:**

- **In-memory processing** — Spark keeps intermediate data in RAM rather than writing to disk after every step (unlike Hadoop MapReduce), making it significantly faster for iterative workloads.
- **Unified engine** — Spark handles batch processing, streaming, SQL queries, machine learning, and graph processing all in one framework.
- **Fault-tolerant** — if a node fails, Spark can recompute lost partitions from the original data using the **lineage graph** (the DAG).
- **Language support** — Scala (native), Java, Python (PySpark), R, SQL.

**When to use Spark over SQL (e.g. BigQuery)?**

Use Spark when:
- Your transformations are too complex for SQL (e.g. custom business logic, loops, ML preprocessing)
- You need to process data that isn't in a warehouse yet (raw files on GCS/S3/HDFS)
- You want a portable pipeline that runs locally, on-premise, or on any cloud

Use SQL/BigQuery when:
- The data is already in a warehouse
- The transformations are expressible in SQL
- You want to avoid infrastructure management

---

## 2. Spark Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     DRIVER PROGRAM                      │
│   SparkContext / SparkSession                           │
│   - Builds the DAG (execution plan)                     │
│   - Schedules tasks                                     │
│   - Coordinates the cluster                             │
└──────────────────────┬──────────────────────────────────┘
                       │ Task assignments
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
   ┌───────────┐ ┌───────────┐ ┌───────────┐
   │ Executor  │ │ Executor  │ │ Executor  │
   │  (Node 1) │ │  (Node 2) │ │  (Node 3) │
   │ - Runs    │ │ - Runs    │ │ - Runs    │
   │   tasks   │ │   tasks   │ │   tasks   │
   │ - Stores  │ │ - Stores  │ │ - Stores  │
   │   cache   │ │   cache   │ │   cache   │
   └───────────┘ └───────────┘ └───────────┘
```

**Driver** — the program you write (your Python script / notebook). It plans the work and coordinates executors. There is always exactly one driver.

**Cluster Manager** — allocates resources (can be YARN, Mesos, Kubernetes, or Spark Standalone). In local mode (`local[*]`), the driver itself acts as the cluster manager.

**Executors** — JVM processes on worker nodes that actually run computation. Each executor has a number of **cores** (threads) and **memory**.

**Tasks** — the smallest unit of work. Each partition of data gets processed by one task on one executor core.

**`local[*]` mode** — runs everything on your local machine, using all available CPU cores as simulated executors. Great for development; no cluster needed.

---

## 3. SparkSession & SparkContext

### SparkContext (older, lower-level)
The original entry point to Spark. Deals with the cluster, RDDs, and low-level configuration. In modern Spark you rarely create it directly.

### SparkSession (modern entry point)
Introduced in Spark 2.0, `SparkSession` wraps `SparkContext` and adds DataFrame/SQL support. This is what you always create first:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]")   # cluster URL — "local[*]" = all local cores
    .appName("my_app")    # visible in Spark UI
    .getOrCreate()        # reuse existing session if one exists
```

- `.master("local[*]")` — use all cores on the local machine. In a cluster this would be `"yarn"` or `"spark://host:7077"`.
- `.getOrCreate()` — safe to call multiple times; won't create duplicate sessions.
- `spark.stop()` — always stop the session when done to release resources.

**Accessing SparkContext from a session:**
```python
sc = spark.sparkContext
```

---

## 4. DataFrames

A Spark **DataFrame** is a distributed collection of rows organised into named, typed columns — conceptually identical to a SQL table or a pandas DataFrame, but stored and processed across a cluster.

**Key differences from pandas:**

| | Pandas DataFrame | Spark DataFrame |
|---|---|---|
| Storage | Single machine RAM | Distributed across cluster |
| Execution | Immediate (eager) | Lazy (builds a plan first) |
| Mutability | Mutable | Immutable — operations return new DataFrames |
| Size limit | Fits in RAM | Effectively unlimited |
| Schema | Inferred, loose | Strongly typed, enforced |

**Reading data:**
```python
# CSV
df = spark.read.option("header", "true").csv("path/to/file.csv")

# CSV with explicit schema
df = spark.read.option("header", "true").schema(my_schema).csv("path/")

# Parquet (schema embedded in file — no need to specify)
df = spark.read.parquet("path/to/folder/")

# Wildcard — read all files matching pattern
df = spark.read.parquet("data/pq/green/*/*")  # all years and months
```

**Common DataFrame operations:**
```python
df.show()            # print first 20 rows
df.head(10)          # return first 10 rows as a list
df.printSchema()     # print column names and types
df.columns           # list of column names
df.count()           # count rows (triggers execution)
df.schema            # StructType schema object

df.select("col1", "col2")          # select columns
df.filter(df.col > value)          # filter rows
df.withColumn("new", expr)         # add/replace a column
df.withColumnRenamed("old", "new") # rename a column
df.drop("col")                     # drop a column
df.groupBy("col").count()          # group and aggregate
df.orderBy("col")                  # sort
df.limit(10)                       # take first N rows
df.toPandas()                      # collect to pandas (only for small results!)
```

---

## 5. Schemas & Data Types

### Why explicit schemas matter

When Spark reads a CSV, it reads every column as a **string** by default (unless you ask it to infer). Schema inference on large files is expensive because Spark has to read the entire file just to guess types. Always define explicit schemas for production pipelines.

```python
from pyspark.sql import types

schema = types.StructType([
    types.StructField('hvfhs_license_num',   types.StringType(),    True),
    types.StructField('dispatching_base_num', types.StringType(),   True),
    types.StructField('pickup_datetime',      types.TimestampType(), True),
    types.StructField('dropoff_datetime',     types.TimestampType(), True),
    types.StructField('PULocationID',         types.IntegerType(),   True),
    types.StructField('DOLocationID',         types.IntegerType(),   True),
    types.StructField('SR_Flag',              types.StringType(),    True),
])
```

`StructField(name, dataType, nullable)` — the third argument `True` means the column can contain nulls.

### Common Spark types

| Spark Type | Python equivalent | Use for |
|------------|------------------|---------|
| `StringType()` | `str` | Text, IDs, flags |
| `IntegerType()` | `int` | Counts, IDs (32-bit) |
| `LongType()` | `int` | Large integers (64-bit) |
| `DoubleType()` | `float` | Decimal numbers, amounts |
| `TimestampType()` | `datetime` | Date + time columns |
| `DateType()` | `date` | Date-only columns |
| `BooleanType()` | `bool` | True/False |

### Inferring schema via pandas trick

For small files, use pandas to infer the schema, then pass it to Spark:

```python
# Sample 1000 rows to let pandas figure out types
df_pandas = pd.read_csv("head.csv")
print(df_pandas.dtypes)

# Let Spark show what it would infer from the pandas frame
spark.createDataFrame(df_pandas).schema
# → Use this as a starting point for your StructType definition
```

This avoids reading the full CSV for schema inference while still giving you accurate types.

---

## 6. Partitions & Parallelism

### What is a partition?

A **partition** is a chunk of data stored on one machine. Spark processes each partition as a separate **task**. The degree of parallelism in Spark is determined by the number of partitions — more partitions means more tasks that can run in parallel across executor cores.

```
Full Dataset (100 GB)
├── Partition 0  (executor core 1)
├── Partition 1  (executor core 2)
├── Partition 2  (executor core 3)
...
└── Partition 23 (executor core N)
```

### Default number of partitions

- When reading a single CSV file → **1 partition** (no parallelism!)
- When reading Parquet with multiple files → one partition per file
- After a shuffle (GroupBy, Join) → controlled by `spark.sql.shuffle.partitions` (default: 200)

### `repartition(n)` vs `coalesce(n)`

| | `repartition(n)` | `coalesce(n)` |
|---|---|---|
| Direction | Can increase or decrease | Can only decrease |
| Shuffle | Always shuffles data | Avoids shuffle where possible |
| Use case | Increasing parallelism | Reducing to fewer files for output |
| Cost | Expensive (network I/O) | Cheap |

```python
# Increase to 24 partitions before writing — creates 24 output files
df = df.repartition(24)
df.write.parquet("fhvhv/2021/01/")

# Reduce to 1 partition before writing — creates 1 output file (use with care on large data)
df_result.coalesce(1).write.parquet("data/report/revenue/")
```

**Rule of thumb:** aim for partition sizes of 100–200 MB. Too small → too many tiny tasks (overhead). Too large → not enough parallelism, possible OOM errors.

---

## 7. Lazy Evaluation & the DAG

### Lazy evaluation

Spark does **not** execute transformations immediately. When you call `.select()`, `.filter()`, `.withColumn()`, etc., Spark just records the instruction in a **logical plan**. No data is actually processed.

Execution only happens when you call an **action** — `.show()`, `.count()`, `.write()`, `.collect()`, etc.

```python
df = spark.read.parquet("data/")     # nothing happens
df2 = df.filter(df.amount > 100)    # nothing happens
df3 = df2.groupBy("zone").count()   # nothing happens
df3.show()                           # NOW Spark executes everything
```

### The DAG (Directed Acyclic Graph)

Spark compiles your chain of transformations into a **DAG** — a graph of stages and tasks. The Catalyst optimizer then analyses and rewrites this plan to make it more efficient (e.g. pushing filters earlier, combining steps).

```
Stage 1: Read Parquet → Filter → Select
              ↓ (shuffle boundary — GroupBy)
Stage 2: Aggregate by key → Sort → Write
```

You can inspect the DAG in the **Spark UI** at `http://localhost:4040` while a job is running.

**Why this matters:** You can build up complex chains of transformations without worrying about performance — Spark will optimize the whole thing as a unit before running anything.

---

## 8. Transformations vs Actions

### Transformations (lazy — return a new DataFrame)

| Transformation | Description |
|---------------|-------------|
| `select(cols)` | Choose specific columns |
| `filter(condition)` | Keep rows matching condition |
| `withColumn(name, expr)` | Add or replace a column |
| `withColumnRenamed(old, new)` | Rename a column |
| `drop(col)` | Remove a column |
| `groupBy(col)` | Start a GroupBy (chain `.agg()` or `.count()`) |
| `orderBy(col)` | Sort rows |
| `repartition(n)` | Reshuffle into n partitions |
| `coalesce(n)` | Reduce to n partitions |
| `unionAll(df2)` | Stack two DataFrames vertically |
| `join(df2, on, how)` | Join two DataFrames |
| `limit(n)` | Take first n rows |

### Actions (eager — trigger execution)

| Action | Description |
|--------|-------------|
| `show(n)` | Print first n rows to console |
| `count()` | Count total rows |
| `collect()` | Bring all rows to the driver as a Python list ⚠️ |
| `take(n)` | Return first n rows as a list |
| `head(n)` | Same as `take(n)` |
| `toPandas()` | Collect to pandas DataFrame ⚠️ |
| `write.parquet(path)` | Write to Parquet files |
| `write.csv(path)` | Write to CSV files |

> ⚠️ `collect()` and `toPandas()` bring **all data to the driver**. Only safe on small/aggregated DataFrames. Never call on a full large dataset.

---

## 9. Parquet Format

**Parquet** is a columnar storage format — the standard for data engineering and the preferred format for Spark.

### Why Parquet over CSV?

| | CSV | Parquet |
|---|---|---|
| Storage | Row-based | Column-based |
| Schema | None (inferred) | Embedded in file |
| Compression | None | Built-in (Snappy, GZIP) |
| Read efficiency | Must read all columns | Reads only needed columns |
| Spark read speed | Slow | Fast |
| Splittable | Yes (by row) | Yes (by row group) |

### How columnar storage helps

If you query `SELECT total_amount FROM trips`, Parquet only reads the `total_amount` column bytes — skipping all other columns entirely. With a CSV it must read every column on every row just to find the ones you want.

### Writing Parquet

```python
# Write with default partitioning
df.write.parquet("output/path/")

# Overwrite if exists
df.write.parquet("output/path/", mode="overwrite")

# Combine to one file (coalesce first!)
df.coalesce(1).write.parquet("output/single/")

# Partition by column (creates subdirectories like year=2020/month=01/)
df.write.partitionBy("year", "month").parquet("output/")
```

### Snappy compression

The `.snappy.parquet` extension you see in the repo (e.g. `part-00000-...snappy.parquet`) means Spark used **Snappy compression** — fast to compress/decompress, moderate compression ratio. This is Spark's default.

---

## 10. Spark SQL & Temp Tables

Spark SQL lets you run standard SQL queries against DataFrames by registering them as **temporary views (temp tables)**.

### Registering a temp table

```python
df_trips_data.registerTempTable('trips_data')
# OR (modern equivalent):
df_trips_data.createOrReplaceTempView('trips_data')
```

This makes `trips_data` available as a table name in `spark.sql(...)` queries. It's temporary — only exists for the lifetime of the SparkSession.

### Running SQL queries

```python
result = spark.sql("""
    SELECT
        service_type,
        COUNT(1) AS trip_count
    FROM
        trips_data
    GROUP BY
        service_type
""")

result.show()
```

The result is a regular Spark DataFrame — you can chain further transformations on it.

### Spark SQL vs DataFrame API — they're equivalent

Under the hood, both go through the same **Catalyst optimizer** and produce the same execution plan. Use whichever is clearer for the task:

```python
# These are identical in performance:

# SQL style
spark.sql("SELECT service_type, COUNT(1) FROM trips_data GROUP BY service_type")

# DataFrame API style
df.groupBy('service_type').count()
```

SQL is often cleaner for complex analytical queries. The DataFrame API is better when logic is programmatic (loops, conditionals, UDFs).

### `date_trunc` in Spark SQL

```sql
date_trunc('month', pickup_datetime)  -- truncates to first day of month
date_trunc('hour',  pickup_datetime)  -- truncates to start of hour
```

Used to group time-series data by month or hour without writing complex date arithmetic.

---

## 11. User Defined Functions (UDFs)

A **UDF** lets you use arbitrary Python logic as a Spark transformation when the built-in functions aren't enough.

### Defining and registering a UDF

```python
from pyspark.sql import functions as F
from pyspark.sql import types

# Step 1: write a normal Python function
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'

# Step 2: register it as a Spark UDF, specifying the return type
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())

# Step 3: use it like any built-in function
df = df.withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num))
```

### ⚠️ UDF performance warning

UDFs are **slow** compared to built-in Spark functions. Every row must be:
1. Serialized from JVM to Python
2. Processed by the Python interpreter
3. Serialized back to JVM

This "Python ↔ JVM serialization overhead" can make UDFs 10–100x slower than equivalent built-in functions. Always check if a built-in `pyspark.sql.functions` equivalent exists before writing a UDF.

**Prefer built-in functions:**
```python
from pyspark.sql import functions as F

F.to_date(df.pickup_datetime)    # extract date from timestamp
F.lit("green")                   # literal/constant value column
F.col("column_name")             # reference a column by name
F.sum("amount")                  # aggregation
F.avg("distance")                # aggregation
F.date_trunc("month", col)       # date truncation
```

---

## 12. GroupBy — How Spark Executes It

Understanding what happens internally when you run a GroupBy helps you write more efficient pipelines.

### Two-stage GroupBy execution

When you run `.groupBy("zone").sum("amount")`, Spark executes it in **two stages** separated by a **shuffle**:

**Stage 1 — Partial aggregation (within each partition):**
Each executor aggregates the rows it already has locally — no network I/O yet. This produces partial sums per key per partition.

```
Partition 0: { zone_1: 100, zone_2: 50 }
Partition 1: { zone_1: 200, zone_3: 75 }
Partition 2: { zone_2: 30,  zone_3: 25 }
```

**Shuffle (network transfer):**
All partial results for the same key are sent to the same executor. This is the most expensive step — data moves over the network.

```
zone_1 → Executor A: [100, 200]
zone_2 → Executor B: [50, 30]
zone_3 → Executor C: [75, 25]
```

**Stage 2 — Final aggregation (after shuffle):**
Each executor finishes the aggregation for its assigned keys.

```
zone_1: 300
zone_2: 80
zone_3: 100
```

### `spark.sql.shuffle.partitions`

After a shuffle, Spark creates **200 partitions by default** (`spark.sql.shuffle.partitions=200`). For small datasets this means 200 mostly empty partition files. Override it:

```python
spark.conf.set("spark.sql.shuffle.partitions", "20")
```

---

## 13. Joins — How Spark Executes Them

### Sort-Merge Join (default for large tables)

When both DataFrames are large, Spark uses a **Sort-Merge Join**:

1. **Shuffle** both DataFrames so matching keys end up on the same executor
2. **Sort** each shuffled partition by the join key
3. **Merge** sorted records by scanning through both sides together

This involves **two full shuffles** (one per table) — expensive but necessary for large data.

### Broadcast Join (when one table is small)

When one DataFrame is small enough to fit in memory on each executor (typically < a few hundred MB), Spark can **broadcast** it — send a full copy to every executor. The large table never moves; each executor does a local lookup.

```python
# Spark often detects this automatically.
# You can also force it:
from pyspark.sql import functions as F

df_result = df_large.join(F.broadcast(df_small), on="key")
```

**Why it's fast:** Eliminates the shuffle of the large table entirely. Each executor already has the small lookup table in memory.

**From the code:**
```python
# df_zones is very small compared to df_join
# Spark automatically broadcasts df_zones to all executors
df_result = df_join.join(df_zones, df_join.zone == df_zones.LocationID)
```

### Join types

```python
df1.join(df2, on="key", how="inner")   # only matching rows
df1.join(df2, on="key", how="left")    # all rows from df1
df1.join(df2, on="key", how="right")   # all rows from df2
df1.join(df2, on="key", how="outer")   # all rows from both
df1.join(df2, on="key", how="semi")    # rows in df1 that have a match in df2
df1.join(df2, on="key", how="anti")    # rows in df1 with NO match in df2
```

---

## 14. RDDs — Resilient Distributed Datasets

### What is an RDD?

An **RDD** is the original Spark abstraction (predating DataFrames). It's a **distributed collection of any Python objects** — not just rows with columns. Think of it as a distributed Python list.

- **Resilient** — fault-tolerant; can recompute lost partitions from lineage
- **Distributed** — partitioned across the cluster
- **Dataset** — a collection of elements

DataFrames are built on top of RDDs. When you call `.rdd` on a DataFrame, you drop down to the lower-level API.

### When to use RDDs

RDDs are rarely needed in modern Spark (the DataFrame API is faster and more optimizable). Use RDDs only when:
- You need to process non-tabular data (e.g. raw text, images, JSON with irregular structure)
- Your logic genuinely can't be expressed as DataFrame transformations

### Core RDD operations

```python
# Convert a DataFrame column to an RDD
rdd = df.select('pickup_datetime', 'PULocationID', 'total_amount').rdd

# take(n) — collect first n elements (action)
rows = rdd.take(10)

# filter — keep elements where function returns True (transformation)
rdd_filtered = rdd.filter(lambda row: row.total_amount > 0)

# map — transform each element (transformation)
# Returns a new RDD with one output per input
rdd_mapped = rdd.map(lambda row: (row.PULocationID, row.total_amount))

# reduceByKey — aggregate values for each key (transformation → action)
# Input: RDD of (key, value) pairs
# Output: RDD of (key, aggregated_value) pairs
rdd_reduced = rdd_mapped.reduceByKey(lambda a, b: a + b)

# toDF — convert RDD back to DataFrame
df_result = rdd_reduced.toDF(schema)
```

### RDD vs DataFrame API for GroupBy

**RDD approach (manual):**
```python
rdd \
  .filter(filter_outliers)         # remove old rows
  .map(prepare_for_grouping)       # (row) → ((hour, zone), (amount, 1))
  .reduceByKey(calculate_revenue)  # sum amounts and counts per (hour, zone)
  .map(unwrap)                     # ((hour, zone), (sum, count)) → RevenueRow
  .toDF(result_schema)
```

**DataFrame API (equivalent, much simpler):**
```python
df.filter(...).groupBy("hour", "zone").agg(F.sum("total_amount"), F.count("*"))
```

Both produce the same result. The DataFrame version lets Spark's Catalyst optimizer do the work; the RDD version gives you explicit control — but at the cost of verbosity and Catalyst optimizations.

### Key RDD concept: `reduceByKey`

`reduceByKey` is the RDD equivalent of `GROUP BY + aggregate`. It:
1. Groups all `(key, value)` pairs by key
2. Applies a binary reduction function to combine all values for the same key

```python
def calculate_revenue(left_value, right_value):
    left_amount, left_count = left_value
    right_amount, right_count = right_value
    return (left_amount + right_amount, left_count + right_count)

rdd.reduceByKey(calculate_revenue)
# ('zone_1', (100, 5)) + ('zone_1', (200, 3)) → ('zone_1', (300, 8))
```

---

## 15. Spark + Google Cloud Storage

Spark can read from and write to **GCS** (`gs://`) using the **GCS Connector for Hadoop**, which implements the Hadoop filesystem interface over GCS.

### Setup

```python
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

credentials_location = 'path/to/google_credentials.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('gcs_app') \
    .set("spark.jars", "lib/gcs-connector-hadoop3-latest.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_location)

sc = SparkContext(conf=conf)

# Configure Hadoop to use GCS filesystem implementations
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl",                   "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", credentials_location)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
```

### Reading from GCS

Once configured, use `gs://` paths exactly like local paths:

```python
df = spark.read.parquet("gs://my-bucket/pq/green/*/*")
df.show()
df.count()
```

### Why the GCS connector JAR is needed

Spark natively understands `hdfs://` and `file://` paths. To understand `gs://`, it needs the GCS connector — a plugin that maps GCS operations to the Hadoop `FileSystem` API that Spark already knows how to use.

---

## 16. Quick Reference Cheatsheet

### SparkSession setup
```python
spark = SparkSession.builder.master("local[*]").appName("name").getOrCreate()
spark.stop()
```

### Read data
```python
spark.read.option("header","true").csv("path")
spark.read.option("header","true").schema(schema).csv("path")
spark.read.parquet("path/*/")
```

### Schema definition
```python
from pyspark.sql import types
schema = types.StructType([
    types.StructField("col", types.StringType(), True),
    ...
])
```

### Common transforms
```python
df.select("a", "b")
df.filter(df.col > 0)
df.withColumn("new", F.to_date(df.ts))
df.withColumnRenamed("old", "new")
df.groupBy("col").agg(F.sum("amount"), F.count("*"))
df.repartition(24)
df.coalesce(1)
df1.unionAll(df2)
df1.join(df2, on=["key"], how="outer")
```

### Built-in functions (`from pyspark.sql import functions as F`)
```python
F.lit("constant")          # literal value
F.col("name")              # column reference
F.to_date(col)             # timestamp → date
F.date_trunc("month", col) # truncate to month
F.sum("col")               # sum aggregation
F.avg("col")               # average aggregation
F.count("*")               # count rows
F.udf(fn, returnType)      # register a UDF
F.broadcast(df)            # hint to broadcast in a join
```

### Spark SQL
```python
df.registerTempTable("name")          # register as SQL temp table
df.createOrReplaceTempView("name")    # modern equivalent
spark.sql("SELECT ... FROM name ...")  # run SQL, returns DataFrame
```

### RDD operations
```python
rdd = df.rdd                          # DataFrame → RDD
rdd.take(10)                          # action: get first 10
rdd.filter(fn)                        # transformation: keep matching
rdd.map(fn)                           # transformation: transform each
rdd.reduceByKey(fn)                   # transformation: aggregate by key
rdd.toDF(schema)                      # RDD → DataFrame
```

### Write data
```python
df.write.parquet("path/", mode="overwrite")
df.coalesce(1).write.parquet("single_file/")
```

### Partition tuning
```python
spark.conf.set("spark.sql.shuffle.partitions", "20")  # default 200
df.rdd.getNumPartitions()  # check current partition count
```

---

*Notes based on Module 6 of the DataTalksClub Data Engineering Zoomcamp.*  
*Course: https://github.com/DataTalksClub/data-engineering-zoomcamp*
