# ⚡ Data Engineering Zoomcamp — Module 6: Batch Processing with Apache Spark

> My hands-on work for **Module 6** of the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) — covering distributed batch processing using **Apache Spark** and **PySpark**, from local setup through to reading and writing data on **Google Cloud Storage**.

---

## 📖 Module Overview

Module 6 introduces **batch processing** with **Apache Spark** — the industry-standard distributed computing engine for large-scale data transformation. The module covers Spark internals (RDDs, partitions, the DAG), practical PySpark transformations and SQL queries, performance concepts like broadcasting and shuffling, and connecting Spark to GCS for cloud-scale processing. NYC Taxi data (FHVHV, Green, Yellow) is used as the working dataset throughout.

---

## 🗂️ Notebooks — What's Covered

### `03_test.ipynb` — Spark Setup & First Steps

The very first Spark session:

- Verifying PySpark installation and locating the Spark library
- Creating a `SparkSession` with `local[*]` master (uses all available CPU cores)
- Downloading the NYC Taxi Zone lookup CSV with `curl`
- Reading a CSV into a Spark DataFrame with `spark.read.option("header", "true").csv(...)`
- Writing a DataFrame out to **Parquet** format (`df.write.parquet('zones')`)

---

### `04_pyspark.ipynb` — PySpark Core Concepts

Deep dive into PySpark fundamentals using the **FHVHV (For-Hire Vehicle High Volume)** January 2021 dataset:

- **Schema inference vs explicit schemas** — using a 1000-row `head.csv` sample with `pandas` to infer dtypes, then defining an explicit `StructType` schema with `TimestampType`, `IntegerType`, and `StringType` fields
- **Partitioning** — understanding why a single-file CSV produces 1 partition, and using `.repartition(24)` to parallelize work across 24 Parquet part-files
- **Filtering with the DataFrame API** — `.select(...).filter(df.hvfhs_license_num == 'HV0003').show()`
- **User Defined Functions (UDFs)** — writing a Python function `crazy_stuff()` that categorises dispatch base numbers into buckets, registering it with `F.udf(..., returnType=StringType())`, and applying it with `.withColumn('base_id', crazy_stuff_udf(...))`
- **Column transformations** — `F.to_date()` to extract date parts from timestamps, `.withColumn()` to add computed columns

---

### `05_sparkSchema.ipynb` — Schema Definition & Bulk CSV → Parquet Conversion

Defining precise schemas for both Green and Yellow taxi datasets and converting 2+ years of raw CSVs to Parquet:

- Full `StructType` schema definitions for **Green taxi** (20 fields) and **Yellow taxi** (18 fields) with correct types for every column
- Batch conversion loop: reads every month of CSV data for 2020 (full year) and 2021 (Jan–Aug) for both taxi types
- Writes each month to a **partitioned Parquet directory** (`data/pq/<taxi_type>/<year>/<month>/`) with `.repartition(4)` — 4 part-files per month
- Produces the organised Parquet dataset consumed by all downstream notebooks

---

### `06_spark.sql.ipynb` — Spark SQL & Revenue Reporting

Running SQL queries directly on Spark DataFrames using temporary views:

- Loading Green and Yellow Parquet datasets and **normalising column names** (renaming `lpep_*` / `tpep_*` → `pickup_datetime` / `dropoff_datetime`)
- **Finding common columns** programmatically using set intersection
- Adding a `service_type` literal column (`'green'` / `'yellow'`) and combining with `unionAll()`
- Registering the combined DataFrame as a **Spark temporary table**: `df_trips_data.registerTempTable('trips_data')`
- Running a **monthly revenue aggregation** query via `spark.sql(...)` computing:
  - `SUM` of fare, extra, MTA tax, tips, tolls, improvement surcharge, total amount, congestion surcharge — all grouped by zone, month, and service type
  - `AVG` of passenger count and trip distance
- Writing the result to a **single Parquet file** with `.coalesce(1)` for easy downstream consumption

---

### `07_groupby_join_spark.ipynb` — GroupBy, Joins & Broadcast Variables

Understanding how Spark executes GroupBy and Join operations internally:

- **Hourly revenue aggregation** for Green and Yellow taxis separately — `date_trunc('hour', ...)` + `GROUP BY hour, zone` + `SUM(total_amount)` + `COUNT(1)`
- Writing intermediate revenue tables to Parquet with `.repartition(20)`
- **Outer Join** — merging Green and Yellow hourly revenue tables on `(hour, zone)` using `how='outer'` to retain all records from both sides
- **Broadcast Join** — joining the small `taxi_zone_lookup` zones table against the large revenue table; Spark automatically broadcasts the smaller table to all executors, avoiding a costly shuffle
- Writing the final zone-enriched revenue report to `tmp/revenue-zones/`

---

### `08_RDD.ipynb` — Low-Level RDD Operations

Implementing the same hourly revenue aggregation from `07` using the **low-level RDD API** to understand what Spark does under the hood:

- Converting a DataFrame to an RDD: `df_green.select(...).rdd`
- **Filter** — `rdd.filter(filter_outliers)` using a Python function that checks `lpep_pickup_datetime >= datetime(2020, 1, 1)`
- **Map** — `prepare_for_grouping()` transforms each row into a `((hour, zone), (amount, count))` key-value pair
- **ReduceByKey** — `calculate_revenue()` merges values for the same key by summing amounts and counts
- **Map back to rows** — `unwrap()` converts key-value pairs back to `namedtuple` `RevenueRow` objects
- **Convert to DataFrame** — `.toDF(result_schema)` with an explicit `StructType` schema
- Writing the RDD-computed result to Parquet for comparison with the SQL approach

---

### `09_spark_gcp.ipynb` — Connecting Spark to Google Cloud Storage

Reading data directly from **GCS** using the Hadoop GCS connector:

- Configuring Spark with the **`gcs-connector-hadoop3`** JAR for GCS filesystem support
- Setting GCP service account authentication via `SparkConf`:
  - `spark.hadoop.google.cloud.auth.service.account.enable = true`
  - `spark.hadoop.google.cloud.auth.service.account.json.keyfile = <path to credentials>`
- Configuring `fs.AbstractFileSystem.gs.impl` and `fs.gs.impl` on the Hadoop context
- Reading Green taxi Parquet files directly from a GCS bucket: `spark.read.parquet('gs://data_lake_zoomcamp_nytaxi/pq/green/*/*')`

---

### `10_spark_local.ipynb` & `10_spark_local.py` — Local Standalone Pipeline

A clean, self-contained end-to-end pipeline combining lessons from all previous notebooks:

- Reads local Green and Yellow Parquet datasets
- Normalises column names and finds common columns programmatically
- Tags each dataset with a `service_type` literal and combines with `unionAll()`
- Registers as a temp table and runs the full **monthly revenue SQL query** (all revenue components + passenger and distance averages)
- Previews results by converting a 10-row sample to pandas with `.toPandas()`
- Writes the final report as a single coalesced Parquet file

---

### `download_file.sh` — Bulk Data Downloader

A Bash script to download and decompress NYC Taxi CSVs for 2020 and 2021:

```bash
bash download_file.sh green    # Downloads green taxi data
bash download_file.sh yellow   # Downloads yellow taxi data
```

- Loops over all 12 months for both years
- Saves to `data/raw/<taxi_type>/<year>/<month>/` with zero-padded month names
- Decompresses `.csv.gz` files in place with `gunzip`

---

## 🗂️ Repository Structure

```
.
├── download_file.sh                    # Bulk CSV downloader for 2020–2021
├── main.py                             # Project entrypoint placeholder
├── pyproject.toml                      # Python dependencies (PySpark 4.x, pandas, jupyter)
├── .python-version                     # Python 3.12
└── code/
    ├── 03_test.ipynb                   # Spark setup, CSV read, Parquet write
    ├── 04_pyspark.ipynb                # Schemas, repartition, UDFs, column transforms
    ├── 05_sparkSchema.ipynb            # Bulk CSV → Parquet conversion (2020–2021)
    ├── 06_spark.sql.ipynb              # Spark SQL, temp tables, revenue aggregation
    ├── 07_groupby_join_spark.ipynb     # GroupBy, outer join, broadcast join
    ├── 08_RDD.ipynb                    # Low-level RDD: map, filter, reduceByKey
    ├── 09_spark_gcp.ipynb              # Spark + GCS connector, reading from GCS
    ├── 10_spark_local.ipynb            # End-to-end local pipeline (notebook)
    ├── 10_spark_local.py               # End-to-end local pipeline (script)
    ├── taxi_zone_lookup.csv            # NYC zone reference data
    ├── fhvhv/2021/01/                  # FHVHV Jan 2021 (24 Parquet partitions)
    ├── zones/                          # Taxi zone lookup as Parquet
    └── tmp/
        ├── green-revenue/              # RDD-computed green hourly revenue
        └── revenue-zones/              # Zone-enriched joined revenue report
```

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.12 | Runtime |
| PySpark | 4.1.2 | Distributed batch processing |
| pandas | 3.0.3 | Schema inference & result preview |
| Jupyter | 1.1.1 | Interactive notebook environment |
| Apache Spark | (via PySpark) | Spark engine |
| GCS Connector | hadoop3-latest | Spark ↔ Google Cloud Storage |
| Google Cloud Storage | — | Cloud data lake |
| uv | latest | Fast Python package manager |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.12+ with [uv](https://docs.astral.sh/uv/) installed
- Java 11+ (required by Spark)
- A GCP service account JSON key (for `09_spark_gcp.ipynb` only)

### Install dependencies

```bash
uv sync
```

### Download raw data

```bash
bash download_file.sh green
bash download_file.sh yellow
```

### Launch Jupyter

```bash
uv run jupyter notebook
```

### Run notebooks in order

| Step | Notebook | What it does |
|------|----------|-------------|
| 1 | `03_test.ipynb` | Verify Spark works, write zones Parquet |
| 2 | `04_pyspark.ipynb` | Learn PySpark basics with FHVHV data |
| 3 | `05_sparkSchema.ipynb` | Convert all CSVs → Parquet |
| 4 | `06_spark.sql.ipynb` | Run SQL revenue queries |
| 5 | `07_groupby_join_spark.ipynb` | GroupBy + join + broadcast |
| 6 | `08_RDD.ipynb` | Replicate GroupBy with raw RDD API |
| 7 | `09_spark_gcp.ipynb` | Connect Spark to GCS |
| 8 | `10_spark_local.ipynb` | Full end-to-end pipeline |

---

## 📚 Resources

- [DataTalksClub DE Zoomcamp — Module 6](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/05-batch)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [GCS Connector for Hadoop](https://github.com/GoogleCloudDataproc/hadoop-connectors/tree/master/gcs)
- [NYC TLC Trip Data](https://github.com/DataTalksClub/nyc-tlc-data)
- [Course YouTube Playlist](https://www.youtube.com/playlist?list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

---

## 🙌 Acknowledgements

Thanks to [Alexey Grigorev](https://linkedin.com/in/agrigorev) and the DataTalksClub team for this excellent free course.
