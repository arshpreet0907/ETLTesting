# ETL Testing Framework

A generic, table-agnostic PySpark-based ETL validation framework.
Extracts data from source and target databases, applies column transformations,
and performs row-by-row comparison to validate ETL correctness.

---

## Project Structure

```
ETL_Testing/
├── config/
│   ├── source_config.yaml        ← source MySQL credentials
│   ├── target_config.yaml        ← target MySQL or Snowflake credentials
│   └── pipeline_config.yaml      ← JAVA_HOME, HADOOP_HOME, log level
│
├── connections/
│   ├── spark_session.py          ← builds local SparkSession, loads JARs
│   ├── source_connection.py      ← get_source_connection() → JDBC opts
│   └── target_connection.py      ← get_target_connection(mode) → JDBC opts
│
├── utils/
│   ├── logger.py                 ← shared logger factory
│   ├── config_loader.py          ← YAML reader
│   ├── rulebook_loader.py        ← JSON rulebook reader + validator
│   ├── csv_writer.py             ← save DataFrame as single CSV
│   ├── comparator.py             ← row-by-row diff logic
│   └── reporter.py               ← diff report writer + summary printer
│
├── input/                        ← place Excel mapping sheets here
├── rulebook/                     ← generated JSON rulebooks (by excel_parser.py)
├── generated/                    ← generated SQL, transform.py, notes.md per table
├── output/                       ← CSV outputs per table
├── jars/                         ← JDBC driver JARs (mysql-connector, snowflake)
│
├── excel_parser.py               ← [PENDING] Excel → JSON rulebook
├── generate_rulebook.py          ← [PENDING] JSON rulebook → DDL + queries + transform.py
├── verify_schema.py              ← DDL vs live DB schema check (called by pipeline)
├── get_source_data.py            ← Stage 4: extract source + transform → source_enriched.csv
├── get_target_data.py            ← Stage 5: extract target → target_actual.csv
├── compare.py                    ← Stage 6: diff both CSVs → diff_report.csv
├── run_pipeline.py               ← single entry point for stages 4–6
└── test_spark.py                 ← smoke test for PySpark + JDBC connectivity
```

---

## Setup

### 1. Prerequisites
- Java 11 or 17 (set `JAVA_HOME`)
- Python 3.8+
- winutils for Windows (set `HADOOP_HOME` → folder containing `bin/winutils.exe`)

### 2. Install dependencies
```
python -m venv venv
venv\Scripts\activate       # Windows
pip install -r requirements.txt
```

### 3. Configure connections
Edit `config/source_config.yaml` and `config/target_config.yaml` with your
database credentials.

Edit `config/pipeline_config.yaml` with your `java_home` and `hadoop_home` paths.

### 4. Add JDBC JARs
Place driver JARs in the `jars/` folder:
- `mysql-connector-j-9.x.x.jar` — for MySQL source/target
- `snowflake-jdbc-3.x.x.jar` + `spark-snowflake_2.12-x.x.x-spark_3.5.jar` — for Snowflake

### 5. Verify setup
```
python test_spark.py
```

---

## Pipeline Stages

### Stages 1–3 (Excel → generated files)
```
python excel_parser.py --input input/orders_mapping.xlsx
python generate_rulebook.py --rulebook rulebook/orders_rulebook.json
```
These produce: JSON rulebook, DDL files, extract queries, transform.py, mapping notes.

### Stages 4–6 (full pipeline via run_pipeline.py)
```
python run_pipeline.py \
  --rulebook rulebook/orders_rulebook.json \
  --source-query-file generated/orders/query_source.sql \
  --target-query-file generated/orders/query_target.sql \
  --transform-file generated/orders/transform.py \
  --target-mode mysql
```

Or with inline queries:
```
python run_pipeline.py \
  --rulebook rulebook/orders_rulebook.json \
  --source-query "SELECT o.order_id, o.status FROM orders_db.orders o" \
  --target-query "SELECT ORDER_ID, ORDER_STATUS FROM dw.fact_orders" \
  --transform-file generated/orders/transform.py
```

### Run comparison only (CSVs already exist)
```
python run_pipeline.py \
  --rulebook rulebook/orders_rulebook.json \
  --compare-only
```

### Run stages individually
```
python get_source_data.py \
  --rulebook rulebook/orders_rulebook.json \
  --query-file generated/orders/query_source.sql \
  --transform-file generated/orders/transform.py

python get_target_data.py \
  --rulebook rulebook/orders_rulebook.json \
  --query-file generated/orders/query_target.sql \
  --target-mode mysql

python compare.py \
  --rulebook rulebook/orders_rulebook.json \
  --source-csv output/orders/source_enriched.csv \
  --target-csv output/orders/target_actual.csv
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success / PASS — no differences |
| 1 | Config or argument error |
| 2 | Schema verification failed |
| 3 | Data differences found (FAIL) |
| 99 | Unexpected runtime error |

---

## Output Files

| File | Produced by | Contents |
|------|-------------|----------|
| `output/<table>/source_enriched.csv` | get_source_data.py | Transformed source data (expected) |
| `output/<table>/target_actual.csv` | get_target_data.py | Raw target data (actual) |
| `output/<table>/diff_report.csv` | compare.py | One row per cell-level difference |

### diff_report.csv columns
| Column | Description |
|--------|-------------|
| `primary_key_value` | PK value of the mismatched row (composite PKs joined with `\|`) |
| `column_name` | Name of the column that differs |
| `expected_value` | Value from source_enriched (transformed source) |
| `actual_value` | Value from target_actual |

---

## Null Handling in Comparisons

| Scenario | Result |
|----------|--------|
| `null == null` | MATCH |
| `null == "null"` (string) | MISMATCH — transform wrote literal "null" |
| `"" == null` | MISMATCH — empty string is not null |
