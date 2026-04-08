# ETL Testing Framework — AI Code Generation Context

## Purpose
This document is a complete specification for generating a PySpark-based ETL validation framework.
Feed this entire file to an AI model and ask it to generate a specific file by name.
All files must be generated consistent with the contracts and conventions described here.

---

## Implementation Status

The table below tracks which files have been fully implemented vs. still pending generation.

| File | Status | Notes |
|------|--------|-------|
| `connections/source_connection.py` | ✅ Implemented | See implemented contract below |
| `connections/target_connection.py` | ✅ Implemented | See implemented contract below |
| `utils/csv_writer.py` | ✅ Implemented | New utility — see contract below |
| `utils/comparator.py` | ✅ Implemented | New utility — see contract below |
| `utils/reporter.py` | ✅ Implemented | New utility — see contract below |
| `connections/spark_session.py` | ⬜ Pending | Spec unchanged |
| `utils/config_loader.py` | ⬜ Pending | Spec unchanged |
| `utils/rulebook_loader.py` | ⬜ Pending | Spec unchanged |
| `utils/logger.py` | ⬜ Pending | Spec unchanged |
| `excel_parser.py` | ⬜ Pending | Spec unchanged |
| `generate_rulebook.py` | ⬜ Pending | Spec unchanged |
| `verify_schema.py` | ⬜ Pending | Spec unchanged |
| `get_source_data.py` | ⬜ Pending | Must import csv_writer — see updated spec |
| `get_target_data.py` | ⬜ Pending | Must import csv_writer — see updated spec |
| `compare.py` | ⬜ Pending | Must import comparator + reporter — see updated spec |

---

## Project Overview

A generic, table-agnostic ETL testing solution that:
1. Reads source/target column mappings from a standardised Excel sheet
2. Converts the Excel sheet into a versioned JSON rulebook for the table under test
3. Uses the rulebook to generate SQL DDL, extract queries, a transform script, and documentation
4. Runs a pipeline: extract source → transform → save CSV → extract target → save CSV → compare
5. Validates actual DB schemas against generated DDL before each extract
6. Reports row-by-row differences between transformed source (expected) and target (actual)

Each Excel sheet represents **one table mapping**. Once processed the sheet is archived; the
rulebook JSON becomes the single source of truth for that table's pipeline run.

---

## Repository Layout

```
etl_validator/
├── config/
│   ├── source_config.yaml          # Source MySQL connection details
│   ├── target_config.yaml          # Target connection details + type flag (mysql | snowflake)
│   └── pipeline_config.yaml        # Paths, output dirs, jar locations
│
├── input/
│   └── <TableName>_mapping.xlsx    # One Excel sheet per table (input, consumed once)
│
├── rulebook/
│   └── <TableName>_rulebook.json   # Generated from Excel; used by all pipeline stages
│
├── generated/
│   └── <TableName>/
│       ├── ddl_source_mysql.sql
│       ├── ddl_target_mysql.sql
│       ├── ddl_target_snowflake.sql
│       ├── query_source.sql
│       ├── query_target.sql
│       ├── transform.py
│       └── mapping_notes.md
│
├── output/
│   └── <TableName>/
│       ├── source_enriched.csv     # Output of get_source_data.py
│       ├── target_actual.csv       # Output of get_target_data.py
│       └── diff_report.csv         # Output of compare.py
│
├── jars/                           # mysql-connector-java.jar, snowflake-jdbc.jar, spark-snowflake.jar
│
├── excel_parser.py                 # Stage 1 — Excel → JSON rulebook
├── generate_rulebook.py            # Stage 2 — JSON rulebook → all generated/* files
├── verify_schema.py                # Stage 3 — called inline during pipeline; DDL vs live DB
├── get_source_data.py              # Stage 4 — extract source via JDBC, transform, save CSV
├── get_target_data.py              # Stage 5 — extract target via JDBC, save CSV
├── compare.py                      # Stage 6 — diff source_enriched vs target_actual, report
│
├── connections/
│   ├── spark_session.py            # SparkSession builder (Windows-safe, loads JARs)
│   ├── source_connection.py        # ✅ get_source_connection() → JDBC options dict
│   └── target_connection.py        # ✅ get_target_connection(mode) → JDBC options dict
│
├── utils/
│   ├── config_loader.py            # Load YAML configs
│   ├── rulebook_loader.py          # Load + validate JSON rulebook
│   ├── logger.py                   # Shared logger
│   ├── csv_writer.py               # ✅ save_dataframe_as_csv(df, file_path)
│   ├── comparator.py               # ✅ compare_dataframes(...) → diff_df + counts
│   └── reporter.py                 # ✅ generate_report(diff_df, counts, output_path)
│
└── requirements.txt
```

---

## Excel Sheet — Standard Format

File name: `<TableName>_mapping.xlsx`
Single sheet named `Mapping`.

### Sheet structure (row by row)

| Row | Column A | Column B | Notes |
|-----|----------|----------|-------|
| 1 | `source_table` | `<schema>.<table>` | Source MySQL table (fully qualified) |
| 2 | `target_table` | `<schema>.<table>` | Target table (MySQL or Snowflake) |
| 3 | `source_joins` | `LEFT JOIN ... ON ...` | Optional; full JOIN clause(s) to append to source SELECT |
| 4 | `primary_key` | `col1,col2` | Comma-separated; used as join key in comparator |
| 5 | `(blank)` | | Separator |
| 6 | `source_column` | `target_column` | Header row for column mapping |
| 7+ | `<src_col>` | `<tgt_col>` | One mapping per row until first blank source_column |

### Column mapping extended columns (columns C–G in row 6+)

| Col | Header | Description |
|-----|--------|-------------|
| C | `source_dtype` | MySQL data type of source column |
| D | `target_dtype` | Data type in target DB |
| E | `transform` | Transformation rule (see Transform Rules section) |
| F | `nullable` | `Y` or `N` |
| G | `notes` | Free-text; ends up in the .md file |

### Example Excel content

```
Row 1:  source_table      | orders_db.orders
Row 2:  target_table      | dw.fact_orders
Row 3:  source_joins      | LEFT JOIN orders_db.customers c ON o.customer_id = c.id
Row 4:  primary_key       | order_id
Row 5:  (blank)
Row 6:  source_column     | target_column  | source_dtype | target_dtype | transform          | nullable | notes
Row 7:  order_id          | ORDER_ID       | INT          | NUMBER(10)   | cast_int           | N        |
Row 8:  status            | ORDER_STATUS   | CHAR(20)     | VARCHAR(20)  | trim               | Y        | CHAR pads with spaces
Row 9:  is_active         | IS_ACTIVE_FLAG | TINYINT(1)   | BOOLEAN      | tinyint_to_bool    | Y        |
Row 10: order_date        | ORDER_DT       | DATE         | DATE         | none               | N        |
Row 11: c.customer_name   | CUSTOMER_NAME  | VARCHAR(100) | VARCHAR(100) | none               | Y        | from joined customers table
Row 12: unit_price        | UNIT_PRICE_USD | DECIMAL(10,2)| NUMBER(10,2) | divide:100         | Y        | stored as cents in source
Row 13: first_name        | FULL_NAME      | VARCHAR(50)  | VARCHAR(150) | concat:last_name   | Y        | concat first+last
```

---

## JSON Rulebook — Schema

File: `rulebook/<TableName>_rulebook.json`

```json
{
  "meta": {
    "table_name": "orders",
    "generated_at": "2025-04-08T10:00:00",
    "source_excel": "input/orders_mapping.xlsx"
  },
  "source": {
    "table": "orders_db.orders",
    "alias": "o",
    "joins": [
      "LEFT JOIN orders_db.customers c ON o.customer_id = c.id"
    ]
  },
  "target": {
    "table": "dw.fact_orders"
  },
  "primary_key": ["order_id"],
  "columns": [
    {
      "source_col": "order_id",
      "target_col": "ORDER_ID",
      "source_dtype": "INT",
      "target_dtype": "NUMBER(10)",
      "transform": "cast_int",
      "nullable": false,
      "notes": ""
    },
    {
      "source_col": "status",
      "target_col": "ORDER_STATUS",
      "source_dtype": "CHAR(20)",
      "target_dtype": "VARCHAR(20)",
      "transform": "trim",
      "nullable": true,
      "notes": "CHAR pads with spaces"
    },
    {
      "source_col": "is_active",
      "target_col": "IS_ACTIVE_FLAG",
      "source_dtype": "TINYINT(1)",
      "target_dtype": "BOOLEAN",
      "transform": "tinyint_to_bool",
      "nullable": true,
      "notes": ""
    },
    {
      "source_col": "unit_price",
      "target_col": "UNIT_PRICE_USD",
      "source_dtype": "DECIMAL(10,2)",
      "target_dtype": "NUMBER(10,2)",
      "transform": "divide:100",
      "nullable": true,
      "notes": "stored as cents in source"
    },
    {
      "source_col": "first_name",
      "target_col": "FULL_NAME",
      "source_dtype": "VARCHAR(50)",
      "target_dtype": "VARCHAR(150)",
      "transform": "concat:last_name",
      "nullable": true,
      "notes": "concat first+last"
    }
  ]
}
```

---

## Transform Rules Catalogue

The `transform` field in the rulebook drives code generation in `transform.py`.
All transforms operate on a PySpark DataFrame column using `pyspark.sql.functions`.

| Rule string | PySpark equivalent | Description |
|---|---|---|
| `none` | no-op, just rename | Column needs no transformation |
| `trim` | `F.trim(col)` | Strip leading/trailing whitespace (for CHAR types) |
| `cast_int` | `col.cast("integer")` | Cast to integer |
| `cast_long` | `col.cast("long")` | Cast to long |
| `cast_string` | `col.cast("string")` | Cast to string |
| `cast_double` | `col.cast("double")` | Cast to double |
| `cast_date` | `col.cast("date")` | Cast to date |
| `cast_timestamp` | `col.cast("timestamp")` | Cast to timestamp |
| `tinyint_to_bool` | `F.when(col == 1, F.lit("true")).otherwise(F.lit("false"))` | MySQL TINYINT(1) → string "true"/"false" for Snowflake BOOLEAN |
| `upper` | `F.upper(col)` | Uppercase string |
| `lower` | `F.lower(col)` | Lowercase string |
| `divide:<n>` | `col / n` | Divide by literal n (e.g. `divide:100`) |
| `multiply:<n>` | `col * n` | Multiply by literal n |
| `concat:<col2>` | `F.concat_ws(" ", col1, F.col(col2))` | Concatenate with another source column, space-separated |
| `date_format:<fmt>` | `F.date_format(col, fmt)` | Reformat date string (e.g. `date_format:yyyy-MM-dd`) |
| `coalesce:<default>` | `F.coalesce(col, F.lit(default))` | Replace null with literal default |
| `md5` | `F.md5(col.cast("string"))` | Hash column value |
| `round:<n>` | `F.round(col, n)` | Round to n decimal places |

Compound transforms (chained): use `|` separator in the field:
`trim|upper` means apply trim then upper.

Unknown transform strings must raise a `ValueError` with a descriptive message at generation time.

---

## File Specifications

### excel_parser.py

**Purpose:** Read the standard-format Excel sheet and emit a validated JSON rulebook.

**Inputs:**
- `--input` : path to `<TableName>_mapping.xlsx`
- `--output` : path to write `<TableName>_rulebook.json` (default: `rulebook/`)

**Key behaviours:**
- Use `openpyxl` to read the sheet (do not use `pandas.read_excel` — header detection is fragile)
- Parse rows 1–4 as metadata key-value pairs
- Detect the header row by scanning for a cell whose value is exactly `source_column`
- Parse all rows after the header until the first row where column A is blank
- Validate: no duplicate `source_col` values, no duplicate `target_col` values
- Validate: every `transform` value is in the known catalogue or matches a parameterised pattern (`divide:`, `multiply:`, `concat:`, `date_format:`, `coalesce:`, `round:`)
- Validate: `primary_key` columns exist in the source_col list
- Write JSON with `indent=2`; include `generated_at` as ISO timestamp
- Exit with code 1 and a clear message on validation failure

**Output:** `rulebook/<TableName>_rulebook.json`

---

### generate_rulebook.py

**Purpose:** Read the JSON rulebook and generate all artefacts into `generated/<TableName>/`.

**Inputs:**
- `--rulebook` : path to JSON rulebook
- `--target-dialect` : `mysql` | `snowflake` | `both` (default: `both`)

**Outputs produced:**

#### ddl_source_mysql.sql
- Standard MySQL `CREATE TABLE IF NOT EXISTS` using `source_table` name
- Columns in rulebook order; use `source_dtype` for types
- Mark NOT NULL where `nullable = false`
- Add `PRIMARY KEY (...)` constraint using `primary_key` list
- Include a comment header: `-- Generated by generate_rulebook.py — for schema verification only`

#### ddl_target_mysql.sql
- `CREATE TABLE IF NOT EXISTS` using `target_table` name
- Use `target_dtype` for types
- MySQL dialect: `NUMBER(p,s)` → `DECIMAL(p,s)`, `BOOLEAN` → `TINYINT(1)`, `VARCHAR` stays
- PRIMARY KEY constraint using `primary_key` list (mapped to target column names)

#### ddl_target_snowflake.sql
- `CREATE TABLE IF NOT EXISTS` using `target_table` name
- Use `target_dtype` as-is (Snowflake native types)
- Snowflake dialect: identifiers in double quotes, `VARCHAR` stays, `NUMBER` stays
- PRIMARY KEY as inline column constraints, not table-level (Snowflake does not enforce FK/PK)

#### query_source.sql
- `SELECT <col_list> FROM <source_table> o <joins>`
- Qualify all source columns with alias `o.` unless the column already contains a `.` (joined table column)
- Columns are those in `source_col` list, in rulebook order
- Add `-- Source extract query` header comment

#### query_target.sql
- `SELECT <col_list> FROM <target_table>`
- Use `target_col` names as-is
- Add `-- Target extract query` header comment

#### transform.py
- A standalone Python/PySpark module with a single public function:
  `def apply_transforms(df: DataFrame, rulebook: dict) -> DataFrame`
- Import section at top: `from pyspark.sql import functions as F`, `from pyspark.sql import DataFrame`
- The function loops over `rulebook["columns"]` and for each entry:
  1. Selects `source_col` from the DataFrame
  2. Applies the transform chain (split on `|`)
  3. Renames the result to `target_col`
- Final line: `return df.select([F.col(c["target_col"]) for c in rulebook["columns"]])`
- Include a `if __name__ == "__main__"` block that loads the rulebook path from `sys.argv[1]`
  and prints the transform plan (column name → transform → output name) without actually running Spark
- Do NOT hardcode any column names. Everything must be driven by the rulebook dict.
- Include inline comments explaining each transform step

#### mapping_notes.md
Markdown document structured as follows:

```
# <TableName> — ETL Mapping Notes

## Source table
- Full name: ...
- Alias: o
- Joins: (list each join)

## Target table
- Full name: ...
- Dialect(s): mysql / snowflake

## Column mappings

| Source column | Source type | Target column | Target type | Transform | Nullable | Notes |
|---|---|---|---|---|---|---|
...

## Transform summary
(Short prose paragraph describing what transformations are applied and why, derived from the notes field)

## Known quirks
- List any CHAR trimming, TINYINT boolean, divide-by-N, concat, etc. issues to watch out for
```

---

### verify_schema.py

**Purpose:** Compare a generated DDL file against the live database schema. Called inline
during the pipeline (before source extract and before target extract). Not a standalone script —
imported as a module.

**Public API:**
```python
def verify_source_schema(rulebook: dict, spark: SparkSession, source_jdbc_opts: dict) -> bool
def verify_target_schema(rulebook: dict, spark: SparkSession, target_jdbc_opts: dict, dialect: str) -> bool
```

**How it works:**
1. Read the generated DDL file from `generated/<table>/ddl_source_mysql.sql` (or target variant)
2. Parse the DDL to extract: column names and their declared types
3. Query the live DB for `INFORMATION_SCHEMA.COLUMNS` filtered by table name and schema
4. Compare column-by-column: name match (case-insensitive), type compatibility
5. For each mismatch, log: `[SCHEMA MISMATCH] column <n>: DDL says <type>, DB says <type>`
6. Return `True` if all columns match, `False` otherwise
7. The pipeline stage (get_source_data.py / get_target_data.py) must check the return value
   and raise `SchemaVerificationError` with a summary if `False`

**Type compatibility rules:**
- `INT` matches `int`, `int(11)`, `integer`
- `VARCHAR(n)` matches `varchar(n)` (must match length)
- `CHAR(n)` matches `char(n)`
- `TINYINT(1)` matches `tinyint(1)`
- `DECIMAL(p,s)` matches `decimal(p,s)`
- `DATE` matches `date`
- Comparison is case-insensitive

**Snowflake target:** Use Snowflake JDBC to query `INFORMATION_SCHEMA.COLUMNS` with
`TABLE_SCHEMA = '<schema>'` and `TABLE_NAME = '<table>'`.

---

### get_source_data.py

**Purpose:** Extract source data via Spark JDBC, apply transforms, save enriched CSV.

**Inputs:**
- `--rulebook` : path to JSON rulebook
- `--output-dir` : directory for `source_enriched.csv` (default: `output/<TableName>/`)
- `--verify-schema` : flag (default: True); if set, run `verify_schema.verify_source_schema()` before extract

**Steps:**
1. Load rulebook (via `utils/rulebook_loader.py`)
2. Build SparkSession (via `connections/spark_session.py`)
3. Get source JDBC options (via `connections/source_connection.py`)
4. If `--verify-schema`: call `verify_schema.verify_source_schema()`; abort on failure
5. Read source query (`generated/<TableName>/query_source.sql`) into a Spark DataFrame via:
   ```python
   df = spark.read.format("jdbc") \
       .options(**jdbc_opts) \
       .option("query", source_query) \
       .load()
   ```
6. Import and call `apply_transforms(df, rulebook)` from `generated/<TableName>/transform.py`
   using `importlib.util.spec_from_file_location` (dynamic import — path resolved from rulebook meta)
7. **Save CSV using `utils/csv_writer.save_dataframe_as_csv(df_transformed, output_path)`**
   Do NOT inline the coalesce/rename logic — always delegate to the utility.
8. Log row count and column list after save

**Error handling:**
- Catch JDBC connection errors and print a clear message with the JDBC URL (masked password)
- Catch `SchemaVerificationError` and exit with code 2

---

### get_target_data.py

**Purpose:** Extract target table data via Spark JDBC and save as CSV.

**Inputs:**
- `--rulebook` : path to JSON rulebook
- `--output-dir` : directory for `target_actual.csv`
- `--target-mode` : `mysql` | `snowflake`
- `--verify-schema` : flag (default: True)

**Steps:**
1. Load rulebook
2. Build SparkSession
3. Get target JDBC options (via `connections/target_connection.py`)
4. If `--verify-schema`: call `verify_schema.verify_target_schema()`; abort on failure
5. Read target query (`generated/<TableName>/query_target.sql`) into DataFrame
6. **Save CSV using `utils/csv_writer.save_dataframe_as_csv(df, output_path)`**
   Do NOT inline the coalesce/rename logic — always delegate to the utility.
7. Log row count

---

### compare.py ← UPDATED: delegates to comparator + reporter utilities

**Purpose:** Load both CSVs, delegate comparison to `utils/comparator`, delegate reporting
to `utils/reporter`. This script is now thin orchestration only — no comparison or
reporting logic lives here directly.

**Inputs:**
- `--source-csv` : path to `source_enriched.csv`
- `--target-csv` : path to `target_actual.csv`
- `--rulebook` : path to JSON rulebook (needed for primary key and column list)
- `--output-dir` : directory for `diff_report.csv`

**Steps:**
```python
# 1. Load both CSVs — always inferSchema=False (all strings)
source_df = spark.read.option("header", True).option("inferSchema", False).csv(source_csv)
target_df = spark.read.option("header", True).option("inferSchema", False).csv(target_csv)

# 2. Derive column lists from rulebook
pk_cols    = rulebook["primary_key"]
compare_cols = [
    c["target_col"] for c in rulebook["columns"]
    if c["target_col"] not in pk_cols
]

# 3. Delegate comparison — returns diff_df + counts
from utils.comparator import compare_dataframes
diff_df, src_count, tgt_count, matched_count = compare_dataframes(
    source_df=source_df,
    target_df=target_df,
    primary_key_cols=pk_cols,
    compare_cols=compare_cols,
)

# 4. Delegate reporting — saves CSV, prints summary, returns/raises exit code
from utils.reporter import generate_report
generate_report(
    diff_df=diff_df,
    source_row_count=src_count,
    target_row_count=tgt_count,
    matched_row_count=matched_count,
    output_path=os.path.join(output_dir, "diff_report.csv"),
    exit_on_differences=True,   # triggers sys.exit(3) on failures
)
```

---

## Connection Utilities — Implemented Contracts

### ✅ connections/source_connection.py

```python
def get_source_connection() -> dict:
    """
    Reads config/source_config.yaml.
    Returns JDBC options dict with keys: url, driver, user, password, fetchsize.
    URL format: jdbc:mysql://<host>:<port>/<database>?useSSL=false&allowPublicKeyRetrieval=true
    Driver:     com.mysql.cj.jdbc.Driver
    fetchsize:  from config key 'fetchsize', default 10000 if absent.
    Password is included in the dict (required by Spark) but NEVER logged.
    Raises KeyError if any of host/port/database/user/password is missing from YAML.
    """
```

**Required config keys:** `host`, `port`, `database`, `user`, `password`
**Optional config keys:** `fetchsize` (default: `10000`)

---

### ✅ connections/target_connection.py

```python
def get_target_connection(mode: str = "mysql") -> dict:
    """
    Reads config/target_config.yaml.
    mode="mysql"     → MySQL JDBC options (same pattern as source connection).
    mode="snowflake" → Snowflake JDBC options using the [snowflake] sub-section.
    Raises ValueError for unknown mode values.
    Raises KeyError if required config keys are missing.
    Password is NEVER logged.
    """
```

**MySQL mode keys returned:** `url`, `driver`, `user`, `password`, `fetchsize`
- URL format: `jdbc:mysql://<host>:<port>/<database>?useSSL=false&allowPublicKeyRetrieval=true`
- Driver: `com.mysql.cj.jdbc.Driver`

**Snowflake mode keys returned:** `url`, `driver`, `user`, `password`, `sfWarehouse`, `sfDatabase`, `sfSchema`, `sfRole`
- URL format: `jdbc:snowflake://<account>.snowflakecomputing.com/`
- Driver: `net.snowflake.client.jdbc.SnowflakeDriver`
- All Snowflake-specific keys come from the `snowflake:` subsection of `target_config.yaml`

**Required config keys (MySQL mode):** `host`, `port`, `database`, `user`, `password`
**Required config keys (Snowflake mode):** `snowflake.account`, `snowflake.warehouse`, `snowflake.database`, `snowflake.schema`, `snowflake.role`, `snowflake.user`, `snowflake.password`

---

## Utils Utilities — Implemented Contracts

### ✅ utils/csv_writer.py

**Purpose:** Persist any PySpark DataFrame as a single named CSV file.
Encapsulates the Spark `coalesce(1)` → part-file rename pattern so no other
file ever needs to implement this logic inline.

```python
def save_dataframe_as_csv(df: DataFrame, file_path: str) -> None:
    """
    Write df to a single CSV at file_path.

    Behaviour:
    - Calls df.coalesce(1) before writing — produces exactly one part file.
    - Writes to a temporary directory (<file_path>_tmp_spark/) then renames
      the single part-*.csv file to file_path.
    - Cleans up the temporary Spark directory after rename (_SUCCESS, .crc, etc.).
    - Creates parent directories automatically (os.makedirs with exist_ok=True).
    - Writes with header=True and nullValue="" (SQL NULLs become empty cells).
    - Overwrites any existing file at file_path.
    - Logs row count and column count after completion.

    Raises:
    - FileNotFoundError if Spark produces no part file (write error).
    - RuntimeError if more than one part file is found (coalesce(1) violated).
    """
```

**Usage in pipeline files:**
```python
from utils.csv_writer import save_dataframe_as_csv

save_dataframe_as_csv(df_transformed, "output/orders/source_enriched.csv")
save_dataframe_as_csv(df_target,      "output/orders/target_actual.csv")
```

**Important notes:**
- Always use this utility instead of inlining `.coalesce(1).write...` anywhere.
- The `nullValue=""` option means NULL values appear as empty strings in the CSV.
  This is intentional: the comparator reads CSVs back as all-string and treats
  empty string and None separately (empty string ≠ null per spec null handling rules).
- `df.count()` is called internally for logging — do not count the DF separately
  after calling this function as it will trigger a second Spark job.

---

### ✅ utils/comparator.py

**Purpose:** Accept two PySpark DataFrames and produce an exploded diff DataFrame.
Separated from `compare.py` so comparison logic is reusable and independently testable.

```python
def compare_dataframes(
    source_df: DataFrame,
    target_df: DataFrame,
    primary_key_cols: List[str],
    compare_cols: List[str],
) -> Tuple[DataFrame, int, int, int]:
    """
    Parameters:
        source_df        — expected data (source_enriched); all columns as strings.
        target_df        — actual data (target_actual); all columns as strings.
        primary_key_cols — column names to join on; must exist in both DataFrames.
        compare_cols     — non-PK columns to diff; must exist in both DataFrames.

    Returns:
        diff_df          — exploded differences (schema below); empty DF if no diffs.
        source_row_count — int: total rows in source_df.
        target_row_count — int: total rows in target_df.
        matched_row_count— int: rows with zero column-level differences.
    """
```

**Output DataFrame schema:**
| Column | Type | Description |
|--------|------|-------------|
| `primary_key_value` | string | PK value(s); composite PKs joined with `\|` |
| `column_name` | string | Name of the column that differs |
| `expected_value` | string | Value from source_df |
| `actual_value` | string | Value from target_df |

**Internal implementation details (for reference when calling or testing):**
- Columns are internally prefixed `src_` and `tgt_` after the join to avoid name collisions.
- Join uses `eqNullSafe` — `null == null` is treated as a match (not a mismatch).
- A `full` outer join is used so rows missing from either side are captured.
- After flagging mismatches, wide mismatch rows are exploded via a Spark `array` + `explode`
  pattern — one output row per differing cell.
- `SparkSession.getActiveSession()` is used inside the explode helper; a session must already
  be active when `compare_dataframes` is called.

**Normalisation applied before comparison:**
- All columns cast to string
- Surrounding whitespace stripped (`F.trim`)
- Boolean literals normalised to lowercase: `"True"` / `"TRUE"` / `"False"` → `"true"` / `"false"`
  using a case-insensitive regex replace

**Null handling:**
- `null == null` → **MATCH** (both-null is not a difference; handled by `eqNullSafe`)
- `null == "null"` (string) → **MISMATCH** (indicates a transform wrote a literal "null" string)
- `"" == null` → **MISMATCH** (empty string is not null)

---

### ✅ utils/reporter.py

**Purpose:** Accept a diff DataFrame from `comparator` and produce a human-readable CSV
report plus a printed summary. Separated from comparison logic for independent reuse.

```python
def generate_report(
    diff_df: DataFrame,
    source_row_count: int,
    target_row_count: int,
    matched_row_count: int,
    output_path: str,
    exit_on_differences: bool = False,
) -> int:
    """
    Parameters:
        diff_df           — output of compare_dataframes(); empty DF = no diffs = PASS.
        source_row_count  — total rows from source CSV.
        target_row_count  — total rows from target CSV.
        matched_row_count — rows with no column-level differences.
        output_path       — full path for diff_report.csv (parent dirs created if needed).
        exit_on_differences — if True, calls sys.exit(3) on diffs, sys.exit(0) on pass.
                              if False (default), returns the exit code to the caller.

    Returns:
        0 if no differences found (PASS).
        3 if differences found (FAIL).

    Raises:
        ValueError if diff_df is missing any of the four required columns.
    """
```

**Required diff_df columns:** `primary_key_value`, `column_name`, `expected_value`, `actual_value`
(matches exactly the output schema of `compare_dataframes`)

**Always writes the CSV** — even when diff_df is empty. An empty diff_report.csv proves
the comparison ran and found nothing. Never skip the write on a PASS.

**Printed summary format:**
```
==================================================
=== Comparison Summary ===
==================================================
  Total source rows  : 10,000
  Total target rows  : 10,000
  Matching rows      : 9,985
  Mismatched rows    : 15
  Columns with diffs : ORDER_STATUS (12), IS_ACTIVE_FLAG (3)
  Report saved to    : output/orders/diff_report.csv

  ✗ FAIL — Differences detected. Review diff_report.csv.
==================================================
```

On a PASS the summary prints `✓ PASS — No differences found between source and target.`

**Column diff counts** are computed by grouping diff_df on `column_name` and are ordered
descending by count so the most-divergent columns appear first.

**Exit codes returned/raised:**
- `EXIT_CODE_OK = 0` — no differences
- `EXIT_CODE_DIFFERENCES = 3` — differences found

---

## Config File Formats

### config/source_config.yaml
```yaml
host: 192.168.1.10
port: 3306
database: orders_db
user: etl_user
password: secret
fetchsize: 10000        # optional; defaults to 10000 if omitted
```

### config/target_config.yaml
```yaml
type: snowflake          # mysql | snowflake

# MySQL target
host: 192.168.1.20
port: 3306
database: dw
user: etl_user
password: secret

# Snowflake target (used when type = snowflake)
snowflake:
  account: xy12345
  warehouse: COMPUTE_WH
  database: DW
  schema: FACT
  role: ETL_ROLE
  user: etl_user
  password: secret
```

### config/pipeline_config.yaml
```yaml
java_home: "C:/Program Files/Java/jdk-11"          # Optional; overrides env if set
hadoop_home: "C:/hadoop"                            # winutils location
jars_dir: "jars"
input_dir: "input"
rulebook_dir: "rulebook"
generated_dir: "generated"
output_dir: "output"
log_level: INFO
```

---

## Error Handling Standards

All pipeline scripts must:
- Use Python `logging` (via `utils/logger.py`) not bare `print()` for operational messages
- Use `print()` only for the final human-readable summary
- Raise specific exception classes (not bare `Exception`):
  - `SchemaVerificationError(table, mismatches: list)`
  - `RulebookValidationError(message)`
  - `TransformError(column, rule, original_error)`
- Exit codes:
  - `0` = success / no differences
  - `1` = validation / config error
  - `2` = schema mismatch
  - `3` = data comparison found differences

---

## PySpark Usage Conventions

- Always use `pyspark.sql.functions as F` — never inline lambda UDFs unless unavoidable
- Read via `.format("jdbc").option("query", sql)` — never `.option("dbtable", ...)`
  when a custom SELECT with joins is needed
- **Never inline `.coalesce(1).write...` CSV logic** — always use `utils/csv_writer.save_dataframe_as_csv()`
- Never cache DataFrames unless the same DF is used more than twice in the same script
- Column names in DataFrames: always use the `target_col` names after transformation
  (downstream compare.py expects target column names in source_enriched.csv)
- Load CSVs with `inferSchema=False` and `header=True` — treat everything as strings for comparison
- Always call `SparkSession.getActiveSession()` inside utility functions rather than passing
  the session as a parameter, unless the function is at the pipeline-script level

---

## Pandas vs PySpark — Key Differences (Reference)

This project uses **PySpark exclusively** in all pipeline files. Pandas is only acceptable
in `excel_parser.py` as a fallback for complex Excel parsing.

| Aspect | Pandas | PySpark |
|--------|--------|---------|
| Execution model | Eager — runs immediately | Lazy — builds a DAG; executes only on action (`.count()`, `.collect()`, `.write`) |
| Data location | Single machine RAM | Distributed across cluster partitions (or `local[*]` for single-machine) |
| Scale | Limited to machine RAM | Can process terabytes |
| Schema | Inferred dynamically, mutable | Strongly typed, immutable, defined upfront |
| Null representation | `NaN` (float) and `None` (object) — two distinct concepts | Unified `null` for all types |
| Row access | Direct indexing `df.iloc[0]` | Must `.collect()` to driver first — expensive, avoid in production |
| API style | Python-native, flexible | SQL-like functional; use `F.*` built-ins over UDFs |
| Boolean normalisation | Python `True`/`False` objects | String `"true"`/`"false"` after CSV read — normalise before compare |
| UDFs | Standard Python functions | Must be registered; serialisation overhead — prefer `F.*` |

---

## Generation Instructions for AI Models

When asked to generate a specific file, follow these rules:

1. **Read the entire context** before writing any code — especially the implemented contracts section
2. **Generate the exact file requested** — do not generate stubs or placeholders
3. **No hardcoded column names, table names, or business logic** — everything must be
   driven by the rulebook dict or the config YAML files
4. **Include a module docstring** at the top of every Python file with: purpose, inputs,
   outputs, and usage example
5. **Include type hints** on all function signatures
6. **Do not use `pandas`** in any file that runs inside the Spark pipeline
   (pandas is acceptable only in `excel_parser.py` as a fallback if openpyxl parsing is complex)
7. **Test block**: every pipeline script must have a `if __name__ == "__main__"` block with
   `argparse` for CLI inputs as documented above
8. **Import order**: stdlib → third-party → local, one blank line between groups
9. **The transform.py file** is generated output, not a fixed file. It must be
   re-generated each time from the rulebook. It should import PySpark but not import
   any local project module (it is a generated artefact that must be self-contained)
10. **CSV saving** — never inline `coalesce(1).write...` logic. Always call
    `utils.csv_writer.save_dataframe_as_csv(df, path)` instead.
11. **Comparison and reporting** — `compare.py` must delegate entirely to
    `utils.comparator.compare_dataframes` and `utils.reporter.generate_report`.
    No comparison or diff-formatting logic belongs in compare.py directly.
12. **Utility functions** (csv_writer, comparator, reporter) accept **DataFrames as input**,
    not file paths. This keeps them reusable and independently testable.

---

## Prompts to Use Per File

Below are the exact prompts to give to an AI model (with this document attached):

**excel_parser.py**
> Using the ETL testing framework context document, generate `excel_parser.py` in full.
> It must use openpyxl, implement all validation rules described, and write the exact
> JSON schema shown. Include full error messages and exit codes.

**generate_rulebook.py**
> Using the context document, generate `generate_rulebook.py` in full.
> It must produce all 7 output files (3 DDLs, 2 queries, transform.py, mapping_notes.md).
> Use Jinja2 templates defined as inline strings within the file (no external template files).
> Handle both mysql and snowflake dialect correctly per the type mapping tables.

**verify_schema.py**
> Using the context document, generate `verify_schema.py` in full.
> Implement both `verify_source_schema` and `verify_target_schema`.
> Use INFORMATION_SCHEMA queries via the Spark JDBC connection (not a separate pymysql call).
> Implement all type compatibility rules listed.

**get_source_data.py**
> Using the context document, generate `get_source_data.py` in full.
> It must import transform.py dynamically using `importlib` from the generated directory.
> Implement schema verification, extraction, and transform steps in order.
> Save the CSV by calling utils.csv_writer.save_dataframe_as_csv — do not inline coalesce/rename logic.

**get_target_data.py**
> Using the context document, generate `get_target_data.py` in full.
> Support both mysql and snowflake target modes.
> Implement schema verification and extraction steps.
> Save the CSV by calling utils.csv_writer.save_dataframe_as_csv — do not inline coalesce/rename logic.

**compare.py**
> Using the context document, generate `compare.py` in full.
> This script is thin orchestration only. Delegate all comparison logic to
> utils.comparator.compare_dataframes and all reporting logic to utils.reporter.generate_report.
> No diff or formatting logic should live in compare.py itself.

**connections/spark_session.py**
> Using the context document, generate `connections/spark_session.py`.
> It must handle Windows HADOOP_HOME/winutils setup, auto-discover JARs from the
> jars/ directory, and set appropriate Spark config for local ETL workloads.

**utils/config_loader.py**
> Using the context document, generate `utils/config_loader.py`.
> It must expose a single function load_config(path: str) -> dict that reads a YAML file
> and returns its contents. Raise FileNotFoundError with a clear message if the file is missing.

**utils/rulebook_loader.py**
> Using the context document, generate `utils/rulebook_loader.py`.
> It must expose load_rulebook(path: str) -> dict that reads and validates a JSON rulebook.
> Raise RulebookValidationError if required top-level keys (meta, source, target, primary_key, columns) are missing.

**utils/logger.py**
> Using the context document, generate `utils/logger.py`.
> Expose get_logger(name: str) -> logging.Logger. Log level must be read from pipeline_config.yaml.
> Format: [%(asctime)s] %(levelname)s %(name)s — %(message)s
