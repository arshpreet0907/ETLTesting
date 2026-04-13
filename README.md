# ETL Testing Framework

Modular PySpark-based ETL validation framework supporting two scenarios:
1. **Excel-based workflow** — Full automation from Excel mapping
2. **Custom workflow** — Direct SQL queries without Excel

## Quick Start

### Scenario 1: Excel-Based
Edit variables in `run_pipeline.py` and run:
```bash
python run_pipeline.py
```

### Scenario 2: Custom (No Excel)
Edit variables in `custom_execution.py` and run:
```bash
python custom_execution.py
```

**Auto-Configuration Mode** (Recommended):
```python
# Just set the table name and target mode!
TABLE_NAME = "cost_ledger"  # Any of the 12 tables
TARGET_MODE = "mysql"       # "mysql" or "snowflake"
VERIFY_SCHEMA = True
```

All paths, queries, transforms, and settings are auto-detected from `input/etl_output_spark/etl_output_mysql/{table_name}/`

**Supports both MySQL and Snowflake targets** - just change `TARGET_MODE`!

### Test
```bash
python test_spark.py
```

## Documentation

See `docs/` folder for complete documentation:
- **[docs/README.md](docs/README.md)** — Main documentation
- **[docs/FINAL_SUMMARY.md](docs/FINAL_SUMMARY.md)** — Implementation summary
- **[docs/QUICKSTART.md](docs/QUICKSTART.md)** — Quick reference

## Structure

```
ETL_Testing/
├── run_pipeline.py          ← Scenario 1 entry point
├── custom_execution.py      ← Scenario 2 entry point
├── test_spark.py            ← Smoke test
├── utils/                   ← Modular functions
├── excel_files/             ← Excel parsing (2 files)
├── config/                  ← YAML configs
├── transforms/              ← Transform examples
└── docs/                    ← Documentation
```

## Key Features

✅ Only 3 root files  
✅ No CLI arguments (variables with comments)  
✅ Execution timing (minutes + seconds)  
✅ Rulebook-free utils (DataFrame only)  
✅ Excel independence (Scenario 2)  
✅ Modular and reusable  

## Exit Codes

- 0 = PASS (no differences)
- 1 = Config error
- 2 = Schema verification failed
- 3 = FAIL (differences found)
- 99 = Unexpected error

## Requirements

```bash
pip install -r requirements.txt
```

Place JDBC drivers in `jars/` directory.

---

For detailed documentation, see **[docs/README.md](docs/README.md)**
