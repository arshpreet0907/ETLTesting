"""
test_spark.py
--------------
Purpose : Quick smoke tests to verify PySpark is working and JDBC connectivity
          is functional before running the full pipeline.

Usage   : Edit the configuration variables below and run:
    python test_spark.py
"""

import glob
import os
import sys

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION VARIABLES
# ═══════════════════════════════════════════════════════════════════════════

# Test options - Set to True to enable specific tests
TEST_MYSQL = False       # True = test source MySQL connectivity, False = skip
TEST_TARGET = False      # True = test target DB connectivity, False = skip
TEST_ALL = True         # True = run all tests, False = run only basic test

# ═══════════════════════════════════════════════════════════════════════════

# ── Set JVM paths from pipeline_config.yaml before importing PySpark ──────────
_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

try:
    from utils.config_loader import load_config
    _cfg = load_config(os.path.join(_PROJECT_ROOT, "config", "pipeline_config.yaml"))
    if _cfg.get("java_home") and not os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = _cfg["java_home"]
    if _cfg.get("hadoop_home") and not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = _cfg["hadoop_home"]
except Exception:
    pass  # Fall back to system environment variables

from pyspark.sql import SparkSession

JAR_DIR = os.path.join(_PROJECT_ROOT, "jars")


def get_spark(app_name: str = "SparkTest") -> SparkSession:
    jars = ",".join(glob.glob(os.path.join(JAR_DIR, "*.jar")))
    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.host", "localhost")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        # Fix for Snowflake duplicate alias issue
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    )
    if jars:
        builder = builder.config("spark.jars", jars)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def test_spark_basic() -> None:
    """Verify PySpark starts and can create/show a DataFrame."""
    print("\n[TEST] Basic PySpark smoke test...")
    spark = get_spark("SmokeTest")
    data = [("Alice", 30), ("Bob", 25), ("Carol", 35)]
    df = spark.createDataFrame(data, ["name", "age"])
    df.show()
    print("[PASS] PySpark is working correctly.\n")


def test_mysql_connectivity() -> None:
    """Verify MySQL JDBC connectivity using source_config.yaml."""
    print("\n[TEST] MySQL JDBC connectivity...")
    from utils.config_loader import load_config
    cfg = load_config(os.path.join(_PROJECT_ROOT, "config", "source_config.yaml"))

    host = cfg["host"]
    port = cfg["port"]
    database = cfg["database"]
    user = cfg["user"]
    password = cfg["password"]

    jdbc_url = (
        f"jdbc:mysql://{host}:{port}/{database}"
        f"?useSSL=false&allowPublicKeyRetrieval=true"
    )

    spark = get_spark("MySQLTest")
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("query", "SELECT 1 AS ping")
            .option("user", user)
            .option("password", password)
            .load()
        )
        df.show()
        print(f"[PASS] MySQL connection successful → {host}:{port}/{database}\n")
    except Exception as exc:
        print(f"[FAIL] MySQL connection failed: {exc}\n")
        print(f"       URL: {jdbc_url}")
        print(f"       Check: host/port reachable? credentials correct? JAR in jars/?\n")

def test_target_connectivity() -> None:
    """Verify target DB connectivity using target_config.yaml."""
    print("\n[TEST] Target DB connectivity...")
    from utils.connections.target_connection import get_target_connection
    from utils.config_loader import load_config

    cfg = load_config(os.path.join(_PROJECT_ROOT, "config", "target_config.yaml"))
    mode = cfg.get("type", "mysql")

    print(f"       Target mode: {mode}")
    opts = get_target_connection(mode=mode)
    spark = get_spark("TargetTest")
    
    try:
        if mode.lower() == "snowflake":
            # For Snowflake, use dbtable with subquery to avoid duplicate alias issue
            df = (
                spark.read.format("jdbc")
                .options(**opts)
                .option("dbtable", "(SELECT 1 AS ping) AS test_query")
                .load()
            )
        else:
            # For MySQL and other databases
            df = (
                spark.read.format("jdbc")
                .options(**opts)
                .option("query", "SELECT 1 AS ping")
                .load()
            )
        df.show()
        print(f"[PASS] Target ({mode}) connection successful.\n")
    except Exception as exc:
        print(f"[FAIL] Target ({mode}) connection failed: {exc}\n")


if __name__ == "__main__":
    # Always run basic PySpark test
    test_spark_basic()

    # Run MySQL test if enabled
    if TEST_ALL or TEST_MYSQL:
        test_mysql_connectivity()

    # Run target test if enabled
    if TEST_ALL or TEST_TARGET:
        test_target_connectivity()
