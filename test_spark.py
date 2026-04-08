"""
test_spark.py
--------------
Purpose : Quick smoke tests to verify PySpark is working and JDBC connectivity
          is functional before running the full pipeline.

Usage   :
    python test_spark.py             # runs smoke test only
    python test_spark.py --mysql     # also tests MySQL JDBC connectivity
    python test_spark.py --all       # runs all tests
"""

import argparse
import glob
import os
import sys

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
    spark.stop()
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
    finally:
        spark.stop()


def test_target_connectivity() -> None:
    """Verify target DB connectivity using target_config.yaml."""
    print("\n[TEST] Target DB connectivity...")
    from connections.target_connection import get_target_connection
    from utils.config_loader import load_config

    cfg = load_config(os.path.join(_PROJECT_ROOT, "config", "target_config.yaml"))
    mode = cfg.get("type", "mysql")

    print(f"       Target mode: {mode}")
    opts = get_target_connection(mode=mode)
    spark = get_spark("TargetTest")
    try:
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
    finally:
        spark.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PySpark connectivity tests.")
    parser.add_argument("--mysql", action="store_true",
                        help="Test source MySQL JDBC connectivity.")
    parser.add_argument("--target", action="store_true",
                        help="Test target DB JDBC connectivity.")
    parser.add_argument("--all", action="store_true",
                        help="Run all tests.")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    test_spark_basic()

    if args.all or args.mysql:
        test_mysql_connectivity()

    if args.all or args.target:
        test_target_connectivity()
