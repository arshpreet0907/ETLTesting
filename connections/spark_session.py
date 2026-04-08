"""
connections/spark_session.py
-----------------------------
Purpose : Build and return a PySpark SparkSession configured for local ETL runs.
          Handles Windows-specific HADOOP_HOME / JAVA_HOME setup, auto-discovers
          all JARs in the project's jars/ directory, and sets sensible defaults
          for driver memory and logging.

Usage   :
    from connections.spark_session import get_spark_session
    spark = get_spark_session()
    # or with a custom app name:
    spark = get_spark_session(app_name="orders_pipeline")
"""

import glob
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# Path to the project root — the directory that contains this file's parent
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_spark_session(app_name: str = "ETLValidator"):
    """
    Build a local[*] SparkSession ready for JDBC-based ETL validation.

    Environment setup (Windows)
    ---------------------------
    1. JAVA_HOME  — set from pipeline_config.yaml if not already in the environment.
    2. HADOOP_HOME — set from pipeline_config.yaml if not already in the environment.
       Without this, Spark on Windows raises:
       "Could not locate executable null\\bin\\winutils.exe"

    JAR discovery
    -------------
    All *.jar files found in the project's jars/ directory are added to
    spark.jars automatically.  Drop any new JDBC driver into jars/ and it
    will be picked up without changing this file.

    Parameters
    ----------
    app_name : str
        The Spark application name shown in the Spark UI / logs.

    Returns
    -------
    pyspark.sql.SparkSession
        A running local SparkSession.
    """
    _set_env_from_config()
    jars_csv = _discover_jars()

    from pyspark.sql import SparkSession  # imported here so env vars are set first

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")   # low for single-machine ETL
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )

    if jars_csv:
        builder = builder.config("spark.jars", jars_csv)
        logger.info("Spark JARs loaded: %s", jars_csv)
    else:
        logger.warning(
            "No JARs found in %s — JDBC reads will fail unless JARs are on the classpath.",
            os.path.join(_PROJECT_ROOT, "jars"),
        )

    spark = builder.getOrCreate()

    # Suppress verbose Spark / Hadoop INFO chatter; keep WARN and above
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession started (app=%s, master=local[*])", app_name)

    return spark


# --------------------------------------------------------------------------- #
# Private helpers                                                              #
# --------------------------------------------------------------------------- #

def _set_env_from_config() -> None:
    """
    Read JAVA_HOME and HADOOP_HOME from pipeline_config.yaml and inject them
    into the process environment if they are not already set.

    This must happen BEFORE PySpark is imported so the JVM picks up the values.
    """
    try:
        from utils.config_loader import load_config
        config_path = os.path.join(_PROJECT_ROOT, "config", "pipeline_config.yaml")
        cfg = load_config(config_path)
    except FileNotFoundError:
        logger.debug("pipeline_config.yaml not found — skipping env injection.")
        return
    except Exception as exc:
        logger.warning("Could not load pipeline_config.yaml: %s", exc)
        return

    java_home: Optional[str] = cfg.get("java_home")
    hadoop_home: Optional[str] = cfg.get("hadoop_home")

    if java_home and not os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = java_home
        logger.debug("JAVA_HOME set from config: %s", java_home)

    if hadoop_home and not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = hadoop_home
        logger.debug("HADOOP_HOME set from config: %s", hadoop_home)


def _discover_jars() -> str:
    """
    Find all *.jar files in the project's jars/ directory and return them
    as a comma-separated string suitable for spark.jars.

    Returns an empty string if the directory does not exist or contains no JARs.
    """
    jars_dir = os.path.join(_PROJECT_ROOT, "jars")
    if not os.path.isdir(jars_dir):
        return ""

    # Use forward slashes for Spark even on Windows
    jar_paths = [
        p.replace("\\", "/")
        for p in glob.glob(os.path.join(jars_dir, "*.jar"))
    ]

    return ",".join(jar_paths)
