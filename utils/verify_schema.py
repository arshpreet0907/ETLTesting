"""
verify_schema.py
-----------------
Purpose : Compare generated DDL files against the live database schema before
          data extraction begins.  Called as a module (not a standalone script)
          by get_source_data.py and get_target_data.py.

          Queries INFORMATION_SCHEMA.COLUMNS via the active Spark JDBC connection
          so no additional DB driver (pymysql etc.) is required.

Public API
----------
    verify_source_schema(rulebook, spark, source_jdbc_opts) -> bool
    verify_target_schema(rulebook, spark, target_jdbc_opts, dialect)  -> bool

Exit codes (when called from pipeline scripts)
----------------------------------------------
    True  → schema matches DDL; extraction may proceed
    False → at least one mismatch found; caller should raise SchemaVerificationError
"""

import logging
import os
import re
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession

from utils.logger import get_logger

logger = get_logger(__name__)

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))


# --------------------------------------------------------------------------- #
# Custom exception                                                             #
# --------------------------------------------------------------------------- #

class SchemaVerificationError(Exception):
    """
    Raised by pipeline scripts when schema verification fails.

    Attributes
    ----------
    table      : str            — fully qualified table name
    mismatches : list of str    — human-readable mismatch descriptions
    """

    def __init__(self, table: str, mismatches: List[str]) -> None:
        self.table = table
        self.mismatches = mismatches
        detail = "\n  ".join(mismatches)
        super().__init__(
            f"Schema verification FAILED for table '{table}':\n  {detail}"
        )


# --------------------------------------------------------------------------- #
# Public API                                                                   #
# --------------------------------------------------------------------------- #

def verify_schema_from_ddl(
    spark: SparkSession,
    jdbc_opts: dict,
    ddl_file: str,
    database: str,
    table: str,
    dialect: str = "mysql",
    schema: str = None,
) -> bool:
    """
    Verify database schema against a DDL file.

    Parameters
    ----------
    spark : SparkSession
    jdbc_opts : dict
        JDBC connection options
    ddl_file : str
        Path to DDL file (CREATE TABLE statement)
    database : str
        Database name (for Snowflake) or schema name (for MySQL)
    table : str
        Table name
    dialect : str
        "mysql" or "snowflake"
    schema : str, optional
        Schema name (only for Snowflake, between database and table)

    Returns
    -------
    bool
        True if schema matches, False if mismatches found
    """
    logger.info(
        "Verifying schema for %s.%s against DDL: %s",
        database, table, ddl_file
    )

    ddl_columns = _parse_ddl_columns(ddl_file)

    if dialect == "snowflake":
        # For Snowflake: database is the DB, schema is the schema layer
        if not schema:
            schema = jdbc_opts.get("sfSchema", "PUBLIC")
        live_columns = _fetch_snowflake_schema(spark, jdbc_opts, database, schema, table)
    else:
        live_columns = _fetch_mysql_schema(spark, jdbc_opts, database, table)

    mismatches = _compare_columns(ddl_columns, live_columns)

    if mismatches:
        for m in mismatches:
            logger.error("[SCHEMA MISMATCH] %s", m)
        return False

    if dialect == "snowflake":
        logger.info("Schema verification PASSED for %s.%s.%s", database, schema, table)
    else:
        logger.info("Schema verification PASSED for %s.%s", database, table)
    return True



def verify_source_schema(
    rulebook: dict,
    spark: SparkSession,
    source_jdbc_opts: dict,
) -> bool:
    """
    Compare the generated source DDL against the live source MySQL schema.

    Parameters
    ----------
    rulebook         : dict  — loaded rulebook (from utils.rulebook_loader)
    spark            : SparkSession
    source_jdbc_opts : dict  — JDBC options from connections.source_connection

    Returns
    -------
    bool  — True if schema matches, False if mismatches were found.
    """
    table_name = rulebook["meta"]["table_name"]
    full_table = rulebook["source"]["table"]            # e.g. "orders_db.orders"
    schema_name, tbl = _split_table(full_table)

    ddl_path = os.path.join(
        _PROJECT_ROOT, "../generated", table_name, "ddl_source_mysql.sql"
    )

    logger.info("Verifying source schema for %s (DDL: %s)", full_table, ddl_path)

    ddl_columns = _parse_ddl_columns(ddl_path)
    live_columns = _fetch_mysql_schema(spark, source_jdbc_opts, schema_name, tbl)

    mismatches = _compare_columns(ddl_columns, live_columns)

    if mismatches:
        for m in mismatches:
            logger.error("[SCHEMA MISMATCH] %s", m)
        return False

    logger.info("Source schema verification PASSED for %s", full_table)
    return True


def verify_target_schema(
    rulebook: dict,
    spark: SparkSession,
    target_jdbc_opts: dict,
    dialect: str = "mysql",
) -> bool:
    """
    Compare the generated target DDL against the live target database schema.

    Parameters
    ----------
    rulebook         : dict
    spark            : SparkSession
    target_jdbc_opts : dict  — JDBC options from connections.target_connection
    dialect          : str   — "mysql" or "snowflake"

    Returns
    -------
    bool
    """
    table_name = rulebook["meta"]["table_name"]
    full_table = rulebook["target"]["table"]
    schema_name, tbl = _split_table(full_table)

    ddl_file = (
        "ddl_target_mysql.sql" if dialect == "mysql" else "ddl_target_snowflake.sql"
    )
    ddl_path = os.path.join(_PROJECT_ROOT, "../generated", table_name, ddl_file)

    logger.info(
        "Verifying target schema for %s (dialect=%s, DDL: %s)",
        full_table, dialect, ddl_path,
    )

    ddl_columns = _parse_ddl_columns(ddl_path)

    if dialect == "snowflake":
        # For Snowflake, schema_name is actually the database
        # We need to get the actual schema from jdbc_opts
        sf_schema = target_jdbc_opts.get("sfSchema", "PUBLIC")
        live_columns = _fetch_snowflake_schema(
            spark, target_jdbc_opts, schema_name, sf_schema, tbl
        )
    else:
        live_columns = _fetch_mysql_schema(
            spark, target_jdbc_opts, schema_name, tbl
        )

    mismatches = _compare_columns(ddl_columns, live_columns)

    if mismatches:
        for m in mismatches:
            logger.error("[SCHEMA MISMATCH] %s", m)
        return False

    logger.info("Target schema verification PASSED for %s", full_table)
    return True


# --------------------------------------------------------------------------- #
# DDL parser                                                                   #
# --------------------------------------------------------------------------- #

def _parse_ddl_columns(ddl_path: str) -> Dict[str, str]:
    """
    Extract {column_name: data_type} from a generated CREATE TABLE DDL file.

    Handles lines like:
        order_id   INT NOT NULL,
        status     VARCHAR(20),
        `col_name` CHAR(10) NOT NULL,
    Ignores PRIMARY KEY lines and comment lines.
    """
    if not os.path.isfile(ddl_path):
        raise FileNotFoundError(
            f"Generated DDL file not found: {ddl_path}\n"
            f"Run generate_rulebook.py first."
        )

    columns: Dict[str, str] = {}
    with open(ddl_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip().rstrip(",")
            if not line or line.startswith("--") or line.startswith("CREATE"):
                continue
            if line.upper().startswith("PRIMARY KEY"):
                continue
            if line in ("(", ")"):
                continue

            # Extract column name (may be backtick-quoted or double-quoted)
            match = re.match(
                r'^[`"]?(\w+)[`"]?\s+([A-Z][A-Z0-9_(),.]+)',
                line,
                re.IGNORECASE,
            )
            if match:
                col_name = match.group(1).upper()
                col_type = match.group(2).upper()
                # Strip NOT NULL / NULL suffix from type
                col_type = re.sub(r"\s+(NOT\s+NULL|NULL)$", "", col_type).strip()
                columns[col_name] = col_type

    return columns


# --------------------------------------------------------------------------- #
# Live schema fetchers                                                         #
# --------------------------------------------------------------------------- #

def _fetch_mysql_schema(
    spark: SparkSession,
    jdbc_opts: dict,
    schema_name: str,
    table_name: str,
) -> Dict[str, str]:
    """Query INFORMATION_SCHEMA.COLUMNS on a MySQL database via JDBC."""
    query = (
        f"SELECT COLUMN_NAME, COLUMN_TYPE "
        f"FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema_name}' "
        f"AND TABLE_NAME = '{table_name}' "
        f"ORDER BY ORDINAL_POSITION"
    )

    rows = (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("query", query)
        .load()
        .collect()
    )

    return {
        row["COLUMN_NAME"].upper(): row["COLUMN_TYPE"].upper()
        for row in rows
    }


def _fetch_snowflake_schema(
    spark: SparkSession,
    jdbc_opts: dict,
    database_name: str,
    schema_name: str,
    table_name: str,
) -> Dict[str, str]:
    """
    Query INFORMATION_SCHEMA.COLUMNS on Snowflake via JDBC.
    
    Parameters
    ----------
    database_name : str
        Snowflake database name
    schema_name : str
        Snowflake schema name (e.g., PUBLIC)
    table_name : str
        Table name
    """
    # Build fully qualified INFORMATION_SCHEMA reference
    info_schema = f"{database_name.upper()}.INFORMATION_SCHEMA"
    
    query = (
        f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
        f"NUMERIC_PRECISION, NUMERIC_SCALE "
        f"FROM {info_schema}.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{schema_name.upper()}' "
        f"AND TABLE_NAME = '{table_name.upper()}' "
        f"ORDER BY ORDINAL_POSITION"
    )
    
    logger.info("Snowflake schema query: %s", query)

    rows = (
        spark.read.format("jdbc")
        .options(**jdbc_opts)
        .option("query", query)
        .load()
        .collect()
    )
    
    logger.info("Found %d columns in Snowflake table %s.%s.%s", len(rows), database_name, schema_name, table_name)

    if len(rows) == 0:
        logger.error("No columns found in Snowflake table %s.%s.%s - table may not exist or connection issue", 
                     database_name, schema_name, table_name)
        return {}

    result: Dict[str, str] = {}
    for row in rows:
        col = row["COLUMN_NAME"].upper()
        dtype = row["DATA_TYPE"].upper()
        # Reconstruct a type string comparable to DDL
        if dtype in ("TEXT", "VARCHAR") and row["CHARACTER_MAXIMUM_LENGTH"]:
            dtype = f"VARCHAR({row['CHARACTER_MAXIMUM_LENGTH']})"
        elif dtype == "NUMBER" and row["NUMERIC_PRECISION"]:
            if row["NUMERIC_SCALE"] is not None and row["NUMERIC_SCALE"] > 0:
                dtype = f"NUMBER({row['NUMERIC_PRECISION']},{row['NUMERIC_SCALE']})"
            else:
                dtype = f"NUMBER({row['NUMERIC_PRECISION']})"
        result[col] = dtype
    
    if result:
        logger.info("Sample columns from Snowflake: %s", list(result.keys())[:5])

    return result


# --------------------------------------------------------------------------- #
# Column comparator                                                            #
# --------------------------------------------------------------------------- #

_TYPE_ALIASES: Dict[str, List[str]] = {
    "INT":         ["INT", "INT(11)", "INTEGER", "INT(10)", "INT(4)"],
    "BIGINT":      ["BIGINT", "BIGINT(20)"],
    "TINYINT(1)":  ["TINYINT(1)", "BOOLEAN", "BOOL"],
    "SMALLINT":    ["SMALLINT", "SMALLINT(6)"],
    "DATE":        ["DATE"],
    "DATETIME":    ["DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"],
    "TEXT":        ["TEXT", "LONGTEXT", "MEDIUMTEXT"],
}


def _types_compatible(ddl_type: str, live_type: str) -> bool:
    """Return True if ddl_type and live_type are considered compatible."""
    ddl_norm = ddl_type.upper().strip()
    live_norm = live_type.upper().strip()

    if ddl_norm == live_norm:
        return True

    # Snowflake NUMBER type compatibility
    # NUMBER(p,0) is equivalent to NUMBER(p)
    if ddl_norm.startswith("NUMBER(") and live_norm.startswith("NUMBER("):
        # Extract precision and scale
        ddl_match = re.match(r"NUMBER\((\d+)(?:,(\d+))?\)", ddl_norm)
        live_match = re.match(r"NUMBER\((\d+)(?:,(\d+))?\)", live_norm)
        
        if ddl_match and live_match:
            ddl_precision = ddl_match.group(1)
            ddl_scale = ddl_match.group(2) or "0"  # Default scale is 0
            live_precision = live_match.group(1)
            live_scale = live_match.group(2) or "0"  # Default scale is 0
            
            # Compare precision and scale
            if ddl_precision == live_precision and ddl_scale == live_scale:
                return True

    # Check alias groups
    for canonical, aliases in _TYPE_ALIASES.items():
        ddl_in = any(ddl_norm == a for a in aliases)
        live_in = any(live_norm == a for a in aliases)
        if ddl_in and live_in:
            return True

    return False


def _compare_columns(
    ddl_cols: Dict[str, str],
    live_cols: Dict[str, str],
) -> List[str]:
    """
    Compare DDL columns to live DB columns.

    Returns a list of human-readable mismatch descriptions (empty = all OK).
    """
    mismatches: List[str] = []

    for col_name, ddl_type in ddl_cols.items():
        if col_name not in live_cols:
            mismatches.append(
                f"Column '{col_name}' present in DDL but NOT FOUND in live DB."
            )
            continue

        live_type = live_cols[col_name]
        if not _types_compatible(ddl_type, live_type):
            mismatches.append(
                f"Column '{col_name}': DDL says '{ddl_type}', "
                f"live DB says '{live_type}' — types not compatible."
            )

    for col_name in live_cols:
        if col_name not in ddl_cols:
            mismatches.append(
                f"Column '{col_name}' present in live DB but NOT in DDL "
                f"(extra column — may be intentional)."
            )

    return mismatches


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #

def _split_table(full_table: str) -> Tuple[str, str]:
    """
    Split "schema.table" into (schema, table).
    Raises ValueError if the format is wrong.
    """
    parts = full_table.split(".")
    if len(parts) != 2 or not all(parts):
        raise ValueError(
            f"Expected 'schema.table' format, got: '{full_table}'"
        )
    return parts[0], parts[1]
