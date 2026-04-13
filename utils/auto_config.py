"""
auto_config.py
--------------
Auto-configuration utility that extracts all pipeline settings from
the etl_output_spark folder structure based on table name.

Usage:
    from utils.auto_config import get_table_config
    
    config = get_table_config("cost_ledger")
    # Returns all paths, database names, table names, primary keys, etc.
"""

import os
import re
import yaml
from typing import Dict, List, Optional


def get_table_config(
    table_name: str,
    base_path: str = None,
    target_mode: str = "mysql"
) -> Dict:
    """
    Auto-configure pipeline settings based on table name.
    
    Parameters
    ----------
    table_name : str
        Table folder name (e.g., "cost_ledger", "employee_master")
    base_path : str, optional
        Base path to etl_output_mysql folder. If None, auto-detects from project root.
    target_mode : str
        "mysql" or "snowflake"
    
    Returns
    -------
    dict
        Configuration dictionary with all paths and settings
    """
    # Auto-detect base path if not provided
    if base_path is None:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        base_path = os.path.join(project_root, "excel_files", "dummy", "etl_output")
    
    table_folder = os.path.join(base_path, table_name)
    
    # Validate table folder exists
    if not os.path.isdir(table_folder):
        raise ValueError(
            f"Table folder not found: {table_folder}\n"
            f"Available tables: {', '.join(_list_available_tables(base_path))}"
        )
    
    # Build file paths
    source_ddl = os.path.join(table_folder, "01_create_source_table.sql")
    source_query_file = os.path.join(table_folder, "03_extract_source.sql")
    transform_file = os.path.join(table_folder, "04_transform.py")
    
    # Select target query file based on target_mode
    if target_mode.lower() == "snowflake":
        target_ddl = os.path.join(table_folder, "02_create_target_sf.sql")
        target_query_file = os.path.join(table_folder, "05_extract_target_sf.sql")
    else:
        target_ddl = os.path.join(table_folder, "02_create_target_ms.sql")
        target_query_file = os.path.join(table_folder, "05_extract_target_ms.sql")
    
    # Validate required files exist
    for label, path in [
        ("Source DDL", source_ddl),
        ("Target DDL", target_ddl),
        ("Source query", source_query_file),
        ("Transform", transform_file),
        ("Target query", target_query_file)
    ]:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"{label} file not found: {path}")
    
    # Extract database and table names from DDL files
    source_db, source_tbl = _parse_ddl_table_name(source_ddl)
    target_db, target_tbl = _parse_ddl_table_name(target_ddl)
    
    # If database not found in DDL, try to get from config files
    if source_db is None:
        source_db = _get_database_from_config("source")
    if target_db is None:
        target_db = _get_database_from_config("target", target_mode)
    
    # For Snowflake, target_db should be the actual database name from config
    # The table name from DDL is just the table name (no schema prefix)
    if target_mode.lower() == "snowflake" and target_db is None:
        # Get database from config
        target_db = _get_database_from_config("target", target_mode)
    
    # Extract primary keys from both source and target DDL
    source_primary_keys = _parse_primary_keys(source_ddl)
    target_primary_keys = _parse_primary_keys(target_ddl)
    
    # Default exclude columns (commonly excluded)
    exclude_cols = ["load_ts"]
    
    # Output directory
    output_dir = os.path.join("output", table_name)
    
    return {
        "table_name": table_name,
        "source_ddl": source_ddl,
        "target_ddl": target_ddl,
        "source_query_file": source_query_file,
        "target_query_file": target_query_file,
        "transform_file": transform_file,
        "source_database": source_db,
        "source_table": source_tbl,
        "target_database": target_db,
        "target_table": target_tbl,
        "source_primary_keys": source_primary_keys,
        "target_primary_keys": target_primary_keys,
        "primary_keys": target_primary_keys,  # For backward compatibility (comparison uses target PKs)
        "exclude_cols": exclude_cols,
        "output_dir": output_dir,
    }


def list_available_tables(target_mode: str = "mysql") -> List[str]:
    """
    List all available table names in etl_output_mysql folder.
    
    Parameters
    ----------
    target_mode : str
        "mysql" or "snowflake" (both use same folder structure)
    
    Returns
    -------
    list of str
        Available table names
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    base_path = os.path.join(project_root, "input", "etl_output_spark")
    return _list_available_tables(base_path)


def _list_available_tables(base_path: str) -> List[str]:
    """List subdirectories in base_path."""
    if not os.path.isdir(base_path):
        return []
    return [
        d for d in os.listdir(base_path)
        if os.path.isdir(os.path.join(base_path, d)) and not d.startswith(".")
    ]


def _parse_ddl_table_name(ddl_file: str) -> tuple:
    """
    Extract database and table name from DDL file.
    
    Looks for patterns like:
    - CREATE TABLE database.table_name (...)
    - CREATE TABLE IF NOT EXISTS table_name (...)
    - -- Source table : table_name
    - -- Target table : table_name
    
    Returns (database, table_name) or (None, table_name)
    """
    with open(ddl_file, "r", encoding="utf-8") as fh:
        content = fh.read()
    
    # Try to find database.table in CREATE TABLE statement
    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)",
        content,
        re.IGNORECASE
    )
    if match:
        return match.group(1), match.group(2)
    
    # Try to find table name in CREATE TABLE statement (no database)
    match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_]+)",
        content,
        re.IGNORECASE
    )
    if match:
        table_name = match.group(1)
        # Try to find database in comments
        db_match = re.search(r"--\s*(?:Source|Target)\s+table\s*:\s*([a-zA-Z0-9_]+)", content)
        if db_match:
            return None, db_match.group(1)
        return None, table_name
    
    raise ValueError(f"Could not parse table name from DDL file: {ddl_file}")


def _parse_primary_keys(ddl_file: str) -> List[str]:
    """
    Extract primary key column names from DDL file.
    
    Looks for:
    - PRIMARY KEY (col1, col2, ...)
    - col_name INT NOT NULL,  -- PK
    """
    with open(ddl_file, "r", encoding="utf-8") as fh:
        content = fh.read()
    
    # Try to find PRIMARY KEY constraint
    match = re.search(
        r"PRIMARY\s+KEY\s*\(([^)]+)\)",
        content,
        re.IGNORECASE
    )
    if match:
        pk_str = match.group(1)
        # Split by comma and clean up
        pks = [col.strip().strip("`\"") for col in pk_str.split(",")]
        return pks
    
    # Try to find columns marked with -- PK comment
    pks = []
    for line in content.split("\n"):
        if "-- PK" in line or "--PK" in line:
            # Extract column name from line
            match = re.match(r"\s*([a-zA-Z0-9_]+)\s+", line)
            if match:
                pks.append(match.group(1))
    
    if pks:
        return pks
    
    raise ValueError(f"Could not parse primary keys from DDL file: {ddl_file}")


def _get_database_from_config(db_type: str, target_mode: str = "mysql") -> Optional[str]:
    """
    Read database name from config YAML files.
    
    Parameters
    ----------
    db_type : str
        "source" or "target"
    target_mode : str
        "mysql" or "snowflake" (only used for target)
    
    Returns
    -------
    str or None
        Database name from config, or None if not found
    """
    try:
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        if db_type == "source":
            config_file = os.path.join(project_root, "config", "source_config.yaml")
            if os.path.isfile(config_file):
                with open(config_file, "r", encoding="utf-8") as fh:
                    config = yaml.safe_load(fh)
                    return config.get("database")
        
        elif db_type == "target":
            config_file = os.path.join(project_root, "config", "target_config.yaml")
            if os.path.isfile(config_file):
                with open(config_file, "r", encoding="utf-8") as fh:
                    config = yaml.safe_load(fh)
                    if target_mode == "snowflake" and "snowflake" in config:
                        return config["snowflake"].get("database")
                    else:
                        return config.get("database")
    
    except Exception:
        pass
    
    return None



def build_filter_for_query(
    query_type: str,
    config: dict,
    pk_filter_mode: str,
    pk_range: dict,
    pk_set: set,
    date_mode: str,
    date_from: str,
    date_from_col: str,
    date_to: str,
    date_to_col: str,
) -> dict:
    """
    Build WHERE clause filter using correct PK column for source or target query.
    
    Parameters
    ----------
    query_type : str
        "source" or "target"
    config : dict
        Config dict from get_table_config() containing source_primary_keys and target_primary_keys
    pk_filter_mode : str
        "full" | "pk_range" | "pk_set"
    pk_range : dict
        {"lower": value, "upper": value}
    pk_set : set
        Set of PK values
    date_mode : str
        "full" | "range"
    date_from : str
        Lower date bound
    date_from_col : str
        Column for lower date bound
    date_to : str
        Upper date bound
    date_to_col : str
        Column for upper date bound
    
    Returns
    -------
    dict
        {"where_clause": str, "pk_mode": str, "date_mode": str, "description": str}
    """
    from utils.query_filter import build_where_clause, get_columns_from_ddl
    
    # Select correct PK based on query type
    if query_type == "source":
        pk_col = config["source_primary_keys"][0] if config.get("source_primary_keys") else None
        ddl_file = config["source_ddl"]
    else:  # target
        pk_col = config["target_primary_keys"][0] if config.get("target_primary_keys") else None
        ddl_file = config["target_ddl"]
    
    # Get available columns from DDL for validation
    available_cols = get_columns_from_ddl(ddl_file) if ddl_file else []
    
    # Build WHERE clause
    where_clause = build_where_clause(
        pk_filter_mode=pk_filter_mode,
        pk_col=pk_col,
        pk_range=pk_range,
        pk_set=pk_set if pk_filter_mode == "pk_set" else None,
        date_mode=date_mode,
        date_from=date_from if date_mode == "range" else None,
        date_from_col=date_from_col if date_mode == "range" else None,
        date_to=date_to if date_mode == "range" else None,
        date_to_col=date_to_col if date_mode == "range" else None,
        available_cols=available_cols,
    )
    
    # Build description
    parts = []
    if pk_filter_mode != "full":
        parts.append(f"PK={pk_filter_mode}")
    if date_mode != "full":
        parts.append(f"DATE={date_mode}")
    description = ", ".join(parts) if parts else "full load (no filters)"
    
    return {
        "where_clause": where_clause,
        "pk_mode": pk_filter_mode,
        "date_mode": date_mode,
        "description": description,
    }
