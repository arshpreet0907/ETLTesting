"""
connections/target_connection.py
----------------------------------
Purpose : Build and return JDBC options dict for the target database (MySQL or Snowflake).
Inputs  : config/target_config.yaml
Outputs : dict of JDBC options ready to pass to spark.read.format("jdbc").options(**opts)
Usage   : from connections.target_connection import get_target_connection
          jdbc_opts = get_target_connection(mode="snowflake")
"""

import logging

from typing import Literal

from utils.config_loader import load_config

logger = logging.getLogger(__name__)

_TARGET_CONFIG_PATH = "config/target_config.yaml"


def get_target_connection(mode: Literal['mysql', 'snowflake'] = "mysql") -> dict:
    """
    Read config/target_config.yaml and return a JDBC options dict for the target DB.

    Parameters
    ----------
    mode : str
        "mysql"     — standard MySQL JDBC connection to the target MySQL server.
        "snowflake" — Snowflake JDBC connection; uses the [snowflake] sub-section of the config.

    Returns
    -------
    dict
        JDBC options dict.  Password is present but never logged.

    Raises
    ------
    ValueError
        If `mode` is not "mysql" or "snowflake", or if the config `type` field is
        inconsistent with the requested mode.
    KeyError
        If any required config key is missing.
    FileNotFoundError
        If target_config.yaml cannot be found.
    """
    if mode not in ("mysql", "snowflake"):
        raise ValueError(f"mode must be 'mysql' or 'snowflake', got: {mode!r}")

    cfg = load_config(_TARGET_CONFIG_PATH)

    if mode == "mysql":
        return _build_mysql_opts(cfg)
    else:
        return _build_snowflake_opts(cfg)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _build_mysql_opts(cfg: dict) -> dict:
    """Construct JDBC options for a MySQL target."""
    required = ("host", "port", "database", "user", "password")
    missing = [k for k in required if k not in cfg]
    if missing:
        raise KeyError(f"target_config.yaml missing MySQL key(s): {missing}")

    host: str = cfg["host"]
    port: int = int(cfg["port"])
    database: str = cfg["database"]
    user: str = cfg["user"]
    password: str = cfg["password"]
    fetchsize: int = int(cfg.get("fetchsize", 10_000))

    jdbc_url = (
        f"jdbc:mysql://{host}:{port}/{database}"
        "?useSSL=false&allowPublicKeyRetrieval=true"
    )

    logger.info("Target MySQL JDBC URL: %s (user=%s)", jdbc_url, user)

    return {
        "url": jdbc_url,
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": user,
        "password": password,
        "fetchsize": str(fetchsize),
    }


def _build_snowflake_opts(cfg: dict) -> dict:
    """Construct JDBC options for a Snowflake target."""
    sf = cfg.get("snowflake")
    if not sf:
        raise KeyError(
            "target_config.yaml must contain a 'snowflake' section when mode='snowflake'."
        )

    required = ("account", "warehouse", "database", "schema", "role", "user", "password")
    missing = [k for k in required if k not in sf]
    if missing:
        raise KeyError(f"target_config.yaml [snowflake] missing key(s): {missing}")

    account: str = sf["account"]
    database: str = sf["database"]
    schema: str = sf["schema"]
    jdbc_url = f"jdbc:snowflake://{account}.snowflakecomputing.com/?db={database}&schema={schema}"

    logger.info(
        "Target Snowflake JDBC URL: %s (user=%s, warehouse=%s, db=%s, schema=%s)",
        jdbc_url,
        sf["user"],
        sf["warehouse"],
        database,
        schema,
    )

    return {
        "url": jdbc_url,
        "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
        "user": sf["user"],
        "password": sf["password"],
        "sfWarehouse": sf["warehouse"],
        "sfDatabase": database,
        "sfSchema": schema,
        "sfRole": sf["role"],
    }
