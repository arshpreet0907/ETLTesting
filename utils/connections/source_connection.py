"""
connections/source_connection.py
---------------------------------
Purpose : Build and return JDBC options dict for the source MySQL database.
Inputs  : config/source_config.yaml
Outputs : dict of JDBC options ready to pass to spark.read.format("jdbc").options(**opts)
Usage   : from connections.source_connection import get_source_connection
          jdbc_opts = get_source_connection()
"""

import logging

from utils.config_loader import load_config

logger = logging.getLogger(__name__)

_SOURCE_CONFIG_PATH = "config/source_config.yaml"


def get_source_connection() -> dict:
    """
    Read config/source_config.yaml and return a JDBC options dict for the source MySQL server.

    Returns
    -------
    dict
        Keys: url, driver, user, password, fetchsize
        The password is present in the dict (required by Spark JDBC) but is never logged.

    Raises
    ------
    KeyError
        If any required config key is missing from source_config.yaml.
    FileNotFoundError
        If source_config.yaml cannot be found.
    """
    cfg = load_config(_SOURCE_CONFIG_PATH)

    required_keys = ("host", "port", "database", "user", "password")
    missing = [k for k in required_keys if k not in cfg]
    if missing:
        raise KeyError(
            f"source_config.yaml is missing required key(s): {missing}"
        )

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

    # Log the URL with the password masked
    masked_url = jdbc_url  # URL itself contains no credentials
    logger.info("Source JDBC URL: %s (user=%s)", masked_url, user)

    return {
        "url": jdbc_url,
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": user,
        "password": password,
        "fetchsize": str(fetchsize),
    }
