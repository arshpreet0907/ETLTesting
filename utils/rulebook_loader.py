"""
utils/rulebook_loader.py
-------------------------
Purpose : Load and validate a JSON rulebook file produced by excel_parser.py.
          Provides a single entry-point so all pipeline scripts that need the
          rulebook share the same validation logic.

Usage   :
    from utils.rulebook_loader import load_rulebook
    rulebook = load_rulebook("rulebook/orders_rulebook.json")
    table_name = rulebook["meta"]["table_name"]
    columns    = rulebook["columns"]
"""

import json
import os
from typing import Any, Dict


class RulebookValidationError(Exception):
    """Raised when a rulebook JSON file is missing required keys or has invalid structure."""


# Top-level keys every valid rulebook must contain
_REQUIRED_KEYS = {"meta", "source", "target", "primary_key", "columns"}

# Keys required inside "meta"
_REQUIRED_META_KEYS = {"table_name", "generated_at", "source_excel"}

# Keys required inside each column entry
_REQUIRED_COLUMN_KEYS = {
    "source_col", "target_col", "source_dtype",
    "target_dtype", "transform", "nullable",
}


def load_rulebook(path: str) -> Dict[str, Any]:
    """
    Read a JSON rulebook file, validate its structure, and return the dict.

    Parameters
    ----------
    path : str
        Path to the rulebook JSON file, e.g. ``"rulebook/orders_rulebook.json"``.

    Returns
    -------
    dict
        Validated rulebook contents.

    Raises
    ------
    FileNotFoundError
        If the rulebook file does not exist.
    json.JSONDecodeError
        If the file cannot be parsed as JSON.
    RulebookValidationError
        If required top-level keys, meta keys, or column entry keys are missing.
    """
    abs_path = os.path.abspath(path)

    if not os.path.isfile(abs_path):
        raise FileNotFoundError(
            f"Rulebook not found: {abs_path}\n"
            f"Run excel_parser.py first to generate the rulebook from the Excel sheet."
        )

    with open(abs_path, "r", encoding="utf-8") as fh:
        rulebook: Dict[str, Any] = json.load(fh)

    _validate_top_level(rulebook, abs_path)
    _validate_meta(rulebook["meta"], abs_path)
    _validate_columns(rulebook["columns"], rulebook["primary_key"], abs_path)

    return rulebook


# --------------------------------------------------------------------------- #
# Private validators                                                           #
# --------------------------------------------------------------------------- #

def _validate_top_level(rulebook: dict, path: str) -> None:
    missing = _REQUIRED_KEYS - set(rulebook.keys())
    if missing:
        raise RulebookValidationError(
            f"Rulebook at {path} is missing required top-level key(s): "
            f"{sorted(missing)}"
        )

    if not isinstance(rulebook["columns"], list) or not rulebook["columns"]:
        raise RulebookValidationError(
            f"Rulebook at {path}: 'columns' must be a non-empty list."
        )

    if not isinstance(rulebook["primary_key"], list) or not rulebook["primary_key"]:
        raise RulebookValidationError(
            f"Rulebook at {path}: 'primary_key' must be a non-empty list of column name(s)."
        )


def _validate_meta(meta: dict, path: str) -> None:
    missing = _REQUIRED_META_KEYS - set(meta.keys())
    if missing:
        raise RulebookValidationError(
            f"Rulebook at {path}: 'meta' section is missing key(s): {sorted(missing)}"
        )


def _validate_columns(columns: list, primary_key: list, path: str) -> None:
    source_cols_seen = set()
    target_cols_seen = set()

    for idx, col in enumerate(columns):
        missing = _REQUIRED_COLUMN_KEYS - set(col.keys())
        if missing:
            raise RulebookValidationError(
                f"Rulebook at {path}: column entry #{idx + 1} is missing key(s): "
                f"{sorted(missing)}"
            )

        src = col["source_col"]
        tgt = col["target_col"]

        if src in source_cols_seen:
            raise RulebookValidationError(
                f"Rulebook at {path}: duplicate source_col '{src}' at entry #{idx + 1}."
            )
        if tgt in target_cols_seen:
            raise RulebookValidationError(
                f"Rulebook at {path}: duplicate target_col '{tgt}' at entry #{idx + 1}."
            )

        source_cols_seen.add(src)
        target_cols_seen.add(tgt)

    # Verify every PK column appears in the source_col list
    all_source_cols = {c["source_col"] for c in columns}
    missing_pk = [pk for pk in primary_key if pk not in all_source_cols]
    if missing_pk:
        raise RulebookValidationError(
            f"Rulebook at {path}: primary_key column(s) {missing_pk} not found in "
            f"the source_col list. Check excel_parser.py output."
        )
