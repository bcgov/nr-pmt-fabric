# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "03759f8c-a2bd-42db-b3bd-c98b5d3ad239",
# META       "default_lakehouse_name": "testETL_lakehouse",
# META       "default_lakehouse_workspace_id": "79dafa8a-b966-4240-80f7-2e9d46baa5c3",
# META       "known_lakehouses": [
# META         {
# META           "id": "03759f8c-a2bd-42db-b3bd-c98b5d3ad239"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
#
# Copyright (c) 2025
# Province of British Columbia – Natural Resource Information & Digital Services
#
# ---------------------------------------------------------------------------
# permit_etl_fabric.py  (Fabric notebook version)
# ---------------------------------------------------------------------------
# Transform permit source data (Fabric table) into **ProcessEventSet**
# JSON Lines for the NR-PIES specification.
#
# This version reads from a Fabric table (e.g. query_trackingnumber_piesid)
# instead of CSV/Parquet files, and writes JSONL to a folder.
# ---------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping

import argparse  # (not used in Fabric runner but kept for completeness)
import csv
import gzip
import json
import logging
import operator
import os
import re
import sys
import time
import uuid
import secrets

# ── uuid7 fallback (NR-PIES-safe) ───────────────────────────
def uuid7() -> uuid.UUID:
    # 1) 48-bit timestamp
    ts_ms = int(time.time() * 1000)
    ts_bytes = ts_ms.to_bytes(6, "big")  # 6 bytes

    # 2) version(4) + rand_a(12)
    rand_a = secrets.randbits(12)
    time_hi_and_version = (0x7 << 12) | rand_a  # version=7
    thv_bytes = time_hi_and_version.to_bytes(2, "big")

    # 3) variant(2) + rand_b(62)
    rand_b = secrets.randbits(62)
    top6 = (rand_b >> 56) & 0x3F  # high 6 bits
    rem56 = rand_b & ((1 << 56) - 1)  # low 56 bits
    variant_byte = top6 | 0x80  # 10xxxxxx → variant=IETF
    rem56_bytes = rem56.to_bytes(7, "big")  # 7 bytes

    # 4) assemble 16-byte UUID
    uuid_bytes = ts_bytes + thv_bytes + bytes([variant_byte]) + rem56_bytes
    return uuid.UUID(bytes=uuid_bytes)


# ------------------------------------------------------------------
# Defaults – you can override these in the Fabric runner section
# ------------------------------------------------------------------
DEFAULT_SOURCE_FILE = "permits.csv"  # unused in Fabric runner
DEFAULT_RULES_FILE = "rules.json"
DEFAULT_LIFECYCLE_FILE = "lifecycle_map.json"
DEFAULT_OUTPUT_DIR = "out"
DEFAULT_OUTPUT_FILENAME = "events.jsonl"

# ─────────────────────────────────────────────────────────────────────────────
# Optional dependencies (fail-soft / import-probe once)
# ─────────────────────────────────────────────────────────────────────────────
try:
    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq

    USE_ARROW = True
except ImportError:  # pragma: no cover
    USE_ARROW = False

try:
    import pandas as pd

    USE_PANDAS = True
except ImportError:  # pragma: no cover
    USE_PANDAS = False

try:
    import orjson as _oj

    def _dumps(obj: Any) -> str:
        """Serialize *obj* with **orjson** then decode to `str`."""
        return _oj.dumps(obj).decode()

except ImportError:  # pragma: no cover

    def _dumps(obj: Any) -> str:  # type: ignore[override]
        """Fallback JSON dump using stdlib `json`."""
        return json.dumps(obj, ensure_ascii=False)


try:
    from pythonjsonlogger import jsonlogger  # type: ignore

    JSON_LOGGER_AVAILABLE = True
except ImportError:  # pragma: no cover
    JSON_LOGGER_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
BYTES_IN_MIB = 1_048_576  # 1024**2


# ─────────────────────────────────────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────────────────────────────────────
class ETLError(RuntimeError):
    """Top-level exception for the ETL engine."""


# ─────────────────────────────────────────────────────────────────────────────
# Configuration dataclass
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Config:
    """Runtime configuration loaded from **ENV** then overridden by CLI flags."""

    small_mb: int = int(os.getenv("ETL_SMALL_MIB", "100"))
    medium_mb: int = int(os.getenv("ETL_MEDIUM_MIB", "200"))
    chunk_rows: int = int(os.getenv("ETL_CHUNK_ROWS", "100000"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_json: bool = bool(int(os.getenv("LOG_JSON", "0")))

    @property
    def small_bytes(self) -> int:
        """Return eager-read threshold in *bytes*."""
        return self.small_mb * BYTES_IN_MIB

    @property
    def medium_bytes(self) -> int:
        """Return chunk/batch threshold in *bytes*."""
        return self.medium_mb * BYTES_IN_MIB


# ─────────────────────────────────────────────────────────────────────────────
# Logging helpers
# ─────────────────────────────────────────────────────────────────────────────
def _init_logging(cfg: Config, verbose: bool) -> None:
    """Configure root logger: plain text or structured JSON."""
    level = logging.DEBUG if verbose else getattr(
        logging,
        cfg.log_level.upper(),
        logging.INFO,
    )

    handler: logging.Handler
    if cfg.log_json and JSON_LOGGER_AVAILABLE:
        handler = logging.StreamHandler(sys.stdout)
        fmt = jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(component)s %(message)s",
        )
        handler.setFormatter(fmt)
    else:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s %(message)s",
        )
        handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)


logger = logging.getLogger("permit_etl")


# ─────────────────────────────────────────────────────────────────────────────
# Rule compilation helpers
# ─────────────────────────────────────────────────────────────────────────────
_OP: Mapping[str, Callable] = {
    "not_null": lambda x: x not in (None, "", "NULL"),
    "null": lambda x: x in (None, "", "NULL"),
    "=": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "in": lambda x, y: x in y,
    "regex": lambda x, rgx: rgx.search(str(x) or "") is not None,
}


def _coerce(value: Any) -> Any:
    """Coerce *YYYY-MM-DD* strings into `datetime` objects for comparisons."""
    if isinstance(value, str) and DATE_RE.match(value):
        try:
            return datetime.fromisoformat(value.replace("Z", ""))
        except ValueError:
            return value
    return value


def _compile_test(test: Dict[str, Any]) -> Callable[[Mapping], bool]:
    """Compile an atomic rule into a predicate function."""
    op_name = test["op"]
    attr = test["attr"]
    op_func = _OP[op_name]
    value = test.get("value")
    other_attr = test.get("other_attr")

    if op_name == "regex" and isinstance(value, str):
        value = re.compile(value)

    def _inner(row: Mapping) -> bool:
        left = _coerce(row.get(attr))
        if op_name in ("null", "not_null"):
            return op_func(left)

        right = _coerce(row.get(other_attr)) if other_attr else value
        if left is None or right is None:
            return False
        try:
            return op_func(left, right)
        except (TypeError, ValueError) as exc:
            logger.debug("Rule op %s failed attr %s: %s", op_name, attr, exc)
            return False

    return _inner


def _compile_logic(node: Dict[str, Any]) -> Callable[[Mapping], bool]:
    """Recursively compile *and/or* logic into a single predicate."""
    if "and" in node:
        parts = [
            _compile_logic(n) if {"and", "or"} & n.keys() else _compile_test(n)
            for n in node["and"]
        ]
        return lambda r: all(fn(r) for fn in parts)
    if "or" in node:
        parts = [
            _compile_logic(n) if {"and", "or"} & n.keys() else _compile_test(n)
            for n in node["or"]
        ]
        return lambda r: any(fn(r) for fn in parts)
    raise ETLError("Rule node missing 'and' or 'or' keys.")


# ─────────────────────────────────────────────────────────────────────────────
# Loaders
# ─────────────────────────────────────────────────────────────────────────────
def load_rules(path: Path, *, system: str | None = None) -> List[Dict]:
    """Load and compile rule definitions from rules.json."""
    logger.info("Loading rules from %s", path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    compiled: List[Dict[str, Any]] = []
    for key, definition in raw.items():
        if (
            system
            and definition.get("source")
            and definition["source"].lower() != system.lower()
        ):
            continue
        start_attr = (
            (definition.get("start_date") or {}).get("attr")
            or definition.get("start_attr")
        )
        end_attr = (
            (definition.get("end_date") or {}).get("attr")
            or definition.get("end_attr")
        )
        compiled.append(
            {
                "key": key,
                "match": _compile_logic(definition["logic"]),
                "start": start_attr,
                "end": end_attr,
                "code_set": definition.get("code_set") or definition.get("class_path"),
            },
        )
    logger.info("Compiled %d rule entries", len(compiled))
    return compiled


def load_lifecycle(path: str | None) -> Dict[str, List[str]]:
    """Load lifecycle map if *path* is provided; otherwise return `{}`."""
    if not path:
        return {}
    lifecycle = json.loads(Path(path).read_text(encoding="utf-8"))
    logger.info("Loaded lifecycle map (%d items)", len(lifecycle))
    return lifecycle


# ─────────────────────────────────────────────────────────────────────────────
# I/O helpers for CSV/Parquet (kept for compatibility, unused in Fabric table mode)
# ─────────────────────────────────────────────────────────────────────────────
def _file_size(path: Path) -> int:
    return path.stat().st_size


def _iter_csv(path: Path, cfg: Config) -> Iterator[Dict[str, Any]]:
    size = _file_size(path)
    log_ctx = {"component": "csv_reader", "bytes": size}

    if USE_ARROW:
        import pyarrow.csv as pacsvmod

        parse_opts = pacsvmod.ParseOptions(newlines_in_values=True)
        if size < cfg.medium_bytes:
            logger.debug("pyarrow eager", extra=log_ctx)
            table = pacsvmod.read_csv(str(path), parse_options=parse_opts)
            for row in table.to_pylist():
                yield row
        else:
            logger.debug("pyarrow batches", extra=log_ctx)
            for batch in pacsvmod.read_csv(str(path), parse_options=parse_opts).to_batches():
                data = batch.to_pydict()
                for i in range(len(batch)):
                    yield {k: v[i] for k, v in data.items()}
        return

    if USE_PANDAS:
        logger.debug("pandas", extra=log_ctx)
        reader: Iterable[Any]
        if size < cfg.small_bytes:
            reader = [pd.read_csv(path, dtype=str)]
        else:
            reader = pd.read_csv(path, dtype=str, chunksize=cfg.chunk_rows)
        for chunk in reader:
            for row in chunk.to_dict(orient="records"):
                yield row
        return

    logger.debug("builtin csv", extra=log_ctx)
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            yield row


def _iter_parquet(path: Path) -> Iterator[Dict[str, Any]]:
    if not USE_ARROW:
        raise ETLError("Parquet support requires pyarrow; please install it.")
    logger.debug("parquet batches", extra={"component": "parquet_reader"})
    for batch in pq.ParquetFile(str(path)).iter_batches():
        data = batch.to_pydict()
        for i in range(len(batch)):
            yield {k: v[i] for k, v in data.items()}


# ─────────────────────────────────────────────────────────────────────────────
# Writer
# ─────────────────────────────────────────────────────────────────────────────
def _open_outfile(out_path: Path):
    """Return a writable handle; gzip transparently if `.gz` suffix."""
    if out_path.suffix == ".gz":
        return gzip.open(out_path, "wt", encoding="utf-8")  # type: ignore[arg-type]
    return out_path.open("w", encoding="utf-8")


def write_jsonl(
    events: Iterable[Dict[str, Any]],
    out_dir: Path,
    outfile: str = "events.jsonl",
) -> None:
    """Write *events* to `out_dir/outfile` (optionally `.gz`)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / outfile
    with _open_outfile(out_path) as fh:
        for event in events:
            fh.write(_dumps(event) + "\n")
    logger.info("Wrote %s", out_path)


# ─────────────────────────────────────────────────────────────────────────
# Code-set resolver & process-info resolver
# ─────────────────────────────────────────────────────────────────────────
def _resolve_process_info(
    status_key: str,
    rule_code_set: Any,
    lifecycle_map: Mapping[str, Any],
) -> tuple[list[str], dict[str, str]]:
    """
    Return ``(code_set_list, status_dict)`` with NO hard-coded level names.
    """
    if rule_code_set is not None:
        if isinstance(rule_code_set, list):
            return [str(x).strip() for x in rule_code_set if x], {}
        if isinstance(rule_code_set, dict):
            return [str(v).strip() for v in rule_code_set.values() if v], {}
        return [seg.strip() for seg in str(rule_code_set).split("/") if seg], {}

    entry = lifecycle_map.get(status_key)
    if entry is None:
        return ["UNKNOWN"], {}

    if isinstance(entry, dict):
        cs_dict = entry.get("code_set", {})
        code_set = [str(v).strip() for v in cs_dict.values() if v] or ["UNKNOWN"]
        return code_set, entry.get("status", {})

    if isinstance(entry, list):
        return [str(x).strip() for x in entry if x], {}

    if isinstance(entry, str):
        return [entry.strip()], {}

    return ["UNKNOWN"], {}


def _safe_get(row: Mapping, col: str | None) -> str | None:
    """Return cell value as *str* or None (if col missing / empty)."""
    if not col:
        return None
    val = row.get(col)
    if val in (None, "", "NULL"):
        return None
    return str(val)


def _to_date_str(value: str | None) -> str | None:
    """Convert datetime or timestamp string to 'YYYY-MM-DD' (or None)."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value[:10]).date().isoformat()
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Event builders
# ─────────────────────────────────────────────────────────────────────────────
def _build_event(
    row: Mapping[str, Any],
    rule: Mapping[str, Any],
    lifecycle_map: Mapping[str, Any],
) -> Dict[str, Any]:
    """Create one process_event compliant with the 2024-12 schema."""
    status_key = rule["key"]
    code_set, stat_info = _resolve_process_info(
        status_key,
        rule.get("code_set"),
        lifecycle_map,
    )

    start_val = _to_date_str(_safe_get(row, rule["start"]))
    end_val = _to_date_str(_safe_get(row, rule["end"]))
    event_blk: Dict[str, Any] = {}
    if start_val:
        event_blk["start_date"] = start_val
    if end_val:
        event_blk["end_date"] = end_val

    proc_blk: Dict[str, Any] = {
        "code": code_set[-1],
        "code_display": code_set[-1].replace("_", " ").title(),
        "code_set": code_set,
        "code_system": (
            "https://bcgov.github.io/nr-pies/docs/spec"
            "/code_system/application_process"
        ),
    }
    if stat_info:
        if stat := stat_info.get("STATUS"):
            proc_blk["status"] = stat
        if scode := stat_info.get("status_code"):
            proc_blk["status_code"] = scode
        if desc := stat_info.get("status_description"):
            proc_blk["status_description"] = desc

    return {"event": event_blk, "process": proc_blk}


def _build_pes(row: Mapping[str, Any], events: List[Mapping]) -> Dict[str, Any]:
    """Create a *ProcessEventSet* wrapper for *events*."""
    return {
        "transaction_id": str(uuid7()),
        "version": "0.1.0",
        "kind": "ProcessEventSet",
        "system_id": row.get("system_id", "ITSM-6117"),
        "record_id": str(
            row.get("AUTHORIZATION_ID") or row.get("PROJECT_ID") or "Error record_id"
        ),
        "record_kind": "Permit",
        "process_event": events,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Core engine (Fabric table variant)
# ─────────────────────────────────────────────────────────────────────────────
def smart_engine_from_rows(
    rows: Iterable[Dict[str, Any]],
    rules: List[Mapping],
    lifecycle_map: Mapping[str, List[str]],
    out_dir: Path,
    outfile: str,
    cfg: Config,
) -> None:
    """Run the ETL pipeline using an in-memory / iterator source."""
    start = time.perf_counter()
    row_count = 0
    event_count = 0

    def _row_gen() -> Iterator[Dict[str, Any]]:
        nonlocal row_count, event_count
        for row in rows:
            row_count += 1
            evts = [
                _build_event(row, rule, lifecycle_map)
                for rule in rules
                if rule["match"](row)
            ]
            if evts:
                event_count += len(evts)
                yield _build_pes(row, evts)

    write_jsonl(_row_gen(), out_dir, outfile)

    dur = time.perf_counter() - start
    logger.info(
        "Rows: %d  Events: %d  Elapsed: %.2fs",
        row_count,
        event_count,
        dur,
    )


def run_fabric_from_table(
    table_name: str,
    rules_path: str,
    lifecycle_path: str | None,
    output_dir: str,
    output_filename: str = "events.jsonl",
    verbose: bool = True,
) -> None:
    """Entry point for Fabric: read from a Spark table and emit JSONL."""
    cfg = Config()
    _init_logging(cfg, verbose)

    rules = load_rules(Path(rules_path), system=None)
    lifecycle_map = load_lifecycle(lifecycle_path)

    logger.info("Reading Fabric table: %s", table_name)
    df = spark.sql(f"SELECT * FROM {table_name}")  # Fabric Spark DataFrame

    # Convert rows to Python dicts lazily
    rows = (row.asDict(recursive=True) for row in df.toLocalIterator())

    out_dir_path = Path(output_dir)
    smart_engine_from_rows(
        rows=rows,
        rules=rules,
        lifecycle_map=lifecycle_map,
        out_dir=out_dir_path,
        outfile=output_filename,
        cfg=cfg,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fabric Notebook: configure and run
# ─────────────────────────────────────────────────────────────────────────────

# Adjust these paths/names for your Fabric Lakehouse
TABLE_NAME = "query_trackingnumber_piesid"
# Point to the Files folder in your Lakehouse
RULES_PATH = "/lakehouse/default/Files/rules.json"
LIFECYCLE_PATH = "/lakehouse/default/Files/lifecycle_map.json"
# Write output back under Files/pies_output
OUTPUT_DIR = "/lakehouse/default/Files/pies_output"
OUTPUT_FILENAME = "events_trackingnumber.jsonl.gz"

# Run the ETL for the table
run_fabric_from_table(
    table_name=TABLE_NAME,
    rules_path=RULES_PATH,
    lifecycle_path=LIFECYCLE_PATH,
    output_dir=OUTPUT_DIR,
    output_filename=OUTPUT_FILENAME,
    verbose=True,
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
