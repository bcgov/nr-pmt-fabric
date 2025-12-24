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

# Copyright (c) 2025 
# Province of British Columbia – Natural Resource Information & Digital Services, CSBC
#
# ---------------------------------------------------------------------------
# ETL mapping -water- Authorization id (Fabric notebook version)
# ---------------------------------------------------------------------------
# Transform permit source data (Fabric table) into **ProcessEventSet**
# JSON Lines for the NR-PIES specification.
#
# This version reads from a Fabric table (query_authorizationid_piesi)
# instead of CSV/Parquet files, and writes JSONL to a folder. Push to PEACH
# ---------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping

import argparse  # (not used in Fabric runner but kept for completeness)
import csv
# import gzip  # CHANGED: gzip no longer needed, output is plain JSONL only
import json
import logging
import operator
import os
import re
import sys
import time
import uuid
import secrets
import requests  # CHANGED: for optional API posting

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
# Writer & optional API posting
# ─────────────────────────────────────────────────────────────────────────────
def _open_outfile(out_path: Path):
    """Return a writable handle for plain-text JSONL (no gzip).  # CHANGED"""
    return out_path.open("w", encoding="utf-8")  # CHANGED: always plain text, no gzip


def write_jsonl(
    events: Iterable[Dict[str, Any]],
    out_dir: Path,
    outfile: str = "events.jsonl",
) -> Path:  # CHANGED: now returns Path
    """Write *events* to `out_dir/outfile` as plain `.jsonl` (no gzip).  # CHANGED"""
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / outfile
    with _open_outfile(out_path) as fh:
        for event in events:
            # record_id is already coerced to int (if possible) in _build_pes  # CHANGED
            fh.write(_dumps(event) + "\n")

    logger.info("Wrote %s", out_path)
    return out_path  # CHANGED


def post_jsonl_to_api(out_path: Path, api_url: str) -> None:  # CHANGED
    """
    Optionally POST each JSON line from the output file to an API endpoint,
    one record per request.  # CHANGED
    """
    logger.info("Posting JSONL records from %s to API: %s", out_path, api_url)
    total = 0
    with out_path.open("rt", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                logger.warning(
                    "Skipping invalid JSON at line %d in %s: %s",
                    line_no,
                    out_path,
                    e,
                )
                continue

            try:
                resp = requests.post(api_url, json=record, timeout=30)
                total += 1
                logger.info(
                    "Posted record #%d (line %d) - status %s",
                    total,
                    line_no,
                    resp.status_code,
                )
            except Exception as e:
                logger.error(
                    "Failed to POST record at line %d in %s: %s",
                    line_no,
                    out_path,
                    e,
                )
    logger.info("Finished posting %d records from %s", total, out_path)


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
    """
    Convert datetime or timestamp-like string to 'YYYY-MM-DD' (or None).

    - Safely handles Spark-style timestamps: '2020-01-01T00:00:00.000Z'
    - Ignores malformed values
    - Ignores historical dates with year < 1900 (NR-PIES doesn't want those)

    Used by date-mode fallbacks in _build_event and other modules.
    """
    if not value:
        return None

    v = str(value)[:10]  # keep only 'YYYY-MM-DD'

    # Must match YYYY-MM-DD
    if not DATE_RE.match(v):
        logger.warning("Invalid date format ignored: %r", value)
        return None

    # Guard: ignore very old dates (e.g. '1000-04-10')
    try:
        year = int(v[:4])
    except ValueError:
        logger.warning("Invalid year in date ignored: %r", value)
        return None

    if year < 1900:
        logger.warning("Historical date (<1900) ignored: %s", v)
        return None

    try:
        return datetime.fromisoformat(v).date().isoformat()
    except Exception:
        logger.warning("Failed to parse date, ignored: %r", value)
        return None


def _to_datetime_str(value: str | None) -> str | None:
    """
    Convert a timestamp-like string to RFC3339 'YYYY-MM-DDTHH:MM:SSZ'
    for **DATETIME mode**.

    DATETIME mode rules:
    - Pure dates 'YYYY-MM-DD' → 'YYYY-MM-DDT00:00:00Z'
    - Timestamps with or without fractional seconds → '...T...:...:...Z'
    - Optional timezone offsets or trailing 'Z' in the source are normalized to 'Z'
    - Year < 1900 is ignored (returns None)
    """
    if not value:
        return None

    v = str(value).strip()
    if not v or v.upper() == "NULL":
        return None

    # Case 1: pure date 'YYYY-MM-DD' -> midnight UTC
    if len(v) == 10 and DATE_RE.match(v):
        try:
            year = int(v[:4])
        except ValueError:
            logger.warning("Invalid year in datetime ignored: %r", value)
            return None

        if year < 1900:
            logger.warning("Historical datetime (<1900) ignored: %s", v)
            return None

        # DATETIME mode: must emit a full timestamp at midnight UTC
        return f"{v}T00:00:00Z"

    # Case 2: has a time part, normalize spaces to 'T'
    if " " in v and "T" not in v:
        v = v.replace(" ", "T", 1)

    # Strip trailing 'Z' for fromisoformat (it doesn't accept 'Z')
    if v.endswith("Z"):
        v_core = v[:-1]
    else:
        v_core = v

    try:
        dt = datetime.fromisoformat(v_core)
    except Exception:
        logger.warning("Invalid datetime format ignored: %r", value)
        return None

    if dt.year < 1900:
        logger.warning("Historical datetime (<1900) ignored: %s", dt.isoformat())
        return None

    # If timezone-aware (e.g. 2025-01-10T12:31:00-08:00), convert to UTC
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

    # Drop microseconds for strict 'YYYY-MM-DDTHH:MM:SSZ'
    dt = dt.replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_int_no_decimal(value: Any) -> Any:  # CHANGED
    """
    Convert numeric-like values such as '100141399.0' → 100141399.

    If conversion fails, return the original value.  # CHANGED
    """
    if value is None:
        return None
    try:
        return int(float(str(value)))
    except Exception:
        return value


# ─────────────────────────────────────────────────────────────────────────────
# Event builders
# ─────────────────────────────────────────────────────────────────────────────
def _build_event(
    row: Mapping[str, Any],
    rule: Mapping[str, Any],
    lifecycle_map: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    Create one process_event.

    Primary mode:
      - DATETIME: start_datetime / end_datetime (RFC3339)

    Fallback:
      - If BOTH timestamps are present and end_datetime < start_datetime,
        consider the timestamps invalid and fall back to DATE mode:
        start_date / end_date (YYYY-MM-DD), derived from the same raw values.
    """
    status_key = rule["key"]
    code_set, stat_info = _resolve_process_info(
        status_key,
        rule.get("code_set"),
        lifecycle_map,
    )

    # Raw source values (usually datetime/timestamp columns)
    start_raw = _safe_get(row, rule["start"])
    end_raw = _safe_get(row, rule["end"])

    # DATETIME candidates (RFC3339, e.g. 2022-02-25T13:02:49Z)
    start_dt = _to_datetime_str(start_raw)
    end_dt = _to_datetime_str(end_raw)

    use_datetime = True

    # If both timestamps exist, check if end < start
    if start_dt and end_dt:
        try:
            # Convert RFC3339 string back to timezone-aware datetime for comparison
            start_obj = datetime.fromisoformat(start_dt.replace("Z", "+00:00"))
            end_obj = datetime.fromisoformat(end_dt.replace("Z", "+00:00"))
            if end_obj < start_obj:
                # Invalid interval → fall back to DATE mode
                logger.warning(
                    (
                        "Event %s has end_datetime < start_datetime "
                        "(start=%s, end=%s). Falling back to DATE mode "
                        "(start_date/end_date only)."
                    ),
                    status_key,
                    start_dt,
                    end_dt,
                )
                use_datetime = False
        except Exception as exc:
            # If comparison fails for any reason, keep datetime mode
            logger.debug(
                "Failed to compare datetimes for %s (start=%r, end=%r): %s",
                status_key,
                start_dt,
                end_dt,
                exc,
            )

    event_blk: Dict[str, Any] = {}

    if use_datetime:
        # Normal DATETIME mode
        if start_dt:
            event_blk["start_datetime"] = start_dt
        if end_dt:
            event_blk["end_datetime"] = end_dt
    else:
        # Fallback: DATE mode (no timestamps)
        start_date = _to_date_str(start_raw)
        end_date = _to_date_str(end_raw)
        if start_date:
            event_blk["start_date"] = start_date
        if end_date:
            event_blk["end_date"] = end_date

    if not event_blk:
        logger.warning(
            "Event %s has no valid start/end (raw start=%r, end=%r, datetime_mode=%s)",
            status_key,
            start_raw,
            end_raw,
            use_datetime,
        )

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
        if (stat := stat_info.get("STATUS")) is not None:
            proc_blk["status"] = stat
        if (scode := stat_info.get("status_code")) is not None:
            proc_blk["status_code"] = scode
        if (desc := stat_info.get("status_description")) is not None:
            proc_blk["status_description"] = desc

    return {"event": event_blk, "process": proc_blk}


def _build_pes(row: Mapping[str, Any], events: List[Mapping]) -> Dict[str, Any]:
    """Create a *ProcessEventSet* now called Record wrapper for *events*."""
    raw_record_id = row.get("PIESID")
    record_id_value = _to_int_no_decimal(raw_record_id)
    if record_id_value is None:
        record_id_value = "Error record_id"

    return {
        "transaction_id": str(uuid7()),
        "version": "0.1.0",
        "kind": "Record",
        "system_id": row.get("system_id", "ITSM-5314"),
        "record_id": record_id_value,
        "record_kind": "Permit",
        "on_hold_event_set": [],
        "process_event_set": events,
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
    post_to_api: bool = False,  # CHANGED
    api_url: str | None = None,  # CHANGED
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

    out_path = write_jsonl(_row_gen(), out_dir, outfile)  # CHANGED

    if post_to_api and api_url:  # CHANGED
        post_jsonl_to_api(out_path, api_url)  # CHANGED

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
    post_to_api: bool = False,  # CHANGED
    api_url: str | None = None,  # CHANGED
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
        post_to_api=post_to_api,  # CHANGED
        api_url=api_url,          # CHANGED
    )


# ─────────────────────────────────────────────────────────────────────────────
# Fabric Notebook: configure and run
# ─────────────────────────────────────────────────────────────────────────────

# ⚙️ Spark config: allow ancient dates/timestamps in Parquet without failing
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

# Adjust these paths/names for your Fabric Lakehouse
TABLE_NAME = "testETL_lakehouse.query_authorizationid_piesid"

# Point to the Files folder in your Lakehouse
RULES_PATH = "/lakehouse/default/Files/rules.json"
LIFECYCLE_PATH = "/lakehouse/default/Files/lifecycle_map.json"
# Write output back under Files/pies_output
OUTPUT_DIR = "/lakehouse/default/Files/pies_output"
# OUTPUT_FILENAME = "water_events_authorizationid.jsonl"  # CHANGED: no .gz, plain JSONL
timestamp = datetime.now().strftime("%Y%m%d")
OUTPUT_FILENAME = f"water_events_authorizationid_{timestamp}.jsonl"

API_URL = "https://nr-peach-test-main-api.azurewebsites.net/api/v1/records/"  # CHANGED
POST_TO_API = True  # CHANGED: set to True to enable POST after writing JSONL

# Run the ETL for the table
run_fabric_from_table(
    table_name=TABLE_NAME,
    rules_path=RULES_PATH,
    lifecycle_path=LIFECYCLE_PATH,
    output_dir=OUTPUT_DIR,
    output_filename=OUTPUT_FILENAME,
    verbose=True,
    post_to_api=POST_TO_API,  # CHANGED
    api_url=API_URL,          # CHANGED
)
#Make new file. 1.change TABLE_NAME. 2. output_filename. 3.rules_path. 4. lifecycle_path. 5. system_id" 6.Header comments.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
