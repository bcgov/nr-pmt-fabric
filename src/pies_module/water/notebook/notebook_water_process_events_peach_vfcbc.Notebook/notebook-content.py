# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

# Copyright (c) 2025
# Province of British Columbia – Natural Resource Information & Digital Services, CSBC
#
# ---------------------------------------------------------------------------
# ETL mapping - vfcbc tracking number (Fabric notebook version)
# ---------------------------------------------------------------------------
# Transform permit source data (Fabric table) into ProcessEventSet JSON Lines
# for the NR-PIES specification; optionally POST each record to PEACH.
#
# This version reads from a Fabric table (Delta) using explicit ABFS paths,
# and writes JSONL to an explicit ABFS Lakehouse Files folder (CI/CD-safe).
# ---------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Set, Tuple

import json
import logging
import operator
import os
import re
import secrets
import sys
import time
import uuid

import requests  # optional API posting
from collections.abc import Mapping as _Mapping


try:
    import orjson as _oj

    def _dumps(obj: Any) -> str:
        return _oj.dumps(obj).decode()

except ImportError:  # pragma: no cover
    def _dumps(obj: Any) -> str:  # type: ignore[override]
        return json.dumps(obj, ensure_ascii=False)

try:
    from pythonjsonlogger import jsonlogger  # type: ignore
    JSON_LOGGER_AVAILABLE = True
except ImportError:  # pragma: no cover
    JSON_LOGGER_AVAILABLE = False


def _get_notebookutils():
    """
    Fabric notebooks typically provide a global `notebookutils`.
    In non-Fabric contexts, it may not exist.
    """
    try:
        return notebookutils  # type: ignore[name-defined]
    except NameError:
        return None


def load_env_vars_from_variable_library(
    library_name: str = "pmt_env_vars",
    required_keys: tuple[str, ...] = ("fabric_env",),
    *,
    env_fallback: bool = True,
) -> dict[str, str]:
    """
    Load required variables from Fabric Variable Library.

    This version:
      - uses Variable Library when available (Fabric)
      - optionally falls back to OS ENV for local/unit testing
    """
    nb = _get_notebookutils()

    def _get_required(key: str) -> str:
        # 1) Fabric Variable Library
        if nb is not None:
            try:
                v = nb.variableLibrary.get(f"$(/**/{library_name}/{key})")
                if v is not None and str(v).strip() != "":
                    return str(v).strip()
            except Exception:
                pass

        # 2) OS env fallback (note: hyphen keys won't exist in OS env; still fine)
        if env_fallback:
            v2 = os.getenv(key) or os.getenv(key.upper())
            if v2 is not None and str(v2).strip() != "":
                return str(v2).strip()

        raise RuntimeError(
            f"Missing/empty variable '{key}'. "
            f"Tried Variable Library '{library_name}' and OS env (fallback={env_fallback}). "
            f"In Fabric: ensure library exists in THIS workspace and an active value set is selected."
        )

    env = {k: _get_required(k) for k in required_keys}
    if "fabric_env" in env:
        env["fabric_env"] = env["fabric_env"].strip().upper()

    if "fabric_env" in env:
        print(f"[ENV] fabric_env = {env['fabric_env']}")

    if nb is not None:
        try:
            ctx = nb.runtime.context
            print(f"[ENV] Workspace: {ctx.get('currentWorkspaceName')}  Lakehouse: {ctx.get('defaultLakehouseName')}")
        except Exception:
            pass

    return env


# ---------------------------------------------------------------------------
# CI/CD helpers: ABFS base + Fabric FS read/copy
# ---------------------------------------------------------------------------

_GUID_RE = re.compile(r"^[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}$")


def _is_guid(s: str) -> bool:
    return bool(s and _GUID_RE.match(s.strip()))


def build_abfs_base(workspace_ref: str, lakehouse_ref: str) -> str:
    """
    Supports either:
      - GUID style:  abfss://<workspace_guid>@.../<lakehouse_guid>
      - Name style:  abfss://<workspace_name>@.../<lakehouse_name>.Lakehouse
      - If lakehouse_ref already ends with .Lakehouse, keep it.
    """
    w = (workspace_ref or "").strip()
    l = (lakehouse_ref or "").strip()

    if not w or not l:
        raise RuntimeError(f"Invalid workspace/lakehouse values: workspace={workspace_ref!r}, lakehouse={lakehouse_ref!r}")

    # If lakehouse is a name, Fabric OneLake commonly uses "<name>.Lakehouse"
    if not _is_guid(l) and not l.lower().endswith(".lakehouse"):
        l = f"{l}.Lakehouse"

    return f"abfss://{w}@onelake.dfs.fabric.microsoft.com/{l}"


def _fs():
    nb = _get_notebookutils()
    if nb is None or not hasattr(nb, "fs"):
        raise RuntimeError("Fabric notebookutils.fs is not available in this environment.")
    return nb.fs


def read_text_any(path_str: str) -> str:
    """
    Read text from:
      - local mounted path (e.g., /lakehouse/...)
      - ABFS path (abfss://...) via notebookutils.fs
    """
    p = (path_str or "").strip()
    if p.lower().startswith("abfss://"):
        # Try fs.open first; fall back to fs.head if needed
        try:
            fh = _fs().open(p, "r")
            try:
                return fh.read()
            finally:
                fh.close()
        except Exception:
            # head(size) exists in many Fabric/Synapse fs impls
            try:
                return _fs().head(p, 1024 * 1024 * 50)  # up to 50MB
            except Exception as e:
                raise RuntimeError(f"Failed to read ABFS text file: {p}. Error: {e}") from e

    # Local path
    return Path(p).read_text(encoding="utf-8")


def ensure_dir_any(path_str: str) -> None:
    """
    Ensure directory exists for:
      - local path: mkdir
      - ABFS path: notebookutils.fs.mkdirs
    """
    p = (path_str or "").rstrip("/")
    if p.lower().startswith("abfss://"):
        try:
            _fs().mkdirs(p)
        except Exception:
            # some impls use 'mkdir'
            try:
                _fs().mkdir(p)
            except Exception as e:
                raise RuntimeError(f"Failed to create ABFS directory: {p}. Error: {e}") from e
    else:
        Path(p).mkdir(parents=True, exist_ok=True)


def copy_local_to_abfs(local_path: Path, abfs_path: str, *, overwrite: bool = True) -> None:
    """
    Copy a local file (driver filesystem) to ABFS via notebookutils.fs.cp if available.
    Falls back to fs.put for smaller files if cp fails (may be slow/limited for huge files).
    """
    src = f"file:{local_path.as_posix()}"
    dst = abfs_path

    # Ensure parent exists
    parent = dst.rsplit("/", 1)[0]
    ensure_dir_any(parent)

    # Try cp variants
    try:
        _fs().cp(src, dst, overwrite)  # some signatures support overwrite
        return
    except TypeError:
        pass
    except Exception:
        pass

    try:
        _fs().cp(src, dst)  # signature without overwrite
        return
    except Exception:
        pass

    # Fallback: put content (OK for smaller outputs; not ideal for very large files)
    try:
        content = local_path.read_text(encoding="utf-8")
        _fs().put(dst, content, overwrite)
        return
    except Exception as e:
        raise RuntimeError(f"Failed to copy local file to ABFS. local={local_path} dst={dst}. Error: {e}") from e


# ---------------------------------------------------------------------------
# uuid7 fallback (NR-PIES-safe)
# ---------------------------------------------------------------------------
def uuid7() -> uuid.UUID:
    """
    Generate a UUIDv7-like value (time-ordered) without external dependencies.
    """
    ts_ms = int(time.time() * 1000)
    ts_bytes = ts_ms.to_bytes(6, "big")

    rand_a = secrets.randbits(12)
    time_hi_and_version = (0x7 << 12) | rand_a  # version=7
    thv_bytes = time_hi_and_version.to_bytes(2, "big")

    rand_b = secrets.randbits(62)
    top6 = (rand_b >> 56) & 0x3F
    rem56 = rand_b & ((1 << 56) - 1)
    variant_byte = top6 | 0x80  # 10xxxxxx
    rem56_bytes = rem56.to_bytes(7, "big")

    uuid_bytes = ts_bytes + thv_bytes + bytes([variant_byte]) + rem56_bytes
    return uuid.UUID(bytes=uuid_bytes)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
BYTES_IN_MIB = 1_048_576

DEFAULT_CODE_SYSTEM_URL = (
    "https://bcgov.github.io/nr-pies/docs/spec/code_system/application_process"
)

# POST behaviour (timeout + retry)
POST_TIMEOUT_SECONDS = int(os.getenv("PEACH_POST_TIMEOUT", "30"))
POST_MAX_RETRIES = int(os.getenv("PEACH_POST_MAX_RETRIES", "3"))
POST_RETRY_STATUSES = {408, 429, 500, 502, 503, 504}

# Event time mode:
#   - DATETIME: start_datetime/end_datetime RFC3339
#   - DATE:     start_date/end_date YYYY-MM-DD
EVENT_TIME_MODE = os.getenv("EVENT_TIME_MODE", "DATETIME").strip().upper()  # DATETIME|DATE


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class ETLError(RuntimeError):
    """Top-level exception for the ETL engine."""


# ---------------------------------------------------------------------------
# Null/case helpers
# ---------------------------------------------------------------------------
def _is_nullish(v: Any) -> bool:
    """
    Treat None / blank / "null"/"NULL" / "NaT" as null-ish.
    """
    if v is None:
        return True
    try:
        s = str(v).strip()
    except Exception:
        return False
    if s == "":
        return True
    if s.lower() in ("null", "nat"):
        return True
    return False


class CaseInsensitiveRow(_Mapping):
    """
    Mapping wrapper that makes key access case-insensitive for string keys.
    """

    def __init__(self, data: Mapping[str, Any]):
        self._data = dict(data)
        self._lower_map = {str(k).lower(): k for k in self._data.keys()}

    def _resolve(self, key: Any) -> Any:
        if key is None:
            return None
        if key in self._data:
            return key
        lk = str(key).lower()
        return self._lower_map.get(lk)

    def get(self, key: Any, default: Any = None) -> Any:
        rk = self._resolve(key)
        if rk is None:
            return default
        return self._data.get(rk, default)

    def __getitem__(self, key: Any) -> Any:
        rk = self._resolve(key)
        if rk is None:
            raise KeyError(key)
        return self._data[rk]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
@dataclass
class Config:
    small_mb: int = int(os.getenv("ETL_SMALL_MIB", "100"))
    medium_mb: int = int(os.getenv("ETL_MEDIUM_MIB", "200"))
    chunk_rows: int = int(os.getenv("ETL_CHUNK_ROWS", "100000"))

    # logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    log_json: bool = bool(int(os.getenv("LOG_JSON", "0")))

    # diagnostics / safety
    max_rows: int = int(os.getenv("ETL_MAX_ROWS", "0"))  # 0 = no cap
    log_first_n_rows: int = int(os.getenv("ETL_LOG_FIRST_N_ROWS", "0"))  # 0 = off
    log_rule_hit_topn: int = int(os.getenv("ETL_RULE_HIT_TOPN", "15"))
    fail_on_empty_output: bool = bool(int(os.getenv("ETL_FAIL_ON_EMPTY_OUTPUT", "0")))

    @property
    def small_bytes(self) -> int:
        return self.small_mb * BYTES_IN_MIB

    @property
    def medium_bytes(self) -> int:
        return self.medium_mb * BYTES_IN_MIB


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _init_logging(cfg: Config, log_path: Optional[Path] = None) -> None:
    console_level = getattr(logging, cfg.log_level.upper(), logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # capture everything; handlers filter
    root.handlers.clear()  # important in notebooks

    console_handler = logging.StreamHandler(sys.stdout)
    if cfg.log_json and JSON_LOGGER_AVAILABLE:
        fmt = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
        console_handler.setFormatter(fmt)
    else:
        fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s %(message)s")
        console_handler.setFormatter(fmt)
    console_handler.setLevel(console_level)
    root.addHandler(console_handler)

    if log_path is not None:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)s %(message)s")
        file_handler.setFormatter(file_fmt)
        file_handler.setLevel(logging.DEBUG)
        root.addHandler(file_handler)


logger = logging.getLogger("permit_etl")


# ---------------------------------------------------------------------------
# Rule compilation
# ---------------------------------------------------------------------------
_OP: Mapping[str, Callable] = {
    "not_null": lambda x: not _is_nullish(x),
    "null": lambda x: _is_nullish(x),
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
    """
    Coerce YYYY-MM-DD strings into datetime objects for comparisons.
    """
    if isinstance(value, str) and DATE_RE.match(value):
        try:
            return datetime.fromisoformat(value.replace("Z", ""))
        except ValueError:
            return value
    return value


def _compile_test(test: Dict[str, Any]) -> Callable[[Mapping], bool]:
    op_name = test.get("op")
    attr = test.get("attr")

    if not op_name or not attr:
        raise ETLError(f"Malformed rule test node (missing 'op' or 'attr'): {test!r}")

    if op_name not in _OP:
        raise ETLError(f"Unknown operator in rule test node: op={op_name!r} node={test!r}")

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
        if _is_nullish(left) or _is_nullish(right):
            return False
        try:
            return op_func(left, right)
        except (TypeError, ValueError) as exc:
            logger.debug("Rule op %s failed attr %s: %s", op_name, attr, exc)
            return False

    return _inner


def _compile_logic(node: Dict[str, Any]) -> Callable[[Mapping], bool]:
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

    raise ETLError(f"Rule node missing 'and' or 'or' keys: {node!r}")


def _collect_logic_attrs(node: Any, out: Set[str]) -> None:
    if isinstance(node, dict):
        if "attr" in node and node["attr"]:
            out.add(str(node["attr"]))
        if "other_attr" in node and node["other_attr"]:
            out.add(str(node["other_attr"]))
        if "and" in node:
            for n in node["and"]:
                _collect_logic_attrs(n, out)
        if "or" in node:
            for n in node["or"]:
                _collect_logic_attrs(n, out)
    elif isinstance(node, list):
        for n in node:
            _collect_logic_attrs(n, out)


def _compute_required_cols_for_rule(definition: Dict[str, Any]) -> Set[str]:
    cols: Set[str] = set()

    start_attr = (definition.get("start_date") or {}).get("attr") or definition.get("start_attr")
    end_attr = (definition.get("end_date") or {}).get("attr") or definition.get("end_attr")

    if start_attr:
        cols.add(str(start_attr))
    if end_attr:
        cols.add(str(end_attr))

    logic = definition.get("logic")
    if logic is not None:
        _collect_logic_attrs(logic, cols)

    cols.add("PIESID")
    cols.add("system_id")

    return cols


# ---------------------------------------------------------------------------
# Loaders (CI/CD-safe: can read from ABFS)
# ---------------------------------------------------------------------------
def load_rules(path_str: str, *, system: Optional[str] = None) -> List[Dict[str, Any]]:
    logger.info("Loading rules from %s", path_str)
    raw = json.loads(read_text_any(path_str))
    compiled: List[Dict[str, Any]] = []

    for key, definition in raw.items():
        if (
            system
            and definition.get("source")
            and str(definition["source"]).lower() != system.lower()
        ):
            continue

        start_attr = (definition.get("start_date") or {}).get("attr") or definition.get("start_attr")
        end_attr = (definition.get("end_date") or {}).get("attr") or definition.get("end_attr")

        required_cols = _compute_required_cols_for_rule(definition)

        try:
            match_fn = _compile_logic(definition["logic"])
        except Exception as exc:
            raise ETLError(f"Invalid rule '{key}': {exc}") from exc

        compiled.append(
            {
                "key": key,
                "match": match_fn,
                "start": start_attr,
                "end": end_attr,
                "code_set": definition.get("code_set") or definition.get("class_path"),
                "required_cols": required_cols,
            }
        )

    logger.info("Compiled %d rule entries", len(compiled))
    return compiled


def load_lifecycle(path_str: Optional[str]) -> Dict[str, Any]:
    if not path_str:
        return {}
    lifecycle = json.loads(read_text_any(path_str))
    if not isinstance(lifecycle, dict):
        logger.warning("Lifecycle map is not a dict; got %s", type(lifecycle))
        return {}
    logger.info("Loaded lifecycle map (%d items)", len(lifecycle))
    return lifecycle


# ---------------------------------------------------------------------------
# Process-info resolver (unchanged)
# ---------------------------------------------------------------------------
def _resolve_process_info(
    status_key: str,
    rule_code_set: Any,
    lifecycle_map: Mapping[str, Any],
) -> Tuple[List[str], Dict[str, Any]]:
    if rule_code_set is not None:
        if isinstance(rule_code_set, list):
            cs = [str(x).strip() for x in rule_code_set if x]
            return (cs or ["UNKNOWN"]), {}
        if isinstance(rule_code_set, dict):
            cs = [str(v).strip() for v in rule_code_set.values() if v]
            return (cs or ["UNKNOWN"]), {}
        cs = [seg.strip() for seg in str(rule_code_set).split("/") if seg]
        return (cs or ["UNKNOWN"]), {}

    entry = lifecycle_map.get(status_key)
    if entry is None:
        return ["UNKNOWN"], {}

    if isinstance(entry, dict):
        cs_dict = entry.get("code_set") or entry.get("class_path") or {}
        if isinstance(cs_dict, dict):
            code_set = [str(v).strip() for v in cs_dict.values() if v] or ["UNKNOWN"]
        elif isinstance(cs_dict, list):
            code_set = [str(x).strip() for x in cs_dict if x] or ["UNKNOWN"]
        else:
            code_set = [str(cs_dict).strip()] if cs_dict else ["UNKNOWN"]

        status_blk = entry.get("status") or entry.get("STATUS") or entry.get("Status") or {}
        status_blk = status_blk if isinstance(status_blk, dict) else {}

        if not code_set:
            code_set = ["UNKNOWN"]

        return code_set, status_blk

    if isinstance(entry, list):
        code_set = [str(x).strip() for x in entry if x] or ["UNKNOWN"]
        if not code_set:
            code_set = ["UNKNOWN"]
        return code_set, {}

    if isinstance(entry, str):
        code_set = [entry.strip()] if entry.strip() else ["UNKNOWN"]
        if not code_set:
            code_set = ["UNKNOWN"]
        return code_set, {}

    return ["UNKNOWN"], {}


def _safe_get(row: Mapping, col: Optional[str]) -> Optional[str]:
    if not col:
        return None
    val = row.get(col)
    if _is_nullish(val):
        return None
    return str(val)


def _to_date_str(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = str(value).strip()[:10]
    if not DATE_RE.match(v):
        logger.debug("Invalid date format ignored: %r", value)
        return None
    try:
        year = int(v[:4])
        if year < 1900:
            logger.debug("Historical date (<1900) ignored: %s", v)
            return None
        return datetime.fromisoformat(v).date().isoformat()
    except Exception:
        logger.debug("Failed to parse date, ignored: %r", value)
        return None


def _to_datetime_str(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = str(value).strip()
    if _is_nullish(v):
        return None

    if len(v) == 10 and DATE_RE.match(v):
        try:
            year = int(v[:4])
            if year < 1900:
                return None
            return f"{v}T00:00:00Z"
        except Exception:
            return None

    if " " in v and "T" not in v:
        v = v.replace(" ", "T", 1)

    v_core = v[:-1] if v.endswith("Z") else v

    try:
        dt = datetime.fromisoformat(v_core)
    except Exception:
        logger.debug("Invalid datetime format ignored: %r", value)
        return None

    if dt.year < 1900:
        return None

    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

    dt = dt.replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _to_int_no_decimal(value: Any) -> Any:
    if value is None:
        return None
    try:
        return int(float(str(value)))
    except Exception:
        return value


# ---------------------------------------------------------------------------
# Event builders (unchanged)
# ---------------------------------------------------------------------------
def _build_event(
    row: Mapping[str, Any],
    rule: Mapping[str, Any],
    lifecycle_map: Mapping[str, Any],
) -> Dict[str, Any]:
    status_key = rule["key"]
    code_set, stat_info = _resolve_process_info(status_key, rule.get("code_set"), lifecycle_map)

    if not code_set:
        code_set = ["UNKNOWN"]

    start_raw = _safe_get(row, rule.get("start"))
    end_raw = _safe_get(row, rule.get("end"))

    event_blk: Dict[str, Any] = {}

    if EVENT_TIME_MODE == "DATE":
        sd = _to_date_str(start_raw)
        ed = _to_date_str(end_raw)
        if sd:
            event_blk["start_date"] = sd
        if ed:
            event_blk["end_date"] = ed
    else:
        start_dt = _to_datetime_str(start_raw)
        end_dt = _to_datetime_str(end_raw)
        use_datetime = True

        if start_dt and end_dt:
            try:
                start_obj = datetime.fromisoformat(start_dt.replace("Z", "+00:00"))
                end_obj = datetime.fromisoformat(end_dt.replace("Z", "+00:00"))
                if end_obj < start_obj:
                    record_id = row.get("PIESID") or row.get("record_id") or "UNKNOWN"
                    logger.debug(
                        "Record %s: Event '%s' end_datetime < start_datetime "
                        "(start=%s, end=%s). Falling back to DATE mode.",
                        record_id,
                        status_key,
                        start_dt,
                        end_dt,
                    )
                    use_datetime = False
            except Exception as exc:
                record_id = row.get("PIESID") or row.get("record_id") or "UNKNOWN"
                logger.debug(
                    "Record %s: Failed datetime compare for event '%s' (start=%r, end=%r): %s",
                    record_id,
                    status_key,
                    start_dt,
                    end_dt,
                    exc,
                )

        if use_datetime:
            if start_dt:
                event_blk["start_datetime"] = start_dt
            else:
                sd = _to_date_str(start_raw)
                if sd:
                    event_blk["start_date"] = sd

            if end_dt:
                event_blk["end_datetime"] = end_dt
            else:
                ed = _to_date_str(end_raw)
                if ed:
                    event_blk["end_date"] = ed
        else:
            sd = _to_date_str(start_raw)
            ed = _to_date_str(end_raw)
            if sd:
                event_blk["start_date"] = sd
            if ed:
                event_blk["end_date"] = ed

    if not event_blk:
        record_id = row.get("PIESID") or row.get("record_id") or "UNKNOWN"
        logger.warning(
            "Record %s: Event '%s' produced no valid time fields "
            "(raw start=%r, end=%r, mode=%s)",
            record_id,
            status_key,
            start_raw,
            end_raw,
            EVENT_TIME_MODE,
        )

    code_leaf = code_set[-1] if code_set else "UNKNOWN"
    proc_blk: Dict[str, Any] = {
        "code": code_leaf,
        "code_display": code_leaf.replace("_", " ").title(),
        "code_set": code_set or ["UNKNOWN"],
        "code_system": DEFAULT_CODE_SYSTEM_URL,
    }

    if stat_info:
        stat = stat_info.get("STATUS") or stat_info.get("Status") or stat_info.get("status")
        if stat is not None and not _is_nullish(stat):
            proc_blk["status"] = str(stat)

        scode = stat_info.get("status_code") or stat_info.get("Status_code") or stat_info.get("STATUS_CODE")
        if scode is not None and not _is_nullish(scode):
            proc_blk["status_code"] = str(scode)

        desc = (
            stat_info.get("status_description")
            or stat_info.get("Status_description")
            or stat_info.get("STATUS_DESCRIPTION")
        )
        if desc is not None and not _is_nullish(desc):
            proc_blk["status_description"] = str(desc)

    return {"event": event_blk, "process": proc_blk}


def _build_pes(
    row: Mapping[str, Any],
    events: List[Mapping[str, Any]],
    *,
    version: str = "0.1.0",
    default_system_id: str = "ITSM-6117",
    record_kind: str = "Permit",
) -> Dict[str, Any]:
    raw_record_id = row.get("PIESID")
    record_id_value = _to_int_no_decimal(raw_record_id)
    if record_id_value is None:
        record_id_value = "Error record_id"

    system_id_value = row.get("system_id")
    if _is_nullish(system_id_value):
        system_id_value = default_system_id

    return {
        "transaction_id": str(uuid7()),
        "version": version,
        "kind": "Record",
        "system_id": str(system_id_value),
        "record_id": record_id_value,
        "record_kind": record_kind,
        "on_hold_event_set": [],
        "process_event_set": events,
    }


# ---------------------------------------------------------------------------
# Writer & optional API posting
# ---------------------------------------------------------------------------
def write_jsonl(
    records: Iterable[Dict[str, Any]],
    out_dir: Path,
    outfile: str = "events.jsonl",
) -> Tuple[Path, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / outfile

    line_count = 0
    with out_path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(_dumps(rec) + "\n")
            line_count += 1

    logger.info("Wrote %s (lines=%d)", out_path, line_count)
    return out_path, line_count


def post_jsonl_to_api(out_path: Path, api_url: str, *, expected_lines: Optional[int] = None) -> None:
    if expected_lines is not None and expected_lines == 0:
        logger.warning("Skipping POST: output file has 0 lines: %s", out_path)
        return

    logger.info("Starting POST of JSONL records from %s to API: %s", out_path, api_url)

    total_ok = 0
    total_failed = 0
    error_path = out_path.with_name(out_path.stem + "_errors.jsonl")
    error_fh = None

    def _write_error_entry(rec: Dict[str, Any], line_no: int, record_id: Any, error: str, status_code: Optional[int] = None):
        nonlocal error_fh
        if error_fh is None:
            error_fh = error_path.open("a", encoding="utf-8")
            logger.warning("Opening error bucket file: %s", error_path)
        payload = {
            "line_no": line_no,
            "record_id": record_id,
            "status_code": status_code,
            "error": error,
            "record": rec,
        }
        error_fh.write(_dumps(payload) + "\n")

    sess = requests.Session()

    try:
        with out_path.open("rt", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    logger.warning("Skipping invalid JSON at line %d in %s: %s", line_no, out_path, e)
                    continue

                record_id = record.get("record_id", "UNKNOWN")
                attempt = 0
                sent_ok = False

                while attempt < POST_MAX_RETRIES and not sent_ok:
                    attempt += 1
                    try:
                        resp = sess.post(api_url, json=record, timeout=POST_TIMEOUT_SECONDS)
                    except Exception as e:
                        if attempt < POST_MAX_RETRIES:
                            logger.warning(
                                "POST exception record_id=%s (line %d) attempt %d/%d: %s",
                                record_id,
                                line_no,
                                attempt,
                                POST_MAX_RETRIES,
                                e,
                            )
                            time.sleep(min(2 ** (attempt - 1), 10))
                            continue
                        logger.error(
                            "POST give up record_id=%s (line %d) after %d attempts (exception): %s",
                            record_id,
                            line_no,
                            POST_MAX_RETRIES,
                            e,
                        )
                        total_failed += 1
                        _write_error_entry(record, line_no, record_id, error=str(e), status_code=None)
                        break

                    status = resp.status_code

                    if status in POST_RETRY_STATUSES and attempt < POST_MAX_RETRIES:
                        logger.warning(
                            "POST retryable status %s record_id=%s (line %d) attempt %d/%d",
                            status,
                            record_id,
                            line_no,
                            attempt,
                            POST_MAX_RETRIES,
                        )
                        time.sleep(min(2 ** (attempt - 1), 10))
                        continue

                    if 200 <= status < 300:
                        total_ok += 1
                        logger.debug("Posted record_id=%s (line %d) status=%s", record_id, line_no, status)
                        sent_ok = True
                        break

                    total_failed += 1
                    if status in POST_RETRY_STATUSES:
                        logger.error(
                            "POST give up record_id=%s (line %d) after %d attempts, final status=%s",
                            record_id,
                            line_no,
                            POST_MAX_RETRIES,
                            status,
                        )
                    else:
                        logger.error(
                            "POST non-retryable status %s record_id=%s (line %d) -> error bucket",
                            status,
                            record_id,
                            line_no,
                        )
                    _write_error_entry(record, line_no, record_id, error=f"HTTP {status}", status_code=status)
                    break

    finally:
        sess.close()
        if error_fh is not None:
            error_fh.close()

    logger.info("Finished posting records (success=%d, failed=%d)", total_ok, total_failed)
    if total_failed > 0:
        logger.warning("Some records failed and were written to: %s", error_path)


# ---------------------------------------------------------------------------
# Preflight: compare table columns vs rules required columns
# ---------------------------------------------------------------------------
def _preflight_rules_vs_table_columns(
    table_columns: List[str],
    rules: List[Mapping[str, Any]],
) -> None:
    table_cols_lower = {c.lower() for c in table_columns}

    required_all: Set[str] = set()
    for r in rules:
        for c in r.get("required_cols", set()):
            required_all.add(str(c))

    missing = sorted([c for c in required_all if c.lower() not in table_cols_lower])

    if missing:
        logger.warning("Preflight: table is missing %d required columns referenced by rules.", len(missing))
        logger.warning("Missing columns (first 80): %s", missing[:80])
    else:
        logger.info("Preflight: all rule-referenced columns exist in the table schema.")


# ---------------------------------------------------------------------------
# Core engine (unchanged)
# ---------------------------------------------------------------------------
def smart_engine_from_rows(
    rows: Iterable[Mapping[str, Any]],
    rules: List[Mapping[str, Any]],
    lifecycle_map: Mapping[str, Any],
    out_dir: Path,
    outfile: str,
    cfg: Config,
    *,
    default_system_id: str = "ITSM-6117",
    record_kind: str = "Permit",
    version: str = "0.1.0",
    post_to_api: bool = False,
    api_url: Optional[str] = None,
) -> Tuple[Path, int]:
    start = time.perf_counter()
    row_count = 0
    record_count = 0
    event_count = 0

    rule_hits: Dict[str, int] = {r["key"]: 0 for r in rules}

    def _row_gen() -> Iterator[Dict[str, Any]]:
        nonlocal row_count, record_count, event_count

        for row in rows:
            row_count += 1

            if cfg.max_rows and row_count > cfg.max_rows:
                logger.warning("Max rows cap reached (%d). Stopping early.", cfg.max_rows)
                break

            if cfg.log_first_n_rows and row_count <= cfg.log_first_n_rows:
                try:
                    rid = row.get("PIESID")
                except Exception:
                    rid = None
                logger.info("Sample row #%d PIESID=%r keys=%s", row_count, rid, list(row)[:25])

            evts: List[Dict[str, Any]] = []
            for rule in rules:
                try:
                    matched = rule["match"](row)
                except Exception as exc:
                    matched = False
                    rid = row.get("PIESID") or "UNKNOWN"
                    logger.error("Rule eval error rule=%s record=%s: %s", rule["key"], rid, exc)

                if matched:
                    rule_hits[rule["key"]] += 1
                    evts.append(_build_event(row, rule, lifecycle_map))

            if evts:
                event_count += len(evts)
                record_count += 1
                yield _build_pes(
                    row,
                    evts,
                    version=version,
                    default_system_id=default_system_id,
                    record_kind=record_kind,
                )

    out_path, lines = write_jsonl(_row_gen(), out_dir, outfile)

    if cfg.fail_on_empty_output and lines == 0:
        raise ETLError(f"Empty output produced (0 lines): {out_path}")

    if rule_hits:
        topn = sorted(rule_hits.items(), key=lambda kv: kv[1], reverse=True)[: cfg.log_rule_hit_topn]
        logger.info("Rule hit counts (top %d): %s", cfg.log_rule_hit_topn, topn)

    if post_to_api and api_url:
        post_jsonl_to_api(out_path, api_url, expected_lines=lines)

    dur = time.perf_counter() - start
    logger.info(
        "Done. Source rows=%d  Output records=%d  Total events=%d  Elapsed=%.2fs  Output=%s",
        row_count,
        record_count,
        event_count,
        dur,
        out_path,
    )
    return out_path, lines


# ---------------------------------------------------------------------------
# Fabric entry point (CI/CD-safe: no dbo.*, no default lakehouse)
# ---------------------------------------------------------------------------
def run_fabric_from_table(
    table_name: str,
    table_abfs_path: str,
    rules_path: str,
    lifecycle_path: Optional[str],
    output_dir_abfs: str,
    output_filename: str = "events.jsonl",
    *,
    post_to_api: bool = False,
    api_url: Optional[str] = None,
    default_system_id: str = "ITSM-6117",
    record_kind: str = "Permit",
    version: str = "0.1.0",
) -> Tuple[Path, int]:
    """
    Entry point for Fabric:
      - read from explicit ABFS Delta table path
      - stage output locally
      - copy output (+ log + error bucket if exists) to ABFS output dir
    """
    cfg = Config()

    # Stage locally so Python open() works, then copy to ABFS
    stage_dir = Path("/tmp") / f"pies_etl_stage_{uuid.uuid4().hex}"
    stage_dir.mkdir(parents=True, exist_ok=True)

    log_path = stage_dir / (Path(output_filename).stem + ".log")
    _init_logging(cfg, log_path)

    rules = load_rules(rules_path, system=None)
    lifecycle_map = load_lifecycle(lifecycle_path)

    logger.info("CI/CD SAFE: reading Delta table via ABFS (ignoring dbo binding).")
    logger.info("Requested table_name (for reference only): %s", table_name)
    logger.info("Delta table ABFS path: %s", table_abfs_path)

    # Read Delta table explicitly (no spark.sql dbo.*)
    df = spark.read.format("delta").load(table_abfs_path)  # type: ignore[name-defined]

    try:
        table_cols = list(df.columns)
        logger.info("Table columns=%d", len(table_cols))
        _preflight_rules_vs_table_columns(table_cols, rules)
    except Exception as exc:
        logger.warning("Preflight schema check failed (non-fatal): %s", exc)

    rows = (CaseInsensitiveRow(r.asDict(recursive=True)) for r in df.toLocalIterator())

    # Write locally (so post_to_api can stream from local file)
    local_out_path, lines = smart_engine_from_rows(
        rows=rows,
        rules=rules,
        lifecycle_map=lifecycle_map,
        out_dir=stage_dir,
        outfile=output_filename,
        cfg=cfg,
        default_system_id=default_system_id,
        record_kind=record_kind,
        version=version,
        post_to_api=post_to_api,
        api_url=api_url,
    )

    # Copy outputs to ABFS
    ensure_dir_any(output_dir_abfs)
    remote_out_path = output_dir_abfs.rstrip("/") + "/" + output_filename
    copy_local_to_abfs(local_out_path, remote_out_path, overwrite=True)
    logger.info("Copied output JSONL to ABFS: %s", remote_out_path)

    # Copy log
    remote_log_path = output_dir_abfs.rstrip("/") + "/" + log_path.name
    copy_local_to_abfs(log_path, remote_log_path, overwrite=True)
    logger.info("Copied log to ABFS: %s", remote_log_path)

    # Copy error bucket if exists
    err_local = local_out_path.with_name(local_out_path.stem + "_errors.jsonl")
    if err_local.exists():
        remote_err = output_dir_abfs.rstrip("/") + "/" + err_local.name
        copy_local_to_abfs(err_local, remote_err, overwrite=True)
        logger.info("Copied error bucket to ABFS: %s", remote_err)

    return local_out_path, lines


# ---------------------------------------------------------------------------
# Fabric Notebook: configure and run (CI/CD SAFE SECTION)
# ---------------------------------------------------------------------------

# Spark config: allow ancient dates/timestamps in Parquet without failing
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")  # type: ignore[name-defined]
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")     # type: ignore[name-defined]

# -------------------------
# CI/CD SAFE: ENV + ABFS BASE
# -------------------------
ENV = load_env_vars_from_variable_library(
    "pmt_env_vars",
    required_keys=(
        "fabric_env",
        "peach_api_url",
        "pmt-workspace-id",
        "pmt-lakehouse-id",
    ),
    env_fallback=True,
)

FABRIC_ENV = ENV["fabric_env"]
API_URL = ENV["peach_api_url"]

WORKSPACE_REF = ENV["pmt-workspace-id"]
LAKEHOUSE_REF = ENV["pmt-lakehouse-id"]

ABFS_BASE = build_abfs_base(WORKSPACE_REF, LAKEHOUSE_REF)

print(f"[ENV] fabric_env={FABRIC_ENV}")
print(f"[ENV] workspace_ref={WORKSPACE_REF}")
print(f"[ENV] lakehouse_ref={LAKEHOUSE_REF}")
print(f"[ENV] abfs_base={ABFS_BASE}")

# -------------------------
# Change here
# -------------------------
TABLE_NAME = "dbo.pies_staging_water_vfcbctracking"

# CI/CD safe
TABLE_SHORT = TABLE_NAME.split(".")[-1]
TABLE_ABFS_PATH = f"{ABFS_BASE}/Tables/dbo/{TABLE_SHORT}"


# Rules & lifecycle (explicit ABFS paths)
RULES_PATH = f"{ABFS_BASE}/Files/pies_module/water/water_lifecyle_rules/rules.json"
LIFECYCLE_PATH = f"{ABFS_BASE}/Files/pies_module/water/water_lifecyle_mapping/lifecycle_map.json"

# Output folder + file
timestamp = datetime.now().strftime("%Y%m%d")
OUTPUT_DIR_ABFS = f"{ABFS_BASE}/Files/pies_module/water/water_events_peach/{FABRIC_ENV.lower()}"
OUTPUT_FILENAME = f"water_events_trackingnumber_{FABRIC_ENV.lower()}_{timestamp}.jsonl"

POST_TO_API = True  # set False to only write JSONL (no POST)

DEFAULT_SYSTEM_ID = "ITSM-6117"
RECORD_KIND = "Permit"
PES_VERSION = "0.1.0"

# Run (CI/CD SAFE)
run_fabric_from_table(
    table_name=TABLE_NAME,                 
    table_abfs_path=TABLE_ABFS_PATH,      
    rules_path=RULES_PATH,                
    lifecycle_path=LIFECYCLE_PATH,         
    output_dir_abfs=OUTPUT_DIR_ABFS,       
    output_filename=OUTPUT_FILENAME,
    post_to_api=POST_TO_API,
    api_url=API_URL,
    default_system_id=DEFAULT_SYSTEM_ID,
    record_kind=RECORD_KIND,
    version=PES_VERSION,
)

# Checklist (only change these when making a new dataset):
# 1) TABLE_NAME
# 2) OUTPUT_FILENAME pattern
# 3) RULES_PATH / LIFECYCLE_PATH (only if different module)
# 4) DEFAULT_SYSTEM_ID / RECORD_KIND / PES_VERSION
# 5) Header comments


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
