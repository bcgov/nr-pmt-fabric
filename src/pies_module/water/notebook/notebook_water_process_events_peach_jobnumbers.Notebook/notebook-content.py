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
# ETL mapping -Water job number (Fabric notebook version)
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

import requests
from collections.abc import Mapping as _Mapping

# Fabric Key Vault helper
try:
    from notebookutils import mssparkutils
except Exception:
    mssparkutils = None


# =============================================================================
# SWITCHES (TURN ON/OFF HERE)
# =============================================================================
POST_TO_API = True
USE_PEACH_AUTH = True


# =============================================================================
# ENV (GLOBAL)
# =============================================================================
ENV: dict[str, str] = {}


def _env_pick(env: Mapping[str, str], key: str) -> Optional[str]:
    # exact
    v = env.get(key)
    if v is not None and str(v).strip() != "":
        return str(v).strip()
    # upper/lower convenience
    v = env.get(key.upper())
    if v is not None and str(v).strip() != "":
        return str(v).strip()
    v = env.get(key.lower())
    if v is not None and str(v).strip() != "":
        return str(v).strip()
    return None


def env_get_any(env: Mapping[str, str], *keys: str, default: str) -> str:
    for k in keys:
        v = _env_pick(env, k)
        if v is not None:
            return v
    return default


def env_get_int(env: Mapping[str, str], *keys: str, default: int) -> int:
    s = None
    for k in keys:
        s = _env_pick(env, k)
        if s is not None:
            break
    if s is None:
        return default
    try:
        return int(str(s).strip())
    except Exception:
        return default


def env_get_bool(env: Mapping[str, str], *keys: str, default: bool) -> bool:
    s = None
    for k in keys:
        s = _env_pick(env, k)
        if s is not None:
            break
    if s is None:
        return default
    v = str(s).strip().lower()
    if v in ("1", "true", "t", "yes", "y", "on"):
        return True
    if v in ("0", "false", "f", "no", "n", "off"):
        return False
    return default


# =============================================================================
# PEACH AUTH CONFIG (loaded from ENV later; defaults applied if missing)
# =============================================================================
# NOTE: These are assigned in the Fabric Notebook section after ENV is loaded.
PEACH_TOKEN_URL = ""
PEACH_KEYVAULT_URL = ""
PEACH_CLIENT_ID = ""
PEACH_SECRET_NAME = ""


# =============================================================================
# OAuth Token Manager (requests-based, no Authlib)
# =============================================================================
class PeachTokenManager:
    """
    Client Credentials token cache for Keycloak OIDC token endpoint.
    Uses requests (form post). Refreshes automatically near expiry.
    """
    def __init__(
        self,
        *,
        token_url: str,
        client_id: str,
        client_secret: str,
        refresh_skew_seconds: int = 30,
        timeout_seconds: int = 30,
    ):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_skew_seconds = refresh_skew_seconds
        self.timeout_seconds = timeout_seconds

        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0

    def _fetch_token(self) -> Tuple[str, float]:
        # Keycloak standard: x-www-form-urlencoded
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        resp = requests.post(self.token_url, data=data, timeout=self.timeout_seconds)
        if resp.status_code < 200 or resp.status_code >= 300:
            raise RuntimeError(
                f"Token fetch failed: HTTP {resp.status_code}. "
                f"token_url={self.token_url}. "
                f"resp_text={resp.text[:500]}"
            )
        payload = resp.json()
        token = payload.get("access_token")
        expires_in = int(payload.get("expires_in", 0) or 0)
        if not token:
            raise RuntimeError(f"Token fetch succeeded but access_token missing. payload_keys={list(payload.keys())}")
        expires_at = time.time() + expires_in
        return token, expires_at

    def refresh(self) -> None:
        token, expires_at = self._fetch_token()
        self._access_token = token
        self._expires_at = expires_at

    def get_access_token(self) -> str:
        if not self._access_token:
            self.refresh()
            return self._access_token  # type: ignore[return-value]

        if self._expires_at <= (time.time() + self.refresh_skew_seconds):
            self.refresh()

        return self._access_token

    def auth_headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self.get_access_token()}"}


def build_peach_token_manager_from_keyvault(
    *,
    keyvault_url: str,
    secret_name: str,
    token_url: str,
    client_id: str,
) -> PeachTokenManager:
    if mssparkutils is None:
        raise RuntimeError("mssparkutils is not available. This must run inside Fabric notebooks.")
    client_secret = mssparkutils.credentials.getSecret(keyvault_url, secret_name)
    if not client_secret or str(client_secret).strip() == "":
        raise RuntimeError(f"Key Vault secret is empty: keyvault_url={keyvault_url} secret_name={secret_name}")
    return PeachTokenManager(
        token_url=token_url,
        client_id=client_id,
        client_secret=str(client_secret),
    )


# =============================================================================
# JSON dumping (orjson optional)
# =============================================================================
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

    FIXED:
      - fabric_env will default to 'DEV' if missing (prevents hard failure)
      - other keys still must exist

    NOTE:
      - This is the ONLY place we do OS env fallback (os.getenv).
      - All other code must read config from the returned ENV dict.
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

        # 2) OS env fallback (centralized here only)
        if env_fallback:
            v2 = os.getenv(key) or os.getenv(key.upper())
            if v2 is not None and str(v2).strip() != "":
                return str(v2).strip()

        # 3) Default for fabric_env only
        if key == "fabric_env":
            return "DEV"

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


def load_optional_env_vars_from_variable_library(
    library_name: str = "pmt_env_vars",
    optional_keys: tuple[str, ...] = (),
    *,
    env_fallback: bool = True,
) -> dict[str, str]:
    """
    Load OPTIONAL variables from Fabric Variable Library.
    - Missing keys are skipped (no hard fail).
    - Optional OS env fallback is still centralized here only.
    """
    nb = _get_notebookutils()
    out: dict[str, str] = {}

    for key in optional_keys:
        v = None

        # 1) Fabric Variable Library
        if nb is not None:
            try:
                v = nb.variableLibrary.get(f"$(/**/{library_name}/{key})")
            except Exception:
                v = None

        # 2) OS env fallback (centralized here only)
        if (v is None or str(v).strip() == "") and env_fallback:
            v = os.getenv(key) or os.getenv(key.upper())

        if v is not None and str(v).strip() != "":
            out[key] = str(v).strip()

    return out


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

    if not _is_guid(l) and not l.lower().endswith(".lakehouse"):
        l = f"{l}.Lakehouse"

    return f"abfss://{w}@onelake.dfs.fabric.microsoft.com/{l}"


def _fs():
    nb = _get_notebookutils()
    if nb is None or not hasattr(nb, "fs"):
        raise RuntimeError("Fabric notebookutils.fs is not available in this environment.")
    return nb.fs


def read_text_any(path_str: str) -> str:
    p = (path_str or "").strip()
    if p.lower().startswith("abfss://"):
        try:
            fh = _fs().open(p, "r")
            try:
                return fh.read()
            finally:
                fh.close()
        except Exception:
            try:
                return _fs().head(p, 1024 * 1024 * 50)
            except Exception as e:
                raise RuntimeError(f"Failed to read ABFS text file: {p}. Error: {e}") from e

    return Path(p).read_text(encoding="utf-8")


def ensure_dir_any(path_str: str) -> None:
    p = (path_str or "").rstrip("/")
    if p.lower().startswith("abfss://"):
        try:
            _fs().mkdirs(p)
        except Exception:
            try:
                _fs().mkdir(p)
            except Exception as e:
                raise RuntimeError(f"Failed to create ABFS directory: {p}. Error: {e}") from e
    else:
        Path(p).mkdir(parents=True, exist_ok=True)


def copy_local_to_abfs(local_path: Path, abfs_path: str, *, overwrite: bool = True) -> None:
    src = f"file:{local_path.as_posix()}"
    dst = abfs_path

    parent = dst.rsplit("/", 1)[0]
    ensure_dir_any(parent)

    try:
        _fs().cp(src, dst, overwrite)
        return
    except TypeError:
        pass
    except Exception:
        pass

    try:
        _fs().cp(src, dst)
        return
    except Exception:
        pass

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
    ts_ms = int(time.time() * 1000)
    ts_bytes = ts_ms.to_bytes(6, "big")

    rand_a = secrets.randbits(12)
    time_hi_and_version = (0x7 << 12) | rand_a
    thv_bytes = time_hi_and_version.to_bytes(2, "big")

    rand_b = secrets.randbits(62)
    top6 = (rand_b >> 56) & 0x3F
    rem56 = rand_b & ((1 << 56) - 1)
    variant_byte = top6 | 0x80
    rem56_bytes = rem56.to_bytes(7, "big")

    uuid_bytes = ts_bytes + thv_bytes + bytes([variant_byte]) + rem56_bytes
    return uuid.UUID(bytes=uuid_bytes)


# ---------------------------------------------------------------------------
# Constants (non-env)
# ---------------------------------------------------------------------------
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}")
BYTES_IN_MIB = 1_048_576

DEFAULT_CODE_SYSTEM_URL = "https://bcgov.github.io/nr-pies/docs/spec/code_system/application_process"

POST_RETRY_STATUSES = {408, 429, 500, 502, 503, 504}

# These are set from ENV in the Fabric Notebook section (bottom)
POST_TIMEOUT_SECONDS = 30
POST_MAX_RETRIES = 3
EVENT_TIME_MODE = "DATETIME"  # DATETIME|DATE


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------
class ETLError(RuntimeError):
    """Top-level exception for the ETL engine."""


# ---------------------------------------------------------------------------
# Null/case helpers
# ---------------------------------------------------------------------------
def _is_nullish(v: Any) -> bool:
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
# Configuration (all values sourced from ENV at runtime)
# ---------------------------------------------------------------------------
@dataclass
class Config:
    # Defaults (will be overridden by ENV if provided)
    small_mb: int = 100
    medium_mb: int = 200
    chunk_rows: int = 100000

    log_level: str = "INFO"
    log_json: bool = False

    max_rows: int = 0
    log_first_n_rows: int = 0
    log_rule_hit_topn: int = 15
    fail_on_empty_output: bool = False

    def __post_init__(self) -> None:
        # Pull from ENV (no direct os.getenv)
        self.small_mb = env_get_int(ENV, "ETL_SMALL_MIB", "etl_small_mib", default=self.small_mb)
        self.medium_mb = env_get_int(ENV, "ETL_MEDIUM_MIB", "etl_medium_mib", default=self.medium_mb)
        self.chunk_rows = env_get_int(ENV, "ETL_CHUNK_ROWS", "etl_chunk_rows", default=self.chunk_rows)

        self.log_level = env_get_any(ENV, "LOG_LEVEL", "log_level", default=self.log_level)
        self.log_json = env_get_bool(ENV, "LOG_JSON", "log_json", default=self.log_json)

        self.max_rows = env_get_int(ENV, "ETL_MAX_ROWS", "etl_max_rows", default=self.max_rows)
        self.log_first_n_rows = env_get_int(ENV, "ETL_LOG_FIRST_N_ROWS", "etl_log_first_n_rows", default=self.log_first_n_rows)
        self.log_rule_hit_topn = env_get_int(ENV, "ETL_RULE_HIT_TOPN", "etl_rule_hit_topn", default=self.log_rule_hit_topn)
        self.fail_on_empty_output = env_get_bool(ENV, "ETL_FAIL_ON_EMPTY_OUTPUT", "etl_fail_on_empty_output", default=self.fail_on_empty_output)

        # Normalize
        self.log_level = (self.log_level or "INFO").strip().upper()

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
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

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


def load_rules(path_str: str, *, system: Optional[str] = None) -> List[Dict[str, Any]]:
    logger.info("Loading rules from %s", path_str)
    raw = json.loads(read_text_any(path_str))
    compiled: List[Dict[str, Any]] = []

    for key, definition in raw.items():
        if system and definition.get("source") and str(definition["source"]).lower() != system.lower():
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


def post_jsonl_to_api(
    out_path: Path,
    api_url: str,
    *,
    expected_lines: Optional[int] = None,
    token_mgr: Optional[PeachTokenManager] = None,  # <-- auth optional
) -> None:
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
                refreshed_once = False

                while attempt < POST_MAX_RETRIES and not sent_ok:
                    attempt += 1
                    try:
                        headers = token_mgr.auth_headers() if token_mgr else None
                        resp = sess.post(api_url, json=record, headers=headers, timeout=POST_TIMEOUT_SECONDS)
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

                    # If auth fails, refresh token ONCE and retry
                    if token_mgr and status in (401, 403) and (not refreshed_once) and attempt < POST_MAX_RETRIES:
                        logger.warning(
                            "POST auth failed status=%s record_id=%s (line %d). Refreshing token and retrying once.",
                            status, record_id, line_no
                        )
                        try:
                            token_mgr.refresh()
                            refreshed_once = True
                            time.sleep(1)
                            continue
                        except Exception as e:
                            logger.error("Token refresh failed: %s", e)
                            # fall through to normal error handling

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
                            "POST non-retryable status %s record_id=%s (line %d) -> error bucket. resp=%s",
                            status,
                            record_id,
                            line_no,
                            (resp.text or "")[:300],
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
    token_mgr: Optional[PeachTokenManager] = None,  # <-- add
    default_system_id: str = "ITSM-6117",
    record_kind: str = "Permit",
    version: str = "0.1.0",
) -> Tuple[Path, int]:
    cfg = Config()

    stage_dir = Path("/tmp") / f"pies_etl_stage_{uuid.uuid4().hex}"
    stage_dir.mkdir(parents=True, exist_ok=True)

    log_path = stage_dir / (Path(output_filename).stem + ".log")
    _init_logging(cfg, log_path)

    rules = load_rules(rules_path, system=None)
    lifecycle_map = load_lifecycle(lifecycle_path)

    logger.info("CI/CD SAFE: reading Delta table via ABFS (ignoring dbo binding).")
    logger.info("Requested table_name (for reference only): %s", table_name)
    logger.info("Delta table ABFS path: %s", table_abfs_path)

    df = spark.read.format("delta").load(table_abfs_path)  # type: ignore[name-defined]

    try:
        table_cols = list(df.columns)
        logger.info("Table columns=%d", len(table_cols))
        _preflight_rules_vs_table_columns(table_cols, rules)
    except Exception as exc:
        logger.warning("Preflight schema check failed (non-fatal): %s", exc)

    rows = (CaseInsensitiveRow(r.asDict(recursive=True)) for r in df.toLocalIterator())

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
    )

    ensure_dir_any(output_dir_abfs)
    remote_out_path = output_dir_abfs.rstrip("/") + "/" + output_filename
    copy_local_to_abfs(local_out_path, remote_out_path, overwrite=True)
    logger.info("Copied output JSONL to ABFS: %s", remote_out_path)

    if post_to_api and api_url:
        logger.info("Starting POST AFTER ABFS copy (artifact already saved).")
        post_jsonl_to_api(local_out_path, api_url, expected_lines=lines, token_mgr=token_mgr)

    remote_log_path = output_dir_abfs.rstrip("/") + "/" + log_path.name
    copy_local_to_abfs(log_path, remote_log_path, overwrite=True)
    logger.info("Copied log to ABFS: %s", remote_log_path)

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

# REQUIRED env vars (must exist)
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

# OPTIONAL env vars (do not hard-fail if missing)
ENV.update(
    load_optional_env_vars_from_variable_library(
        "pmt_env_vars",
        optional_keys=(
            # PEACH auth config (recommend lowercase keys in variable library)
            "peach_token_url",
            "peach_keyvault_url",
            "peach_client_id",
            "peach_secret_name",

            # Back-compat if you used uppercase keys in the library or OS env
            "PEACH_TOKEN_URL",
            "PEACH_KEYVAULT_URL",
            "PEACH_CLIENT_ID",
            "PEACH_SECRET_NAME",

            # Posting/retry/time-mode (optional)
            "peach_post_timeout",
            "peach_post_max_retries",
            "event_time_mode",

            "PEACH_POST_TIMEOUT",
            "PEACH_POST_MAX_RETRIES",
            "EVENT_TIME_MODE",

            # ETL config (optional)
            "etl_small_mib",
            "etl_medium_mib",
            "etl_chunk_rows",
            "log_level",
            "log_json",
            "etl_max_rows",
            "etl_log_first_n_rows",
            "etl_rule_hit_topn",
            "etl_fail_on_empty_output",

            "ETL_SMALL_MIB",
            "ETL_MEDIUM_MIB",
            "ETL_CHUNK_ROWS",
            "LOG_LEVEL",
            "LOG_JSON",
            "ETL_MAX_ROWS",
            "ETL_LOG_FIRST_N_ROWS",
            "ETL_RULE_HIT_TOPN",
            "ETL_FAIL_ON_EMPTY_OUTPUT",
        ),
        env_fallback=True,
    )
)

FABRIC_ENV = ENV["fabric_env"]
API_URL = ENV["peach_api_url"]

WORKSPACE_REF = ENV["pmt-workspace-id"]
LAKEHOUSE_REF = ENV["pmt-lakehouse-id"]

# Set runtime constants from ENV (NO os.getenv here)
POST_TIMEOUT_SECONDS = env_get_int(ENV, "peach_post_timeout", "PEACH_POST_TIMEOUT", default=30)
POST_MAX_RETRIES = env_get_int(ENV, "peach_post_max_retries", "PEACH_POST_MAX_RETRIES", default=3)
EVENT_TIME_MODE = env_get_any(ENV, "event_time_mode", "EVENT_TIME_MODE", default="DATETIME").strip().upper()

# PEACH auth vars from ENV with defaults
PEACH_TOKEN_URL = env_get_any(
    ENV,
    "peach_token_url", "PEACH_TOKEN_URL",
    default="https://dev.loginproxy.gov.bc.ca/auth/realms/permittingexchange/protocol/openid-connect/token",
)
PEACH_KEYVAULT_URL = env_get_any(
    ENV,
    "peach_keyvault_url", "PEACH_KEYVAULT_URL",
    default="https://permittingfabrickv.vault.azure.net/",
)
PEACH_CLIENT_ID = env_get_any(
    ENV,
    "peach_client_id", "PEACH_CLIENT_ID",
    default="dev-w1zhang",
)
PEACH_SECRET_NAME = env_get_any(
    ENV,
    "peach_secret_name", "PEACH_SECRET_NAME",
    default=PEACH_CLIENT_ID,
)

ABFS_BASE = build_abfs_base(WORKSPACE_REF, LAKEHOUSE_REF)

print(f"[ENV] fabric_env={FABRIC_ENV}")
print(f"[ENV] workspace_ref={WORKSPACE_REF}")
print(f"[ENV] lakehouse_ref={LAKEHOUSE_REF}")
print(f"[ENV] abfs_base={ABFS_BASE}")
print(f"[ENV] peach_api_url={API_URL}")
print(f"[ENV] peach_token_url={PEACH_TOKEN_URL}")
print(f"[ENV] peach_keyvault_url={PEACH_KEYVAULT_URL}")
print(f"[ENV] peach_client_id={PEACH_CLIENT_ID}")
print(f"[ENV] PEACH_POST_TIMEOUT={POST_TIMEOUT_SECONDS}  PEACH_POST_MAX_RETRIES={POST_MAX_RETRIES}  EVENT_TIME_MODE={EVENT_TIME_MODE}")
print(f"[SWITCH] POST_TO_API={POST_TO_API}  USE_PEACH_AUTH={USE_PEACH_AUTH}")

# -------------------------
# Change here
# -------------------------
TABLE_NAME = "dbo.pies_staging_water_jobnumber"

TABLE_SHORT = TABLE_NAME.split(".")[-1]
TABLE_ABFS_PATH = f"{ABFS_BASE}/Tables/dbo/{TABLE_SHORT}"

RULES_PATH = f"{ABFS_BASE}/Files/pies_module/water/water_lifecyle_rules/rules.json"
LIFECYCLE_PATH = f"{ABFS_BASE}/Files/pies_module/water/water_lifecyle_mapping/lifecycle_map.json"

timestamp = datetime.now().strftime("%Y%m%d")
OUTPUT_DIR_ABFS = f"{ABFS_BASE}/Files/pies_module/water/water_events_peach/{FABRIC_ENV.lower()}"
OUTPUT_FILENAME = f"water_events_jobnumber_{FABRIC_ENV.lower()}_{timestamp}.jsonl"

DEFAULT_SYSTEM_ID = "ITSM-6197"
RECORD_KIND = "Permit"
PES_VERSION = "0.1.0"

# Build token manager ONLY if auth is enabled
token_mgr = None
if POST_TO_API and USE_PEACH_AUTH:
    token_mgr = build_peach_token_manager_from_keyvault(
        keyvault_url=PEACH_KEYVAULT_URL,
        secret_name=PEACH_SECRET_NAME,
        token_url=PEACH_TOKEN_URL,
        client_id=PEACH_CLIENT_ID,
    )
    logger.info("PEACH auth enabled for POST. client_id=%s token_host=%s", PEACH_CLIENT_ID, PEACH_TOKEN_URL.split("/")[2])

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
    token_mgr=token_mgr,  # None if auth disabled
    default_system_id=DEFAULT_SYSTEM_ID,
    record_kind=RECORD_KIND,
    version=PES_VERSION,
)

# Checklist:
# 1) TABLE_NAME
# 2) OUTPUT_FILENAME pattern
# 3) RULES_PATH / LIFECYCLE_PATH
# 4) DEFAULT_SYSTEM_ID / RECORD_KIND / PES_VERSION
# 5) Switches: POST_TO_API / USE_PEACH_AUTH
# 6) Variable Library keys (pmt_env_vars): fabric_env, peach_api_url, pmt-workspace-id, pmt-lakehouse-id
#    Optional: peach_token_url, peach_keyvault_url, peach_client_id, peach_secret_name, peach_post_timeout, peach_post_max_retries, event_time_mode


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
