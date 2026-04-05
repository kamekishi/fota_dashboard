from __future__ import annotations

import html
import json
import re
import socket
import sqlite3
import threading
import time
import textwrap
from datetime import datetime
from math import ceil
from pathlib import Path
from typing import Any
from uuid import uuid4
import xml.etree.ElementTree as ET

import streamlit as st
import streamlit.components.v1 as components

import dc3
import ota


BASE_DIR = Path(__file__).resolve().parent
DEVICES_PATH = BASE_DIR / "devices.json"
DB_PATH = BASE_DIR / "fumo_history.db"
DECRYPTED_DB_PATH = BASE_DIR / "decrypted_firmware.db"
IMEI_DB_DIR = BASE_DIR / "imei_database"
ACTIVITY_DB_PATH = BASE_DIR / "activity.db"
USER_AGENT = "SyncML DM Client"
ADMIN_SECRET_CODE = "A7K9M2Q4X8P1L6N3R5T7V9Y2B4C6D8F1"
MAX_ACTIVITY = 10
TASK_LOCK = threading.Lock()
TASKS: dict[str, dict[str, Any]] = {}
ACTIVITY_QUEUE: list[dict[str, str]] = []
PATROL_LOCK = threading.Lock()
PATROL_THREADS: dict[str, threading.Thread] = {}
PATROL_COORDINATOR_KEY = "__night_patrol__"


st.set_page_config(
    page_title="KinZoKu Dashboard",
    page_icon="KZ",
    layout="wide",
    initial_sidebar_state="expanded",
)


def init_state() -> None:
    defaults = {
        "activity_feed": [],
        "status_snapshot": {},
        "snapshot_time": None,
        "dialog_payload": None,
        "last_result": None,
        "imei_scan_results": [],
        "imei_last_hit": None,
        "fota_task_id": None,
        "imei_task_id": None,
        "fota_completed_task_id": None,
        "imei_completed_task_id": None,
        "fota_live_request": None,
        "fota_live_result": None,
        "fota_live_error": None,
        "imei_live_request": None,
        "imei_live_result": None,
        "imei_live_error": None,
        "imei_live_state": None,
        "active_tab": "Dashboard",
        "activity_feed_visible": True,
        "logout_refresh_pending": False,
        "is_authenticated": False,
        "user_mode": None,
        "login_error": None,
        "_selected_device_key": None,
        "_scan_selected_device_key": None,
        "_decrypt_selected_device_key": None,
        "decrypt_results": [],
        "decrypt_error": None,
        "decrypt_latest_key": None,
        "decrypt_lookup_model": "",
        "decrypt_lookup_csc": "",
        "firmware_picker_request": None,
        "delta_scan_request": None,
        "delta_scan_result": None,
        "delta_scan_error": None,
        "fota_scanned_imei": "",
        "library_detail_row": None,
        "last_seen_notice_id": 0,
        "_legacy_sync_signature": "",
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def push_activity(level: str, message: str) -> None:
    event = prepare_activity_event(level, message)
    if not event:
        return
    write_activity_event(event["level"], event["tool"], event["message"])
    timestamp = datetime.now().strftime("%H:%M:%S")
    item = {"time": timestamp, "level": event["level"], "message": event["message"]}
    try:
        st.session_state.activity_feed.insert(0, item)
        del st.session_state.activity_feed[MAX_ACTIVITY:]
    except Exception:
        pass


def queue_activity(level: str, message: str) -> None:
    event = prepare_activity_event(level, message)
    if not event:
        return
    write_activity_event(event["level"], event["tool"], event["message"])


def sync_activity_feed() -> None:
    st.session_state.activity_feed = recent_activity_events(MAX_ACTIVITY)


def with_activity_db(query: str, params: tuple[Any, ...] = (), *, one: bool = False) -> Any:
    with sqlite3.connect(ACTIVITY_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(query, params)
        return cursor.fetchone() if one else cursor.fetchall()


def execute_activity_db(query: str, params: tuple[Any, ...] = ()) -> None:
    with sqlite3.connect(ACTIVITY_DB_PATH) as conn:
        conn.execute(query, params)
        conn.commit()


def extract_model_and_csc(message: str) -> tuple[str, str]:
    text = str(message or "")
    match = re.search(r"(SM-[A-Z0-9-]+)\s*(?:[/|(]\s*([A-Z0-9]{2,4})\s*[)|])?", text, re.IGNORECASE)
    if not match:
        return "", ""
    model = normalize_model_number(match.group(1))
    csc = normalize_csc_code(match.group(2) or "")
    return model, csc


def prepare_activity_event(level: str, message: str) -> dict[str, str] | None:
    raw = str(message or "").strip()
    if not raw:
        return None
    lowered = raw.lower()

    ignored_markers = [
        "dashboard initialized",
        "dashboard status checks refreshed",
        "admin mode activated",
        "guest mode activated",
        "added device ",
        "updated device ",
        "removed device ",
        " already uses imei ",
        " to imei ",
    ]
    if any(marker in lowered for marker in ignored_markers):
        return None

    model, csc = extract_model_and_csc(raw)
    normalized = raw
    tool = "System"
    event_level = str(level or "INFO").upper()

    if lowered.startswith("a user is using decryption tool"):
        tool = "Decryption"
        normalized = raw
    elif lowered.startswith("a user is fetching an ota"):
        tool = "FOTA Scanner"
        normalized = raw
    elif lowered.startswith("a user is running imei scanner"):
        tool = "IMEI Scanner"
        normalized = raw
    elif lowered.startswith("a new ota is found"):
        tool = "FOTA Scanner"
        normalized = raw
        event_level = "HIT"
    elif lowered.startswith("a user found a new imei"):
        tool = "IMEI Scanner"
        normalized = raw
        event_level = "HIT"
    elif lowered.startswith("a user found latest update using the decryption tool"):
        tool = "Decryption"
        normalized = raw
        event_level = "HIT"
    elif "running a live samsung lookup" in lowered or "cache miss for" in lowered or "reused cached link for" in lowered:
        tool = "FOTA Scanner"
        if model:
            normalized = f"A user is fetching an OTA for {model}" + (f" ({csc})." if csc else ".")
        else:
            normalized = "A user is fetching an OTA."
        event_level = "INFO"
    elif lowered.startswith("fetched ") and " for " in lowered:
        tool = "FOTA Scanner"
        normalized = f"A new OTA is found for {model}! Check out the Library." if model else "A new OTA is found! Check out the Library."
        event_level = "HIT"
    elif "already on the newest package" in lowered:
        tool = "FOTA Scanner"
        normalized = f"A user checked OTA for {model} and no newer package was found." if model else "A user checked OTA and no newer package was found."
    elif lowered.startswith("lookup failed for"):
        tool = "FOTA Scanner"
        normalized = f"A user encountered an OTA lookup error for {model}." if model else "A user encountered an OTA lookup error."
        event_level = "ERROR"
    elif "imei scanner started for" in lowered:
        tool = "IMEI Scanner"
        normalized = "A user is running IMEI Scanner."
    elif "encountered auth maked failed during an imei scanner" in lowered:
        tool = "IMEI Scanner"
        normalized = raw
        event_level = "ERROR"
    elif lowered.startswith("imei scan completed for"):
        tool = "IMEI Scanner"
        hit_match = re.search(r"with\s+(\d+)\s+hits", lowered)
        hit_count = int(hit_match.group(1)) if hit_match else 0
        normalized = (
            "A user found a new IMEI that hits an update! Check out IMEI Database!"
            if hit_count > 0
            else "A user completed IMEI Scanner."
        )
        event_level = "HIT" if hit_count > 0 else "INFO"
    elif lowered.startswith("imei scan for") and "terminated due to" in lowered:
        tool = "IMEI Scanner"
        normalized = (
            f"A user has encountered Auth Maked Failed during an IMEI Scanner for {model}."
            if "auth" in lowered
            else f"A user encountered an IMEI Scanner error for {model}."
        )
        event_level = "ERROR"
    elif lowered.startswith("decryption ran by previous user for"):
        tool = "Decryption"
        normalized = raw
        event_level = "HIT" if "new firmware is found" in lowered else "INFO"
    elif lowered.startswith("decryption scan completed for") or lowered.startswith("pathfinder decrypted firmware for"):
        tool = "Decryption"
        normalized = "A user found latest update using the Decryption tool."
        event_level = "HIT"
    elif lowered.startswith("decryption failed for") or lowered.startswith("pathfinder decryption failed for"):
        tool = "Decryption"
        normalized = f"A decryption run failed for {model}" + (f" ({csc})." if csc else ".")
        event_level = "ERROR"
    elif lowered.startswith("imported ") and " from fumo history into " in lowered:
        tool = "IMEI Database"
        target = re.search(r"into\s+(SM-[A-Z0-9-]+)", raw, re.IGNORECASE)
        normalized = f"A user refreshed IMEI Database from FUMO History for {normalize_model_number(target.group(1))}." if target else "A user refreshed IMEI Database from FUMO History."
        event_level = "INFO"
    elif lowered.startswith("night patrol failed for"):
        tool = "Night Patrol"
        normalized = f"A Night Patrol job failed for {model}" + (f" ({csc})." if csc else ".")
        event_level = "ERROR"
    elif lowered.startswith("new firmware decrypted for"):
        tool = "Night Patrol"
        normalized = f"A user found latest update using the Decryption tool for {model}." if model else "A user found latest update using the Decryption tool."
        event_level = "HIT"
    elif lowered.startswith("night patrol stopped for"):
        tool = "Night Patrol"
        normalized = f"A user stopped Night Patrol for {model}" + (f" ({csc})." if csc else ".")
        event_level = "WARN"
    else:
        return None

    return {"level": event_level, "tool": tool, "message": normalized}


def write_activity_event(level: str, tool: str, message: str) -> None:
    execute_activity_db(
        """
        INSERT INTO activity_events (created_at, level, tool_name, message)
        VALUES (?, ?, ?, ?)
        """,
        (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(level).upper(), tool, message),
    )


def recent_activity_events(limit: int = MAX_ACTIVITY) -> list[dict[str, str]]:
    if not ACTIVITY_DB_PATH.exists():
        return []
    rows = with_activity_db(
        """
        SELECT created_at, level, message
        FROM activity_events
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )
    items: list[dict[str, str]] = []
    for row in rows:
        created_at = str(row["created_at"] or "")
        items.append(
            {
                "time": created_at[11:19] if len(created_at) >= 19 else created_at,
                "level": str(row["level"] or "INFO").upper(),
                "message": str(row["message"] or ""),
            }
        )
    return items


def path_signature(path: Path) -> str:
    if not path.exists():
        return "missing"
    stat = path.stat()
    return f"{stat.st_mtime_ns}:{stat.st_size}"


def combined_path_signature(*paths: Path) -> str:
    return "|".join(f"{path.name}:{path_signature(path)}" for path in paths)


def imei_database_signature() -> str:
    if not IMEI_DB_DIR.exists():
        return "missing"
    parts: list[str] = []
    for path in sorted(IMEI_DB_DIR.glob("imei-*.db")):
        parts.append(f"{path.name}:{path_signature(path)}")
    return "|".join(parts) if parts else "empty"


@st.cache_data(show_spinner=False)
def _load_device_catalog_cached(signature: str) -> dict[str, list[dict[str, Any]]]:
    if signature == "missing":
        return {}
    with DEVICES_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_device_catalog() -> dict[str, list[dict[str, Any]]]:
    return _load_device_catalog_cached(path_signature(DEVICES_PATH))


def save_device_catalog(catalog: dict[str, list[dict[str, Any]]]) -> None:
    with DEVICES_PATH.open("w", encoding="utf-8") as handle:
        json.dump(catalog, handle, indent=4)
    clear_data_caches()


def flatten_devices(catalog: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    devices: list[dict[str, Any]] = []
    for category, entries in catalog.items():
        for entry in entries:
            item = dict(entry)
            item.setdefault("category", category)
            devices.append(item)
    return devices


def update_device_imei(category: str, device_index: int, new_imei: str, source_label: str) -> bool:
    if not new_imei.isdigit() or len(new_imei) != 15:
        st.error("IMEI must be exactly 15 digits.")
        return False

    catalog = load_device_catalog()
    entries = catalog.get(category, [])
    if device_index < 0 or device_index >= len(entries):
        st.error("Selected device could not be updated.")
        return False

    device = entries[device_index]
    old_imei = str(device.get("imei", ""))
    if old_imei == new_imei:
        push_activity("info", f"{device.get('model', 'Unknown')} already uses IMEI {new_imei}.")
        return False

    device["imei"] = new_imei
    save_device_catalog(catalog)

    model = str(device.get("model", "")).upper()
    csc = str(device.get("csc", "")).upper()
    if st.session_state.get("scan_model", "").upper() == model and st.session_state.get("scan_csc", "").upper() == csc:
        st.session_state.scan_imei = new_imei
    if st.session_state.get("model_input", "").upper() == model and st.session_state.get("csc_input", "").upper() == csc:
        st.session_state.imei_input = new_imei

    push_activity("info", f"Updated {model} / {csc} to IMEI {new_imei} from {source_label}.")
    return True


def mask_imei(imei: str | None, hidden_digits: int = 7) -> str:
    raw = str(imei or "")
    if len(raw) <= hidden_digits:
        return "•" * len(raw)
    visible = raw[:-hidden_digits]
    return f"{visible}{'•' * hidden_digits}"


def redact_guest_text(text: str) -> str:
    def replacer(match: re.Match[str]) -> str:
        return mask_imei(match.group(0))

    return re.sub(r"\b\d{15}\b", replacer, str(text))


def current_user_mode() -> str:
    return str(st.session_state.get("user_mode") or "guest").lower()


def is_guest_mode() -> bool:
    return current_user_mode() == "guest"


def increment_imei(imei: str, step: int = 1) -> str:
    if not imei.isdigit():
        return imei
    return str(int(imei) + max(step, 1)).zfill(len(imei))


def with_db(query: str, params: tuple[Any, ...] = (), *, one: bool = False) -> Any:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(query, params)
        return cursor.fetchone() if one else cursor.fetchall()


def execute_db(query: str, params: tuple[Any, ...]) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(query, params)
        conn.commit()
    clear_data_caches()


def create_task(task_type: str, payload: dict[str, Any]) -> str:
    task_id = uuid4().hex
    with TASK_LOCK:
        TASKS[task_id] = {
            "id": task_id,
            "type": task_type,
            "status": "running",
            "message": "Starting...",
            "progress": 0.0,
            "payload": payload,
            "result": None,
            "results": [],
            "error": None,
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    return task_id


def update_task(task_id: str, **updates: Any) -> None:
    with TASK_LOCK:
        if task_id in TASKS:
            TASKS[task_id].update(updates)


def get_task(task_id: str | None) -> dict[str, Any] | None:
    if not task_id:
        return None
    with TASK_LOCK:
        task = TASKS.get(task_id)
        return dict(task) if task else None


def readable_size(size_in_bytes: Any) -> str:
    try:
        size = float(size_in_bytes or 0)
    except (TypeError, ValueError):
        return "0 B"

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024 or unit == "TB":
            return f"{size:.2f} {unit}"
        size /= 1024

    return "0 B"


def short_version(version_value: str | None) -> str:
    if not version_value:
        return "00000"
    clean = version_value.split("/")[0]
    return clean[-5:] if len(clean) >= 5 else clean


def short_triplet_version(version_value: str | None) -> str:
    raw = str(version_value or "").strip()
    if not raw:
        return "-"
    parts = [part.strip() for part in raw.split("/") if part.strip()]
    if not parts:
        return "-"
    return "/".join(part[-5:] if len(part) >= 5 else part for part in parts)


def resolve_real_csc(default_csc: str, version_value: str | None) -> str:
    real_csc = default_csc or "UNK"
    if not version_value or "/" not in version_value:
        return real_csc

    middle = version_value.split("/")[1]
    for group in ["OXM", "OYN", "OYM", "OWO", "OXE", "OJM", "OLM", "IND"]:
        if group in middle:
            return group
    return real_csc


def build_download_filename(
    model: str,
    csc: str,
    base_version: str | None,
    target_version: str | None,
    url: str | None,
) -> str:
    filename = (
        f"{model}_{resolve_real_csc(csc, target_version)}_"
        f"{short_version(base_version)}_{short_version(target_version)}.zip"
    )
    if ".DM" in (url or "").upper() or ".DM" in (target_version or "").upper():
        return filename.replace(".zip", ".DM.zip")
    return filename


def normalize_download_url(url: str | None) -> str:
    if not url:
        return ""

    clean = html.unescape(str(url)).strip()
    if "px-nb=" in clean and "px-rmtime=" in clean:
        return clean

    suffix = "&px-nb=fota&px-rmtime=fota"
    return f"{clean}{suffix}" if "?" in clean else f"{clean}?{suffix.lstrip('&')}"


def build_curl_command(filename: str, url: str) -> str:
    return f'curl -L -A "{USER_AGENT}" -o "{filename}" "{url}"'


def parse_descriptor_full(url: str, source_version: str) -> dict[str, Any] | None:
    try:
        response = ota.session.get(url, timeout=30)
        response.raise_for_status()
        root = ET.fromstring(response.text)
        namespace = {"dd": "http://www.openmobilealliance.org/xmlns/dd"}
        object_uri = root.findtext(".//dd:objectURI", namespaces=namespace) or ""
        install_param = root.findtext(".//dd:installParam", namespaces=namespace) or ""
        param_dict = dict(
            item.split("=", 1) for item in install_param.split(";") if "=" in item
        )

        size = 0
        size_element = root.find(".//dd:size", namespaces=namespace)
        if size_element is not None and size_element.text:
            size = int(size_element.text)

        os_version = (param_dict.get("updateFwOsv") or "").replace("B(", "").replace(")", "")
        return {
            "sourceFwV": source_version,
            "updateFwV": param_dict.get("updateFwV") or "Unknown",
            "size": size,
            "url": object_uri,
            "oneUiVersion": param_dict.get("updateOneUiVersion") or "",
            "os_ver": os_version,
            "security": param_dict.get("securityPatchVersion") or "",
        }
    except Exception:
        return None


def fetch_cached_result(model: str, csc: str, imei: str) -> dict[str, Any] | None:
    row = with_db(
        """
        SELECT *
        FROM firmware_hits
        WHERE device_model = ?
          AND csc = ?
          AND imei = ?
        ORDER BY datetime(timestamp) DESC, id DESC
        LIMIT 1
        """,
        (model, csc, imei),
        one=True,
    )
    if row is None:
        return None

    link = row["dm_url"] or row["fota_url"] or ""
    final_url = normalize_download_url(link)
    filename = build_download_filename(
        row["device_model"],
        row["csc"],
        row["request_base_version"],
        row["found_pda"],
        link,
    )
    return {
        "source": "cache",
        "status": "Cached link ready",
        "kind": "dm" if ".DM" in (row["found_pda"] or "").upper() else "update",
        "model": row["device_model"],
        "csc": row["csc"],
        "imei": row["imei"],
        "base": row["request_base_version"] or "",
        "found_pda": row["found_pda"] or "Unknown",
        "size": None,
        "security": "",
        "one_ui": "",
        "timestamp": row["timestamp"],
        "download_url": final_url,
        "filename": filename,
        "curl_command": build_curl_command(filename, final_url) if final_url else "",
    }


def store_lookup_result(payload: dict[str, Any]) -> None:
    execute_db(
        """
        INSERT INTO firmware_hits (
            finder_name,
            device_model,
            imei,
            csc,
            request_base_version,
            found_pda,
            is_fumo,
            fota_url,
            dm_url,
            raw_response
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "streamlit_dashboard",
            payload["model"],
            payload["imei"],
            payload["csc"],
            payload["base"],
            payload["found_pda"],
            1,
            payload["download_url"] if payload["kind"] != "dm" else "",
            payload["download_url"] if payload["kind"] == "dm" else "",
            json.dumps(payload, ensure_ascii=True),
        ),
    )


def perform_remote_lookup(model: str, csc: str, imei: str, base: str | None) -> dict[str, Any]:
    working_base = (base or "").strip()
    if not working_base:
        working_base = ota.fetch_latest_version(model, csc)
        if not working_base:
            return {
                "source": "remote",
                "kind": "error",
                "status": "Unable to resolve a base firmware version.",
            }

    mcc, mnc = ("460", "01") if csc in {"CHC", "CHM"} else ("310", "410")
    client = ota.Client(
        {
            "Model": model,
            "DeviceId": f"IMEI:{imei}",
            "CustomerCode": csc,
            "FirmwareVersion": working_base,
            "Registered": True,
            "Mcc": mcc,
            "Mnc": mnc,
        }
    )

    result = client.check_update(working_base)
    if isinstance(result, str) and result.startswith("http"):
        descriptor = parse_descriptor_full(result, working_base)
        if not descriptor or not descriptor.get("url"):
            return {
                "source": "remote",
                "kind": "error",
                "status": "Update descriptor was returned, but the download link could not be parsed.",
            }

        found_pda = descriptor.get("updateFwV") or "Unknown"
        download_url = normalize_download_url(descriptor.get("url"))
        kind = "dm" if ".DM" in found_pda.upper() else "update"
        filename = build_download_filename(model, csc, working_base, found_pda, found_pda)

        payload = {
            "source": "remote",
            "kind": kind,
            "status": "New link fetched from Samsung servers",
            "model": model,
            "csc": csc,
            "imei": imei,
            "base": working_base,
            "found_pda": found_pda,
            "size": descriptor.get("size", 0),
            "security": descriptor.get("security") or "",
            "one_ui": descriptor.get("oneUiVersion") or descriptor.get("os_ver") or "",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "download_url": download_url,
            "filename": filename,
            "curl_command": build_curl_command(filename, download_url),
        }
        store_lookup_result(payload)
        return payload

    if isinstance(result, str) and "260" in result:
        return {
            "source": "remote",
            "kind": "uptodate",
            "status": f"No newer package found. Server replied with {result}.",
            "model": model,
            "csc": csc,
            "imei": imei,
            "base": working_base,
            "found_pda": working_base,
        }

    return {
        "source": "remote",
        "kind": "error",
        "status": str(result),
        "model": model,
        "csc": csc,
        "imei": imei,
        "base": working_base,
        "found_pda": "",
    }


def lookup_download_link(
    model: str,
    csc: str,
    imei: str,
    base: str | None,
    *,
    use_cache: bool = True,
) -> dict[str, Any]:
    if use_cache:
        cached = fetch_cached_result(model, csc, imei)
        if cached:
            queue_activity("cache", f"Reused cached link for {model} / {csc} / {imei[-4:]}.")
            return cached
        queue_activity("query", f"Cache miss for {model} / {csc}. Reaching Samsung endpoints.")
    else:
        queue_activity("query", f"Running a live Samsung lookup for {model} / {csc} / {imei[-4:]}.")

    result = perform_remote_lookup(model, csc, imei, base)
    if result["kind"] in {"update", "dm"}:
        queue_activity("hit", f"Fetched {result['found_pda']} for {model}.")
    elif result["kind"] == "uptodate":
        queue_activity("info", f"{model} is already on the newest package.")
    else:
        queue_activity("error", f"Lookup failed for {model}: {result['status']}")
    return result


def start_fota_task(model: str, csc: str, imei: str, base: str | None) -> str:
    task_id = create_task("fota", {"model": model, "csc": csc, "imei": imei, "base": base})

    def worker() -> None:
        try:
            update_task(task_id, message="Checking cache and Samsung endpoints...", progress=0.15)
            result = lookup_download_link(model, csc, imei, base)
            update_task(
                task_id,
                status="completed",
                message=result.get("status", "Finished"),
                progress=1.0,
                result=result,
            )
        except Exception as exc:
            update_task(task_id, status="failed", message=str(exc), error=str(exc), progress=1.0)
            queue_activity("error", f"FOTA task failed for {model}: {exc}")

    threading.Thread(target=worker, daemon=True).start()
    return task_id


def start_imei_task(model: str, csc: str, start_imei: str, base: str, attempts: int, step: int) -> str:
    task_id = create_task(
        "imei",
        {
            "model": model,
            "csc": csc,
            "start_imei": start_imei,
            "base": base,
            "attempts": attempts,
            "step": step,
        },
    )

    def worker() -> None:
        try:
            results: list[dict[str, Any]] = []
            last_hit: dict[str, Any] | None = None
            current = start_imei
            for index in range(attempts):
                update_task(
                    task_id,
                    message=f"Scanning {current} ({index + 1}/{attempts})",
                    progress=(index / max(attempts, 1)),
                    results=list(results),
                )
                response = lookup_download_link(model, csc, current, base)
                results.append(
                    {
                        "attempt": str(index + 1),
                        "imei": current,
                        "status": response.get("status", response.get("kind", "Unknown")),
                        "source": response.get("source", "remote"),
                        "firmware": response.get("found_pda", ""),
                        "kind": response.get("kind", ""),
                    }
                )
                if response.get("kind") in {"update", "dm"} and last_hit is None:
                    last_hit = response
                current = increment_imei(current, step)

            update_task(
                task_id,
                status="completed",
                message=f"Completed {attempts} IMEI attempts.",
                progress=1.0,
                result=last_hit,
                results=results,
            )
        except Exception as exc:
            update_task(task_id, status="failed", message=str(exc), error=str(exc), progress=1.0)
            queue_activity("error", f"IMEI task failed for {model}: {exc}")

    threading.Thread(target=worker, daemon=True).start()
    return task_id


def check_endpoint(host: str, port: int = 443, timeout: float = 3.0) -> tuple[bool, str]:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True, f"{host}:{port} reachable"
    except OSError as exc:
        return False, str(exc)


def get_probe_device(catalog: dict[str, list[dict[str, Any]]]) -> dict[str, Any] | None:
    for entries in catalog.values():
        for entry in entries:
            if entry.get("model") == "SM-X706B" and entry.get("csc") == "EUX":
                return entry
    return None


def probe_fota_endpoint(catalog: dict[str, list[dict[str, Any]]]) -> tuple[bool, str]:
    probe = get_probe_device(catalog)
    if not probe:
        return False, "Tab S8 5G probe preset missing"

    model = str(probe.get("model", "")).strip().upper()
    csc = str(probe.get("csc", "")).strip().upper()
    imei = str(probe.get("imei", "")).strip()
    base = str(probe.get("base", "")).strip()

    if not model or not csc or not imei or not base:
        return False, "Tab S8 5G probe preset incomplete"

    try:
        mcc, mnc = ("460", "01") if csc in {"CHC", "CHM"} else ("310", "410")
        client = ota.Client(
            {
                "Model": model,
                "DeviceId": f"IMEI:{imei}",
                "CustomerCode": csc,
                "FirmwareVersion": base,
                "Registered": True,
                "Mcc": mcc,
                "Mnc": mnc,
            }
        )
        result = client.check_update(base)
        result_text = str(result or "").strip()
        lowered = result_text.lower()

        interrupted_markers = [
            "403",
            "auth_failed_banned",
            "http 403",
            "forbidden",
        ]
        if any(marker in lowered for marker in interrupted_markers):
            return False, f"Interrupted: {result_text}"

        healthy_markers = [
            "http://",
            "https://",
            "260",
            "220",
            "261",
            "no_pkg_marker",
            "bad_csc",
        ]
        if any(marker in lowered for marker in healthy_markers):
            return True, f"OTA probe responded: {result_text[:72]}"

        if result_text:
            return True, f"OTA probe responded: {result_text[:72]}"
        return False, "OTA probe returned no response"
    except Exception as exc:
        message = str(exc)
        lowered = message.lower()
        if "403" in lowered or "forbidden" in lowered:
            return False, f"Interrupted: {message}"
        return False, message


def collect_status_snapshot() -> dict[str, Any]:
    db_ok = DB_PATH.exists()
    devices_ok = DEVICES_PATH.exists()
    catalog = load_device_catalog() if devices_ok else {}
    fota_ok, fota_detail = probe_fota_endpoint(catalog) if devices_ok else (False, "devices.json missing")
    fumo_ok, fumo_detail = check_endpoint("fota-secure-dn.ospserver.net")
    dms_ok, dms_detail = check_endpoint("dms.ospserver.net")
    flat_devices = flatten_devices(catalog) if devices_ok else []
    total_devices = len(flat_devices)
    category_count = len(catalog)
    complete_devices = 0
    if devices_ok:
        for item in flat_devices:
            if item.get("model") and item.get("csc") and item.get("imei") and item.get("base"):
                complete_devices += 1

    counts = {
        "hits": with_db("SELECT COUNT(*) AS total FROM firmware_hits", one=True)["total"] if db_ok else 0,
        "fumo_hits": with_db(
            "SELECT COUNT(*) AS total FROM firmware_hits WHERE is_fumo = 1",
            one=True,
        )["total"]
        if db_ok
        else 0,
        "valid_imeis": with_db("SELECT COUNT(*) AS total FROM valid_imeis", one=True)["total"] if db_ok else 0,
    }

    checks = [
        {"title": "History DB", "ok": db_ok, "detail": "Connected" if db_ok else "Missing file"},
        {"title": "Device Catalog", "ok": devices_ok, "detail": f"{total_devices} presets ready" if devices_ok else "devices.json missing"},
        {"title": "FOTA Endpoint", "ok": fota_ok, "detail": fota_detail},
        {"title": "FUMO Endpoint", "ok": fumo_ok, "detail": fumo_detail},
        {"title": "DMS Endpoint", "ok": dms_ok, "detail": dms_detail},
    ]

    healthy = sum(1 for item in checks if item["ok"])
    endpoint_health = round((healthy / len(checks)) * 100, 1) if checks else 0.0
    imei_coverage = round(min(counts["valid_imeis"] / 100000, 1.0) * 100, 1)
    cache_depth = round((counts["fumo_hits"] / max(counts["hits"], 1)) * 100, 1)
    catalog_readiness = round((complete_devices / max(total_devices, 1)) * 100, 1)
    ip_alive = fota_ok and fumo_ok and dms_ok

    return {
        "checks": checks,
        "counts": counts,
        "total_devices": total_devices,
        "category_count": category_count,
        "complete_devices": complete_devices,
        "fota_ok": fota_ok,
        "fumo_ok": fumo_ok,
        "dms_ok": dms_ok,
        "ip_alive": ip_alive,
        "progress": [
            {"title": "Endpoint Health", "value": endpoint_health, "subtitle": f"{healthy}/{len(checks)} checks online"},
            {"title": "Catalog Readiness", "value": catalog_readiness, "subtitle": f"{complete_devices}/{total_devices or 0} presets complete"},
            {"title": "Valid IMEI Coverage", "value": imei_coverage, "subtitle": f"{counts['valid_imeis']:,} known IMEIs"},
            {"title": "FUMO Hit Share", "value": cache_depth, "subtitle": f"{counts['fumo_hits']:,} FUMO hits"},
        ],
    }


def refresh_snapshot(log_message: bool) -> None:
    st.session_state.status_snapshot = collect_status_snapshot()
    st.session_state.snapshot_time = datetime.now()
    if log_message:
        push_activity("sync", "Dashboard status checks refreshed.")


def render_dashboard_cards(snapshot: dict[str, Any]) -> None:
    counts = snapshot.get("counts", {})
    progress_map = {item["title"]: item for item in snapshot.get("progress", [])}
    cards = st.columns(4, gap="medium")

    with cards[0]:
        st.markdown(
            textwrap.dedent(
                f"""
                <section class="glass-card dashboard-card">
                    <div class="dashboard-card-title">Device Vault</div>
                    <div class="dashboard-line">Database History</div>
                    <div class="dashboard-status">{connection_text(DB_PATH.exists(), 'Connected', 'Missing')}</div>
                    <div class="dashboard-line">{snapshot.get('category_count', 0)} CSC entries loaded</div>
                    <div class="dashboard-line">{snapshot.get('total_devices', 0)} Devices found</div>
                    {render_metric_bar(
                        'Devices with Valid Recent Updates',
                        progress_map.get('Catalog Readiness', {}).get('value', 0.0),
                        f"{snapshot.get('complete_devices', 0)}/{snapshot.get('category_count', 0)} device variants have a valid firmware base",
                    )}
                </section>
                """
            ).strip(),
            unsafe_allow_html=True,
        )

    with cards[1]:
        st.markdown(
            textwrap.dedent(
                f"""
                <section class="glass-card dashboard-card">
                    <div class="dashboard-card-title">Unique Identifier</div>
                    <div class="dashboard-big-number">{counts.get('valid_imeis', 0):,}</div>
                    <div class="dashboard-line">Valid IMEIs</div>
                    {render_metric_bar(
                        'Current Valid IMEI Coverage',
                        progress_map.get('Valid IMEI Coverage', {}).get('value', 0.0),
                        progress_map.get('Valid IMEI Coverage', {}).get('subtitle', 'Coverage unavailable'),
                    )}
                </section>
                """
            ).strip(),
            unsafe_allow_html=True,
        )

    with cards[2]:
        st.markdown(
            textwrap.dedent(
                f"""
                <section class="glass-card dashboard-card">
                    <div class="dashboard-card-title">FUMO Records</div>
                    <div class="dashboard-big-number">{counts.get('fumo_hits', 0):,} IMEIs</div>
                    <div class="dashboard-line">Available for instant reuse</div>
                    {render_metric_bar(
                        'FUMO Hit Share',
                        progress_map.get('FUMO Hit Share', {}).get('value', 0.0),
                        progress_map.get('FUMO Hit Share', {}).get('subtitle', 'Share unavailable'),
                    )}
                    <div class="dashboard-divider"></div>
                    <div class="dashboard-line dashboard-strong">Quick Pulse</div>
                    <div class="dashboard-line">Cached firmware hits</div>
                    <div class="dashboard-big-number small">{counts.get('hits', 0):,}</div>
                    <div class="dashboard-line">{counts.get('fumo_hits', 0):,} flagged as FUMO-ready</div>
                </section>
                """
            ).strip(),
            unsafe_allow_html=True,
        )

    with cards[3]:
        st.markdown(
            textwrap.dedent(
                f"""
                <section class="glass-card dashboard-card">
                    <div class="dashboard-card-title">Connectivity</div>
                    <div class="dashboard-line">FOTA Endpoint</div>
                    <div class="dashboard-status">{connection_text(snapshot.get('fota_ok', False))}</div>
                    <div class="dashboard-line">DMS Endpoint</div>
                    <div class="dashboard-status">{connection_text(snapshot.get('dms_ok', False))}</div>
                    <div class="dashboard-line">IP Status</div>
                    <div class="dashboard-status">{'🟢 Alive' if snapshot.get('ip_alive') else '⛔ Interrupted'}</div>
                    <div class="progress-caption">IP status reflects FOTA, FUMO, and DMS reachability.</div>
                </section>
                """
            ).strip(),
            unsafe_allow_html=True,
        )


def render_guest_dashboard(snapshot: dict[str, Any], catalog: dict[str, list[dict[str, Any]]]) -> None:
    lower_left, lower_right = st.columns(2, gap="large")
    with lower_left:
        render_html_table(
            "Category Overview",
            ["Category", "Devices", "Ready"],
            [list(row) for row in category_rows(catalog)],
            "No device categories found.",
        )
    with lower_right:
        render_html_table(
            "Latest Cached Discoveries",
            ["Model", "CSC", "PDA", "Time"],
            [
                [row["device_model"], row["csc"], short_version(row["found_pda"]), row["timestamp"]]
                for row in latest_cached_discoveries(10)
            ],
            "No firmware history is available yet.",
        )


def render_decryption_results_card(result: dict[str, Any]) -> None:
    latest_found = str(result.get("latest_found", "") or "")
    previous_latest = str(result.get("previous_latest", "") or "")
    if latest_found:
        copy = f"Latest Firmware: {latest_found}"
        if previous_latest and latest_found != previous_latest and firmware_sort_key(latest_found) > firmware_sort_key(previous_latest):
            copy += f" • newer than recorded {previous_latest}"
        st.markdown(
            f"""
            <section class="glass-card decrypt-highlight-card">
                <div class="section-kicker">Latest Firmware</div>
                <div class="result-title">{html.escape(latest_found)}</div>
                <div class="result-meta">{html.escape(copy)}</div>
            </section>
            """,
            unsafe_allow_html=True,
        )

    st.markdown(
        f"""
        <section class="glass-card result-card">
            <div class="result-top">
                <div>
                    <div class="section-kicker">Decryption Result</div>
                    <div class="result-title">{html.escape(result.get('model', 'Unknown'))} / {html.escape(result.get('csc', 'UNK'))}</div>
                    <div class="result-meta">Base CSC: {html.escape(result.get('base_csc') or result.get('csc', 'UNK'))} • Android: {html.escape(result.get('android') or 'Unknown')}</div>
                </div>
                <span class="pill badge-indigo">{result.get('resolved_count', 0)} Resolved</span>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    st.divider()
    info_cols = st.columns(4, gap="medium")
    info_cols[0].metric("Latest Stable", short_version(result.get("latest_stable") or ""))
    info_cols[1].metric("Server MD5s", str(result.get("server_md5s", 0)))
    info_cols[2].metric("Resolved", str(result.get("resolved_count", 0)))
    info_cols[3].metric("Unresolved", str(result.get("unresolved_count", 0)))
    st.markdown("<div class='section-spacer-sm'></div>", unsafe_allow_html=True)


def render_decryption_firmware_table(
    model: str,
    csc: str,
    highlight_version: str = "",
    highlight_versions: set[str] | None = None,
) -> None:
    rows = with_decrypt_db(
        """
        SELECT firmware_found, security_patch_date, release_type, build_type, date_discovered
        FROM firmware_decryptions
        WHERE device_model = ?
          AND (? = '' OR csc = ?)
        ORDER BY year_value DESC, month_value DESC, firmware_found DESC, datetime(date_discovered) DESC, id DESC
        """,
        (normalize_model_number(model), normalize_csc_code(csc), normalize_csc_code(csc)),
    )
    st.markdown("<div class='section-spacer-sm'></div>", unsafe_allow_html=True)
    st.markdown(
        """
        <section class="glass-card table-card">
            <div class="section-kicker">Discovered / Decrypted Firmware</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    if not rows:
        st.info("No decrypted firmware has been recorded for this model yet.")
        return

    total_pages = max(1, ceil(len(rows) / 10))
    page_cols = st.columns([1, 4], gap="medium")
    with page_cols[0]:
        page = st.number_input(
            "Page",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key=f"decrypt_history_page_{normalize_model_number(model)}_{normalize_csc_code(csc) or 'all'}",
        )
    with page_cols[1]:
        st.markdown(
            f"<div class='history-page-note'>Showing page {page} of {total_pages} • 10 firmware per page</div>",
            unsafe_allow_html=True,
        )

    st.markdown(
        """
        <div class="history-grid-header decrypt-grid-header">
            <span>Triplet</span>
            <span>Security Patch</span>
            <span>Release Type</span>
            <span>Build Type</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    start = (page - 1) * 10
    highlight_set = {str(highlight_version or "").strip()} if highlight_version else set()
    if highlight_versions:
        highlight_set.update({str(version or "").strip() for version in highlight_versions if str(version or "").strip()})
    for row in rows[start : start + 10]:
        version = str(row["firmware_found"] or "").strip()
        row_class = "history-list-row decrypt-grid-row highlighted" if version in highlight_set else "history-list-row decrypt-grid-row"
        st.markdown(
            textwrap.dedent(
                f"""
                <div class="{row_class}">
                    <span>{html.escape(str(row['firmware_found']))}</span>
                    <span>{html.escape(str(row['security_patch_date'] or 'Unknown'))}</span>
                    <span>{html.escape(str(row['release_type'] or 'Unknown'))}</span>
                    <span>{html.escape(str(row['build_type'] or 'Unknown'))}</span>
                </div>
                """
            ).strip(),
            unsafe_allow_html=True,
        )


def render_decryption_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    model_options = model_options_from_decrypt_db()
    st.markdown(
        """
        <div style="margin-bottom:0.9rem;">
            <span class="pill badge-indigo decrypt-title-pill">Decryption Tool</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("Select a known model or type one manually. The discovered firmware list loads immediately.")

    picker_cols = st.columns([1.4, 1.1], gap="medium")
    with picker_cols[0]:
        selected_model = st.selectbox(
            "Known Model",
            [""] + model_options,
            format_func=lambda value: "Select from database" if not value else value,
            key="decrypt_known_model",
        )
        manual_model = st.text_input("Manual Model Number", key="decrypt_model_input")
    with picker_cols[1]:
        effective_model = normalize_model_number(manual_model or selected_model)
        csc_options = csc_options_for_model(effective_model)
        selected_csc = st.selectbox(
            "Known CSC",
            [""] + csc_options,
            format_func=lambda value: "Select CSC" if not value else value,
            key="decrypt_known_csc",
        )
        manual_csc = st.text_input("Manual CSC", key="decrypt_csc_input")

    effective_csc = normalize_csc_code(manual_csc or selected_csc)
    if effective_model:
        ensure_known_device(effective_model, effective_csc)

    if st.button("Start Decryption", key="start_decryption", use_container_width=True):
        if not effective_model or not effective_csc:
            st.error("Model and CSC are required.")
        else:
            status_box = st.status("Starting decryption...", expanded=True)
            progress = st.progress(0)
            progress_subtitle = st.empty()
            progress_subtitle.caption("0.0%")

            def progress_callback(stage: str, completed: int, total: int, label: str) -> None:
                update_decryption_progress_ui(
                    stage,
                    completed,
                    total,
                    label,
                    status_box=status_box,
                    progress_bar=progress,
                    subtitle_box=progress_subtitle,
                )

            try:
                result = decrypt_device_live(effective_model, effective_csc, persist=True, progress_callback=progress_callback)
                progress.progress(100)
                status_box.update(label="Decryption complete", state="complete")
                st.session_state.decrypt_results = [result]
                st.session_state.decrypt_error = None
                st.session_state.decrypt_latest_key = f"{effective_model}|{effective_csc}|{result.get('latest_found', '')}"
                push_activity("info", decryption_completion_message(effective_model, effective_csc, result))
            except Exception as exc:
                st.session_state.decrypt_results = []
                st.session_state.decrypt_error = str(exc)
                progress.progress(100)
                status_box.update(label="Decryption failed", state="error")
                push_activity("error", f"Decryption failed for {effective_model} / {effective_csc}: {exc}")

    if st.session_state.get("decrypt_error"):
        st.error(str(st.session_state.get("decrypt_error")))

    results = st.session_state.get("decrypt_results", [])
    if results:
        render_decryption_results_card(results[0])

    if effective_model:
        highlight_version = ""
        highlight_versions: set[str] = set()
        if results:
            highlight_version = str(results[0].get("latest_found", "") or "")
            highlight_versions = {
                str(version or "").strip()
                for version in results[0].get("new_versions", [])
                if str(version or "").strip()
            }
        render_decryption_firmware_table(
            effective_model,
            effective_csc,
            highlight_version=highlight_version,
            highlight_versions=highlight_versions,
        )


def render_database_history_tab() -> None:
    model_options = [row["device_model"] for row in with_db("SELECT DISTINCT device_model FROM firmware_hits ORDER BY device_model")]
    selected_model = st.selectbox("Device Model", [""] + model_options, format_func=lambda value: "Select a model" if not value else value, key="library_model")
    st.markdown(
        """
        <section class="glass-card table-card">
            <div class="section-kicker">Library</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    if not selected_model:
        st.info("Select a device model number to load the library.")
        return

    rows = library_rows_for_model(selected_model)
    total_pages = max(1, ceil(len(rows) / 10))
    page_cols = st.columns([1, 4], gap="medium")
    with page_cols[0]:
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1, key="library_page")
    with page_cols[1]:
        st.markdown(
            f"<div class='history-page-note'>Showing page {page} of {total_pages} • 10 discoveries per page</div>",
            unsafe_allow_html=True,
        )

    st.markdown(
        """
        <div class="history-grid-header library-grid">
            <span>Time</span>
            <span>Model</span>
            <span>CSC</span>
            <span>Firmware Base</span>
            <span>Firmware Found</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if not rows:
        st.info("No firmware discoveries are stored for this model yet.")
        return

    start = (page - 1) * 10
    for row in rows[start : start + 10]:
        row_html = textwrap.dedent(
            f"""
            <div class="history-list-row">
                <span>{html.escape(str(row['timestamp']))}</span>
                <span>{html.escape(str(row['device_model']))}</span>
                <span>{html.escape(str(row['csc']))}</span>
                <span>{html.escape(short_version(str(row['request_base_version'] or '')))}</span>
                <span>{html.escape(str(row['found_pda'] or '-'))}</span>
            </div>
            """
        ).strip()
        row_cols = st.columns([1, 0.13], gap="small")
        with row_cols[0]:
            st.markdown(row_html, unsafe_allow_html=True)
        with row_cols[1]:
            if st.button("Link", key=f"library_link_{row['id']}", use_container_width=True):
                show_history_detail_dialog(dict(row))


def render_device_vault_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    grouped = device_vault_rows()
    if not grouped:
        st.info("No devices are available in the vault yet.")
        return

    for model, rows in grouped.items():
        with st.expander(f"{model} ({len(rows)})", expanded=False):
            st.markdown(
                """
                <div class="history-grid-header vault-grid-header">
                    <span>CSC</span>
                    <span>Latest Decrypted Firmware</span>
                    <span>Date Decrypted</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
            for row in rows:
                st.markdown(
                    textwrap.dedent(
                        f"""
                        <div class="history-list-row vault-list-row">
                            <span>{html.escape(row['csc'])}</span>
                            <span>{html.escape(row['latest'])}</span>
                            <span>{html.escape(row['date'])}</span>
                        </div>
                        """
                    ).strip(),
                    unsafe_allow_html=True,
                )


def render_guest_device_vault_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    render_device_vault_tab(catalog)


def render_imei_database_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    model_options = model_options_from_decrypt_db()
    selected_model = st.selectbox(
        "Device Model",
        [""] + model_options,
        format_func=lambda value: "Select a model" if not value else value,
        key="imei_db_model_v2",
    )
    manual_model = st.text_input("Manual Model Number", key="imei_db_manual_model")
    effective_model = normalize_model_number(manual_model or selected_model)
    if not effective_model:
        st.info("Select or type a model number to open its IMEI database.")
        return

    csc_options = csc_options_for_model(effective_model, include_all=True)
    selected_csc = st.selectbox("CSC", csc_options or ["All CSCs"], key="imei_db_csc_v2")

    top_actions = st.columns([1.1, 3.5], gap="medium")
    with top_actions[0]:
        if st.button("Read FUMO History", key="imei_db_read_history", use_container_width=True):
            imported = import_fumo_history_to_imei_db(effective_model, selected_csc)
            if imported:
                push_activity("sync", f"Imported {imported} IMEIs from FUMO History into {effective_model}.")
            else:
                push_activity("info", f"No new IMEIs were imported for {effective_model}.")
            st.rerun()

    rows = imei_database_rows_v2(effective_model, selected_csc)
    st.markdown(
        """
        <section class="glass-card table-card">
            <div class="section-kicker">IMEI Database</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    if not rows:
        st.info("No IMEIs are stored for this model yet.")
        return

    header_cols = st.columns([1.4, 0.7, 1.45, 0.8, 0.95], gap="small")
    headers = ["IMEI", "CSC", "Firmware Hit", "Amount of Hit", "Action"]
    for col, label in zip(header_cols, headers):
        col.markdown(f"<div class='imei-db-header'>{html.escape(label)}</div>", unsafe_allow_html=True)

    default_csc = normalize_csc_code(selected_csc if selected_csc != "All CSCs" else csc_options_for_model(effective_model)[0] if csc_options_for_model(effective_model) else "")
    for idx, row in enumerate(rows):
        cols = st.columns([1.4, 0.7, 1.45, 0.8, 0.95], gap="small")
        cols[0].markdown(f"<div class='imei-db-cell'>{html.escape(mask_imei(row['imei']))}</div>", unsafe_allow_html=True)
        cols[1].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['csc']))}</div>", unsafe_allow_html=True)
        cols[2].markdown(
            f"<div class='imei-db-cell'>{html.escape(short_version(str(row['firmware_hit'] or '')) or '-')}</div>",
            unsafe_allow_html=True,
        )
        cols[3].markdown(f"<div class='imei-db-cell'>{int(row['hit_count'] or 0)}</div>", unsafe_allow_html=True)
        with cols[4]:
            if st.button("USE IMEI", key=f"imei_db_use_v2_{idx}", use_container_width=True):
                target_csc = normalize_csc_code(str(row["csc"] or default_csc))
                if update_device_imei_by_model_csc(effective_model, target_csc, str(row["imei"]), "IMEI Database"):
                    st.rerun()


def render_terminal_tab(snapshot_text: str) -> None:
    st.markdown(
        f"""
        <section class="glass-card table-card">
            <div class="section-kicker">Snapshot</div>
            <div class="dashboard-big-number small">{html.escape(snapshot_text)}</div>
            <div class="progress-caption">The latest dashboard health snapshot is stored here for admin review.</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div class='section-spacer'></div>", unsafe_allow_html=True)
    st.markdown(
        """
        <section class="glass-card table-card">
            <div class="section-kicker">Terminal</div>
            <div class="progress-caption">App settings and deployment feedback routing will live here later.</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    st.toggle("Enable experimental feedback routing", value=False, disabled=True)
    st.text_area(
        "Feedback draft",
        value="GitHub feedback integration will be connected here later.",
        height=140,
        disabled=True,
    )


def latest_decrypted_versions(model: str, csc: str, limit: int = 10) -> list[str]:
    rows = with_decrypt_db(
        """
        SELECT firmware_found
        FROM firmware_decryptions
        WHERE device_model = ?
          AND (? = '' OR csc = ?)
        ORDER BY year_value DESC, month_value DESC, firmware_found DESC, datetime(date_discovered) DESC, id DESC
        LIMIT ?
        """,
        (normalize_model_number(model), normalize_csc_code(csc), normalize_csc_code(csc), limit),
    )
    return [str(row["firmware_found"]) for row in rows]


@st.dialog("Use Decryptor", width="medium")
def show_firmware_picker_dialog() -> None:
    request = st.session_state.get("firmware_picker_request")
    if not request:
        st.info("No decryptor request is active.")
        return

    model = normalize_model_number(request.get("model", ""))
    csc = normalize_csc_code(request.get("csc", ""))
    prefix = str(request.get("prefix", "picker"))
    if not request.get("versions"):
        status_box = st.status("Decrypting latest firmware list...", expanded=True)
        progress = st.progress(0)
        progress_subtitle = st.empty()
        progress_subtitle.caption("0.0%")

        def progress_callback(stage: str, completed: int, total: int, label: str) -> None:
            update_decryption_progress_ui(
                stage,
                completed,
                total,
                label,
                status_box=status_box,
                progress_bar=progress,
                subtitle_box=progress_subtitle,
            )

        try:
            result = decrypt_device_live(model, csc, persist=True, progress_callback=progress_callback)
            versions = [str(item.get("version", "")) for item in result.get("items", [])[:10] if item.get("version")]
            request["versions"] = versions
            st.session_state.firmware_picker_request = request
            progress.progress(100)
            status_box.update(label="Decryptor complete", state="complete")
        except Exception as exc:
            status_box.update(label="Decryptor failed", state="error")
            st.error(str(exc))
            return

    versions = request.get("versions", [])
    st.caption(f"Select a firmware base for {model} / {csc}.")
    if not versions:
        st.info("No decrypted firmware versions were produced.")
    else:
        display_full_triplet = prefix == "fota_v3"
        choice = st.radio(
            "Latest 10 firmwares",
            versions,
            format_func=(lambda value: value) if display_full_triplet else (lambda value: short_version(value)),
            key=f"{prefix}_firmware_picker_choice",
        )
        if st.button("Use Selected Firmware", key=f"{prefix}_use_picker_version", use_container_width=True):
            st.session_state[f"{prefix}_base"] = choice
            st.session_state.firmware_picker_request = None
            st.rerun()

    if st.button("Close", key=f"{prefix}_close_picker", use_container_width=True):
        st.session_state.firmware_picker_request = None
        st.rerun()


@st.dialog("Delta Scan", width="medium")
def show_delta_scan_dialog() -> None:
    request = st.session_state.get("delta_scan_request")
    if not request:
        st.info("No delta scan is active.")
        return

    if st.session_state.get("delta_scan_result") is None and st.session_state.get("delta_scan_error") is None:
        status_box = st.status("Running OTA delta scan...", expanded=True)
        progress = st.progress(0)
        rows: list[dict[str, Any]] = []
        versions = list(request.get("versions", []))
        model = request["model"]
        csc = request["csc"]
        imei = request["imei"]
        for index, base_version in enumerate(versions, start=1):
            progress.progress(int((index - 1) / max(len(versions), 1) * 100))
            status_box.write(f"Checking `{base_version}` ({index}/{len(versions)})")
            result = lookup_download_link(model, csc, imei, base_version, use_cache=False)
            rows.append(
                {
                    "timestamp": result.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    "device_model": model,
                    "csc": csc,
                    "request_base_version": base_version,
                    "found_pda": result.get("found_pda", ""),
                    "security_patch_date": format_security_patch_value(result.get("security") or infer_security_patch_from_version(result.get("found_pda", ""))),
                    "dm_url": result.get("download_url", "") if result.get("kind") == "dm" else "",
                    "fota_url": result.get("download_url", "") if result.get("kind") != "dm" else "",
                    "kind": result.get("kind", ""),
                }
            )
        st.session_state.delta_scan_result = rows
        progress.progress(100)
        status_box.update(label="Delta scan complete", state="complete")

    if st.session_state.get("delta_scan_error"):
        st.error(str(st.session_state.get("delta_scan_error")))
    else:
        rows = st.session_state.get("delta_scan_result", [])
        if not rows:
            st.info("No delta scan results are available.")
        else:
            header = st.columns([1.5, 1.5, 1.1, 0.85], gap="small")
            for col, label in zip(header, ["Firmware base", "Firmware Update Found", "Security Patch Date", "Action"]):
                col.markdown(f"<div class='imei-db-header'>{html.escape(label)}</div>", unsafe_allow_html=True)
            for idx, row in enumerate(rows):
                cols = st.columns([1.45, 1.65, 1.05, 0.85], gap="small")
                cols[0].markdown(
                    f"<div class='imei-db-cell'>{html.escape(str(row['request_base_version'] or '-'))}</div>",
                    unsafe_allow_html=True,
                )
                cols[1].markdown(
                    f"<div class='imei-db-cell'>{html.escape(str(row['found_pda'] or '-'))}</div>",
                    unsafe_allow_html=True,
                )
                cols[2].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['security_patch_date'] or 'Unknown'))}</div>", unsafe_allow_html=True)
                with cols[3]:
                    if row.get("fota_url") or row.get("dm_url"):
                        if st.button("Link", key=f"delta_scan_link_{idx}", use_container_width=True):
                            show_history_detail_dialog(dict(row))
                    else:
                        st.markdown("<div class='imei-db-cell'>-</div>", unsafe_allow_html=True)

    if st.button("Close", key="delta_scan_close", use_container_width=True):
        st.session_state.delta_scan_request = None
        st.session_state.delta_scan_result = None
        st.session_state.delta_scan_error = None
        st.rerun()


def scanner_device_input(prefix: str) -> tuple[str, str, str, str]:
    mode = st.toggle("Use database device source", key=f"{prefix}_use_db")
    model_options = model_options_from_decrypt_db()
    model = ""
    csc = ""
    imei = ""
    base = ""
    left, right = st.columns(2, gap="medium")
    with left:
        selected_model = st.selectbox(
            "Device Model (Database)",
            [""] + model_options,
            format_func=lambda value: "Select a model" if not value else value,
            key=f"{prefix}_selected_model",
            disabled=not mode,
        )
        manual_model = st.text_input(
            "Device Model (Manual)",
            key=f"{prefix}_manual_model",
            disabled=mode,
        )
    resolved_model = normalize_model_number(selected_model if mode else manual_model)
    with right:
        csc_options = csc_options_for_model(resolved_model)
        selected_csc = st.selectbox(
            "CSC (Database)",
            [""] + csc_options,
            format_func=lambda value: "Select a CSC" if not value else value,
            key=f"{prefix}_selected_csc",
            disabled=not mode,
        )
        manual_csc = st.text_input(
            "CSC (Manual)",
            key=f"{prefix}_manual_csc",
            disabled=mode,
        )
    model = resolved_model
    csc = normalize_csc_code(selected_csc if mode else manual_csc)
    if model:
        context = best_device_context(model, csc)
        imei = context.get("imei", "")
        base = context.get("base", "")
    return model, csc, imei, base


def render_fota_tab(catalog: dict[str, list[dict[str, Any]]], *, guest_mode: bool = False) -> None:
    model, csc, db_imei, db_base = scanner_device_input("fota_v2")
    ensure_known_device(model, csc, imei=db_imei, base=db_base)
    base_source = st.radio("Base Firmware Source", ["Manual", "Decrypted Database", "Use decryptor"], horizontal=True, key="fota_base_source")
    available_bases = latest_decrypted_versions(model, csc, limit=10) if model and csc else []
    if base_source == "Manual":
        base = st.text_input("Base Firmware", value=db_base, key="fota_base_manual")
    elif base_source == "Decrypted Database":
        base = st.selectbox("Base Firmware", [""] + available_bases, key="fota_base_db")
    else:
        current_value = st.session_state.get("fota_v2_base", db_base)
        st.text_input("Base Firmware", value=current_value, disabled=True, key="fota_base_picker_display")
        if st.button("Use decryptor", key="fota_open_decryptor", use_container_width=True):
            st.session_state.firmware_picker_request = {"prefix": "fota_v2", "model": model, "csc": csc}
            st.rerun()
        base = current_value

    if guest_mode:
        imei = db_imei
        st.text_input("IMEI", value="Locked in Guest Mode", disabled=True)
    else:
        imei_source = st.radio("IMEI Source", ["Database", "Manual", "Scan for IMEI"], horizontal=True, key="fota_imei_source")
        if imei_source == "Database":
            imei = st.text_input("IMEI", value=db_imei, key="fota_imei_db")
        elif imei_source == "Manual":
            imei = st.text_input("IMEI", value=db_imei, key="fota_imei_manual")
        else:
            imei = st.text_input("IMEI", value=st.session_state.get("fota_scanned_imei", db_imei), key="fota_imei_scanned")
            if st.button("Scan for IMEI", key="fota_scan_for_imei", use_container_width=True):
                st.session_state.imei_scan_results = []
                st.session_state.imei_last_hit = None
                st.session_state.imei_live_request = {
                    "model": model,
                    "csc": csc,
                    "start_imei": db_imei or imei,
                    "base": base,
                    "attempts": 50,
                    "step": 4,
                }
                st.session_state.imei_live_result = None
                st.session_state.imei_live_error = None
                st.rerun()
            live_result = st.session_state.get("imei_live_result") or {}
            hits = live_result.get("hits", [])
            if hits:
                hit_options = {f"{item['imei']} - Found {short_version(item['firmware'])}!": item["imei"] for item in hits}
                choice = st.selectbox("Scanned IMEI Hits", list(hit_options.keys()), key="fota_scanned_imei_choice")
                if st.button("Use Scanned IMEI", key="fota_use_scanned_imei", use_container_width=True):
                    selected_imei = hit_options[choice]
                    st.session_state.fota_scanned_imei = selected_imei
                    imei = selected_imei
                    st.rerun()
            elif st.session_state.get("imei_live_result") and not hits:
                st.info("No HIT IMEIs were found. It is recommended to use the IMEI from the database.")

    if st.button("Fetch Download Link", key="fota_fetch_live", use_container_width=True):
        if not model or not csc or not imei:
            st.error("Model, CSC, and IMEI are required.")
        elif not imei.isdigit() or len(imei) != 15:
            st.error("IMEI must be exactly 15 digits.")
        else:
            ensure_known_device(model, csc, imei=imei, base=base)
            st.session_state.fota_live_request = {"model": model, "csc": csc, "imei": imei, "base": base}
            st.session_state.fota_live_result = None
            st.session_state.fota_live_error = None
            st.rerun()

    render_result_panel(st.session_state.get("last_result"))
    if st.session_state.get("firmware_picker_request"):
        show_firmware_picker_dialog()
    if st.session_state.get("fota_live_request") is not None:
        show_fota_fetch_dialog()
    if st.session_state.get("imei_live_request") is not None and not guest_mode:
        show_imei_scan_dialog()


def render_imei_scanner_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    model, csc, db_imei, db_base = scanner_device_input("imei_v2")
    ensure_known_device(model, csc, imei=db_imei, base=db_base)
    base_source = st.radio("Base Firmware Source", ["Manual", "Decrypted Database", "Use decryptor"], horizontal=True, key="imei_base_source_v2")
    available_bases = latest_decrypted_versions(model, csc, limit=10) if model and csc else []
    if base_source == "Manual":
        base = st.text_input("Base Firmware", value=db_base, key="imei_base_manual_v2")
    elif base_source == "Decrypted Database":
        base = st.selectbox("Base Firmware", [""] + available_bases, key="imei_base_db_v2")
    else:
        current_value = st.session_state.get("imei_v2_base", db_base)
        st.text_input("Base Firmware", value=current_value, disabled=True, key="imei_base_picker_display")
        if st.button("Use decryptor", key="imei_open_decryptor", use_container_width=True):
            st.session_state.firmware_picker_request = {"prefix": "imei_v2", "model": model, "csc": csc}
            st.rerun()
        base = current_value

    left, right = st.columns(2, gap="medium")
    with left:
        start_imei = st.text_input("Start IMEI", value=db_imei, key="scan_imei")
    with right:
        step = st.number_input("Thread [Recommended: 4]", min_value=1, max_value=999, value=4, step=1, key="scan_step")
    attempts = st.number_input("No. of IMEI", min_value=1, max_value=50, value=50, step=1, key="scan_attempts")

    if st.button("Start IMEI Scan", key="start_imei_scan_v2", use_container_width=True):
        if not model or not csc or not base or not start_imei:
            st.error("Model, CSC, start IMEI, and base firmware are required.")
        elif not start_imei.isdigit() or len(start_imei) != 15:
            st.error("Start IMEI must be exactly 15 digits.")
        else:
            st.session_state.imei_scan_results = []
            st.session_state.imei_last_hit = None
            st.session_state.imei_live_request = {
                "model": model,
                "csc": csc,
                "start_imei": start_imei,
                "base": base,
                "attempts": int(attempts),
                "step": int(step),
            }
            st.session_state.imei_live_result = None
            st.session_state.imei_live_error = None
            st.rerun()

    st.divider()
    rows = st.session_state.get("imei_scan_results", [])
    if rows:
        header_cols = st.columns([0.7, 1.2, 1.25, 0.9, 1.35, 0.95], gap="small")
        headers = ["Attempt", "IMEI", "Status", "Source", "Firmware", "Action"]
        for col, label in zip(header_cols, headers):
            col.markdown(f"<div class='imei-db-header'>{html.escape(label)}</div>", unsafe_allow_html=True)
        for idx, row in enumerate(rows):
            cols = st.columns([0.7, 1.2, 1.2, 0.9, 1.45, 1.0], gap="small")
            cols[0].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['attempt']))}</div>", unsafe_allow_html=True)
            cols[1].markdown(f"<div class='imei-db-cell'>{html.escape(mask_imei(str(row['imei'])))}</div>", unsafe_allow_html=True)
            cols[2].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['status']))}</div>", unsafe_allow_html=True)
            cols[3].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['source']))}</div>", unsafe_allow_html=True)
            cols[4].markdown(f"<div class='imei-db-cell'>{html.escape(short_version(str(row['firmware'] or '')))}</div>", unsafe_allow_html=True)
            with cols[5]:
                if st.button("USE IMEI", key=f"scan_use_imei_v2_{idx}", use_container_width=True):
                    if update_device_imei_by_model_csc(model, csc, str(row["imei"]), "IMEI Scanner"):
                        st.rerun()
    else:
        st.info("Run a scan to see IMEI results here.")

    if st.session_state.get("imei_live_request") is not None:
        show_imei_scan_dialog()


def render_night_patrol_tab() -> None:
    st.markdown(
        """
        <section class="glass-card table-card">
            <div class="section-kicker">Night Patrol</div>
            <div class="progress-caption">Select up to three model / CSC pairs for repeating background decryption cycles, one device at a time, with one shared interval between completed cycles.</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    entries: list[dict[str, Any]] = []
    model_options = model_options_from_decrypt_db()
    interval_minutes = st.number_input(
        "Cycle Interval (min)",
        min_value=5,
        max_value=1440,
        value=60,
        step=5,
        key="patrol_cycle_interval",
    )
    st.markdown("<div class='section-spacer-sm'></div>", unsafe_allow_html=True)
    for idx in range(3):
        st.markdown(f"**Slot {idx + 1}**")
        cols = st.columns([1.35, 1.05], gap="medium")
        with cols[0]:
            selected_model = st.selectbox(
                "Known Model",
                [""] + model_options,
                format_func=lambda value: "Select a model" if not value else value,
                key=f"patrol_model_{idx}",
            )
            manual_model = st.text_input("Or type Model", key=f"patrol_manual_model_{idx}")
            model = normalize_model_number(manual_model or selected_model)
        with cols[1]:
            selected_csc = st.selectbox(
                "Known CSC",
                [""] + csc_options_for_model(model),
                format_func=lambda value: "Select CSC" if not value else value,
                key=f"patrol_csc_{idx}",
            )
            manual_csc = st.text_input("Or type CSC", key=f"patrol_manual_csc_{idx}")
            csc = normalize_csc_code(manual_csc or selected_csc)
        if model and csc:
            entries.append(
                {
                    "job_id": f"{model}|{csc}",
                    "model": model,
                    "csc": csc,
                    "interval_seconds": int(interval_minutes) * 60,
                    "status": "Scheduled",
                    "next_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
        st.markdown("<div class='section-spacer-sm'></div>", unsafe_allow_html=True)

    if st.button("Start", key="night_patrol_start", use_container_width=True):
        upsert_patrol_jobs(entries)
        ensure_patrol_workers()
        push_activity("sync", f"Night Patrol saved {len(entries)} selected devices.")
        st.rerun()

    rows = patrol_job_rows()
    if rows:
        st.markdown("<div class='section-spacer'></div>", unsafe_allow_html=True)
        header = st.columns([1.05, 0.65, 0.78, 1.05, 1.35, 0.85], gap="small")
        for col, label in zip(header, ["Model", "CSC", "Interval", "Next Run", "Status", "Action"]):
            col.markdown(f"<div class='imei-db-header'>{html.escape(label)}</div>", unsafe_allow_html=True)
        for row in rows:
            cols = st.columns([1.05, 0.65, 0.78, 1.05, 1.35, 0.85], gap="small")
            cols[0].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['device_model']))}</div>", unsafe_allow_html=True)
            cols[1].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['csc']))}</div>", unsafe_allow_html=True)
            cols[2].markdown(f"<div class='imei-db-cell'>{int(row['interval_seconds']) // 60} min</div>", unsafe_allow_html=True)
            cols[3].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['next_run'] or '-'))}</div>", unsafe_allow_html=True)
            cols[4].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['last_message'] or row['status'] or '-'))}</div>", unsafe_allow_html=True)
            with cols[5]:
                if int(row["enabled"] or 0) == 1:
                    if st.button("Stop", key=f"patrol_stop_{row['job_id']}", use_container_width=True):
                        if stop_patrol_job(str(row["job_id"])):
                            st.rerun()
                else:
                    st.markdown("<div class='imei-db-cell'>Stopped</div>", unsafe_allow_html=True)


def render_pathfinder_tab() -> None:
    model_options = model_options_from_decrypt_db()
    selected_model = st.selectbox(
        "Known Model",
        [""] + model_options,
        format_func=lambda value: "Select a model" if not value else value,
        key="pathfinder_model",
    )
    manual_model = st.text_input("Or type Model Number", key="pathfinder_manual_model")
    model = normalize_model_number(manual_model or selected_model)
    csc_options = csc_options_for_model(model)
    selected_csc = st.selectbox(
        "Known CSC",
        [""] + csc_options,
        format_func=lambda value: "Select a CSC" if not value else value,
        key="pathfinder_csc",
    )
    manual_csc = st.text_input("Or type CSC", key="pathfinder_manual_csc")
    csc = normalize_csc_code(manual_csc or selected_csc)
    versions = latest_decrypted_versions(model, csc, limit=10) if model and csc else []

    st.markdown(
        """
        <section class="glass-card table-card">
            <div class="section-kicker">Pathfinder</div>
            <div class="progress-caption">Select a model number from decrypted_firmware.db or type one manually. The latest 10 decrypted firmwares load below immediately.</div>
        </section>
        """,
        unsafe_allow_html=True,
    )

    if model:
        st.markdown("<div class='section-spacer-sm'></div>", unsafe_allow_html=True)
        rows = with_decrypt_db(
            """
            SELECT firmware_found, security_patch_date, release_type, build_type
            FROM firmware_decryptions
            WHERE device_model = ?
              AND (? = '' OR csc = ?)
            ORDER BY year_value DESC, month_value DESC, firmware_found DESC, datetime(date_discovered) DESC, id DESC
            LIMIT 10
            """,
            (model, csc, csc),
        )
        if not rows:
            st.warning("Nothing has been decrypted for this device yet. Run decryption here to obtain the latest 10 firmwares.")
            action_cols = st.columns([1.3, 1.6, 1.3], gap="medium")
            with action_cols[1]:
                if st.button("Run Decryption Here", key="pathfinder_run_decryption", use_container_width=True):
                    if not csc:
                        st.error("CSC is required before running decryption.")
                    else:
                        push_activity("info", "A user is using Decryption tool. Performance might get impacted.")
                        status_box = st.status("Starting decryption...", expanded=True)
                        progress = st.progress(0)
                        progress_subtitle = st.empty()
                        progress_subtitle.caption("0.0%")

                        def progress_callback(stage: str, completed: int, total: int, label: str) -> None:
                            update_decryption_progress_ui(
                                stage,
                                completed,
                                total,
                                label,
                                status_box=status_box,
                                progress_bar=progress,
                                subtitle_box=progress_subtitle,
                            )

                        try:
                            result = decrypt_device_live(model, csc, persist=True, progress_callback=progress_callback)
                            progress.progress(100)
                            status_box.update(label="Decryption complete", state="complete")
                            st.session_state.decrypt_results = [result]
                            st.session_state.decrypt_error = None
                            st.session_state.decrypt_latest_key = f"{model}|{csc}|{result.get('latest_found', '')}"
                            push_activity("info", decryption_completion_message(model, csc, result))
                            st.rerun()
                        except Exception as exc:
                            progress.progress(100)
                            status_box.update(label="Decryption failed", state="error")
                            st.error(str(exc))
                            push_activity("error", f"Pathfinder decryption failed for {model} / {csc}: {exc}")
        else:
            header = st.columns(4, gap="small")
            for col, label in zip(header, ["Triplet", "Security Patch", "Release Type", "Build Type"]):
                col.markdown(f"<div class='imei-db-header'>{html.escape(label)}</div>", unsafe_allow_html=True)
            for row in rows:
                cols = st.columns([2.2, 1.05, 1.0, 0.95], gap="small")
                cols[0].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['firmware_found']))}</div>", unsafe_allow_html=True)
                cols[1].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['security_patch_date'] or 'Unknown'))}</div>", unsafe_allow_html=True)
                cols[2].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['release_type'] or 'Unknown'))}</div>", unsafe_allow_html=True)
                cols[3].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['build_type'] or 'Unknown'))}</div>", unsafe_allow_html=True)
            st.caption("Showing the latest 10 decrypted firmwares.")

    if st.button("Delta Scan", key="pathfinder_delta_scan", use_container_width=True):
        context = best_device_context(model, csc)
        imei = context.get("imei", "")
        if not model:
            st.error("Model number is required.")
        elif not csc:
            st.error("CSC is required for Delta Scan.")
        elif not imei:
            st.error("An IMEI source is required for Delta Scan.")
        elif not versions:
            st.error("No decrypted firmware is available yet. Run the Decryption tool first.")
        else:
            st.session_state.delta_scan_request = {"model": model, "csc": csc, "imei": imei, "versions": versions[:10]}
            st.session_state.delta_scan_result = None
            st.session_state.delta_scan_error = None
            st.rerun()

    if st.session_state.get("delta_scan_request") is not None:
        show_delta_scan_dialog()


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --bg-main: #edf2fb;
            --bg-gradient-1: rgba(110, 155, 255, 0.18);
            --bg-gradient-2: rgba(128, 224, 255, 0.16);
            --card-bg: rgba(255, 255, 255, 0.76);
            --card-soft: rgba(248, 251, 255, 0.64);
            --card-stroke: rgba(255, 255, 255, 0.92);
            --text-main: #101b2b;
            --text-soft: #617089;
            --text-muted: #7a879e;
            --accent: #5d8dff;
            --accent-strong: #2f6dff;
            --accent-soft: rgba(93, 141, 255, 0.18);
            --success: #19b56b;
            --warn: #e1a100;
            --danger: #d04f68;
            --shadow: 0 22px 48px rgba(62, 84, 129, 0.15);
            --line: rgba(133, 151, 184, 0.25);
            --sidebar-width: 320px;
        }

        @media (prefers-color-scheme: dark) {
            :root {
                --bg-main: #0d1117;
                --bg-gradient-1: rgba(84, 109, 163, 0.26);
                --bg-gradient-2: rgba(46, 89, 140, 0.24);
                --card-bg: rgba(23, 29, 39, 0.82);
                --card-soft: rgba(28, 35, 47, 0.7);
                --card-stroke: rgba(255, 255, 255, 0.06);
                --text-main: #f3f6fb;
                --text-soft: #a7b2c5;
                --text-muted: #8a95a8;
                --accent: #8caeff;
                --accent-strong: #a9c1ff;
                --accent-soft: rgba(115, 151, 255, 0.22);
                --success: #37d58b;
                --warn: #ffc247;
                --danger: #ff7b91;
                --shadow: 0 24px 56px rgba(0, 0, 0, 0.4);
                --line: rgba(255, 255, 255, 0.08);
            }
        }

        html[data-theme="dark"], body[data-theme="dark"] {
            --bg-main: #0d1117;
            --bg-gradient-1: rgba(84, 109, 163, 0.26);
            --bg-gradient-2: rgba(46, 89, 140, 0.24);
            --card-bg: rgba(23, 29, 39, 0.82);
            --card-soft: rgba(28, 35, 47, 0.7);
            --card-stroke: rgba(255, 255, 255, 0.06);
            --text-main: #f3f6fb;
            --text-soft: #a7b2c5;
            --text-muted: #8a95a8;
            --accent: #8caeff;
            --accent-strong: #a9c1ff;
            --accent-soft: rgba(115, 151, 255, 0.22);
            --success: #37d58b;
            --warn: #ffc247;
            --danger: #ff7b91;
            --shadow: 0 24px 56px rgba(0, 0, 0, 0.4);
            --line: rgba(255, 255, 255, 0.08);
        }

        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top left, var(--bg-gradient-1), transparent 28%),
                radial-gradient(circle at top right, var(--bg-gradient-2), transparent 26%),
                var(--bg-main);
            color: var(--text-main);
            font-family: "Segoe UI Variable Display", "Segoe UI", sans-serif;
        }

        [data-testid="stHeader"] { background: transparent; }

        .block-container {
            padding-top: 1.55rem;
            padding-bottom: 10rem;
            max-width: 1440px;
        }

        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, var(--card-bg), var(--card-soft));
            border-right: 1px solid var(--card-stroke);
            box-shadow: 10px 0 30px rgba(0, 0, 0, 0.08);
        }

        [data-testid="stSidebar"] > div:first-child { width: 320px; }
        [data-testid="stSidebar"] > div:first-child,
        [data-testid="stSidebar"] [data-testid="stSidebarContent"] {
            background: transparent;
        }

        [data-testid="stSidebarContent"] {
            padding-top: 1.2rem;
            padding-bottom: 1.2rem;
        }

        * { color: inherit; }

        .glass-card,
        .oneui-header,
        .activity-dock,
        .left-pane-shell {
            background: var(--card-bg);
            border: 1px solid var(--card-stroke);
            box-shadow: var(--shadow);
            backdrop-filter: blur(24px);
            -webkit-backdrop-filter: blur(24px);
        }

        .glass-card {
            border-radius: 28px;
            padding: 1.15rem 1.2rem;
            color: var(--text-main);
        }

        .oneui-header {
            border-radius: 30px;
            padding: 1.05rem 1.35rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
        }

        .header-title {
            font-size: 1.85rem;
            font-weight: 800;
            letter-spacing: -0.05em;
            color: var(--text-main);
        }

        .header-subtitle,
        .progress-caption,
        .left-pane-copy,
        .result-meta,
        .history-page-note {
            color: var(--text-soft);
            line-height: 1.55;
        }

        .header-chip,
        .pill {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border-radius: 999px;
            padding: 0.42rem 0.9rem;
            font-weight: 700;
            font-size: 0.88rem;
            background: var(--accent-soft);
            color: var(--accent-strong);
            border: 1px solid rgba(255, 255, 255, 0.08);
        }

        .decrypt-title-pill {
            font-size: 2rem;
            padding: 0.9rem 1.55rem;
            border-radius: 999px;
        }

        .badge-green { background: rgba(27, 181, 107, 0.16); color: var(--success); }
        .badge-indigo, .badge-blue, .badge-cyan { background: var(--accent-soft); color: var(--accent-strong); }
        .badge-silver { background: rgba(120, 134, 160, 0.16); color: var(--text-soft); }
        .badge-red { background: rgba(208, 79, 104, 0.16); color: var(--danger); }

        .section-kicker,
        .dashboard-card-title,
        .left-pane-title,
        .vault-device-name,
        .activity-title {
            color: var(--text-main);
            font-weight: 780;
            letter-spacing: -0.02em;
        }

        .dashboard-card { min-height: 310px; }
        .dashboard-line, .vault-device-line, .dashboard-status { color: var(--text-soft); margin-top: 0.35rem; }
        .dashboard-big-number {
            font-size: 2rem;
            font-weight: 800;
            letter-spacing: -0.05em;
            color: var(--text-main);
            margin-top: 0.15rem;
        }
        .dashboard-big-number.small { font-size: 1.6rem; }
        .dashboard-divider {
            height: 1px;
            background: var(--line);
            margin: 0.95rem 0;
        }
        .dashboard-strong { color: var(--text-main); font-weight: 700; }

        .progress-track {
            height: 10px;
            border-radius: 999px;
            background: rgba(125, 145, 184, 0.18);
            overflow: hidden;
            margin-top: 0.45rem;
        }

        .progress-fill {
            height: 100%;
            border-radius: inherit;
            background: linear-gradient(90deg, var(--accent), #72d2ff);
        }

        .oneui-table,
        .oneui-table th,
        .oneui-table td {
            color: var(--text-main);
        }

        .oneui-table {
            width: 100%;
            border-collapse: separate;
            border-spacing: 0 10px;
        }
        .oneui-table th {
            text-align: center;
            color: var(--text-soft);
            font-size: 0.86rem;
            font-weight: 700;
        }
        .oneui-table td {
            text-align: center;
            background: rgba(255, 255, 255, 0.04);
            padding: 0.82rem 0.7rem;
            border-top: 1px solid var(--line);
            border-bottom: 1px solid var(--line);
        }
        .oneui-table td:first-child { border-left: 1px solid var(--line); border-radius: 16px 0 0 16px; }
        .oneui-table td:last-child { border-right: 1px solid var(--line); border-radius: 0 16px 16px 0; }

        .oneui-empty {
            padding: 1.2rem;
            text-align: center;
            color: var(--text-soft);
        }

        .result-title {
            font-size: 1.4rem;
            font-weight: 800;
            letter-spacing: -0.04em;
            color: var(--text-main);
        }
        .result-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.9rem;
            margin-top: 1rem;
        }
        .meta-label {
            display: block;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.04em;
            color: var(--text-muted);
            margin-bottom: 0.22rem;
        }
        .meta-value { color: var(--text-main); font-weight: 650; }

        .history-grid-header,
        .history-list-row,
        .imei-db-header,
        .imei-db-cell {
            display: grid;
            align-items: center;
            text-align: center;
            gap: 0.8rem;
        }

        .history-grid-header {
            grid-template-columns: repeat(5, minmax(0, 1fr));
            color: var(--text-soft);
            font-weight: 700;
            margin: 0.2rem 0 0.55rem;
            padding: 0 0.35rem;
        }
        .decrypt-grid-header { grid-template-columns: 2fr 1fr 1fr 1fr; }
        .vault-grid-header { grid-template-columns: 0.8fr 1.8fr 1.2fr; }

        .history-list-row {
            grid-template-columns: repeat(5, minmax(0, 1fr));
            padding: 0.86rem 0.65rem;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.04);
            border: 1px solid var(--line);
            color: var(--text-main);
            margin-bottom: 0.55rem;
        }
        .vault-list-row { grid-template-columns: 0.8fr 1.8fr 1.2fr; }
        .history-list-row.highlighted {
            background: rgba(93, 141, 255, 0.12);
            border-color: rgba(93, 141, 255, 0.35);
        }

        .imei-db-header {
            color: var(--text-soft);
            font-size: 0.86rem;
            font-weight: 700;
            padding-bottom: 0.28rem;
        }
        .imei-db-cell {
            min-height: 48px;
            border-radius: 16px;
            background: rgba(255, 255, 255, 0.04);
            border: 1px solid var(--line);
            color: var(--text-main);
            padding: 0.72rem 0.55rem;
        }

        .scan-status-row {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 0.8rem;
            color: var(--text-main);
        }
        .scan-status-main {
            display: inline-flex;
            align-items: center;
            gap: 0.6rem;
            font-weight: 650;
        }
        .scan-spinner {
            width: 14px;
            height: 14px;
            border: 2px solid rgba(255,255,255,0.18);
            border-top-color: var(--accent);
            border-radius: 50%;
            animation: kz-spin 0.9s linear infinite;
        }
        @keyframes kz-spin { to { transform: rotate(360deg); } }

        .scan-status-pill {
            border-radius: 999px;
            padding: 0.28rem 0.65rem;
            font-size: 0.78rem;
            font-weight: 800;
        }
        .scan-badge-hit { background: rgba(27,181,107,0.16); color: var(--success); }
        .scan-badge-valid { background: rgba(225,161,0,0.16); color: var(--warn); }
        .scan-badge-error { background: rgba(208,79,104,0.16); color: var(--danger); }
        .scan-badge-neutral { background: rgba(120,134,160,0.16); color: var(--text-soft); }

        .left-pane-shell {
            border-radius: 28px;
            padding: 1rem;
            margin-bottom: 1rem;
        }

        [data-testid="stSidebar"] div[role="radiogroup"] {
            gap: 0.65rem;
            display: grid;
        }
        [data-testid="stSidebar"] div[role="radiogroup"] label {
            width: 100%;
            border-radius: 18px;
            padding: 0.72rem 0.78rem;
            background: rgba(255,255,255,0.04);
            border: 1px solid var(--line);
        }

        .activity-dock {
            position: fixed;
            left: calc(320px + 2rem);
            right: 2rem;
            bottom: 1rem;
            border-radius: 26px;
            padding: 0.9rem 1rem;
            z-index: 20;
        }
        .activity-head {
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            margin-bottom: 0.7rem;
        }
        .activity-list {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.7rem;
        }
        .activity-item {
            display: grid;
            grid-template-columns: auto auto 1fr;
            align-items: center;
            gap: 0.5rem;
            padding: 0.7rem 0.75rem;
            border-radius: 16px;
            background: rgba(255,255,255,0.04);
            border: 1px solid var(--line);
        }
        .activity-time, .activity-message { color: var(--text-main); }

        .section-spacer { height: 1.05rem; }
        .section-spacer-sm { height: 0.55rem; }

        .stButton > button,
        .stDownloadButton > button,
        .stLinkButton > a {
            border-radius: 18px !important;
            border: 1px solid var(--line) !important;
            background: linear-gradient(180deg, rgba(255,255,255,0.07), rgba(255,255,255,0.03)) !important;
            color: var(--text-main) !important;
            font-weight: 700 !important;
            box-shadow: none !important;
        }

        .stTextInput input,
        .stTextArea textarea,
        .stNumberInput input,
        .stSelectbox [data-baseweb="select"] > div,
        .stMultiSelect [data-baseweb="select"] > div {
            background: rgba(255,255,255,0.05) !important;
            border: 1px solid var(--line) !important;
            color: var(--text-main) !important;
            border-radius: 16px !important;
        }

        label, .stMarkdown, .stCaption, .stRadio, .stSelectbox, .stToggle, .stTextInput, .stNumberInput {
            color: var(--text-main) !important;
        }

        @media (max-width: 1024px) {
            [data-testid="stSidebar"] > div:first-child { width: 280px; }
            .activity-dock { left: 1rem; right: 1rem; }
        }

        @media (max-width: 760px) {
            .oneui-header { flex-direction: column; align-items: flex-start; }
            .result-grid { grid-template-columns: 1fr; }
            .activity-list { grid-template-columns: 1fr; }
            .activity-dock { left: 0.8rem; right: 0.8rem; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

def logout_to_login() -> None:
    st.session_state.is_authenticated = False
    st.session_state.user_mode = None
    st.session_state.login_error = None
    st.session_state.secret_code_input = ""
    st.session_state.active_tab = "Dashboard"
    st.session_state.dialog_payload = None
    st.session_state.fota_live_request = None
    st.session_state.fota_live_result = None
    st.session_state.fota_live_error = None
    st.session_state.imei_live_request = None
    st.session_state.imei_live_result = None
    st.session_state.imei_live_error = None
    st.session_state.imei_live_state = None
    st.session_state.firmware_picker_request = None
    st.session_state.delta_scan_request = None
    st.session_state.delta_scan_result = None
    st.session_state.delta_scan_error = None
    st.session_state.fota_scanned_imei = ""
    st.session_state.logout_refresh_pending = True
    st.rerun()


def decryption_completion_message(model: str, csc: str, result: dict[str, Any]) -> str:
    clean_model = normalize_model_number(model)
    clean_csc = normalize_csc_code(csc)
    previous_latest = str(result.get("previous_latest", "") or "").strip()
    latest_found = str(result.get("latest_found", "") or "").strip()
    has_new_firmware = bool(
        latest_found
        and (
            not previous_latest
            or firmware_sort_key(latest_found) > firmware_sort_key(previous_latest)
        )
    )
    outcome = (
        "New firmware is found! Check out Decryption tab."
        if has_new_firmware
        else "No new firmware is found."
    )
    return f"Decryption ran by previous user for {clean_model} ({clean_csc}) is completed. {outcome}"


def parse_decryption_progress_label(label: str) -> tuple[str, int]:
    raw = str(label or "").strip()
    resolved = 0
    display = raw
    marker = "|resolved="
    if marker in raw:
        display, tail = raw.split(marker, 1)
        match = re.search(r"\d+", tail)
        if match:
            resolved = int(match.group(0))
    return display.strip(), max(resolved, 0)


def update_decryption_progress_ui(
    stage: str,
    completed: int,
    total: int,
    label: str,
    *,
    status_box: Any,
    progress_bar: Any,
    subtitle_box: Any | None = None,
) -> None:
    ratio = min(max(completed / max(total, 1), 0.0), 1.0)
    stage_map = {
        "prepare": "Preparing server MD5 list",
        "decrypt": "Decrypting firmware map",
        "finalize": "Finalizing database records",
    }
    display_label, _ = parse_decryption_progress_label(label)
    status_box.update(label=f"{stage_map.get(stage, stage.title())}: {display_label}", expanded=True)
    progress_bar.progress(int(ratio * 100))
    if subtitle_box is not None:
        subtitle_box.caption(f"{ratio * 100:.1f}%")


def latest_firmware_lookup() -> tuple[dict[tuple[str, str, str], str], dict[tuple[str, str], str]]:
    rows = with_db(
        """
        SELECT device_model, csc, imei, found_pda
        FROM firmware_hits
        WHERE found_pda IS NOT NULL
          AND found_pda != ''
        ORDER BY datetime(timestamp) DESC, id DESC
        """
    )

    exact_map: dict[tuple[str, str, str], str] = {}
    fallback_map: dict[tuple[str, str], str] = {}
    for row in rows:
        exact_key = (row["device_model"], row["csc"], row["imei"])
        fallback_key = (row["device_model"], row["csc"])
        exact_map.setdefault(exact_key, row["found_pda"])
        fallback_map.setdefault(fallback_key, row["found_pda"])
    return exact_map, fallback_map


def history_rows(limit: int | None = None) -> list[sqlite3.Row]:
    query = """
        SELECT
            id,
            finder_name,
            device_model,
            imei,
            csc,
            request_base_version,
            found_pda,
            is_fumo,
            fota_url,
            dm_url,
            raw_response,
            timestamp
        FROM firmware_hits
        ORDER BY datetime(timestamp) DESC, id DESC
    """
    if limit is None:
        return with_db(query)
    return with_db(f"{query} LIMIT ?", (limit,))


def recent_hits(limit: int = 8) -> list[sqlite3.Row]:
    return with_db(
        """
        SELECT device_model, csc, imei, found_pda, timestamp
        FROM firmware_hits
        ORDER BY datetime(timestamp) DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )


def category_rows(catalog: dict[str, list[dict[str, Any]]]) -> list[tuple[str, str, str]]:
    rows: list[tuple[str, str, str]] = []
    for category, entries in catalog.items():
        complete = sum(
            1
            for item in entries
            if item.get("model") and item.get("csc") and item.get("imei") and item.get("base")
        )
        rows.append((category, str(len(entries)), f"{complete}/{len(entries)} ready"))
    return rows


def level_badge(level: str) -> str:
    mapping = {
        "CACHE": "badge-blue",
        "QUERY": "badge-indigo",
        "HIT": "badge-green",
        "INFO": "badge-silver",
        "WARN": "badge-red",
        "ERROR": "badge-red",
        "SYNC": "badge-cyan",
    }
    return mapping.get(level.upper(), "badge-silver")


def render_html_table(title: str, headers: list[str], rows: list[list[str]], empty_text: str) -> None:
    if rows:
        head_html = "".join(f"<th>{html.escape(col)}</th>" for col in headers)
        body_html = "".join(
            "<tr>" + "".join(f"<td>{html.escape(value)}</td>" for value in row) + "</tr>"
            for row in rows
        )
        table_html = (
            f'<table class="oneui-table"><thead><tr>{head_html}</tr></thead>'
            f"<tbody>{body_html}</tbody></table>"
        )
    else:
        table_html = f'<div class="oneui-empty">{html.escape(empty_text)}</div>'

    st.markdown(
        f"""
        <section class="glass-card table-card">
            <div class="section-kicker">{html.escape(title)}</div>
            {table_html}
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_status_cards(snapshot: dict[str, Any]) -> None:
    cols = st.columns(4, gap="medium")
    for col, item in zip(cols, snapshot.get("checks", [])):
        state_class = "status-live" if item["ok"] else "status-down"
        with col:
            st.markdown(
                f"""
                <section class="glass-card status-card">
                    <div class="status-line">
                        <span class="status-dot {state_class}"></span>
                        <span class="status-title">{html.escape(item['title'])}</span>
                    </div>
                    <div class="status-detail">{html.escape(item['detail'])}</div>
                </section>
                """,
                unsafe_allow_html=True,
            )


def render_progress_cards(snapshot: dict[str, Any]) -> None:
    cols = st.columns(4, gap="medium")
    for col, item in zip(cols, snapshot.get("progress", [])):
        with col:
            st.markdown(
                f"""
                <section class="glass-card progress-card">
                    <div class="progress-head">
                        <span>{html.escape(item['title'])}</span>
                        <span>{item['value']:.1f}%</span>
                    </div>
                    <div class="progress-track">
                        <div class="progress-fill" style="width:{item['value']:.1f}%"></div>
                    </div>
                    <div class="progress-caption">{html.escape(item['subtitle'])}</div>
                </section>
                """,
                unsafe_allow_html=True,
            )


def render_result_panel(result: dict[str, Any] | None) -> None:
    if not result:
        return

    kind = result.get("kind", "info")
    badge_map = {
        "update": ("Live Link Ready", "badge-green"),
        "dm": ("DM Link Ready", "badge-indigo"),
        "uptodate": ("No Update", "badge-silver"),
        "error": ("Lookup Error", "badge-red"),
    }
    label, badge_class = badge_map.get(kind, ("Result", "badge-silver"))
    source = "Database cache" if result.get("source") == "cache" else "Samsung live lookup"

    st.markdown(
        f"""
        <section class="glass-card result-card">
            <div class="result-top">
                <div>
                    <div class="section-kicker">Lookup Result</div>
                    <div class="result-title">{html.escape(result.get('model', 'Unknown'))} / {html.escape(result.get('csc', 'UNK'))}</div>
                    <div class="result-meta">{html.escape(source)} • {html.escape(result.get('status', ''))}</div>
                </div>
                <span class="pill {badge_class}">{html.escape(label)}</span>
            </div>
            <div class="result-grid">
                <div><span class="meta-label">Base</span><span class="meta-value">{html.escape(result.get('base', 'Unknown') or 'Unknown')}</span></div>
                <div><span class="meta-label">Found PDA</span><span class="meta-value">{html.escape(result.get('found_pda', 'Unknown') or 'Unknown')}</span></div>
                <div><span class="meta-label">One UI</span><span class="meta-value">{html.escape(result.get('one_ui', '') or 'Unknown')}</span></div>
                <div><span class="meta-label">Package Size</span><span class="meta-value">{html.escape(readable_size(result.get('size'))) if result.get('size') else 'Unknown'}</span></div>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )

    if kind in {"update", "dm"} and result.get("curl_command"):
        actions = st.columns([1, 1.2, 1.2], gap="medium")
        with actions[0]:
            st.button(
                "Show Curl Command",
                key=f"show_curl_{result['model']}_{result['csc']}",
                use_container_width=True,
                on_click=lambda: st.session_state.update(dialog_payload=result),
            )
        with actions[1]:
            st.link_button(
                "Open Raw Link",
                result["download_url"],
                use_container_width=True,
            )
        with actions[2]:
            st.code(result["filename"], language="text")
    elif kind == "uptodate":
        st.info(result["status"])
    elif kind == "error":
        st.error(result["status"])


def connection_text(ok: bool, up_text: str = "Reachable", down_text: str = "Unavailable") -> str:
    return f"{'🟢' if ok else '⛔'} {up_text if ok else down_text}"


def render_metric_bar(title: str, value: float, subtitle: str) -> str:
    safe_title = html.escape(title)
    safe_subtitle = html.escape(subtitle)
    width = max(0.0, min(100.0, value))
    return textwrap.dedent(
        f"""
        <div class="mini-progress-block">
            <div class="mini-progress-top">
                <span>{safe_title}</span>
                <span>{width:.1f}%</span>
            </div>
            <div class="progress-track">
                <div class="progress-fill" style="width:{width:.1f}%"></div>
            </div>
            <div class="progress-caption">{safe_subtitle}</div>
        </div>
        """
    ).strip()


def render_dashboard_cards(snapshot: dict[str, Any]) -> None:
    counts = snapshot.get("counts", {})
    progress_map = {item["title"]: item for item in snapshot.get("progress", [])}
    cards = st.columns(4, gap="medium")

    with cards[0]:
        st.markdown(
            textwrap.dedent(
                f"""
                <section class="glass-card dashboard-card">
                    <div class="dashboard-card-title">Device Vault</div>
                    <div class="dashboard-line">{snapshot.get('category_count', 0)} Categories loaded</div>
                    <div class="dashboard-line">{snapshot.get('total_devices', 0)} Devices found</div>
                    {render_metric_bar(
                        'Devices with Valid Recent Updates',
                        progress_map.get('Catalog Readiness', {}).get('value', 0.0),
                        f"{snapshot.get('complete_devices', 0)}/{snapshot.get('total_devices', 0)} devices have a valid firmware base",
                    )}
                </section>
                """
            ).strip(),
            unsafe_allow_html=True,
        )

    with cards[1]:
        st.markdown(
            textwrap.dedent(
                f"""
                <section class="glass-card dashboard-card">
                    <div class="dashboard-card-title">Unique Identifier</div>
                    <div class="dashboard-big-number">{counts.get('valid_imeis', 0):,}</div>
                    <div class="dashboard-line">Valid IMEIs</div>
                    {render_metric_bar(
                        'Current Valid IMEI Coverage',
                        progress_map.get('Valid IMEI Coverage', {}).get('value', 0.0),
                        progress_map.get('Valid IMEI Coverage', {}).get('subtitle', 'Coverage unavailable'),
                    )}
                </section>
                """
            ).strip(),
            unsafe_allow_html=True,
        )

    with cards[2]:
        st.markdown(
            textwrap.dedent(
                f"""
                <section class="glass-card dashboard-card">
                    <div class="dashboard-card-title">FUMO Records</div>
                    <div class="dashboard-big-number">{counts.get('fumo_hits', 0):,} IMEIs</div>
                    <div class="dashboard-line">Available for instant reuse</div>
                    {render_metric_bar(
                        'FUMO Hit Share',
                        progress_map.get('FUMO Hit Share', {}).get('value', 0.0),
                        progress_map.get('FUMO Hit Share', {}).get('subtitle', 'Share unavailable'),
                    )}
                    <div class="dashboard-divider"></div>
                    <div class="dashboard-line dashboard-strong">Quick Pulse</div>
                    <div class="dashboard-line">Cached firmware hits</div>
                    <div class="dashboard-big-number small">{counts.get('hits', 0):,}</div>
                    <div class="dashboard-line">{counts.get('fumo_hits', 0):,} flagged as FUMO-ready</div>
                </section>
                """
            ).strip(),
            unsafe_allow_html=True,
        )

    with cards[3]:
        st.markdown(
            textwrap.dedent(
                f"""
                <section class="glass-card dashboard-card">
                    <div class="dashboard-card-title">Connectivity</div>
                    <div class="dashboard-line">Database History</div>
                    <div class="dashboard-status">{connection_text(DB_PATH.exists(), 'Connected', 'Missing')}</div>
                    <div class="dashboard-line">FOTA Endpoint</div>
                    <div class="dashboard-status">{connection_text(snapshot.get('fota_ok', False))}</div>
                    <div class="dashboard-line">DMS Endpoint</div>
                    <div class="dashboard-status">{connection_text(snapshot.get('dms_ok', False))}</div>
                    <div class="dashboard-line">IP Status</div>
                    <div class="dashboard-status">{'🟢 Alive' if snapshot.get('ip_alive') else '⛔ Interrupted'}</div>
                    <div class="progress-caption">IP status reflects FOTA, FUMO, and DMS reachability.</div>
                </section>
                """
            ).strip(),
            unsafe_allow_html=True,
        )


def render_guest_dashboard(snapshot: dict[str, Any], catalog: dict[str, list[dict[str, Any]]]) -> None:
    lower_left, lower_right = st.columns(2, gap="large")
    with lower_left:
        render_html_table(
            "Category Overview",
            ["Category", "Devices", "Ready"],
            [list(row) for row in category_rows(catalog)],
            "No device categories found.",
        )
    with lower_right:
        render_html_table(
            "Latest Cached Discoveries",
            ["Model", "CSC", "PDA", "Time"],
            [
                [
                    row["device_model"],
                    row["csc"],
                    short_version(row["found_pda"]),
                    row["timestamp"],
                ]
                for row in recent_hits()
            ],
            "No firmware history is available yet.",
        )


def render_tool_menu() -> None:
    items = ["FOTA Scanner", "IMEI Scanner", "Device Vault", "Database History", "Terminal"]
    menu_html = "".join(
        f'<div class="tool-chip">{html.escape(item)}</div>'
        for item in items
    )
    st.markdown(
        f"""
        <section class="glass-card tool-menu-card">
            <div class="section-kicker">Menus / Tools</div>
            <div class="tool-chip-row">{menu_html}</div>
            <div class="progress-caption">Use the tabs above to open each workspace.</div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def run_imei_scan(model: str, csc: str, start_imei: str, base: str, attempts: int, step: int) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    results: list[dict[str, Any]] = []
    last_hit: dict[str, Any] | None = None
    progress = st.progress(0, text="Preparing IMEI scan...")

    current = start_imei
    for index in range(attempts):
        progress.progress((index + 1) / attempts, text=f"Scanning {current} ({index + 1}/{attempts})")
        response = lookup_download_link(model, csc, current, base)
        results.append(
            {
                "attempt": str(index + 1),
                "imei": current,
                "status": response.get("status", response.get("kind", "Unknown")),
                "source": response.get("source", "remote"),
                "firmware": response.get("found_pda", ""),
                "kind": response.get("kind", ""),
            }
        )
        if response.get("kind") in {"update", "dm"} and last_hit is None:
            last_hit = response
        current = increment_imei(current, step)

    progress.empty()
    return results, last_hit


def is_auth_failed_result(result: dict[str, Any]) -> bool:
    status = str(result.get("status", "") or "").lower()
    return "auth_failed" in status or "auth failed" in status


def format_imei_status_text(status_value: Any) -> str:
    raw = str(status_value or "").strip()
    lower = raw.lower()
    if "auth_failed" in lower or "auth failed" in lower:
        return "Auth Didn't Maked!"
    if "status: 260 (no update)." in lower or "260 (no update)" in lower:
        return "No Update Maked"
    if "bad_csc." in lower or "bad_csc" in lower:
        return "CSC Maked Bad"
    return raw or "Unknown"


def classify_imei_result(result: dict[str, Any]) -> str:
    kind = str(result.get("kind", "") or "").lower()
    if kind in {"update", "dm"}:
        return "HIT"
    if kind == "uptodate":
        return "VALID"
    if format_imei_status_text(result.get("status", "")).lower() == "no update maked":
        return "VALID"
    return "ERROR"


def init_imei_live_scan_state(request: dict[str, Any]) -> dict[str, Any]:
    attempts = max(int(request.get("attempts", 1)), 1)
    step = max(int(request.get("step", 1)), 1)
    return {
        "model": request["model"],
        "csc": request["csc"],
        "base": request["base"],
        "attempts": attempts,
        "step": step,
        "index": 0,
        "current_imei": request["start_imei"],
        "results": [],
        "hits": [],
        "last_hit": None,
        "hit_count": 0,
        "valid_count": 0,
        "error_count": 0,
        "status": "Preparing IMEI scan...",
        "status_outcome": "",
        "paused_for_auth": False,
        "force_continue": False,
        "auth_error_label": "",
        "auth_error_raw": "",
        "stop_after_index": attempts,
        "terminated": False,
    }


def finalize_imei_live_scan(
    state: dict[str, Any],
    *,
    terminated: bool = False,
    termination_reason: str = "",
) -> None:
    message = ""
    if terminated:
        message = f"The process has been terminated prematurely due to Error {termination_reason}."
    elif state["stop_after_index"] < state["attempts"]:
        fallback_reason = termination_reason or "Auth Didn't Maked!"
        message = f"Scan ended after the extra IMEIs allowed following Error {fallback_reason}."

    st.session_state.imei_scan_results = list(state["results"])
    st.session_state.imei_last_hit = state.get("last_hit")
    st.session_state.imei_live_result = {
        "hits": list(state["hits"]),
        "attempts": state["attempts"],
        "processed": state["index"],
        "model": state["model"],
        "csc": state["csc"],
        "base": state["base"],
        "hit_count": state["hit_count"],
        "valid_count": state["valid_count"],
        "error_count": state["error_count"],
        "terminated": terminated,
        "termination_reason": termination_reason,
        "message": message,
    }
    st.session_state.imei_live_state = None

    if terminated:
        push_activity("warn", f"IMEI scan for {state['model']} was terminated due to {termination_reason}.")
    else:
        push_activity(
            "info",
            f"IMEI scan completed for {state['model']} with {state['hit_count']} hits, {state['valid_count']} valid, and {state['error_count']} errors.",
        )


@st.dialog("Add Device")
def show_add_device_dialog() -> None:
    with st.form("add_device_dialog_form"):
        category = st.text_input("Category")
        name = st.text_input("Name")
        model = st.text_input("Model")
        csc = st.text_input("CSC")
        imei = st.text_input("IMEI")
        base = st.text_input("Firmware Base")
        submitted = st.form_submit_button("Add Device", use_container_width=True)

    if submitted:
        if not category.strip() or not model.strip() or not csc.strip() or not imei.strip():
            st.error("Category, model, CSC, and IMEI are required.")
        elif not imei.strip().isdigit() or len(imei.strip()) != 15:
            st.error("IMEI must be exactly 15 digits.")
        else:
            catalog = load_device_catalog()
            clean_category = category.strip()
            catalog.setdefault(clean_category, []).append(
                {
                    "name": name.strip() or model.strip().upper(),
                    "model": model.strip().upper(),
                    "csc": csc.strip().upper(),
                    "imei": imei.strip(),
                    "category": clean_category,
                    "base": base.strip(),
                }
            )
            save_device_catalog(catalog)
            push_activity("info", f"Added device {model.strip().upper()} to {clean_category}.")
            st.rerun()


@st.dialog("Edit Device")
def show_edit_device_dialog(category: str, device_index: int) -> None:
    catalog = load_device_catalog()
    device = catalog.get(category, [])[device_index]

    with st.form(f"edit_device_dialog_form_{category}_{device_index}"):
        new_name = st.text_input("Name", value=device.get("name", ""))
        new_model = st.text_input("Model", value=device.get("model", ""))
        new_csc = st.text_input("CSC", value=device.get("csc", ""))
        new_imei = st.text_input("IMEI", value=device.get("imei", ""))
        new_base = st.text_input("Firmware Base", value=device.get("base", ""))
        new_category = st.text_input("Category", value=category)
        submitted = st.form_submit_button("Save Changes", use_container_width=True)

    if submitted:
        if not new_imei.strip().isdigit() or len(new_imei.strip()) != 15:
            st.error("IMEI must be exactly 15 digits.")
        else:
            updated = {
                "name": new_name.strip() or new_model.strip().upper(),
                "model": new_model.strip().upper(),
                "csc": new_csc.strip().upper(),
                "imei": new_imei.strip(),
                "category": new_category.strip() or category,
                "base": new_base.strip(),
            }
            catalog[category].pop(device_index)
            if not catalog[category]:
                catalog.pop(category)
            target_category = updated["category"]
            catalog.setdefault(target_category, []).append(updated)
            save_device_catalog(catalog)
            push_activity("info", f"Updated device {updated['model']} in the vault.")
            st.rerun()


@st.dialog("Remove Device")
def show_remove_device_dialog(category: str, device_index: int) -> None:
    catalog = load_device_catalog()
    device = catalog.get(category, [])[device_index]
    st.warning(
        f"Remove {device.get('name', device.get('model', 'Unknown'))} "
        f"({device.get('model', 'Unknown')} / {device.get('csc', 'UNK')})?"
    )
    if st.button("Confirm Remove", key=f"confirm_remove_{category}_{device_index}", use_container_width=True):
        removed = catalog[category].pop(device_index)
        if not catalog[category]:
            catalog.pop(category)
        save_device_catalog(catalog)
        push_activity("warn", f"Removed device {removed.get('model', 'Unknown')} from the vault.")
        st.rerun()


def render_fota_tab(catalog: dict[str, list[dict[str, Any]]], *, guest_mode: bool = False) -> None:
    model, csc, db_imei, db_base = scanner_device_input("fota_v3")
    context_key = f"{model}|{csc}"
    if st.session_state.get("fota_v3_context_key") != context_key:
        st.session_state.fota_v3_context_key = context_key
        st.session_state.fota_scanned_imei = ""
        st.session_state.fota_v3_base = ""
    scanned_imei = str(st.session_state.get("fota_scanned_imei", "") or "")
    effective_db_imei = scanned_imei or db_imei

    st.caption(
        "Use a saved model from decrypted_firmware.db or type the values manually. "
        "Any new model number you enter will be recorded for future use."
    )

    scan_mode = st.radio(
        "Scan Mode",
        ["Auto", "Manual"],
        horizontal=True,
        key="fota_v3_scan_mode",
    )

    available_bases = latest_decrypted_versions(model, csc, limit=10) if model and csc else []
    base_options: list[str] = []
    for value in [db_base, *available_bases]:
        clean_value = str(value or "").strip()
        if clean_value and clean_value not in base_options:
            base_options.append(clean_value)
    imei_options = known_imei_options(model, csc) if model and csc else []

    if scan_mode == "Auto":
        base = st.selectbox(
            "Base Firmware Source",
            [""] + base_options,
            format_func=lambda value: "Select firmware from database" if not value else value,
            key="fota_v3_base_auto",
        )
        if not guest_mode:
            imei = st.selectbox(
                "IMEI Source",
                [""] + imei_options,
                format_func=lambda value: "Select IMEI from database" if not value else mask_imei(value),
                key="fota_v3_imei_auto",
            )
            if imei:
                st.caption("Auto mode uses IMEIs saved from the app databases.")
        else:
            imei = effective_db_imei
            st.text_input("IMEI", value="Locked in Guest Mode", disabled=True, key="fota_v3_guest_imei_auto")
            if effective_db_imei:
                st.caption(f"Using stored IMEI ending in {effective_db_imei[-4:]}.")
            else:
                st.caption("Guest Mode requires a stored IMEI for this model and CSC.")
    else:
        base_source = st.radio(
            "Base Firmware Source",
            ["Manual", "Decrypted Database", "Use decryptor"],
            horizontal=True,
            key="fota_v3_base_source",
        )
        if base_source == "Manual":
            base = st.text_input("Base Firmware", value=db_base, key="fota_v3_base_manual")
        elif base_source == "Decrypted Database":
            base = st.selectbox(
                "Base Firmware",
                [""] + base_options,
                format_func=lambda value: "Select a firmware base" if not value else value,
                key="fota_v3_base_db",
            )
        else:
            current_value = str(st.session_state.get("fota_v3_base", db_base) or "")
            st.text_input(
                "Base Firmware",
                value=current_value,
                placeholder=current_value if current_value else "Select firmware via decryptor",
                disabled=True,
                key="fota_v3_base_picker_display",
            )
            if st.button("Use decryptor", key="fota_v3_open_decryptor", use_container_width=True):
                if not model or not csc:
                    st.error("Model number and CSC are required before using the decryptor.")
                else:
                    ensure_known_device(model, csc)
                    st.session_state.firmware_picker_request = {"prefix": "fota_v3", "model": model, "csc": csc}
                    st.rerun()
            if current_value:
                st.caption(f"Selected firmware: {current_value}")
            base = current_value

        if guest_mode:
            imei = effective_db_imei
            st.text_input("IMEI", value="Locked in Guest Mode", disabled=True, key="fota_v3_guest_imei")
            if effective_db_imei:
                st.caption(f"Using stored IMEI ending in {effective_db_imei[-4:]}.")
            else:
                st.caption("Guest Mode requires a stored IMEI for this model and CSC.")
        else:
            imei_source = st.radio(
                "IMEI Source",
                ["Database", "Manual", "Scan for IMEI"],
                horizontal=True,
                key="fota_v3_imei_source",
            )
            if imei_source == "Database":
                imei = st.selectbox(
                    "IMEI",
                    [""] + imei_options,
                    format_func=lambda value: "Select IMEI from database" if not value else mask_imei(value),
                    key="fota_v3_imei_db",
                )
                st.caption("Database mode uses known IMEIs from decrypted_firmware.db, IMEI Database, or fumo_history.db.")
            elif imei_source == "Manual":
                imei = st.text_input("IMEI", value=effective_db_imei, key="fota_v3_imei_manual")
            else:
                current_imei = scanned_imei or effective_db_imei
                st.text_input(
                    "IMEI",
                    value=current_imei,
                    placeholder=mask_imei(current_imei) if current_imei else "Select IMEI via scanner",
                    disabled=True,
                    key="fota_v3_imei_scanned",
                )
                if st.button("Scan for IMEI", key="fota_v3_scan_for_imei", use_container_width=True):
                    if not model or not csc or not base:
                        st.error("Model number, CSC, and firmware base are required before scanning for IMEI.")
                    else:
                        ensure_known_device(model, csc, imei=effective_db_imei, base=base)
                        st.session_state.imei_scan_results = []
                        st.session_state.imei_last_hit = None
                        st.session_state.imei_live_request = {
                            "model": model,
                            "csc": csc,
                            "start_imei": effective_db_imei or current_imei,
                            "base": base,
                            "attempts": 50,
                            "step": 4,
                            "consumer": "fota_scanner",
                            "database_imei": effective_db_imei,
                        }
                        st.session_state.imei_live_result = None
                        st.session_state.imei_live_error = None
                        st.session_state.imei_live_state = None
                        st.rerun()
                if current_imei:
                    st.caption(f"Selected IMEI: {mask_imei(current_imei)}")

    if st.button("Fetch Download Link", key="fota_v3_fetch_live", use_container_width=True):
        if not model or not csc or not imei:
            st.error("Model number, CSC, and IMEI are required.")
            push_activity("error", "Lookup was blocked because one or more required fields were empty.")
        elif not imei.isdigit() or len(imei) != 15:
            st.error("IMEI must be exactly 15 digits.")
            push_activity("error", f"Rejected invalid IMEI input for {model}.")
        else:
            ensure_known_device(model, csc, imei=imei, base=base)
            st.session_state.fota_live_request = {
                "model": model,
                "csc": csc,
                "imei": imei,
                "base": base,
            }
            st.session_state.fota_live_result = None
            st.session_state.fota_live_error = None
            st.rerun()

    render_result_panel(st.session_state.get("last_result"))
    if st.session_state.get("firmware_picker_request"):
        show_firmware_picker_dialog()
    if st.session_state.get("fota_live_request") is not None:
        show_fota_fetch_dialog()
    if st.session_state.get("imei_live_request") is not None and not guest_mode:
        show_imei_scan_dialog()


def render_imei_scanner_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    model, csc, db_imei, db_base = scanner_device_input("imei_v3")
    ensure_known_device(model, csc, imei=db_imei, base=db_base)
    context_key = f"{model}|{csc}|{db_imei}"
    if st.session_state.get("imei_v3_context_key") != context_key:
        st.session_state.imei_v3_context_key = context_key
        st.session_state.imei_v3_start_imei = db_imei

    st.caption(
        "Use a saved model from decrypted_firmware.db or type the values manually. "
        "The start IMEI defaults to the current IMEI already known for that device."
    )

    base_source = st.radio(
        "Base Firmware Source",
        ["Manual", "Decrypted Database", "Use decryptor"],
        horizontal=True,
        key="imei_v3_base_source",
    )
    available_bases = latest_decrypted_versions(model, csc, limit=10) if model and csc else []
    if base_source == "Manual":
        base = st.text_input("Base Firmware", value=db_base, key="imei_v3_base_manual")
    elif base_source == "Decrypted Database":
        base = st.selectbox(
            "Base Firmware",
            [""] + available_bases,
            format_func=lambda value: "Select a firmware base" if not value else short_version(value),
            key="imei_v3_base_db",
        )
    else:
        current_value = str(st.session_state.get("imei_v3_base", db_base) or "")
        st.text_input("Base Firmware", value=current_value, disabled=True, key="imei_v3_base_picker_display")
        if st.button("Use decryptor", key="imei_v3_open_decryptor", use_container_width=True):
            if not model or not csc:
                st.error("Model number and CSC are required before using the decryptor.")
            else:
                ensure_known_device(model, csc)
                st.session_state.firmware_picker_request = {"prefix": "imei_v3", "model": model, "csc": csc}
                st.rerun()
        base = current_value

    left, right = st.columns(2, gap="medium")
    with left:
        start_imei = st.text_input("Start IMEI", key="imei_v3_start_imei")
    with right:
        step = st.number_input("Thread [Recommended: 4]", min_value=1, max_value=999, value=4, step=1, key="imei_v3_step")
    attempts = st.number_input("No. of IMEI", min_value=1, max_value=50, value=50, step=1, key="imei_v3_attempts")

    if st.button("Start IMEI Scan", key="start_imei_scan_v3", use_container_width=True):
        if not model or not csc or not base or not start_imei:
            st.error("Model, CSC, start IMEI, and base firmware are required.")
        elif not start_imei.isdigit() or len(start_imei) != 15:
            st.error("Start IMEI must be exactly 15 digits.")
        else:
            ensure_known_device(model, csc, imei=start_imei, base=base)
            st.session_state.imei_scan_results = []
            st.session_state.imei_last_hit = None
            st.session_state.imei_live_request = {
                "model": model,
                "csc": csc,
                "start_imei": start_imei,
                "base": base,
                "attempts": int(attempts),
                "step": int(step),
            }
            st.session_state.imei_live_result = None
            st.session_state.imei_live_error = None
            st.session_state.imei_live_state = None
            st.rerun()

    st.divider()

    if st.session_state.imei_last_hit:
        render_result_panel(st.session_state.imei_last_hit)

    rows = st.session_state.get("imei_scan_results", [])
    if rows:
        header_cols = st.columns([0.7, 1.2, 1.25, 0.9, 1.35, 0.95], gap="small")
        headers = ["Attempt", "IMEI", "Status", "Source", "Firmware", "Action"]
        for col, label in zip(header_cols, headers):
            col.markdown(f"<div class='imei-db-header'>{html.escape(label)}</div>", unsafe_allow_html=True)
        for idx, row in enumerate(rows):
            cols = st.columns([0.7, 1.2, 1.25, 0.9, 1.35, 0.95], gap="small")
            cols[0].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['attempt']))}</div>", unsafe_allow_html=True)
            cols[1].markdown(f"<div class='imei-db-cell'>{html.escape(mask_imei(str(row['imei'])))}</div>", unsafe_allow_html=True)
            cols[2].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['status']))}</div>", unsafe_allow_html=True)
            cols[3].markdown(f"<div class='imei-db-cell'>{html.escape(str(row['source']))}</div>", unsafe_allow_html=True)
            cols[4].markdown(f"<div class='imei-db-cell'>{html.escape(short_version(str(row['firmware'] or '')))}</div>", unsafe_allow_html=True)
            with cols[5]:
                if st.button("USE IMEI", key=f"scan_use_imei_v3_{idx}", use_container_width=True):
                    if update_device_imei_by_model_csc(model, csc, str(row["imei"]), "IMEI Scanner"):
                        st.rerun()
    else:
        st.info("Run a scan to see IMEI results here.")

    if st.session_state.get("firmware_picker_request"):
        show_firmware_picker_dialog()
    if st.session_state.get("imei_live_request") is not None:
        show_imei_scan_dialog()


def render_device_vault_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    grouped = device_vault_rows()
    if not grouped:
        st.info("No devices are available in the vault yet.")
        return

    for model, rows in grouped.items():
        with st.expander(f"{model} ({len(rows)})", expanded=False):
            st.markdown(
                """
                <div class="history-grid-header vault-grid-header">
                    <span>CSC</span>
                    <span>Latest Decrypted Firmware</span>
                    <span>Date Decrypted</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
            for row in rows:
                st.markdown(
                    textwrap.dedent(
                        f"""
                        <div class="history-list-row vault-list-row">
                            <span>{html.escape(str(row['csc']))}</span>
                            <span>{html.escape(short_triplet_version(row['latest']))}</span>
                            <span>{html.escape(str(row['date']))}</span>
                        </div>
                        """
                    ).strip(),
                    unsafe_allow_html=True,
                )


def guest_device_vault_rows(
    catalog: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, str]]]:
    fallback_map = latest_firmware_lookup()[1]
    rows_by_category: dict[str, list[dict[str, str]]] = {}
    history_csc_rows = with_db(
        """
        SELECT device_model, csc
        FROM firmware_hits
        WHERE csc IS NOT NULL AND csc != ''
        ORDER BY device_model, csc
        """
    )
    history_csc_map: dict[str, set[str]] = {}
    for row in history_csc_rows:
        history_csc_map.setdefault(str(row["device_model"]).upper(), set()).add(str(row["csc"]).upper())

    for category, entries in catalog.items():
        grouped: dict[str, dict[str, Any]] = {}
        for entry in entries:
            model = str(entry.get("model", "")).upper()
            grouped.setdefault(
                model,
                {
                    "name": str(entry.get("name", model or "Unknown")),
                    "model": model or "Unknown",
                    "cscs": set(),
                    "bases": set(),
                    "latest": "",
                },
            )
            if entry.get("csc"):
                grouped[model]["cscs"].add(str(entry.get("csc", "")).upper())
            if entry.get("base"):
                grouped[model]["bases"].add(str(entry.get("base", "")))
            latest = fallback_map.get((model, str(entry.get("csc", "")).upper()), "") or str(entry.get("latest", "") or "")
            if latest and not grouped[model]["latest"]:
                grouped[model]["latest"] = latest

        category_rows: list[dict[str, str]] = []
        for model, payload in grouped.items():
            cscs = payload["cscs"].union(history_csc_map.get(model, set()))
            bases = sorted(payload["bases"])
            category_rows.append(
                {
                    "name": payload["name"],
                    "model": payload["model"],
                    "cscs": ", ".join(sorted(cscs)) or "Unknown",
                    "base": " | ".join(bases) if bases else "Unknown",
                    "latest": payload["latest"] or "No record",
                }
            )
        rows_by_category[category] = sorted(category_rows, key=lambda item: item["model"])
    return rows_by_category


def render_guest_device_vault_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    grouped_rows = guest_device_vault_rows(catalog)
    for category, rows in grouped_rows.items():
        with st.expander(f"{category} ({len(rows)})", expanded=False):
            for row in rows:
                st.markdown(
                    f"""
                    <section class="glass-card vault-device-card guest-vault-card">
                        <div class="vault-device-name">{html.escape(row['name'])}</div>
                        <div class="vault-device-line"><strong>Device Model</strong> {html.escape(row['model'])}</div>
                        <div class="vault-device-line"><strong>Recorded CSCs</strong> {html.escape(row['cscs'])}</div>
                        <div class="vault-device-line"><strong>Firmware Base</strong> {html.escape(row['base'])}</div>
                        <div class="vault-device-line"><strong>Latest Found Firmware</strong> {html.escape(row['latest'])}</div>
                    </section>
                    """,
                    unsafe_allow_html=True,
                )


def firmware_version_sort_key(version: str | None) -> tuple[int, int, str]:
    clean = str(version or "")
    year, month = dc3.parse_date_from_version(clean)
    pda = clean.split("/")[0] if clean else ""
    return (year, month, pda)


def current_known_firmware(model: str, csc: str, catalog: dict[str, list[dict[str, Any]]]) -> str:
    _, fallback_map = latest_firmware_lookup()
    known = fallback_map.get((model, csc), "")
    if known:
        return known
    for entries in catalog.values():
        for entry in entries:
            if str(entry.get("model", "")).upper() == model and str(entry.get("csc", "")).upper() == csc:
                return str(entry.get("base", "") or "")
    return ""


def render_decryption_firmware_list(result: dict[str, Any]) -> None:
    items = result.get("items", [])
    if not items:
        st.info("No decrypted firmware builds were produced for this target.")
        return

    highlight_version = str(result.get("highlight_version", "") or "")
    highlight_previous = str(result.get("current_known", "") or "")
    if highlight_version:
        message = f"Latest Firmware: {highlight_version}"
        if highlight_previous:
            message += f" • newer than recorded {highlight_previous}"
        st.markdown(
            f"""
            <section class="glass-card decrypt-highlight-card">
                <div class="section-kicker">Latest Firmware</div>
                <div class="result-title">{html.escape(highlight_version)}</div>
                <div class="result-meta">{html.escape(message)}</div>
            </section>
            """,
            unsafe_allow_html=True,
        )

    total_pages = max(1, ceil(len(items) / 10))
    page_cols = st.columns([1, 4], gap="medium")
    with page_cols[0]:
        page = st.number_input(
            "Page",
            min_value=1,
            max_value=total_pages,
            value=1,
            step=1,
            key=f"decrypt_page_{result.get('model', 'UNK')}_{result.get('region', 'UNK')}",
        )
    with page_cols[1]:
        st.markdown(
            f"<div class='history-page-note'>Showing page {page} of {total_pages} • 10 firmware entries per page</div>",
            unsafe_allow_html=True,
        )

    st.markdown(
        """
        <div class="decrypt-list-header">
            <span>Date</span>
            <span>Kind</span>
            <span>Triplet</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    start = (page - 1) * 10
    page_items = items[start : start + 10]
    for item in page_items:
        version = str(item.get("version", "Unknown"))
        row_class = "decrypt-list-row latest" if version == highlight_version else "decrypt-list-row"
        year = int(item.get("year", 0) or 0)
        month = int(item.get("month", 0) or 0)
        date_label = f"{year}-{month:02d}" if year and month else "-"
        kind = str(item.get("kind", "unknown"))
        latest_pill = "<span class='decrypt-pill'>Latest</span>" if version == highlight_version else ""
        st.markdown(
            f"""
            <div class="{row_class}">
                <span>{html.escape(date_label)}</span>
                <span>{html.escape(kind)}</span>
                <span>{html.escape(version)} {latest_pill}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_decryption_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    model_options = model_options_from_decrypt_db()
    st.markdown(
        """
        <div style="margin-bottom:0.9rem;">
            <span class="pill badge-indigo decrypt-title-pill">Decryption Tool</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("Select a known model or type one manually. The discovered firmware list loads immediately from decrypted_firmware.db.")

    picker_cols = st.columns([1.4, 1.1], gap="medium")
    with picker_cols[0]:
        selected_model = st.selectbox(
            "Known Model",
            [""] + model_options,
            format_func=lambda value: "Select from database" if not value else value,
            key="decrypt_known_model_v4",
        )
        manual_model = st.text_input("Manual Model Number", key="decrypt_model_input_v4")
    with picker_cols[1]:
        effective_model = normalize_model_number(manual_model or selected_model)
        csc_options = csc_options_for_model(effective_model)
        selected_csc = st.selectbox(
            "Known CSC",
            [""] + csc_options,
            format_func=lambda value: "Select CSC" if not value else value,
            key="decrypt_known_csc_v4",
        )
        manual_csc = st.text_input("Manual CSC", key="decrypt_csc_input_v4")

    effective_csc = normalize_csc_code(manual_csc or selected_csc)
    if effective_model:
        ensure_known_device(effective_model, effective_csc)

    if st.button("Start Decryption", key="start_decryption_v4", use_container_width=True):
        if not effective_model or not effective_csc:
            st.error("Model and CSC are required.")
        else:
            push_activity("info", "A user is using Decryption tool. Performance might get impacted.")
            status_box = st.status("Starting decryption...", expanded=True)
            progress = st.progress(0)
            progress_subtitle = st.empty()
            progress_subtitle.caption("0.0%")

            def progress_callback(stage: str, completed: int, total: int, label: str) -> None:
                update_decryption_progress_ui(
                    stage,
                    completed,
                    total,
                    label,
                    status_box=status_box,
                    progress_bar=progress,
                    subtitle_box=progress_subtitle,
                )

            try:
                result = decrypt_device_live(effective_model, effective_csc, persist=True, progress_callback=progress_callback)
                progress.progress(100)
                status_box.update(label="Decryption complete", state="complete")
                st.session_state.decrypt_results = [result]
                st.session_state.decrypt_error = None
                st.session_state.decrypt_latest_key = f"{effective_model}|{effective_csc}|{result.get('latest_found', '')}"
                push_activity("info", f"Decryption scan completed for {effective_model} / {effective_csc}.")
            except Exception as exc:
                st.session_state.decrypt_results = []
                st.session_state.decrypt_error = str(exc)
                progress.progress(100)
                status_box.update(label="Decryption failed", state="error")
                push_activity("error", f"Decryption failed for {effective_model} / {effective_csc}: {exc}")

    if st.session_state.get("decrypt_error"):
        st.error(str(st.session_state.get("decrypt_error")))

    results = st.session_state.get("decrypt_results", [])
    if results:
        render_decryption_results_card(results[0])

    if effective_model:
        highlight_version = ""
        if results:
            highlight_version = str(results[0].get("latest_found", "") or "")
        render_decryption_firmware_table(effective_model, effective_csc, highlight_version=highlight_version)


def render_imei_scan_results(rows: list[dict[str, Any]], category: str, device_index: int) -> None:
    st.markdown(
        """
        <section class="glass-card table-card">
            <div class="section-kicker">IMEI Scan Results</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    if not rows:
        st.info("Run a scan to see IMEI results here.")
        return

    header_cols = st.columns([0.7, 1.2, 1.2, 0.9, 1.4, 0.9], gap="small")
    headers = ["Attempt", "IMEI", "Status", "Source", "Firmware", "Action"]
    for col, label in zip(header_cols, headers):
        col.markdown(f"<div class='imei-db-header'>{html.escape(label)}</div>", unsafe_allow_html=True)

    for idx, row in enumerate(rows):
        row_cols = st.columns([0.7, 1.2, 1.2, 0.9, 1.4, 0.9], gap="small")
        values = [
            str(row.get("attempt", "")),
            str(row.get("imei", "")),
            str(row.get("status", "")),
            str(row.get("source", "")),
            short_version(row.get("firmware", "")) if row.get("firmware") else "-",
        ]
        for col, value in zip(row_cols[:-1], values):
            col.markdown(f"<div class='imei-db-cell'>{html.escape(value)}</div>", unsafe_allow_html=True)
        with row_cols[-1]:
            if st.button("USE IMEI", key=f"use_scan_imei_{idx}", use_container_width=True):
                if update_device_imei(category, device_index, str(row.get("imei", "")), "IMEI Scanner"):
                    st.rerun()


def imei_database_rows(
    catalog: dict[str, list[dict[str, Any]]],
    model: str,
    selected_csc: str,
    current_category: str,
    current_device: dict[str, Any],
) -> list[dict[str, Any]]:
    query = """
    SELECT device_model, csc, imei, found_pda, timestamp
    FROM firmware_hits
    WHERE device_model = ?
    """
    params: list[Any] = [model]
    if selected_csc != "All CSCs":
        query += " AND csc = ?"
        params.append(selected_csc)
    query += " ORDER BY datetime(timestamp) DESC, id DESC"

    rows = with_db(query, tuple(params))
    combined: dict[tuple[str, str], dict[str, Any]] = {}

    for row in rows:
        key = (str(row["imei"]), str(row["csc"]))
        if key in combined:
            continue
        combined[key] = {
            "imei": str(row["imei"]),
            "csc": str(row["csc"]),
            "status": "HIT" if row["found_pda"] else "NO",
            "source": "History",
            "found_pda": str(row["found_pda"] or "-"),
            "timestamp": str(row["timestamp"] or "-"),
            "is_current": False,
        }

    for category, entries in catalog.items():
        for entry in entries:
            if str(entry.get("model", "")).upper() != model.upper():
                continue
            entry_csc = str(entry.get("csc", "")).upper()
            if selected_csc != "All CSCs" and entry_csc != selected_csc:
                continue
            entry_imei = str(entry.get("imei", ""))
            key = (entry_imei, entry_csc)
            current = category == current_category and entry_imei == str(current_device.get("imei", "")) and entry_csc == str(current_device.get("csc", "")).upper()
            if key in combined:
                source = combined[key]["source"]
                combined[key]["source"] = "Current + History" if current else "Catalog + History"
                combined[key]["is_current"] = combined[key]["is_current"] or current
            else:
                combined[key] = {
                    "imei": entry_imei,
                    "csc": entry_csc,
                    "status": "NO",
                    "source": "Current Device" if current else "Catalog",
                    "found_pda": "-",
                    "timestamp": "-",
                    "is_current": current,
                }

    return sorted(
        combined.values(),
        key=lambda item: (
            0 if item["is_current"] else 1,
            0 if item["status"] == "HIT" else 1,
            item["csc"],
            item["imei"],
        ),
    )


def render_imei_database_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    model_options = model_options_from_decrypt_db()
    selected_model = st.selectbox(
        "Device Model",
        [""] + model_options,
        format_func=lambda value: "Select a model" if not value else value,
        key="imei_db_model_v3",
    )
    manual_model = st.text_input("Manual Model Number", key="imei_db_manual_model_v3")
    effective_model = normalize_model_number(manual_model or selected_model)
    if not effective_model:
        st.info("Select or type a model number to open its IMEI database.")
        return

    ensure_model_imei_schema(effective_model)
    csc_options = csc_options_for_model(effective_model, include_all=True)
    selected_csc = st.selectbox("CSC", csc_options or ["All CSCs"], key="imei_db_csc_v3")

    top_actions = st.columns([1.2, 1.5, 1.2], gap="medium")
    with top_actions[1]:
        if st.button("Read FUMO History", key="imei_db_read_history_v3", use_container_width=True):
            imported = import_fumo_history_to_imei_db(effective_model, selected_csc)
            if imported:
                push_activity("sync", f"Imported {imported} IMEIs from FUMO History into {effective_model}.")
            else:
                push_activity("info", f"No new IMEIs were imported for {effective_model}.")
            st.rerun()

    rows = imei_database_rows_v2(effective_model, selected_csc)
    st.markdown(
        """
        <section class="glass-card table-card">
            <div class="section-kicker">IMEI Database</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    if not rows:
        st.info("No IMEIs are stored for this model yet.")
        return

        header_cols = st.columns([1.4, 0.75, 1.5, 0.9, 0.95], gap="small")
        headers = ["IMEI", "CSC", "Firmware Hit", "Amount of Hit", "Action"]
        for col, label in zip(header_cols, headers):
            col.markdown(f"<div class='imei-db-header'>{html.escape(label)}</div>", unsafe_allow_html=True)

    fallback_csc_options = csc_options_for_model(effective_model)
    default_csc = normalize_csc_code(
        selected_csc
        if selected_csc != "All CSCs"
        else (fallback_csc_options[0] if fallback_csc_options else "")
    )
    for idx, row in enumerate(rows):
        row_cols = st.columns([1.45, 0.72, 1.55, 0.88, 0.95], gap="small")
        values = [mask_imei(str(row["imei"])), str(row["csc"]), short_version(str(row["firmware_hit"] or "")) or "-", int(row["hit_count"] or 0)]
        for col, value in zip(row_cols[:-1], values):
            col.markdown(f"<div class='imei-db-cell'>{html.escape(str(value))}</div>", unsafe_allow_html=True)
        with row_cols[-1]:
            if st.button("USE IMEI", key=f"imei_db_use_v3_{idx}", use_container_width=True):
                target_csc = normalize_csc_code(str(row["csc"] or default_csc))
                if update_device_imei_by_model_csc(effective_model, target_csc, str(row["imei"]), "IMEI Database"):
                    st.rerun()


def render_database_history_tab() -> None:
    model_options = [row["device_model"] for row in with_db("SELECT DISTINCT device_model FROM firmware_hits ORDER BY device_model")]
    st.markdown(
        """
        <section class="glass-card table-card">
            <div class="section-kicker">Library</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    selected_model = st.selectbox(
        "Device Model",
        [""] + model_options,
        format_func=lambda value: "Select a model" if not value else value,
        key="library_model_v3",
    )
    if not selected_model:
        st.info("Select a device model number to load the library.")
        return

    rows = library_rows_for_model(selected_model)
    st.markdown("<div class='section-spacer'></div>", unsafe_allow_html=True)
    total_pages = max(1, ceil(len(rows) / 10))
    page_cols = st.columns([1, 4], gap="medium")
    with page_cols[0]:
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1, key="library_page_v3")
    with page_cols[1]:
        st.markdown(
            f"<div class='history-page-note'>Showing page {page} of {total_pages} • 10 discoveries per page</div>",
            unsafe_allow_html=True,
        )
    start = (page - 1) * 10
    page_rows = rows[start : start + 10]
    st.markdown(
        """
        <div class="history-grid-header">
            <span>Time</span>
            <span>Model</span>
            <span>CSC</span>
            <span>Firmware Base</span>
            <span>Firmware Found</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if not page_rows:
        st.info("No firmware history is available for this model yet.")
        return

    for row in page_rows:
        row_html = textwrap.dedent(
            f"""
            <div class="history-list-row library-grid">
                <span>{html.escape(str(row["timestamp"]))}</span>
                <span>{html.escape(str(row["device_model"]))}</span>
                <span>{html.escape(str(row["csc"]))}</span>
                <span>{html.escape(short_triplet_version(row["request_base_version"]))}</span>
                <span>{html.escape(short_triplet_version(row["found_pda"]))}</span>
            </div>
            """
        )
        row_cols = st.columns([12, 1.8], gap="small")
        with row_cols[0]:
            st.markdown(row_html, unsafe_allow_html=True)
        with row_cols[1]:
            if st.button("Link", key=f"library_row_link_{row['id']}", use_container_width=True):
                show_history_detail_dialog(dict(row))


def render_terminal_tab(snapshot_text: str) -> None:
    st.markdown(
        f"""
        <section class="glass-card table-card">
            <div class="section-kicker">Snapshot</div>
            <div class="dashboard-big-number small">{html.escape(snapshot_text)}</div>
            <div class="progress-caption">The latest dashboard health snapshot is shown here for admin review.</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div class='section-spacer'></div>", unsafe_allow_html=True)
    st.toggle("Enable experimental feedback routing", value=False, disabled=True)
    st.text_area(
        "Feedback draft",
        value="GitHub feedback integration will be connected here later.",
        height=140,
        disabled=True,
    )


def render_login_page() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] { display: none; }
        .login-shell {
            max-width: 540px;
            margin: 10vh auto 0;
        }
        .login-card {
            padding: 30px 28px;
        }
        .login-title {
            font-size: 2rem;
            font-weight: 800;
            letter-spacing: -0.04em;
            color: var(--text-main);
        }
        .login-subtitle {
            margin-top: 10px;
            color: var(--text-soft);
            line-height: 1.6;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        """
        <div class="login-shell">
            <section class="glass-card login-card">
                <div class="login-title">Project Killshot Authentication</div>
                <div class="login-subtitle">Enter the secret code for Admin Mode, or continue as a guest.</div>
            </section>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div class='section-spacer'></div>", unsafe_allow_html=True)
    st.text_input("Secret Code", key="secret_code_input", type="password")
    if st.session_state.get("login_error"):
        st.error(str(st.session_state.get("login_error")))

    action_cols = st.columns(2, gap="medium")
    with action_cols[0]:
        if st.button("Enter", key="enter_admin_mode", use_container_width=True):
            if st.session_state.get("secret_code_input", "") == ADMIN_SECRET_CODE:
                st.session_state.is_authenticated = True
                st.session_state.user_mode = "admin"
                st.session_state.login_error = None
                st.session_state.active_tab = "Dashboard"
                push_activity("info", "Admin Mode activated.")
                st.rerun()
            st.session_state.login_error = "Secret Code not accepted."
            st.rerun()
    with action_cols[1]:
        if st.button("Enter as Guest", key="enter_guest_mode", use_container_width=True):
            st.session_state.is_authenticated = True
            st.session_state.user_mode = "guest"
            st.session_state.login_error = None
            st.session_state.active_tab = "Dashboard"
            push_activity("info", "Guest Mode activated.")
            st.rerun()
    st.button("Send Feedback", disabled=True, use_container_width=True)


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --bg-main: linear-gradient(180deg, #eff4ff 0%, #e7eefb 45%, #dde8f8 100%);
            --card-bg: rgba(255, 255, 255, 0.72);
            --card-stroke: rgba(255, 255, 255, 0.92);
            --text-main: #122033;
            --text-soft: #607089;
            --sidebar-width: 320px;
            --content-max: 1440px;
            --content-width: min(var(--content-max), calc(100vw - var(--sidebar-width) - 72px));
            --content-left: calc(var(--sidebar-width) + ((100vw - var(--sidebar-width) - var(--content-width)) / 2));
        }

        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top left, rgba(93, 141, 255, 0.22), transparent 28%),
                radial-gradient(circle at top right, rgba(159, 217, 255, 0.22), transparent 26%),
                var(--bg-main);
            color: var(--text-main);
            font-family: "Segoe UI Variable Display", "Segoe UI", sans-serif;
        }

        .block-container {
            padding-top: 2rem;
            padding-bottom: 11rem;
            max-width: 1440px;
        }

        [data-testid="stHeader"] {
            background: transparent;
        }

        [data-testid="stToolbar"] {
            display: none !important;
        }

        [data-testid="stDecoration"] {
            display: none !important;
        }

        #MainMenu {
            display: none !important;
        }

        a[title="GitHub"],
        a[title*="Fork" i],
        a[title*="fork" i],
        button[title*="Fork" i],
        button[title*="fork" i] {
            display: none !important;
            visibility: hidden !important;
            pointer-events: none !important;
        }

        .oneui-header {
            position: relative;
            top: auto;
            left: auto;
            width: 100%;
            z-index: auto;
            padding: 18px 26px;
            border-radius: 30px;
            backdrop-filter: blur(22px);
            background: rgba(248, 251, 255, 0.8);
            border: 1px solid rgba(255, 255, 255, 0.9);
            box-shadow: 0 18px 48px rgba(65, 101, 156, 0.16);
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 24px;
            margin-bottom: 1rem;
        }

        .header-title {
            font-size: 1.35rem;
            font-weight: 700;
            letter-spacing: -0.02em;
            color: var(--text-main);
        }

        .header-subtitle {
            color: var(--text-soft);
            margin-top: 4px;
            font-size: 0.95rem;
        }

        .header-chip {
            white-space: nowrap;
            border-radius: 999px;
            padding: 10px 16px;
            background: linear-gradient(135deg, rgba(45, 114, 255, 0.12), rgba(95, 169, 255, 0.2));
            color: #214c9c;
            font-weight: 600;
        }

        .glass-card {
            border-radius: 32px;
            padding: 22px 24px;
            backdrop-filter: blur(24px);
            background: var(--card-bg);
            border: 1px solid var(--card-stroke);
            box-shadow: 0 16px 40px rgba(75, 102, 150, 0.13);
        }

        .hero-card {
            padding: 28px 30px;
            margin-bottom: 1rem;
        }

        .hero-title {
            font-size: 2rem;
            font-weight: 750;
            letter-spacing: -0.035em;
            margin-bottom: 8px;
        }

        .hero-copy {
            color: var(--text-soft);
            line-height: 1.65;
            max-width: 64ch;
        }

        .section-spacer {
            height: 1rem;
        }

        .section-kicker {
            font-size: 0.82rem;
            text-transform: uppercase;
            letter-spacing: 0.14em;
            color: #6a7a93;
            margin-bottom: 14px;
            font-weight: 700;
        }

        .metric-card {
            min-height: 148px;
        }

        .metric-value {
            font-size: 2rem;
            font-weight: 760;
            letter-spacing: -0.04em;
            margin-bottom: 10px;
        }

        .metric-subtitle {
            color: #4c6ca5;
            font-size: 0.92rem;
        }

        .status-card,
        .progress-card,
        .table-card,
        .result-card,
        .dashboard-card,
        .tool-menu-card {
            min-height: 100%;
        }

        .status-line {
            display: flex;
            align-items: center;
            gap: 10px;
            font-weight: 700;
        }

        .status-detail {
            margin-top: 14px;
            color: var(--text-soft);
            line-height: 1.55;
        }

        .status-dot {
            width: 12px;
            height: 12px;
            border-radius: 999px;
            display: inline-block;
        }

        .status-live {
            background: radial-gradient(circle at 30% 30%, #b2ffe0, #2f9b6c);
            box-shadow: 0 0 0 6px rgba(47, 155, 108, 0.12);
        }

        .status-down {
            background: radial-gradient(circle at 30% 30%, #ffc1cb, #cf4d62);
            box-shadow: 0 0 0 6px rgba(207, 77, 98, 0.12);
        }

        .progress-head {
            display: flex;
            justify-content: space-between;
            gap: 16px;
            font-weight: 700;
            margin-bottom: 16px;
        }

        .progress-track {
            width: 100%;
            height: 12px;
            border-radius: 999px;
            overflow: hidden;
            background: rgba(111, 134, 172, 0.16);
        }

        .progress-fill {
            height: 100%;
            border-radius: 999px;
            background: linear-gradient(90deg, #2d72ff, #66beff);
        }

        .progress-caption {
            margin-top: 12px;
            color: var(--text-soft);
        }

        .pill {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border-radius: 999px;
            padding: 8px 14px;
            font-size: 0.86rem;
            font-weight: 700;
        }

        .badge-green { background: rgba(47, 155, 108, 0.14); color: #1d7550; }
        .badge-red { background: rgba(207, 77, 98, 0.14); color: #9f3043; }
        .badge-blue { background: rgba(45, 114, 255, 0.14); color: #1f59cf; }
        .badge-indigo { background: rgba(97, 84, 217, 0.14); color: #4b3ab3; }
        .badge-silver { background: rgba(94, 112, 137, 0.14); color: #52647d; }
        .badge-cyan { background: rgba(45, 177, 222, 0.14); color: #0f7898; }

        .dashboard-card-title {
            font-size: 1.08rem;
            font-weight: 760;
            margin-bottom: 18px;
            letter-spacing: -0.02em;
        }

        .dashboard-card {
            min-height: 430px;
            display: flex;
            flex-direction: column;
        }

        .dashboard-big-number {
            font-size: 2rem;
            font-weight: 780;
            letter-spacing: -0.04em;
            margin-bottom: 4px;
        }

        .dashboard-big-number.small {
            font-size: 1.5rem;
        }

        .dashboard-line {
            color: var(--text-main);
            margin-bottom: 8px;
        }

        .dashboard-line.dashboard-strong {
            font-weight: 760;
        }

        .dashboard-status {
            font-weight: 700;
            margin-bottom: 14px;
        }

        .dashboard-spacer {
            height: 8px;
        }

        .dashboard-divider {
            height: 1px;
            background: rgba(96, 112, 137, 0.14);
            margin: 16px 0;
        }

        .mini-progress-block {
            margin-top: 16px;
        }

        .mini-progress-top {
            display: flex;
            justify-content: space-between;
            gap: 12px;
            font-weight: 700;
            margin-bottom: 10px;
        }

        .tool-chip-row {
            display: grid;
            grid-template-columns: repeat(5, minmax(0, 1fr));
            gap: 12px;
        }

        .tool-chip {
            border-radius: 22px;
            padding: 14px 12px;
            text-align: center;
            font-weight: 700;
            background: rgba(255, 255, 255, 0.56);
            border: 1px solid rgba(255, 255, 255, 0.86);
        }

        .vault-device-card {
            padding-bottom: 14px;
            margin-bottom: 0.55rem;
        }

        .guest-vault-card {
            min-height: 0 !important;
            height: auto !important;
            padding-top: 18px;
            padding-bottom: 12px;
        }

        .decrypt-highlight-card {
            margin: 0 0 14px 0;
            border: 1px solid rgba(45, 114, 255, 0.24);
            background: linear-gradient(135deg, rgba(45, 114, 255, 0.10), rgba(99, 187, 255, 0.12));
        }

        .decrypt-list-header {
            display: grid;
            grid-template-columns: 0.72fr 0.9fr 3fr;
            gap: 12px;
            padding: 0 12px 10px 12px;
            color: #65758f;
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-weight: 700;
            text-align: center;
        }

        .decrypt-list-row {
            display: grid;
            grid-template-columns: 0.72fr 0.9fr 3fr;
            gap: 12px;
            align-items: center;
            padding: 12px;
            margin-bottom: 10px;
            border-radius: 22px;
            background: rgba(255, 255, 255, 0.58);
            border: 1px solid rgba(255, 255, 255, 0.86);
        }

        .decrypt-list-row span {
            color: var(--text-main);
            font-size: 0.92rem;
            text-align: center;
            word-break: break-word;
        }

        .decrypt-list-row.latest {
            border-color: rgba(45, 114, 255, 0.3);
            background: linear-gradient(135deg, rgba(45, 114, 255, 0.08), rgba(99, 187, 255, 0.12));
        }

        .decrypt-pill {
            display: inline-flex;
            margin-left: 8px;
            padding: 4px 8px;
            border-radius: 999px;
            background: rgba(24, 173, 115, 0.16);
            color: #0f7f54;
            font-size: 0.74rem;
            font-weight: 800;
            letter-spacing: 0.08em;
            vertical-align: middle;
        }

        .vault-device-name {
            font-size: 1.08rem;
            font-weight: 760;
            margin-bottom: 12px;
        }

        .vault-device-line {
            margin-bottom: 8px;
            color: var(--text-main);
            word-break: break-word;
        }

        .history-page-note {
            color: var(--text-soft);
            padding-top: 2.2rem;
            text-align: right;
        }

        .history-grid-header {
            display: grid;
            grid-template-columns: 1.35fr 0.95fr 0.55fr 0.8fr 0.85fr 1.35fr;
            gap: 12px;
            padding: 0 12px 10px 12px;
            color: #65758f;
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-weight: 700;
            text-align: center;
        }

        .history-list-row {
            display: grid;
            grid-template-columns: 1.35fr 0.95fr 0.55fr 0.8fr 0.85fr 1.35fr;
            gap: 12px;
            align-items: center;
            padding: 12px;
            margin-bottom: 10px;
            border-radius: 22px;
            background: rgba(255, 255, 255, 0.58);
            border: 1px solid rgba(255, 255, 255, 0.86);
        }

        .history-list-row span {
            color: var(--text-main);
            font-size: 0.92rem;
            overflow-wrap: anywhere;
            word-break: break-word;
            white-space: normal;
            line-height: 1.35;
            text-align: center;
        }

        .history-grid-header.library-grid,
        .history-list-row.library-grid {
            grid-template-columns: 1.35fr 1.05fr 0.7fr 1.2fr 1.2fr;
        }

        .history-grid-header.vault-grid-header,
        .history-list-row.vault-list-row {
            grid-template-columns: 0.75fr 1.8fr 1.15fr;
        }

        .history-grid-header.decrypt-grid-header,
        .history-list-row.decrypt-grid-row {
            grid-template-columns: 2.15fr 1.05fr 1fr 0.95fr;
        }

        .imei-db-header {
            text-align: center;
            color: #65758f;
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-weight: 750;
            padding: 4px 6px 8px;
        }

        .imei-db-cell {
            min-height: 44px;
            display: flex;
            align-items: center;
            justify-content: center;
            text-align: center;
            border-radius: 18px;
            background: rgba(255, 255, 255, 0.48);
            border: 1px solid rgba(255, 255, 255, 0.74);
            color: var(--text-main);
            font-size: 0.92rem;
            padding: 10px 12px;
            margin-bottom: 8px;
            word-break: break-word;
        }

        .scan-status-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
            min-height: 30px;
            margin-bottom: 4px;
        }

        .scan-status-main {
            display: inline-flex;
            align-items: center;
            gap: 10px;
            color: var(--text-main);
            font-weight: 650;
        }

        .scan-spinner {
            width: 14px;
            height: 14px;
            border-radius: 50%;
            border: 2px solid rgba(45, 114, 255, 0.2);
            border-top-color: #2d72ff;
            animation: scan-spin 0.8s linear infinite;
            flex-shrink: 0;
        }

        .scan-status-pill {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            min-width: 74px;
            padding: 6px 10px;
            border-radius: 999px;
            font-size: 0.78rem;
            font-weight: 800;
            letter-spacing: 0.08em;
        }

        .scan-badge-hit {
            background: rgba(24, 173, 115, 0.16);
            color: #0f7f54;
        }

        .scan-badge-valid {
            background: rgba(255, 184, 0, 0.18);
            color: #9a6a00;
        }

        .scan-badge-error {
            background: rgba(227, 72, 72, 0.14);
            color: #b12d2d;
        }

        .scan-badge-neutral {
            background: rgba(91, 108, 133, 0.14);
            color: #44546d;
        }

        @keyframes scan-spin {
            from { transform: rotate(0deg); }
            to { transform: rotate(360deg); }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <style>
        .oneui-table {
            width: 100%;
            border-collapse: collapse;
        }

        .oneui-table th,
        .oneui-table td {
            padding: 14px 10px;
            text-align: center;
            border-bottom: 1px solid rgba(96, 112, 137, 0.14);
        }

        .oneui-table thead th {
            color: #65758f;
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.12em;
        }

        .oneui-empty {
            padding: 26px 18px;
            text-align: center;
            color: var(--text-soft);
        }

        .result-top {
            display: flex;
            justify-content: space-between;
            gap: 18px;
            align-items: flex-start;
        }

        .result-title {
            font-size: 1.5rem;
            font-weight: 750;
            letter-spacing: -0.03em;
        }

        .result-meta {
            margin-top: 8px;
            color: var(--text-soft);
        }

        .result-grid {
            margin-top: 20px;
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 14px;
        }

        .result-grid > div {
            border-radius: 24px;
            padding: 16px 18px;
            background: rgba(255, 255, 255, 0.48);
            border: 1px solid rgba(255, 255, 255, 0.7);
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .meta-label {
            color: #6f809a;
            font-size: 0.8rem;
            text-transform: uppercase;
            letter-spacing: 0.12em;
        }

        .meta-value {
            color: var(--text-main);
            font-weight: 650;
            word-break: break-word;
        }

        .activity-dock {
            position: fixed;
            left: var(--content-left);
            width: var(--content-width);
            bottom: 18px;
            z-index: 999;
            border-radius: 32px;
            padding: 18px 20px;
            background: rgba(248, 251, 255, 0.88);
            backdrop-filter: blur(24px);
            border: 1px solid rgba(255, 255, 255, 0.96);
            box-shadow: 0 18px 46px rgba(80, 102, 146, 0.16);
        }

        .activity-head {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 16px;
            margin-bottom: 12px;
        }

        .activity-title {
            font-weight: 750;
            font-size: 1rem;
        }

        .activity-list {
            display: flex;
            flex-wrap: nowrap;
            overflow-x: auto;
            gap: 12px;
            padding-bottom: 2px;
        }

        .activity-item {
            border-radius: 20px;
            padding: 10px 12px;
            background: rgba(255, 255, 255, 0.58);
            border: 1px solid rgba(255, 255, 255, 0.82);
            display: flex;
            gap: 12px;
            align-items: center;
            min-width: 290px;
        }

        .activity-time {
            color: #6e7f97;
            font-size: 0.82rem;
            min-width: 52px;
        }

        .activity-message {
            color: var(--text-main);
            font-size: 0.9rem;
            line-height: 1.35;
        }

        .left-pane-footer {
            margin-top: 1rem;
            padding-top: 0.85rem;
            border-top: 1px solid rgba(96, 112, 137, 0.12);
        }

        .stButton > button,
        .stLinkButton a {
            border-radius: 999px !important;
            min-height: 46px;
            font-weight: 700;
            border: 0 !important;
            background: linear-gradient(135deg, #2d72ff, #63bbff) !important;
            color: white !important;
            box-shadow: 0 12px 24px rgba(58, 117, 228, 0.25);
        }

        .stTextInput input,
        .stSelectbox div[data-baseweb="select"] > div,
        .stTextArea textarea {
            border-radius: 22px !important;
            background: rgba(255, 255, 255, 0.82) !important;
            border: 1px solid rgba(255, 255, 255, 0.92) !important;
        }

        .stCodeBlock, pre {
            border-radius: 22px !important;
        }

        .compact {
            margin-bottom: 0;
        }

        [data-testid="stSidebar"] {
            background:
                linear-gradient(180deg, rgba(248, 251, 255, 0.98), rgba(235, 243, 255, 0.96));
            border-right: 1px solid rgba(255, 255, 255, 0.96);
            box-shadow: 8px 0 28px rgba(87, 110, 150, 0.10);
        }

        [data-testid="stSidebar"] > div:first-child {
            width: 320px;
        }

        [data-testid="stSidebarContent"] {
            padding-top: 1.2rem;
            padding-bottom: 1.2rem;
        }

        .left-pane-shell {
            border-radius: 30px;
            padding: 18px 16px 10px 16px;
            background: rgba(255, 255, 255, 0.54);
            border: 1px solid rgba(255, 255, 255, 0.84);
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.6);
            margin-bottom: 1rem;
        }

        .left-pane-title {
            font-size: 1.05rem;
            font-weight: 770;
            letter-spacing: -0.02em;
            margin-bottom: 6px;
            color: var(--text-main);
        }

        .left-pane-copy {
            color: var(--text-soft);
            font-size: 0.92rem;
            line-height: 1.5;
            margin-bottom: 14px;
        }

        [data-testid="stSidebar"] div[role="radiogroup"] {
            gap: 10px;
            padding: 0;
            background: transparent;
            border: 0;
            margin-bottom: 0;
            display: grid;
        }

        [data-testid="stSidebar"] div[role="radiogroup"] label {
            width: 100%;
            box-sizing: border-box;
            display: flex;
            border-radius: 22px;
            padding: 12px 14px;
            font-weight: 700;
            color: #465b7f;
            background: rgba(255, 255, 255, 0.58);
            border: 1px solid rgba(255, 255, 255, 0.86);
            margin: 0;
        }

        [data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) {
            background: linear-gradient(135deg, #2d72ff, #63bbff) !important;
            color: white !important;
        }

        @media (max-width: 1100px) {
            .result-grid {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }

            .tool-chip-row {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }

            [data-testid="stSidebar"] > div:first-child {
                width: 280px;
            }
        }

        @media (max-width: 760px) {
            .oneui-header {
                width: 100%;
                padding: 16px 18px;
                border-radius: 24px;
                flex-direction: column;
                align-items: flex-start;
            }

            .block-container {
                padding-top: 1rem;
                padding-bottom: 14rem;
            }

            .activity-dock {
                left: 14px;
                width: calc(100vw - 28px);
                bottom: 12px;
            }

            .result-grid {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.dialog("Download Link")
def show_download_dialog(payload: dict[str, Any]) -> None:
    st.caption("Copy this command and run it anywhere you want. The FOTA suffix has already been appended.")
    st.code(payload["curl_command"], language="bash")
    st.text_area("Copy-ready command", value=payload["curl_command"], height=160)
    st.link_button("Open final URL", payload["download_url"], use_container_width=True)
    if st.button("Close", key="close_dialog", use_container_width=True):
        st.session_state.dialog_payload = None
        st.rerun()


@st.dialog("Fetch Download Link", width="medium")
def show_fota_fetch_dialog() -> None:
    request = st.session_state.get("fota_live_request")
    if not request:
        st.info("No active FOTA fetch request.")
        return

    if st.session_state.get("fota_live_result") is None and st.session_state.get("fota_live_error") is None:
        status_box = st.status("Starting live fetch...", expanded=True)
        progress = st.progress(0)
        status_box.write("Checking local cache and preparing the Samsung OTA request...")
        progress.progress(18)
        try:
            status_box.write("Running live FOTA fetch...")
            progress.progress(42)
            result = lookup_download_link(
                request["model"],
                request["csc"],
                request["imei"],
                request.get("base"),
                use_cache=False,
            )
            progress.progress(82)
            st.session_state.fota_live_result = result
            st.session_state.last_result = result
            progress.progress(100)
            status_box.update(label="Fetch complete", state="complete")
        except Exception as exc:
            st.session_state.fota_live_error = str(exc)
            progress.progress(100)
            status_box.update(label="Fetch failed", state="error")

    result = st.session_state.get("fota_live_result")
    error = st.session_state.get("fota_live_error")

    if error:
        st.error(error)
    elif result:
        st.markdown("**Fetch Result**")
        detail_cols = st.columns(2, gap="medium")
        with detail_cols[0]:
            st.markdown("**Model / CSC**")
            st.code(f"{result.get('model', 'Unknown')} / {result.get('csc', 'UNK')}", language="text")
            st.markdown("**Base**")
            st.code(result.get("base", "Unknown") or "Unknown", language="text")
        with detail_cols[1]:
            st.markdown("**Found PDA**")
            st.code(result.get("found_pda", "Unknown") or "Unknown", language="text")
            st.markdown("**Status**")
            st.code(result.get("status", "Unknown") or "Unknown", language="text")

        if result.get("kind") in {"update", "dm"} and result.get("curl_command"):
            st.markdown("**Download Command**")
            st.code(result["curl_command"], language="bash")
            st.text_area("curl command", value=result["curl_command"], height=140)
            action_cols = st.columns(2, gap="medium")
            with action_cols[0]:
                st.link_button("Open Raw Link", result["download_url"], use_container_width=True)
            with action_cols[1]:
                if st.button("Open Copy Popup", key="open_copy_popup_from_live_fetch", use_container_width=True):
                    st.session_state.dialog_payload = result
                    st.rerun()
        elif result.get("kind") == "uptodate":
            st.info(result.get("status", "No update found."))
        else:
            st.error(result.get("status", "Lookup failed."))

    if st.button("Close", key="close_fota_live_dialog", use_container_width=True):
        st.session_state.fota_live_request = None
        st.session_state.fota_live_result = None
        st.session_state.fota_live_error = None
        st.rerun()


@st.dialog("IMEI Scanner", width="medium")
def show_imei_scan_dialog() -> None:
    request = st.session_state.get("imei_live_request")
    if not request:
        st.info("No active IMEI scan request.")
        return

    result = st.session_state.get("imei_live_result")
    error = st.session_state.get("imei_live_error")
    state = st.session_state.get("imei_live_state")

    if result is None and error is None and state is None:
        state = init_imei_live_scan_state(request)
        st.session_state.imei_live_state = state
        queue_activity("info", f"IMEI scanner started for {request['model']} with {state['attempts']} attempts.")

    status_placeholder = st.empty()
    progress_cols = st.columns([7, 1.2], gap="small")
    with progress_cols[0]:
        progress_placeholder = st.empty()
    with progress_cols[1]:
        percent_placeholder = st.empty()
    counter_cols = st.columns(3, gap="small")
    hit_placeholder = counter_cols[0].empty()
    valid_placeholder = counter_cols[1].empty()
    error_placeholder = counter_cols[2].empty()

    def render_scan_shell(scan_state: dict[str, Any]) -> None:
        percent_value = (scan_state["index"] / max(scan_state["attempts"], 1)) * 100
        badge = ""
        outcome = scan_state.get("status_outcome", "")
        if outcome:
            badge_class = {
                "HIT": "scan-badge-hit",
                "VALID": "scan-badge-valid",
                "ERROR": "scan-badge-error",
            }.get(outcome, "scan-badge-neutral")
            badge = f'<span class="scan-status-pill {badge_class}">{html.escape(outcome)}</span>'
        status_placeholder.markdown(
            f"""
            <div class="scan-status-row">
                <div class="scan-status-main">
                    <span class="scan-spinner"></span>
                    <span>{html.escape(scan_state["status"])}</span>
                </div>
                <div>{badge}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        progress_placeholder.progress(min(max(percent_value / 100, 0.0), 1.0))
        percent_placeholder.markdown(
            f"<div style='text-align:right;padding-top:0.35rem;font-size:0.92rem;color:var(--text-main);font-weight:700;'>{percent_value:.1f}%</div>",
            unsafe_allow_html=True,
        )
        hit_placeholder.markdown(f"🟢 **HIT** - {scan_state['hit_count']}")
        valid_placeholder.markdown(f"⚠️ **VALID** - {scan_state['valid_count']}")
        error_placeholder.markdown(f"⛔ **ERROR** - {scan_state['error_count']}")

    if state and result is None and error is None:
        render_scan_shell(state)

        while state["index"] < state["stop_after_index"] and not state["paused_for_auth"]:
            current_imei = state["current_imei"]
            current_attempt = state["index"] + 1
            state["status"] = f"Scanning {current_imei} ({current_attempt}/{state['attempts']})"
            state["status_outcome"] = ""
            render_scan_shell(state)

            try:
                response = lookup_download_link(
                    state["model"],
                    state["csc"],
                    current_imei,
                    state["base"],
                    use_cache=False,
                )
            except Exception as exc:
                response = {
                    "kind": "error",
                    "status": str(exc),
                    "source": "remote",
                    "found_pda": "",
                }

            state["results"].append(
                {
                    "attempt": str(current_attempt),
                    "imei": current_imei,
                    "status": format_imei_status_text(response.get("status", response.get("kind", "Unknown"))),
                    "source": response.get("source", "remote"),
                    "firmware": response.get("found_pda", ""),
                    "kind": response.get("kind", ""),
                }
            )

            outcome = classify_imei_result(response)
            state["status_outcome"] = outcome

            if outcome == "HIT":
                state["hit_count"] += 1
                if state["last_hit"] is None:
                    state["last_hit"] = response
                state["hits"].append(
                    {
                        "imei": current_imei,
                        "firmware": response.get("found_pda", "Unknown") or "Unknown",
                    }
                )
            elif outcome == "VALID":
                state["valid_count"] += 1
            else:
                state["error_count"] += 1

            state["index"] += 1
            state["current_imei"] = increment_imei(current_imei, state["step"])
            render_scan_shell(state)

            if is_auth_failed_result(response) and not state.get("force_continue", False):
                state["paused_for_auth"] = True
                state["auth_error_label"] = format_imei_status_text(response.get("status", "auth_failed"))
                state["auth_error_raw"] = str(response.get("status", "auth_failed"))
                push_activity(
                    "error",
                    f"A user has encountered Auth Maked Failed during an IMEI Scanner for {state['model']}.",
                )
                st.session_state.imei_live_state = state
                break

        if state["index"] >= state["stop_after_index"] and not state["paused_for_auth"]:
            finalize_imei_live_scan(
                state,
                terminated=False,
                termination_reason=state.get("auth_error_label", ""),
            )
            result = st.session_state.get("imei_live_result")
            state = None

    if error:
        st.error(error)
    elif result:
        if result.get("message"):
            if result.get("terminated"):
                st.warning(result["message"])
            else:
                st.info(result["message"])

        st.markdown("**Successful IMEIs**")
        hits = result.get("hits", [])
        if hits:
            for hit in hits:
                st.code(f"{hit['imei']} - Found {hit['firmware']}!", language="text")
        else:
            st.info("No IMEIs in this live scan produced an update from the firmware base you entered.")

        if request.get("consumer") == "fota_scanner":
            database_imei = str(request.get("database_imei", "") or "")
            st.markdown("<div class='section-spacer-sm'></div>", unsafe_allow_html=True)
            if hits:
                hit_map = {
                    f"{item['imei']} - Found {short_version(item['firmware'])}!": item["imei"]
                    for item in hits
                }
                selected_label = st.selectbox(
                    "Choose an IMEI for FOTA Scanner",
                    list(hit_map.keys()),
                    key="fota_popup_selected_imei_hit",
                )
                action_cols = st.columns(2, gap="medium")
                with action_cols[0]:
                    if st.button("Use Scanned IMEI", key="fota_popup_use_scanned_imei", use_container_width=True):
                        st.session_state.fota_scanned_imei = hit_map[selected_label]
                        st.session_state.imei_live_request = None
                        st.session_state.imei_live_result = None
                        st.session_state.imei_live_error = None
                        st.session_state.imei_live_state = None
                        st.rerun()
                with action_cols[1]:
                    if st.button("Use IMEI from Database", key="fota_popup_use_db_imei", use_container_width=True):
                        st.session_state.fota_scanned_imei = database_imei
                        st.session_state.imei_live_request = None
                        st.session_state.imei_live_result = None
                        st.session_state.imei_live_error = None
                        st.session_state.imei_live_state = None
                        st.rerun()
            else:
                st.info("No HIT IMEIs were found. It is recommended to use the IMEI from the database.")
                if st.button("Use IMEI from Database", key="fota_popup_use_db_fallback", use_container_width=True):
                    st.session_state.fota_scanned_imei = database_imei
                    st.session_state.imei_live_request = None
                    st.session_state.imei_live_result = None
                    st.session_state.imei_live_error = None
                    st.session_state.imei_live_state = None
                    st.rerun()

        summary_cols = st.columns(3, gap="small")
        with summary_cols[0]:
            st.markdown(f"🟢 **HIT** - {result.get('hit_count', 0)}")
        with summary_cols[1]:
            st.markdown(f"⚠️ **VALID** - {result.get('valid_count', 0)}")
        with summary_cols[2]:
            st.markdown(f"⛔ **ERROR** - {result.get('error_count', 0)}")
    elif state and state.get("paused_for_auth"):
        st.warning(
            f"{state['auth_error_label']} Detected. Use VPNs and either continue for 3 more IMEIs or terminate the process."
        )
        st.caption(f"Error detail: {state['auth_error_raw']}")
        action_cols = st.columns(3, gap="medium")
        with action_cols[0]:
            if st.button("Continue", key="imei_auth_continue", use_container_width=True):
                state["paused_for_auth"] = False
                state["stop_after_index"] = min(state["index"] + 3, state["attempts"])
                st.session_state.imei_live_state = state
                st.rerun()
        with action_cols[1]:
            if st.button("Force Continue", key="imei_auth_force_continue", use_container_width=True):
                state["paused_for_auth"] = False
                state["force_continue"] = True
                state["stop_after_index"] = state["attempts"]
                st.session_state.imei_live_state = state
                st.rerun()
        with action_cols[2]:
            if st.button("Terminate Process", key="imei_auth_terminate", use_container_width=True):
                finalize_imei_live_scan(
                    state,
                    terminated=True,
                    termination_reason=state["auth_error_label"],
                )
                st.rerun()

    if st.button("Close", key="close_imei_live_dialog", use_container_width=True):
        st.session_state.imei_live_request = None
        st.session_state.imei_live_result = None
        st.session_state.imei_live_error = None
        st.session_state.imei_live_state = None
        st.rerun()


@st.dialog("Discovery Details", width="medium")
def show_history_detail_dialog(row: dict[str, Any]) -> None:
    link = row.get("dm_url") or row.get("fota_url") or ""
    final_url = normalize_download_url(link) if link else ""
    filename = build_download_filename(
        row.get("device_model", "UNKNOWN"),
        row.get("csc", "UNK"),
        row.get("request_base_version"),
        row.get("found_pda"),
        link,
    )
    curl_command = build_curl_command(filename, final_url) if final_url else ""

    fields = [
        ("ID", str(row.get("id", ""))),
        ("Finder", str(row.get("finder_name", "") or "Unknown")),
        ("Model", str(row.get("device_model", "") or "Unknown")),
        ("CSC", str(row.get("csc", "") or "UNK")),
        ("IMEI", str(row.get("imei", "") or "Unknown")),
        ("Requested Base", str(row.get("request_base_version", "") or "Unknown")),
        ("Found PDA", str(row.get("found_pda", "") or "Unknown")),
        ("Is FUMO", "Yes" if row.get("is_fumo") else "No"),
        ("Timestamp", str(row.get("timestamp", "") or "Unknown")),
        ("FOTA URL", str(row.get("fota_url", "") or "-")),
        ("DM URL", str(row.get("dm_url", "") or "-")),
    ]

    detail_cols = st.columns(2, gap="medium")
    left_fields = fields[:6]
    right_fields = fields[6:]
    with detail_cols[0]:
        for label, value in left_fields:
            st.markdown(f"**{label}**")
            st.code(value, language="text")
    with detail_cols[1]:
        for label, value in right_fields:
            st.markdown(f"**{label}**")
            st.code(value, language="text")

    if final_url:
        st.markdown("**Copy-ready curl command**")
        st.code(curl_command, language="bash")
        st.text_area("curl command", value=curl_command, height=140)
        st.link_button("Open final link", final_url, use_container_width=True)

    raw_response = row.get("raw_response")
    if raw_response:
        st.markdown("**Raw Response**")
        st.text_area("raw_response", value=str(raw_response), height=180)


def normalize_model_number(value: str | None) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    return raw if raw.startswith("SM-") else f"SM-{raw}"


def normalize_csc_code(value: str | None) -> str:
    return str(value or "").strip().upper()


def with_decrypt_db(query: str, params: tuple[Any, ...] = (), *, one: bool = False) -> Any:
    with sqlite3.connect(DECRYPTED_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(query, params)
        return cursor.fetchone() if one else cursor.fetchall()


def execute_decrypt_db(query: str, params: tuple[Any, ...] = ()) -> None:
    with sqlite3.connect(DECRYPTED_DB_PATH) as conn:
        conn.execute(query, params)
        conn.commit()
    clear_data_caches()


def imei_db_path_for_model(model: str) -> Path:
    IMEI_DB_DIR.mkdir(parents=True, exist_ok=True)
    safe_model = re.sub(r"[^A-Z0-9-]", "", normalize_model_number(model))
    return IMEI_DB_DIR / f"imei-{safe_model}.db"


def with_model_imei_db(model: str, query: str, params: tuple[Any, ...] = (), *, one: bool = False) -> Any:
    path = imei_db_path_for_model(model)
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(query, params)
        return cursor.fetchone() if one else cursor.fetchall()


def execute_model_imei_db(model: str, query: str, params: tuple[Any, ...] = ()) -> None:
    path = imei_db_path_for_model(model)
    with sqlite3.connect(path) as conn:
        conn.execute(query, params)
        conn.commit()
    clear_data_caches()


def ensure_workspace_databases() -> None:
    IMEI_DB_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DECRYPTED_DB_PATH) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS known_devices (
                model TEXT PRIMARY KEY,
                display_name TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS device_cscs (
                model TEXT NOT NULL,
                csc TEXT NOT NULL,
                current_imei TEXT,
                base_firmware TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (model, csc)
            );

            CREATE TABLE IF NOT EXISTS firmware_decryptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_model TEXT NOT NULL,
                csc TEXT NOT NULL,
                firmware_found TEXT NOT NULL,
                release_type TEXT,
                build_type TEXT,
                security_patch_date TEXT,
                year_value INTEGER DEFAULT 0,
                month_value INTEGER DEFAULT 0,
                date_discovered TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(device_model, csc, firmware_found)
            );

            CREATE TABLE IF NOT EXISTS patrol_jobs (
                job_id TEXT PRIMARY KEY,
                device_model TEXT NOT NULL,
                csc TEXT NOT NULL,
                interval_seconds INTEGER NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                last_run TEXT,
                next_run TEXT,
                status TEXT,
                last_message TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                level TEXT NOT NULL,
                model TEXT,
                csc TEXT,
                message TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS app_meta (
                meta_key TEXT PRIMARY KEY,
                meta_value TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_firmware_decryptions_lookup
            ON firmware_decryptions (
                device_model,
                csc,
                year_value DESC,
                month_value DESC,
                firmware_found DESC,
                date_discovered DESC
            );

            CREATE INDEX IF NOT EXISTS idx_device_cscs_model_updated
            ON device_cscs (model, updated_at DESC);
            """
        )
        conn.commit()
    with sqlite3.connect(ACTIVITY_DB_PATH) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS activity_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                level TEXT NOT NULL,
                tool_name TEXT,
                message TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_activity_events_created
            ON activity_events (created_at DESC, id DESC);
            """
        )
        conn.commit()


def get_app_meta(key: str) -> str:
    row = with_decrypt_db("SELECT meta_value FROM app_meta WHERE meta_key = ?", (key,), one=True)
    return str(row["meta_value"] or "") if row else ""


def set_app_meta(key: str, value: str) -> None:
    execute_decrypt_db(
        """
        INSERT INTO app_meta (meta_key, meta_value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(meta_key) DO UPDATE SET
            meta_value = excluded.meta_value,
            updated_at = excluded.updated_at
        """,
        (key, value, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )


def ensure_model_imei_schema(model: str) -> None:
    execute_model_imei_db(
        model,
        """
        CREATE TABLE IF NOT EXISTS imei_hits (
            imei TEXT NOT NULL,
            csc TEXT NOT NULL,
            firmware_hit TEXT,
            hit_count INTEGER NOT NULL DEFAULT 0,
            last_seen TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (imei, csc)
        )
        """,
    )


def ensure_known_device(
    model: str,
    csc: str | None = None,
    *,
    display_name: str | None = None,
    imei: str | None = None,
    base: str | None = None,
) -> None:
    clean_model = normalize_model_number(model)
    clean_csc = normalize_csc_code(csc)
    if not clean_model:
        return

    execute_decrypt_db(
        """
        INSERT INTO known_devices (model, display_name, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(model) DO UPDATE SET
            display_name = COALESCE(excluded.display_name, known_devices.display_name),
            updated_at = excluded.updated_at
        """,
        (
            clean_model,
            (display_name or clean_model).strip(),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )

    if clean_csc:
        execute_decrypt_db(
            """
            INSERT INTO device_cscs (model, csc, current_imei, base_firmware, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(model, csc) DO UPDATE SET
                current_imei = CASE
                    WHEN excluded.current_imei IS NOT NULL AND excluded.current_imei != '' THEN excluded.current_imei
                    ELSE device_cscs.current_imei
                END,
                base_firmware = CASE
                    WHEN excluded.base_firmware IS NOT NULL AND excluded.base_firmware != '' THEN excluded.base_firmware
                    ELSE device_cscs.base_firmware
                END,
                updated_at = excluded.updated_at
            """,
            (
                clean_model,
                clean_csc,
                str(imei or "").strip(),
                str(base or "").strip(),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )


def legacy_source_signature() -> str:
    return combined_path_signature(DEVICES_PATH, DB_PATH)


def sync_legacy_sources(*, force: bool = False) -> bool:
    current_signature = legacy_source_signature()
    session_signature = str(st.session_state.get("_legacy_sync_signature", "") or "")
    if not force and session_signature == current_signature:
        return False

    stored_signature = get_app_meta("legacy_sync_signature")
    if not force and stored_signature == current_signature:
        st.session_state._legacy_sync_signature = current_signature
        return False

    known_devices: dict[str, str] = {}
    csc_rows: dict[tuple[str, str], dict[str, str]] = {}

    def merge_device(
        model: str,
        csc: str,
        *,
        display_name: str = "",
        imei: str = "",
        base: str = "",
    ) -> None:
        clean_model = normalize_model_number(model)
        clean_csc = normalize_csc_code(csc)
        if not clean_model:
            return
        label = str(display_name or clean_model).strip() or clean_model
        known_devices.setdefault(clean_model, label)
        if clean_csc:
            entry = csc_rows.setdefault(
                (clean_model, clean_csc),
                {
                    "imei": "",
                    "base": "",
                },
            )
            if imei and not entry["imei"]:
                entry["imei"] = str(imei).strip()
            if base and not entry["base"]:
                entry["base"] = str(base).strip()

    if DEVICES_PATH.exists():
        for entry in flatten_devices(load_device_catalog()):
            merge_device(
                entry.get("model", ""),
                entry.get("csc", ""),
                display_name=str(entry.get("name", "")),
                imei=str(entry.get("imei", "")),
                base=str(entry.get("base", "")),
            )

    if DB_PATH.exists():
        rows = with_db(
            """
            SELECT device_model, csc, imei, request_base_version
            FROM firmware_hits
            WHERE device_model IS NOT NULL AND device_model != ''
            ORDER BY datetime(timestamp) DESC, id DESC
            """
        )
        for row in rows:
            merge_device(
                row["device_model"],
                row["csc"],
                imei=str(row["imei"] or ""),
                base=str(row["request_base_version"] or ""),
            )

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DECRYPTED_DB_PATH) as conn:
        conn.executemany(
            """
            INSERT INTO known_devices (model, display_name, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(model) DO UPDATE SET
                display_name = COALESCE(excluded.display_name, known_devices.display_name),
                updated_at = excluded.updated_at
            """,
            [(model, label, timestamp) for model, label in known_devices.items()],
        )
        conn.executemany(
            """
            INSERT INTO device_cscs (model, csc, current_imei, base_firmware, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(model, csc) DO UPDATE SET
                current_imei = CASE
                    WHEN excluded.current_imei IS NOT NULL AND excluded.current_imei != '' THEN excluded.current_imei
                    ELSE device_cscs.current_imei
                END,
                base_firmware = CASE
                    WHEN excluded.base_firmware IS NOT NULL AND excluded.base_firmware != '' THEN excluded.base_firmware
                    ELSE device_cscs.base_firmware
                END,
                updated_at = excluded.updated_at
            """,
            [
                (model, csc, payload["imei"], payload["base"], timestamp)
                for (model, csc), payload in csc_rows.items()
            ],
        )
        conn.execute(
            """
            INSERT INTO app_meta (meta_key, meta_value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(meta_key) DO UPDATE SET
                meta_value = excluded.meta_value,
                updated_at = excluded.updated_at
            """,
            ("legacy_sync_signature", current_signature, timestamp),
        )
        conn.commit()

    st.session_state._legacy_sync_signature = current_signature
    clear_data_caches()
    return True


def build_type_and_release_type(model: str, csc: str, firmware: str) -> tuple[str, str]:
    if not firmware or firmware in {"Not Set", "UNK"}:
        return "Unknown", "Unknown"

    pda = firmware.split("/")[0] if "/" in firmware else firmware
    build_type = "Unknown"
    if len(pda) >= 4 and pda[-4:].upper().startswith("Z"):
        build_type = "Beta"
    elif len(pda) >= 4 and pda[-4].upper() == "Z":
        build_type = "Beta"
    elif len(pda) >= 6 and pda[-6].upper() == "S":
        build_type = "Security"
    elif len(pda) >= 6 and pda[-6].upper() == "U":
        build_type = "Stable"
    elif pda.upper().endswith(".DM"):
        build_type = "DM Build"

    release_type = "Internal"
    clean_model = normalize_model_number(model)
    clean_csc = normalize_csc_code(csc)
    if clean_model and clean_csc:
        try:
            url = f"https://fota-cloud-dn.ospserver.net/firmware/{clean_csc}/{clean_model}/version.xml"
            response = ota.session.get(url, timeout=3)
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                latest = (root.findtext(".//latest") or "").split("/")[0]
                upgrade = (root.findtext(".//upgrade") or "").split("/")[0]
                if pda == latest or pda == upgrade:
                    release_type = "Official"
        except Exception:
            pass
    return release_type, build_type


def format_security_patch_value(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Unknown"
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y.%m.%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw


def infer_security_patch_from_version(version: str | None) -> str:
    year, month = dc3.parse_date_from_version(str(version or ""))
    if year and month:
        return f"{year:04d}-{month:02d}-01"
    return "Unknown"


def firmware_sort_key(version: str | None) -> tuple[int, int, str]:
    clean = str(version or "")
    year, month = dc3.parse_date_from_version(clean)
    return (year, month, clean)


def latest_firmware_for_model_csc(model: str, csc: str) -> str:
    row = with_decrypt_db(
        """
        SELECT firmware_found
        FROM firmware_decryptions
        WHERE device_model = ? AND csc = ?
        ORDER BY year_value DESC, month_value DESC, firmware_found DESC, datetime(date_discovered) DESC, id DESC
        LIMIT 1
        """,
        (normalize_model_number(model), normalize_csc_code(csc)),
        one=True,
    )
    if row:
        return str(row["firmware_found"] or "")
    history = with_db(
        """
        SELECT found_pda
        FROM firmware_hits
        WHERE device_model = ? AND csc = ? AND found_pda IS NOT NULL AND found_pda != ''
        ORDER BY datetime(timestamp) DESC, id DESC
        LIMIT 1
        """,
        (normalize_model_number(model), normalize_csc_code(csc)),
        one=True,
    )
    return str(history["found_pda"] or "") if history else ""


def existing_decrypted_versions(model: str, csc: str) -> set[str]:
    rows = with_decrypt_db(
        """
        SELECT firmware_found
        FROM firmware_decryptions
        WHERE device_model = ? AND csc = ?
        """,
        (normalize_model_number(model), normalize_csc_code(csc)),
    )
    return {str(row["firmware_found"] or "").strip() for row in rows if str(row["firmware_found"] or "").strip()}


def upsert_decrypted_firmware_rows(model: str, csc: str, items: list[dict[str, Any]]) -> tuple[str, str, str]:
    clean_model = normalize_model_number(model)
    clean_csc = normalize_csc_code(csc)
    before_latest = latest_firmware_for_model_csc(clean_model, clean_csc)
    existing_versions = existing_decrypted_versions(clean_model, clean_csc)
    new_versions = [
        str(item.get("version", "")).strip()
        for item in items
        if str(item.get("version", "")).strip() and str(item.get("version", "")).strip() not in existing_versions
    ]
    discovered_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for item in items:
        version = str(item.get("version", "")).strip()
        if not version:
            continue
        year, month = dc3.parse_date_from_version(version)
        release_type, build_type = build_type_and_release_type(clean_model, clean_csc, version)
        execute_decrypt_db(
            """
            INSERT INTO firmware_decryptions (
                device_model, csc, firmware_found, release_type, build_type,
                security_patch_date, year_value, month_value, date_discovered
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(device_model, csc, firmware_found) DO UPDATE SET
                release_type = excluded.release_type,
                build_type = excluded.build_type,
                security_patch_date = excluded.security_patch_date,
                year_value = excluded.year_value,
                month_value = excluded.month_value,
                date_discovered = excluded.date_discovered
            """,
            (
                clean_model,
                clean_csc,
                version,
                release_type,
                build_type,
                infer_security_patch_from_version(version),
                year,
                month,
                discovered_at,
            ),
        )
    after_latest = latest_firmware_for_model_csc(clean_model, clean_csc)
    ensure_known_device(clean_model, clean_csc, base=after_latest or before_latest)
    latest_new = max(new_versions, key=firmware_sort_key) if new_versions else ""
    return before_latest, after_latest, latest_new


def decrypt_device_live(
    model: str,
    csc: str,
    *,
    persist: bool = True,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    clean_model = normalize_model_number(model)
    clean_csc = normalize_csc_code(csc)
    ensure_known_device(clean_model, clean_csc)
    existing_versions_before = existing_decrypted_versions(clean_model, clean_csc)
    server_md5s = dc3.get_md5_list(clean_model, clean_csc)
    if not server_md5s:
        raise RuntimeError("No firmware list was returned for this device and CSC.")

    latest_version, osver, base_cc = dc3.get_latest_with_fallback(clean_model, clean_csc)
    decrypted_map = dc3.decrypt_firmware(
        clean_model,
        clean_csc,
        set(server_md5s),
        latest_version,
        full_brute=True,
        progress_callback=progress_callback,
    )
    items = sorted(
        decrypted_map.values(),
        key=lambda item: (
            int(item.get("year", 0) or 0),
            int(item.get("month", 0) or 0),
            str(item.get("version", "")),
        ),
        reverse=True,
    )
    new_versions = [
        str(item.get("version", "")).strip()
        for item in items
        if str(item.get("version", "")).strip()
        and str(item.get("version", "")).strip() not in existing_versions_before
    ]

    previous_latest = latest_firmware_for_model_csc(clean_model, clean_csc)
    current_latest = previous_latest
    latest_new = ""
    if persist and items:
        _, current_latest, latest_new = upsert_decrypted_firmware_rows(clean_model, clean_csc, items)

    return {
        "model": clean_model,
        "csc": clean_csc,
        "latest_stable": latest_version,
        "android": osver,
        "base_csc": base_cc or clean_csc,
        "server_md5s": len(server_md5s),
        "resolved_count": len(items),
        "unresolved_count": max(0, len(server_md5s) - len(items)),
        "items": items,
        "new_versions": sorted(new_versions, key=firmware_sort_key, reverse=True),
        "previous_latest": previous_latest,
        "recorded_latest": current_latest or (items[0].get("version", "") if items else ""),
        "latest_found": latest_new,
    }


@st.cache_data(show_spinner=False)
def _model_options_from_decrypt_db_cached(decrypt_signature: str) -> list[str]:
    if decrypt_signature == "missing":
        return []
    rows = with_decrypt_db("SELECT model FROM known_devices ORDER BY model")
    return [str(row["model"]) for row in rows]


def model_options_from_decrypt_db() -> list[str]:
    return _model_options_from_decrypt_db_cached(path_signature(DECRYPTED_DB_PATH))


@st.cache_data(show_spinner=False)
def _csc_options_for_model_cached(model: str, include_all: bool, decrypt_signature: str, history_signature: str) -> list[str]:
    clean_model = normalize_model_number(model)
    options: set[str] = set()
    if not clean_model:
        return ["All CSCs"] if include_all else []

    if decrypt_signature != "missing":
        for row in with_decrypt_db("SELECT csc FROM device_cscs WHERE model = ? ORDER BY csc", (clean_model,)):
            if row["csc"]:
                options.add(str(row["csc"]).upper())
        for row in with_decrypt_db(
            "SELECT DISTINCT csc FROM firmware_decryptions WHERE device_model = ? ORDER BY csc",
            (clean_model,),
        ):
            if row["csc"]:
                options.add(str(row["csc"]).upper())

    if history_signature != "missing":
        for row in with_db("SELECT DISTINCT csc FROM firmware_hits WHERE device_model = ? ORDER BY csc", (clean_model,)):
            if row["csc"]:
                options.add(str(row["csc"]).upper())

    ordered = sorted(options)
    return (["All CSCs"] + ordered) if include_all else ordered


def csc_options_for_model(model: str, *, include_all: bool = False) -> list[str]:
    return _csc_options_for_model_cached(
        model,
        include_all,
        path_signature(DECRYPTED_DB_PATH),
        path_signature(DB_PATH),
    )


def best_device_context(model: str, csc: str | None = None) -> dict[str, str]:
    clean_model = normalize_model_number(model)
    clean_csc = normalize_csc_code(csc)
    payload = {
        "model": clean_model,
        "csc": clean_csc,
        "imei": "",
        "base": "",
        "name": clean_model or "Unknown",
    }
    if not clean_model:
        return payload

    row = with_decrypt_db(
        """
        SELECT kd.display_name, dc.csc, dc.current_imei, dc.base_firmware
        FROM known_devices kd
        LEFT JOIN device_cscs dc ON dc.model = kd.model
        WHERE kd.model = ?
          AND (? = '' OR dc.csc = ?)
        ORDER BY CASE WHEN dc.csc = ? THEN 0 ELSE 1 END, dc.updated_at DESC
        LIMIT 1
        """,
        (clean_model, clean_csc, clean_csc, clean_csc),
        one=True,
    )
    if row:
        payload["name"] = str(row["display_name"] or clean_model)
        payload["csc"] = str(row["csc"] or clean_csc)
        payload["imei"] = str(row["current_imei"] or "")
        payload["base"] = str(row["base_firmware"] or "")

    if not payload["imei"]:
        row = with_db(
            """
            SELECT imei, csc, request_base_version
            FROM firmware_hits
            WHERE device_model = ?
              AND (? = '' OR csc = ?)
            ORDER BY datetime(timestamp) DESC, id DESC
            LIMIT 1
            """,
            (clean_model, clean_csc, clean_csc),
            one=True,
        )
        if row:
            payload["imei"] = str(row["imei"] or "")
            payload["csc"] = str(row["csc"] or payload["csc"])
            payload["base"] = str(row["request_base_version"] or payload["base"])

    if not payload["base"] and payload["csc"]:
        payload["base"] = latest_firmware_for_model_csc(clean_model, payload["csc"])
    return payload


@st.cache_data(show_spinner=False)
def _known_imei_options_cached(model: str, csc: str, history_signature: str, imei_signature: str) -> list[str]:
    clean_model = normalize_model_number(model)
    clean_csc = normalize_csc_code(csc)
    if not clean_model:
        return []

    options: list[str] = []
    seen: set[str] = set()

    def add_option(value: str) -> None:
        clean_value = str(value or "").strip()
        if not clean_value or clean_value in seen:
            return
        seen.add(clean_value)
        options.append(clean_value)

    context = best_device_context(clean_model, clean_csc)
    add_option(str(context.get("imei", "") or ""))

    if imei_signature not in {"missing", "empty"}:
        path = imei_db_path_for_model(clean_model)
        if path.exists():
            query = "SELECT imei, csc FROM imei_hits"
            params: tuple[Any, ...] = ()
            if clean_csc:
                query += " WHERE csc = ?"
                params = (clean_csc,)
            query += " ORDER BY hit_count DESC, datetime(last_seen) DESC, imei"
            for row in with_model_imei_db(clean_model, query, params):
                add_option(str(row["imei"] or ""))

    if history_signature != "missing":
        for row in with_db(
            """
            SELECT imei
            FROM firmware_hits
            WHERE device_model = ?
              AND imei IS NOT NULL
              AND imei != ''
              AND (? = '' OR csc = ?)
            ORDER BY datetime(timestamp) DESC, id DESC
            """,
            (clean_model, clean_csc, clean_csc),
        ):
            add_option(str(row["imei"] or ""))

    return options


def known_imei_options(model: str, csc: str) -> list[str]:
    return _known_imei_options_cached(
        model,
        csc,
        path_signature(DB_PATH),
        imei_database_signature(),
    )


def update_device_imei_by_model_csc(model: str, csc: str, new_imei: str, source_label: str) -> bool:
    clean_model = normalize_model_number(model)
    clean_csc = normalize_csc_code(csc)
    if not new_imei.isdigit() or len(new_imei) != 15:
        st.error("IMEI must be exactly 15 digits.")
        return False

    ensure_known_device(clean_model, clean_csc, imei=new_imei)
    catalog = load_device_catalog()
    changed = False
    for category, entries in catalog.items():
        for entry in entries:
            if normalize_model_number(entry.get("model", "")) == clean_model and normalize_csc_code(entry.get("csc", "")) == clean_csc:
                entry["imei"] = new_imei
                changed = True
    if changed:
        save_device_catalog(catalog)

    if st.session_state.get("scan_model", "").upper() == clean_model and st.session_state.get("scan_csc", "").upper() == clean_csc:
        st.session_state.scan_imei = new_imei
    if st.session_state.get("model_input", "").upper() == clean_model and st.session_state.get("csc_input", "").upper() == clean_csc:
        st.session_state.imei_input = new_imei

    push_activity("info", f"Updated {clean_model} / {clean_csc} to IMEI {new_imei} from {source_label}.")
    return True


@st.cache_data(show_spinner=False)
def _library_rows_for_model_cached(model: str, history_signature: str) -> list[dict[str, Any]]:
    clean_model = normalize_model_number(model)
    if not clean_model or history_signature == "missing":
        return []
    return [
        dict(row)
        for row in with_db(
            """
            SELECT id, device_model, csc, request_base_version, found_pda, fota_url, dm_url, timestamp
            FROM firmware_hits
            WHERE device_model = ?
            ORDER BY datetime(timestamp) DESC, id DESC
            """,
            (clean_model,),
        )
    ]


def library_rows_for_model(model: str) -> list[dict[str, Any]]:
    return _library_rows_for_model_cached(model, path_signature(DB_PATH))


@st.cache_data(show_spinner=False)
def _latest_cached_discoveries_cached(limit: int, history_signature: str) -> list[dict[str, Any]]:
    if history_signature == "missing":
        return []
    return [
        dict(row)
        for row in with_db(
            """
            SELECT device_model, csc, found_pda, timestamp
            FROM firmware_hits
            WHERE found_pda IS NOT NULL AND found_pda != ''
            ORDER BY datetime(timestamp) DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
    ]


def latest_cached_discoveries(limit: int = 10) -> list[dict[str, Any]]:
    return _latest_cached_discoveries_cached(limit, path_signature(DB_PATH))


@st.cache_data(show_spinner=False)
def _device_vault_rows_cached(decrypt_signature: str, history_signature: str) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    seen: set[tuple[str, str]] = set()

    if decrypt_signature != "missing":
        rows = with_decrypt_db(
            """
            SELECT device_model, csc, firmware_found, date_discovered
            FROM firmware_decryptions
            ORDER BY device_model ASC, csc ASC, year_value DESC, month_value DESC, firmware_found DESC, datetime(date_discovered) DESC, id DESC
            """
        )
        for row in rows:
            key = (str(row["device_model"]), str(row["csc"]))
            if key in seen:
                continue
            seen.add(key)
            grouped.setdefault(str(row["device_model"]), []).append(
                {
                    "csc": str(row["csc"]),
                    "latest": str(row["firmware_found"]),
                    "date": str(row["date_discovered"]),
                }
            )

    if history_signature != "missing":
        history_only = with_db(
            """
            SELECT device_model, csc, MAX(timestamp) AS timestamp
            FROM firmware_hits
            WHERE device_model IS NOT NULL AND device_model != ''
            GROUP BY device_model, csc
            ORDER BY device_model, csc
            """
        )
        for row in history_only:
            model = str(row["device_model"])
            csc = str(row["csc"] or "")
            if (model, csc) in seen:
                continue
            latest = latest_firmware_for_model_csc(model, csc)
            grouped.setdefault(model, []).append(
                {
                    "csc": csc or "UNK",
                    "latest": latest or "No decrypted firmware yet",
                    "date": str(row["timestamp"] or "-"),
                }
            )

    return dict(sorted(grouped.items()))


def device_vault_rows() -> dict[str, list[dict[str, str]]]:
    return _device_vault_rows_cached(path_signature(DECRYPTED_DB_PATH), path_signature(DB_PATH))


def clear_data_caches() -> None:
    _load_device_catalog_cached.clear()
    _model_options_from_decrypt_db_cached.clear()
    _csc_options_for_model_cached.clear()
    _library_rows_for_model_cached.clear()
    _latest_cached_discoveries_cached.clear()
    _device_vault_rows_cached.clear()
    _imei_database_totals_cached.clear()


def record_imei_scan_hit(model: str, csc: str, imei: str, firmware_hit: str) -> None:
    clean_model = normalize_model_number(model)
    clean_csc = normalize_csc_code(csc)
    ensure_model_imei_schema(clean_model)
    row = with_model_imei_db(
        clean_model,
        "SELECT hit_count FROM imei_hits WHERE imei = ? AND csc = ?",
        (imei, clean_csc),
        one=True,
    )
    current_hits = int(row["hit_count"]) if row else 0
    execute_model_imei_db(
        clean_model,
        """
        INSERT INTO imei_hits (imei, csc, firmware_hit, hit_count, last_seen)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(imei, csc) DO UPDATE SET
            firmware_hit = excluded.firmware_hit,
            hit_count = excluded.hit_count,
            last_seen = excluded.last_seen
        """,
        (
            imei,
            clean_csc,
            firmware_hit,
            current_hits + 1,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )


def import_fumo_history_to_imei_db(model: str, csc: str) -> int:
    clean_model = normalize_model_number(model)
    clean_csc = normalize_csc_code(csc)
    ensure_model_imei_schema(clean_model)
    params: tuple[Any, ...]
    query = """
        SELECT imei, csc, found_pda
        FROM firmware_hits
        WHERE device_model = ?
          AND imei IS NOT NULL AND imei != ''
    """
    if clean_csc and clean_csc != "ALL CSCS":
        query += " AND csc = ?"
        params = (clean_model, clean_csc)
    else:
        params = (clean_model,)

    rows = with_db(query, params)
    inserted = 0
    for row in rows:
        existing = with_model_imei_db(
            clean_model,
            "SELECT imei FROM imei_hits WHERE imei = ? AND csc = ?",
            (row["imei"], row["csc"]),
            one=True,
        )
        if existing:
            continue
        execute_model_imei_db(
            clean_model,
            """
            INSERT INTO imei_hits (imei, csc, firmware_hit, hit_count, last_seen)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                row["imei"],
                row["csc"],
                row["found_pda"] or "",
                1 if row["found_pda"] else 0,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        inserted += 1
    return inserted


def imei_database_rows_v2(model: str, csc: str) -> list[sqlite3.Row]:
    clean_model = normalize_model_number(model)
    ensure_model_imei_schema(clean_model)
    query = "SELECT imei, csc, firmware_hit, hit_count, last_seen FROM imei_hits"
    params: tuple[Any, ...] = ()
    if csc and csc != "All CSCs":
        query += " WHERE csc = ?"
        params = (normalize_csc_code(csc),)
    query += " ORDER BY hit_count DESC, datetime(last_seen) DESC, csc, imei"
    return with_model_imei_db(clean_model, query, params)


@st.cache_data(show_spinner=False)
def _imei_database_totals_cached(imei_signature: str) -> tuple[int, int]:
    total_rows = 0
    hit_rows = 0
    if imei_signature in {"missing", "empty"}:
        return total_rows, hit_rows
    for path in IMEI_DB_DIR.glob("imei-*.db"):
        try:
            with sqlite3.connect(path) as conn:
                conn.row_factory = sqlite3.Row
                total_rows += int(conn.execute("SELECT COUNT(*) AS total FROM imei_hits").fetchone()["total"])
                hit_rows += int(
                    conn.execute(
                        "SELECT COUNT(*) AS total FROM imei_hits WHERE firmware_hit IS NOT NULL AND firmware_hit != ''"
                    ).fetchone()["total"]
                )
        except sqlite3.Error:
            continue
    return total_rows, hit_rows


def imei_database_totals() -> tuple[int, int]:
    return _imei_database_totals_cached(imei_database_signature())


def notify_everyone(level: str, message: str, model: str = "", csc: str = "") -> None:
    execute_decrypt_db(
        "INSERT INTO notifications (level, model, csc, message) VALUES (?, ?, ?, ?)",
        (level.upper(), normalize_model_number(model), normalize_csc_code(csc), message),
    )


def surface_new_notifications() -> None:
    last_seen = int(st.session_state.get("last_seen_notice_id", 0) or 0)
    rows = with_decrypt_db(
        "SELECT id, level, message FROM notifications WHERE id > ? ORDER BY id ASC",
        (last_seen,),
    )
    if not rows:
        return
    newest = last_seen
    for row in rows:
        newest = max(newest, int(row["id"]))
        try:
            st.toast(str(row["message"]), icon="🔔")
        except Exception:
            pass
    st.session_state.last_seen_notice_id = newest


def patrol_job_rows() -> list[sqlite3.Row]:
    return with_decrypt_db(
        """
        SELECT job_id, device_model, csc, interval_seconds, enabled, last_run, next_run, status, last_message
        FROM patrol_jobs
        ORDER BY device_model, csc
        """
    )


def upsert_patrol_jobs(jobs: list[dict[str, Any]]) -> None:
    current_ids = {job["job_id"] for job in jobs}
    execute_decrypt_db("UPDATE patrol_jobs SET enabled = 0, updated_at = ?", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),))
    for job in jobs:
        execute_decrypt_db(
            """
            INSERT INTO patrol_jobs (
                job_id, device_model, csc, interval_seconds, enabled, last_run, next_run, status, last_message, updated_at
            ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                device_model = excluded.device_model,
                csc = excluded.csc,
                interval_seconds = excluded.interval_seconds,
                enabled = 1,
                next_run = excluded.next_run,
                status = excluded.status,
                last_message = excluded.last_message,
                updated_at = excluded.updated_at
            """,
            (
                job["job_id"],
                normalize_model_number(job["model"]),
                normalize_csc_code(job["csc"]),
                int(job["interval_seconds"]),
                job.get("last_run"),
                job.get("next_run"),
                job.get("status", "Scheduled"),
                job.get("last_message", ""),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        ensure_known_device(job["model"], job["csc"])


def stop_patrol_job(job_id: str) -> bool:
    row = with_decrypt_db(
        "SELECT device_model, csc, enabled FROM patrol_jobs WHERE job_id = ?",
        (job_id,),
        one=True,
    )
    if row is None:
        return False
    if int(row["enabled"] or 0) != 1:
        return False

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    execute_decrypt_db(
        """
        UPDATE patrol_jobs
        SET enabled = 0,
            next_run = NULL,
            status = ?,
            last_message = ?,
            updated_at = ?
        WHERE job_id = ?
        """,
        ("Stopped", "Patrol stopped manually.", now, job_id),
    )
    with PATROL_LOCK:
        if not with_decrypt_db("SELECT job_id FROM patrol_jobs WHERE enabled = 1 LIMIT 1", one=True):
            PATROL_THREADS.pop(PATROL_COORDINATOR_KEY, None)
    push_activity("warn", f"Night Patrol stopped for {row['device_model']} / {row['csc']}.")
    return True


def enabled_patrol_rows() -> list[sqlite3.Row]:
    return with_decrypt_db(
        """
        SELECT job_id, device_model, csc, interval_seconds, enabled, last_run, next_run, status, last_message
        FROM patrol_jobs
        WHERE enabled = 1
        ORDER BY datetime(updated_at) ASC, device_model, csc
        """
    )


def patrol_pause_with_stop(seconds: int) -> bool:
    end_at = time.time() + max(int(seconds), 0)
    while time.time() < end_at:
        if not with_decrypt_db("SELECT job_id FROM patrol_jobs WHERE enabled = 1 LIMIT 1", one=True):
            return False
        time.sleep(1)
    return True


def patrol_worker() -> None:
    while True:
        rows = enabled_patrol_rows()
        if not rows:
            break

        now = datetime.now()
        next_candidates: list[datetime] = []
        for row in rows:
            next_run = str(row["next_run"] or "")
            try:
                next_candidates.append(datetime.strptime(next_run, "%Y-%m-%d %H:%M:%S") if next_run else now)
            except ValueError:
                next_candidates.append(now)
        due_at = min(next_candidates) if next_candidates else now
        if due_at > now:
            time.sleep(min(2, max((due_at - now).total_seconds(), 0)))
            continue

        cycle_interval = int(rows[0]["interval_seconds"] or 3600)
        cycle_started = datetime.now()
        for index, row in enumerate(enabled_patrol_rows()):
            current = with_decrypt_db("SELECT * FROM patrol_jobs WHERE job_id = ?", (str(row["job_id"]),), one=True)
            if current is None or int(current["enabled"] or 0) != 1:
                continue
            model = str(current["device_model"])
            csc = str(current["csc"])
            running_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            execute_decrypt_db(
                """
                UPDATE patrol_jobs
                SET status = ?, last_message = ?, updated_at = ?
                WHERE job_id = ?
                """,
                ("Running", f"Decrypting {model} / {csc}...", running_at, str(current["job_id"])),
            )
            try:
                before = latest_firmware_for_model_csc(model, csc)
                result = decrypt_device_live(model, csc, persist=True)
                after = str(result.get("latest_found", "") or "")
                message = f"Patrol completed for {model} / {csc}"
                if after and after != before and firmware_sort_key(after) > firmware_sort_key(before):
                    notice = f"New firmware decrypted for {model} | {csc}: {after}"
                    notify_everyone("HIT", notice, model, csc)
                    queue_activity("hit", notice)
                    message = notice
                else:
                    queue_activity("info", message)
                execute_decrypt_db(
                    """
                    UPDATE patrol_jobs
                    SET last_run = ?, status = ?, last_message = ?, updated_at = ?
                    WHERE job_id = ?
                    """,
                    (
                        running_at,
                        "Completed",
                        message,
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        str(current["job_id"]),
                    ),
                )
            except Exception as exc:
                execute_decrypt_db(
                    """
                    UPDATE patrol_jobs
                    SET last_run = ?, status = ?, last_message = ?, updated_at = ?
                    WHERE job_id = ?
                    """,
                    (
                        running_at,
                        "Error",
                        str(exc),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        str(current["job_id"]),
                    ),
                )
                queue_activity("error", f"Night Patrol failed for {current['device_model']} / {current['csc']}: {exc}")

            remaining_rows = enabled_patrol_rows()
            is_last_enabled = index >= max(len(remaining_rows) - 1, 0)
            if remaining_rows and not is_last_enabled:
                pause_message = "Waiting 15 seconds before the next device in this cycle."
                execute_decrypt_db(
                    "UPDATE patrol_jobs SET status = ?, last_message = ?, updated_at = ? WHERE enabled = 1",
                    ("Cycle Pause", pause_message, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                )
                if not patrol_pause_with_stop(15):
                    break

        active_rows = enabled_patrol_rows()
        if not active_rows:
            break
        next_at_value = datetime.fromtimestamp(time.time() + cycle_interval).strftime("%Y-%m-%d %H:%M:%S")
        waiting_message = f"Waiting for next cycle at {next_at_value}"
        execute_decrypt_db(
            """
            UPDATE patrol_jobs
            SET next_run = ?, status = ?, last_message = ?, updated_at = ?
            WHERE enabled = 1
            """,
            (next_at_value, "Idle", waiting_message, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        if not patrol_pause_with_stop(cycle_interval):
            break


def ensure_patrol_workers() -> None:
    with PATROL_LOCK:
        if not with_decrypt_db("SELECT job_id FROM patrol_jobs WHERE enabled = 1 LIMIT 1", one=True):
            PATROL_THREADS.pop(PATROL_COORDINATOR_KEY, None)
            return
        worker = PATROL_THREADS.get(PATROL_COORDINATOR_KEY)
        if worker and worker.is_alive():
            return
        thread = threading.Thread(target=patrol_worker, daemon=True)
        PATROL_THREADS[PATROL_COORDINATOR_KEY] = thread
        thread.start()


def finalize_imei_live_scan(
    state: dict[str, Any],
    *,
    terminated: bool = False,
    termination_reason: str = "",
) -> None:
    message = ""
    if terminated:
        message = f"The process has been terminated prematurely due to Error {termination_reason}."
    elif state["stop_after_index"] < state["attempts"]:
        fallback_reason = termination_reason or "Auth Didn't Maked!"
        message = f"Scan ended after the extra IMEIs allowed following Error {fallback_reason}."

    for hit in state.get("hits", []):
        imei_value = str(hit.get("imei", "") or "")
        firmware_hit = str(hit.get("firmware", "") or "")
        if imei_value and firmware_hit:
            record_imei_scan_hit(state["model"], state["csc"], imei_value, firmware_hit)

    st.session_state.imei_scan_results = list(state["results"])
    st.session_state.imei_last_hit = state.get("last_hit")
    st.session_state.imei_live_result = {
        "hits": list(state["hits"]),
        "attempts": state["attempts"],
        "processed": state["index"],
        "model": state["model"],
        "csc": state["csc"],
        "base": state["base"],
        "hit_count": state["hit_count"],
        "valid_count": state["valid_count"],
        "error_count": state["error_count"],
        "terminated": terminated,
        "termination_reason": termination_reason,
        "message": message,
    }
    st.session_state.imei_live_state = None

    if terminated:
        push_activity("warn", f"IMEI scan for {state['model']} was terminated due to {termination_reason}.")
    else:
        push_activity(
            "info",
            f"IMEI scan completed for {state['model']} with {state['hit_count']} hits, {state['valid_count']} valid, and {state['error_count']} errors.",
        )


@st.dialog("Link", width="medium")
def show_history_detail_dialog(row: dict[str, Any]) -> None:
    link = row.get("dm_url") or row.get("fota_url") or ""
    final_url = normalize_download_url(link) if link else ""
    filename = build_download_filename(
        row.get("device_model", "UNKNOWN"),
        row.get("csc", "UNK"),
        row.get("request_base_version"),
        row.get("found_pda"),
        link,
    )
    curl_command = build_curl_command(filename, final_url) if final_url else ""

    fields = [
        ("Timestamp", str(row.get("timestamp", "") or "Unknown")),
        ("Model number", str(row.get("device_model", "") or "Unknown")),
        ("CSC", str(row.get("csc", "") or "UNK")),
        ("Firmware Base", str(row.get("request_base_version", "") or "Unknown")),
        ("Firmware Found", str(row.get("found_pda", "") or "Unknown")),
    ]
    for label, value in fields:
        st.markdown(f"**{label}**")
        st.code(value, language="text")

    if curl_command:
        st.markdown("**Curl command**")
        st.code(curl_command, language="bash")
        st.text_area("Copy-ready curl command", value=curl_command, height=160)


def collect_status_snapshot() -> dict[str, Any]:
    decrypt_ok = DECRYPTED_DB_PATH.exists()
    history_ok = DB_PATH.exists()
    devices_ok = DEVICES_PATH.exists()
    model_count_row = with_decrypt_db("SELECT COUNT(*) AS total FROM known_devices", one=True) if decrypt_ok else {"total": 0}
    csc_count_row = with_decrypt_db("SELECT COUNT(*) AS total FROM device_cscs", one=True) if decrypt_ok else {"total": 0}
    decryption_count_row = (
        with_decrypt_db("SELECT COUNT(*) AS total FROM firmware_decryptions", one=True) if decrypt_ok else {"total": 0}
    )
    valid_imeis, imei_hits = imei_database_totals()
    fota_ok, fota_detail = probe_fota_endpoint(load_device_catalog()) if devices_ok else (False, "devices.json missing")
    fumo_ok, fumo_detail = check_endpoint("fota-secure-dn.ospserver.net")
    dms_ok, dms_detail = check_endpoint("dms.ospserver.net")
    latest_hits = with_db("SELECT COUNT(*) AS total FROM firmware_hits", one=True)["total"] if history_ok else 0
    fumo_hits = (
        with_db("SELECT COUNT(*) AS total FROM firmware_hits WHERE is_fumo = 1", one=True)["total"] if history_ok else 0
    )
    recent_valid_devices = with_decrypt_db(
        """
        SELECT COUNT(*) AS total
        FROM (
            SELECT device_model, csc, MAX(year_value * 100 + month_value) AS ym
            FROM firmware_decryptions
            GROUP BY device_model, csc
            HAVING ym > 0
        )
        """,
        one=True,
    )["total"] if decrypt_ok else 0
    total_device_variants = int(csc_count_row["total"] or 0)

    checks = [
        {"title": "Decrypt DB", "ok": decrypt_ok, "detail": "Connected" if decrypt_ok else "Missing file"},
        {"title": "History DB", "ok": history_ok, "detail": "Connected" if history_ok else "Missing file"},
        {"title": "FOTA Endpoint", "ok": fota_ok, "detail": fota_detail},
        {"title": "FUMO Endpoint", "ok": fumo_ok, "detail": fumo_detail},
        {"title": "DMS Endpoint", "ok": dms_ok, "detail": dms_detail},
    ]
    healthy = sum(1 for item in checks if item["ok"])
    endpoint_health = round((healthy / len(checks)) * 100, 1) if checks else 0.0
    valid_coverage = round((imei_hits / max(valid_imeis, 1)) * 100, 1) if valid_imeis else 0.0
    fumo_share = round((fumo_hits / max(latest_hits, 1)) * 100, 1) if latest_hits else 0.0
    device_validity = round((recent_valid_devices / max(total_device_variants, 1)) * 100, 1) if total_device_variants else 0.0

    return {
        "checks": checks,
        "counts": {
            "hits": latest_hits,
            "fumo_hits": fumo_hits,
            "valid_imeis": valid_imeis,
            "imei_hits": imei_hits,
            "decrypted_rows": int(decryption_count_row["total"] or 0),
        },
        "total_devices": int(model_count_row["total"] or 0),
        "category_count": int(csc_count_row["total"] or 0),
        "complete_devices": recent_valid_devices,
        "fota_ok": fota_ok,
        "fumo_ok": fumo_ok,
        "dms_ok": dms_ok,
        "ip_alive": fota_ok and fumo_ok and dms_ok,
        "progress": [
            {"title": "Endpoint Health", "value": endpoint_health, "subtitle": f"{healthy}/{len(checks)} checks online"},
            {"title": "Catalog Readiness", "value": device_validity, "subtitle": f"{recent_valid_devices}/{total_device_variants} model/CSC pairs valid"},
            {"title": "Valid IMEI Coverage", "value": valid_coverage, "subtitle": f"{valid_imeis:,} IMEIs across model databases"},
            {"title": "FUMO Hit Share", "value": fumo_share, "subtitle": f"{fumo_hits:,} FUMO hits"},
        ],
    }


def refresh_snapshot(log_message: bool) -> None:
    st.session_state.status_snapshot = collect_status_snapshot()
    st.session_state.snapshot_time = datetime.now()
    if log_message:
        push_activity("sync", "Dashboard status checks refreshed.")


def render_layout_bridge() -> None:
    components.html(
        """
        <script>
        const doc = window.parent.document;
        const root = doc.documentElement;

        function updateDockLayout() {
          const sidebar = doc.querySelector('[data-testid="stSidebar"]');
          let sidebarWidth = 0;
          if (sidebar) {
            const rect = sidebar.getBoundingClientRect();
            if (rect.width > 40 && rect.right > 0) {
              sidebarWidth = rect.width;
            }
          }
          const viewportWidth = window.parent.innerWidth || doc.documentElement.clientWidth || 1280;
          const contentMax = 1440;
          const gutter = 72;
          const contentWidth = Math.min(contentMax, Math.max(360, viewportWidth - sidebarWidth - gutter));
          const contentLeft = Math.max(16, sidebarWidth + ((viewportWidth - sidebarWidth - contentWidth) / 2));
          root.style.setProperty('--sidebar-width', `${sidebarWidth}px`);
          root.style.setProperty('--content-width', `${contentWidth}px`);
          root.style.setProperty('--content-left', `${contentLeft}px`);
        }

        updateDockLayout();
        const bodyObserver = new MutationObserver(updateDockLayout);
        bodyObserver.observe(doc.body, { childList: true, subtree: true, attributes: true });
        if (window.parent.ResizeObserver) {
          const resizeObserver = new window.parent.ResizeObserver(updateDockLayout);
          resizeObserver.observe(doc.body);
          const sidebar = doc.querySelector('[data-testid="stSidebar"]');
          if (sidebar) resizeObserver.observe(sidebar);
        }
        window.parent.addEventListener('resize', updateDockLayout);
        </script>
        """,
        height=0,
    )


def render_activity_feed() -> None:
    if is_guest_mode() or not st.session_state.activity_feed_visible:
        return

    items = recent_activity_events(2)
    if not items:
        return
    items_html = "".join(
        f"""
        <div class="activity-item">
            <span class="activity-time">{html.escape(item['time'])}</span>
            <span class="pill {level_badge(item['level'])}">{html.escape(item['level'])}</span>
            <span class="activity-message">{html.escape(item['message'])}</span>
        </div>
        """
        for item in items[:2]
    )
    st.markdown(
        f"""
        <div class="activity-dock expanded">
            <div class="activity-head">
                <div class="activity-title">Activity Feed</div>
                <div class="header-subtitle">Bottom dock stays visible while you work</div>
            </div>
            <div class="activity-list">{items_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def poll_running_task(task: dict[str, Any] | None, interval_seconds: float = 0.8) -> None:
    if task and task.get("status") == "running":
        time.sleep(interval_seconds)
        st.rerun()


def main() -> None:
    init_state()
    sync_activity_feed()
    ensure_workspace_databases()
    sync_legacy_sources()
    ensure_patrol_workers()
    inject_styles()

    if not st.session_state.get("is_authenticated"):
        render_login_page()
        if st.session_state.get("logout_refresh_pending"):
            st.session_state.logout_refresh_pending = False
            components.html(
                """
                <script>
                const target = window.parent || window;
                target.location.reload();
                </script>
                """,
                height=0,
            )
        return

    surface_new_notifications()

    if st.session_state.snapshot_time is None:
        refresh_snapshot(log_message=False)

    catalog = load_device_catalog()
    guest_mode = is_guest_mode()
    if guest_mode:
        tab_descriptions = {
            "Dashboard": "Overview of the whole system with select details redacted and limited to cached intelligence.",
            "FOTA Scanner": "Firmware fetching console with manual or database-backed model and CSC selection, with IMEI hidden in guest mode.",
            "Decryption": "Firmware decryptor that stores its own library of discovered firmwares by model number and CSC.",
            "Library": "Firmware discovery library grouped by model number with copy-ready curl links.",
            "Device Vault": "Model-grouped device vault built from decrypted firmware records and firmware history.",
        }
    else:
        tab_descriptions = {
            "Dashboard": "Overview of connectivity, cached firmware intelligence, device readiness, and quick database health.",
            "FOTA Scanner": "Live Samsung FOTA lookup console with manual entry, database-backed selection, decryptor-assisted bases, and optional IMEI scanning.",
            "Decryption": "Firmware decryption workspace backed by decrypted_firmware.db for enumerating builds per model and CSC.",
            "IMEI Scanner": "Sequential live IMEI probing console using manual or database-backed device selection and decryptor-assisted firmware bases.",
            "IMEI Database": "Per-model IMEI databases sourced from scan hits, with FUMO history import and one-tap IMEI replacement.",
            "Library": "Firmware discovery library grouped by model number with copy-ready curl links.",
            "Device Vault": "Model-grouped device vault sourced from decrypted firmware records and firmware history.",
            "Night Patrol": "Persistent background decryption cycles for up to three selected devices with shared update notifications.",
            "Pathfinder": "Delta scan workspace that probes the latest decrypted firmwares to map their next OTA targets and links.",
            "Terminal": "Settings placeholder for future deployment integrations and GitHub-connected feedback tools.",
        }
    tab_names = list(tab_descriptions.keys())

    active_tab = st.session_state.get("active_tab", "Dashboard")
    if active_tab not in tab_names:
        active_tab = "Dashboard"
        st.session_state.active_tab = active_tab
    snapshot_text = st.session_state.snapshot_time.strftime("%Y-%m-%d %H:%M:%S") if st.session_state.snapshot_time else "Pending"
    mode_label = "Admin Mode" if not guest_mode else "Guest Mode"
    st.markdown(
        f"""
        <div class="oneui-header">
            <div>
                <div class="header-title">Project Killshot Dashboard</div>
                <div class="header-subtitle">{html.escape(tab_descriptions[active_tab])}</div>
            </div>
            <div class="header-chip">User Mode: {mode_label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div class='section-spacer'></div>", unsafe_allow_html=True)

    with st.sidebar:
        st.markdown(
            """
            <div class="left-pane-shell">
                <div class="left-pane-title">Workspace</div>
                <div class="left-pane-copy">Navigate through the tools here.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        active_tab = st.radio(
            "Workspace",
            tab_names,
            horizontal=False,
            label_visibility="collapsed",
            key="active_tab",
        )
        st.markdown("<div class='left-pane-footer'></div>", unsafe_allow_html=True)
        if not guest_mode:
            activity_label = "Hide Activity Feed" if st.session_state.activity_feed_visible else "Show Activity Feed"
            if st.button(activity_label, key="left_pane_activity_toggle", use_container_width=True):
                st.session_state.activity_feed_visible = not st.session_state.activity_feed_visible
                st.rerun()
        if st.button("Logout", key="left_pane_logout", use_container_width=True):
            logout_to_login()

    if active_tab == "Dashboard":
        if guest_mode:
            render_guest_dashboard(st.session_state.status_snapshot, catalog)
        else:
            top_action_cols = st.columns([4.5, 1.2], gap="medium")
            with top_action_cols[1]:
                if st.button("↻ Refresh Dashboard", key="header_refresh_dashboard", use_container_width=True):
                    refresh_snapshot(log_message=True)
                    st.rerun()
            # Admin mode keeps the full dashboard card set and detailed tables.
            render_dashboard_cards(st.session_state.status_snapshot)
            st.markdown("<div class='section-spacer'></div>", unsafe_allow_html=True)
            lower_left, lower_right = st.columns(2, gap="large")
            with lower_left:
                render_html_table(
                    "Category Overview",
                    ["Category", "Devices", "Ready"],
                    [list(row) for row in category_rows(catalog)],
                    "No device categories found.",
                )
            with lower_right:
                render_html_table(
                    "Latest Cached Discoveries",
                    ["Model", "CSC", "IMEI", "PDA", "Time"],
                    [
                        [
                            row["device_model"],
                            row["csc"],
                            mask_imei(row["imei"], hidden_digits=4),
                            short_version(row["found_pda"]),
                            row["timestamp"],
                        ]
                        for row in recent_hits()
                    ],
                    "No firmware history is available yet.",
                )
    elif active_tab == "FOTA Scanner":
        render_fota_tab(catalog, guest_mode=guest_mode)
    elif active_tab == "Decryption":
        render_decryption_tab(catalog)
    elif active_tab == "IMEI Scanner" and not guest_mode:
        render_imei_scanner_tab(catalog)
    elif active_tab == "IMEI Database" and not guest_mode:
        render_imei_database_tab(catalog)
    elif active_tab == "Library":
        render_database_history_tab()
    elif active_tab == "Device Vault":
        render_guest_device_vault_tab(catalog) if guest_mode else render_device_vault_tab(catalog)
    elif active_tab == "Night Patrol" and not guest_mode:
        render_night_patrol_tab()
    elif active_tab == "Pathfinder" and not guest_mode:
        render_pathfinder_tab()
    elif active_tab == "Terminal" and not guest_mode:
        render_terminal_tab(snapshot_text)

    if st.session_state.dialog_payload:
        show_download_dialog(st.session_state.dialog_payload)

    render_layout_bridge()
    render_activity_feed()


if __name__ == "__main__":
    main()
