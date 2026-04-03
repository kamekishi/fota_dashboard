from __future__ import annotations

import html
import json
import socket
import sqlite3
import threading
import textwrap
from datetime import datetime
from math import ceil
from pathlib import Path
from typing import Any
from uuid import uuid4
import xml.etree.ElementTree as ET

import streamlit as st

import ota


BASE_DIR = Path(__file__).resolve().parent
DEVICES_PATH = BASE_DIR / "devices.json"
DB_PATH = BASE_DIR / "fumo_history.db"
USER_AGENT = "SyncML DM Client"
MAX_ACTIVITY = 10
TASK_LOCK = threading.Lock()
TASKS: dict[str, dict[str, Any]] = {}
ACTIVITY_QUEUE: list[dict[str, str]] = []


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
        "_selected_device_key": None,
        "_scan_selected_device_key": None,
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)


def push_activity(level: str, message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    item = {"time": timestamp, "level": level.upper(), "message": message}
    st.session_state.activity_feed.insert(0, item)
    del st.session_state.activity_feed[MAX_ACTIVITY:]


def queue_activity(level: str, message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    with TASK_LOCK:
        ACTIVITY_QUEUE.append({"time": timestamp, "level": level.upper(), "message": message})


def sync_activity_feed() -> None:
    with TASK_LOCK:
        items = list(ACTIVITY_QUEUE)
        ACTIVITY_QUEUE.clear()
    for item in items:
        st.session_state.activity_feed.insert(0, item)
    del st.session_state.activity_feed[MAX_ACTIVITY:]


def load_device_catalog() -> dict[str, list[dict[str, Any]]]:
    with DEVICES_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_device_catalog(catalog: dict[str, list[dict[str, Any]]]) -> None:
    with DEVICES_PATH.open("w", encoding="utf-8") as handle:
        json.dump(catalog, handle, indent=4)


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


def render_fota_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    categories = list(catalog.keys())
    category = st.selectbox("Category", categories, key="fota_category")
    category_devices = catalog.get(category, [])
    labels = [
        f"{item.get('name', item.get('model', 'Unknown'))} • {item.get('model', 'Unknown')} • {item.get('csc', 'UNK')}"
        for item in category_devices
    ]
    selected_label = st.selectbox("Preset Device", labels, key="fota_device")
    selected_index = labels.index(selected_label)
    selected_device = category_devices[selected_index]

    selected_key = "|".join(
        [
            str(selected_device.get("model", "")),
            str(selected_device.get("csc", "")),
            str(selected_device.get("imei", "")),
            str(selected_device.get("base", "")),
        ]
    )
    if st.session_state._selected_device_key != selected_key:
        st.session_state._selected_device_key = selected_key
        st.session_state.model_input = selected_device.get("model", "")
        st.session_state.csc_input = selected_device.get("csc", "")
        st.session_state.imei_input = selected_device.get("imei", "")
        st.session_state.base_input = selected_device.get("base", "")

    st.caption("Preset values can be edited before fetching. The live request uses the values currently shown in the form.")

    with st.form("lookup_form"):
        form_cols = st.columns(2, gap="medium")
        with form_cols[0]:
            st.text_input("Device Model", key="model_input")
            st.text_input("IMEI", key="imei_input")
        with form_cols[1]:
            st.text_input("CSC", key="csc_input")
            st.text_input("Base Firmware", key="base_input")
        submitted = st.form_submit_button(
            "Fetch Download Link",
            use_container_width=True,
        )

    if submitted:
        model = st.session_state.model_input.strip().upper()
        csc = st.session_state.csc_input.strip().upper()
        imei = st.session_state.imei_input.strip()
        base = st.session_state.base_input.strip()

        if not model or not csc or not imei:
            st.error("Model, CSC, and IMEI are required.")
            push_activity("error", "Lookup was blocked because one or more required fields were empty.")
        elif not imei.isdigit() or len(imei) != 15:
            st.error("IMEI must be exactly 15 digits.")
            push_activity("error", f"Rejected invalid IMEI input for {model}.")
        else:
            st.session_state.fota_live_request = {
                "model": model,
                "csc": csc,
                "imei": imei,
                "base": base,
            }
            st.session_state.fota_live_result = None
            st.session_state.fota_live_error = None
            st.rerun()

    render_result_panel(st.session_state.last_result)
    if st.session_state.get("fota_live_request") is not None:
        show_fota_fetch_dialog()


def render_imei_scanner_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    categories = list(catalog.keys())
    category = st.selectbox("Scan Category", categories, key="scan_category")
    devices = catalog.get(category, [])
    labels = [
        f"{item.get('name', item.get('model', 'Unknown'))} • {item.get('model', 'Unknown')} • {item.get('csc', 'UNK')}"
        for item in devices
    ]
    selected = st.selectbox("Seed Device", labels, key="scan_device")
    device = devices[labels.index(selected)]
    scan_selected_key = "|".join(
        [
            str(device.get("model", "")),
            str(device.get("csc", "")),
            str(device.get("imei", "")),
            str(device.get("base", "")),
        ]
    )
    if st.session_state._scan_selected_device_key != scan_selected_key:
        st.session_state._scan_selected_device_key = scan_selected_key
        st.session_state.scan_model = device.get("model", "")
        st.session_state.scan_imei = device.get("imei", "")
        st.session_state.scan_csc = device.get("csc", "")
        st.session_state.scan_base = device.get("base", "")

    st.caption("Selecting a device autofills the scanner. You can still edit the values before running the live scan.")

    with st.form("imei_scan_form"):
        cols = st.columns(3, gap="medium")
        with cols[0]:
            model = st.text_input("Device Model", key="scan_model")
            start_imei = st.text_input("Start IMEI", key="scan_imei")
        with cols[1]:
            csc = st.text_input("CSC", key="scan_csc")
            base = st.text_input("Base Firmware", key="scan_base")
        with cols[2]:
            attempts = st.number_input("Attempts", min_value=1, max_value=50, value=5, step=1, key="scan_attempts")
            step = st.number_input("IMEI Step", min_value=1, max_value=999, value=1, step=1, key="scan_step")
        submitted = st.form_submit_button(
            "Start IMEI Scan",
            use_container_width=True,
        )

    if submitted:
        model = model.strip().upper()
        csc = csc.strip().upper()
        start_imei = start_imei.strip()
        base = base.strip()

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

    if st.session_state.imei_last_hit:
        render_result_panel(st.session_state.imei_last_hit)

    render_html_table(
        "IMEI Scan Results",
        ["Attempt", "IMEI", "Status", "Source", "Firmware"],
        [
            [
                row["attempt"],
                row["imei"],
                row["status"],
                row["source"],
                short_version(row["firmware"]) if row["firmware"] else "-",
            ]
            for row in st.session_state.imei_scan_results
        ],
        "Run a scan to see IMEI results here.",
    )

    if st.session_state.get("imei_live_request") is not None:
        show_imei_scan_dialog()


def render_device_vault_tab(catalog: dict[str, list[dict[str, Any]]]) -> None:
    exact_map, fallback_map = latest_firmware_lookup()
    top_cols = st.columns([3, 1], gap="large")
    with top_cols[1]:
        if st.button("Add Device", key="open_add_device_dialog", use_container_width=True):
            show_add_device_dialog()

    st.markdown("<div class='section-spacer'></div>", unsafe_allow_html=True)

    for category, entries in catalog.items():
        with st.expander(f"{category} ({len(entries)})", expanded=False):
            cols = st.columns(2, gap="medium")
            for idx, item in enumerate(entries):
                latest = exact_map.get((item.get("model"), item.get("csc"), item.get("imei"))) or fallback_map.get(
                    (item.get("model"), item.get("csc")),
                    "No record",
                )
                with cols[idx % 2]:
                    st.markdown(
                        f"""
                        <section class="glass-card vault-device-card">
                            <div class="vault-device-name">{html.escape(item.get('name', item.get('model', 'Unknown')))}</div>
                            <div class="vault-device-line"><strong>Model</strong> {html.escape(item.get('model', 'Unknown'))}</div>
                            <div class="vault-device-line"><strong>CSC</strong> {html.escape(item.get('csc', 'UNK'))}</div>
                            <div class="vault-device-line"><strong>IMEI</strong> {html.escape(mask_imei(item.get('imei', '')))}</div>
                            <div class="vault-device-line"><strong>Firmware Base</strong> {html.escape(item.get('base', 'Unknown') or 'Unknown')}</div>
                            <div class="vault-device-line"><strong>Latest</strong> {html.escape(latest)}</div>
                        </section>
                        """,
                        unsafe_allow_html=True,
                    )
                    action_cols = st.columns(2, gap="small")
                    with action_cols[0]:
                        if st.button("Edit", key=f"vault_edit_{category}_{idx}", use_container_width=True):
                            show_edit_device_dialog(category, idx)
                    with action_cols[1]:
                        if st.button("Remove", key=f"vault_remove_{category}_{idx}", use_container_width=True):
                            show_remove_device_dialog(category, idx)


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
    categories = list(catalog.keys())
    category = st.selectbox("Device Category", categories, key="imei_db_category")
    devices = catalog.get(category, [])
    labels = [
        f"{item.get('name', item.get('model', 'Unknown'))} • {item.get('model', 'Unknown')} • {item.get('csc', 'UNK')}"
        for item in devices
    ]
    selected_label = st.selectbox("Selected Device", labels, key="imei_db_device")
    device_index = labels.index(selected_label)
    device = devices[device_index]
    model = str(device.get("model", "")).upper()

    csc_rows = with_db(
        "SELECT DISTINCT csc FROM firmware_hits WHERE device_model = ? ORDER BY csc",
        (model,),
    )
    csc_options = ["All CSCs"] + sorted(
        {
            str(row["csc"]).upper()
            for row in csc_rows
            if row["csc"]
        }.union(
            {
                str(entry.get("csc", "")).upper()
                for entry in flatten_devices(catalog)
                if str(entry.get("model", "")).upper() == model and entry.get("csc")
            }
        )
    )
    if st.session_state.get("imei_db_csc") not in csc_options:
        st.session_state.imei_db_csc = "All CSCs"
    selected_csc = st.selectbox("CSC", csc_options, key="imei_db_csc")

    st.caption(
        f"Selected device: {device.get('name', device.get('model', 'Unknown'))} • "
        f"{device.get('model', 'Unknown')} • Current IMEI {device.get('imei', 'Unknown')}"
    )

    rows = imei_database_rows(catalog, model, selected_csc, category, device)
    st.markdown(
        """
        <section class="glass-card table-card">
            <div class="section-kicker">IMEI Database</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    if not rows:
        st.info("No IMEIs were found for this device and CSC selection.")
        return

    header_cols = st.columns([1.35, 0.8, 0.9, 1.15, 1.35, 1.1, 0.9], gap="small")
    headers = ["IMEI", "CSC", "Status", "Source", "Latest Firmware", "Timestamp", "Action"]
    for col, label in zip(header_cols, headers):
        col.markdown(f"<div class='imei-db-header'>{html.escape(label)}</div>", unsafe_allow_html=True)

    for idx, row in enumerate(rows):
        row_cols = st.columns([1.35, 0.8, 0.9, 1.15, 1.35, 1.1, 0.9], gap="small")
        values = [
            row["imei"],
            row["csc"],
            row["status"],
            row["source"],
            short_version(row["found_pda"]) if row["found_pda"] and row["found_pda"] != "-" else "-",
            row["timestamp"],
        ]
        for col, value in zip(row_cols[:-1], values):
            col.markdown(f"<div class='imei-db-cell'>{html.escape(str(value))}</div>", unsafe_allow_html=True)
        with row_cols[-1]:
            if st.button("USE IMEI", key=f"imei_db_use_{idx}", use_container_width=True):
                if update_device_imei(category, device_index, row["imei"], "IMEI Database"):
                    st.rerun()


def render_database_history_tab() -> None:
    rows = history_rows()
    st.markdown(
        """
        <section class="glass-card table-card">
            <div class="section-kicker">Latest Firmware Discoveries</div>
        </section>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div class='section-spacer'></div>", unsafe_allow_html=True)
    total_pages = max(1, ceil(len(rows) / 10))
    page_cols = st.columns([1, 4], gap="medium")
    with page_cols[0]:
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1, key="history_page")
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
            <span>IMEI</span>
            <span>Requested Base</span>
            <span>Found PDA</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if not page_rows:
        st.info("No firmware history is available yet.")
        return

    for row in page_rows:
        row_html = textwrap.dedent(
            f"""
            <div class="history-list-row">
                <span>{html.escape(str(row["timestamp"]))}</span>
                <span>{html.escape(str(row["device_model"]))}</span>
                <span>{html.escape(str(row["csc"]))}</span>
                <span>{html.escape(mask_imei(row["imei"], hidden_digits=4))}</span>
                <span>{html.escape(short_version(row["request_base_version"]))}</span>
                <span>{html.escape(str(row["found_pda"] or "-"))}</span>
            </div>
            """
        )
        row_cols = st.columns([1, 0.085], gap="small")
        with row_cols[0]:
            st.markdown(row_html, unsafe_allow_html=True)
        with row_cols[1]:
            if st.button("...", key=f"history_row_{row['id']}", use_container_width=True):
                show_history_detail_dialog(dict(row))


def render_terminal_tab() -> None:
    st.toggle("Enable experimental feedback routing", value=False, disabled=True)
    st.text_area(
        "Feedback draft",
        value="GitHub feedback integration will be connected here later.",
        height=140,
        disabled=True,
    )
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
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            text-align: center;
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

        div[role="radiogroup"] {
            gap: 10px;
            padding: 0;
            background: transparent;
            border: 0;
            margin-bottom: 0;
            display: grid;
        }

        div[role="radiogroup"] label {
            border-radius: 22px;
            padding: 12px 14px;
            font-weight: 700;
            color: #465b7f;
            background: rgba(255, 255, 255, 0.58);
            border: 1px solid rgba(255, 255, 255, 0.86);
            margin: 0;
        }

        div[role="radiogroup"] label:has(input:checked) {
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
            f"<div style='text-align:right;padding-top:0.35rem;font-size:0.92rem;color:#233754;font-weight:700;'>{percent_value:.1f}%</div>",
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

            if is_auth_failed_result(response):
                state["paused_for_auth"] = True
                state["auth_error_label"] = format_imei_status_text(response.get("status", "auth_failed"))
                state["auth_error_raw"] = str(response.get("status", "auth_failed"))
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
        action_cols = st.columns(2, gap="medium")
        with action_cols[0]:
            if st.button("Continue 3 More IMEIs", key="imei_auth_continue", use_container_width=True):
                state["paused_for_auth"] = False
                state["stop_after_index"] = min(state["index"] + 3, state["attempts"])
                st.session_state.imei_live_state = state
                st.rerun()
        with action_cols[1]:
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


def render_activity_feed() -> None:
    if not st.session_state.activity_feed_visible:
        return

    items = st.session_state.activity_feed or [{"time": "--:--:--", "level": "INFO", "message": "Dashboard is ready."}]
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
    inject_styles()

    if st.session_state.snapshot_time is None:
        refresh_snapshot(log_message=False)
        push_activity("info", "Dashboard initialized with cache-first lookup mode.")

    catalog = load_device_catalog()
    tab_descriptions = {
        "Dashboard": "Overview of connectivity, cached firmware intelligence, device readiness, and quick database health.",
        "FOTA Scanner": "Cache-first firmware lookup console for fetching Samsung FOTA download links and copy-ready curl commands.",
        "IMEI Scanner": "Sequential IMEI probing console for testing nearby identifiers against the selected firmware base.",
        "IMEI Database": "Device-focused IMEI library combining saved presets and firmware history with CSC filters and one-tap IMEI replacement.",
        "Device Vault": "Grouped preset library with masked IMEIs and modal tools to add, edit, or remove device entries.",
        "Database History": "Latest firmware discoveries from the history database, paginated with the newest records first.",
        "Terminal": "Settings placeholder for future deployment integrations and GitHub-connected feedback tools.",
    }
    tab_names = list(tab_descriptions.keys())

    active_tab = st.session_state.get("active_tab", "Dashboard")
    snapshot_text = st.session_state.snapshot_time.strftime("%Y-%m-%d %H:%M:%S") if st.session_state.snapshot_time else "Pending"
    st.markdown(
        f"""
        <div class="oneui-header">
            <div>
                <div class="header-title">Project Killshot Dashboard</div>
                <div class="header-subtitle">{html.escape(tab_descriptions[active_tab])}</div>
            </div>
            <div class="header-chip">Snapshot: {snapshot_text}</div>
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
        activity_label = "Hide Activity Feed" if st.session_state.activity_feed_visible else "Show Activity Feed"
        footer_cols = st.columns([1.8, 1], gap="small")
        with footer_cols[0]:
            if st.button(activity_label, key="left_pane_activity_toggle", use_container_width=True):
                st.session_state.activity_feed_visible = not st.session_state.activity_feed_visible
                st.rerun()

    if active_tab == "Dashboard":
        top_action_cols = st.columns([4.5, 1.2], gap="medium")
        with top_action_cols[1]:
            if st.button("↻ Refresh Dashboard", key="header_refresh_dashboard", use_container_width=True):
                refresh_snapshot(log_message=True)
                st.rerun()

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
        render_fota_tab(catalog)
    elif active_tab == "IMEI Scanner":
        render_imei_scanner_tab(catalog)
    elif active_tab == "IMEI Database":
        render_imei_database_tab(catalog)
    elif active_tab == "Device Vault":
        render_device_vault_tab(catalog)
    elif active_tab == "Database History":
        render_database_history_tab()
    elif active_tab == "Terminal":
        render_terminal_tab()

    if st.session_state.dialog_payload:
        show_download_dialog(st.session_state.dialog_payload)

    render_activity_feed()


if __name__ == "__main__":
    main()
