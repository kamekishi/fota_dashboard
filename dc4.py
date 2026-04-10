#!/usr/bin/env python3
"""
Samsung Firmware Decryptor - Manual Mode

Brute-forces Samsung version strings against mixed hashes from version.test.xml.
Supports both MD5 (32 hex chars) and HMAC-SHA256 (64 hex chars) from Samsung version.test.xml.
Outputs a single JSON file: Decrypted/<model>_<csc>_decrypted.json
"""

import hashlib
import hmac
import re
import time
import requests
from lxml import etree
import random
import string
from datetime import datetime
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TaskProgressColumn
from rich.panel import Panel
from rich.table import Table
from rich.rule import Rule
from rich import box
import os
import json
import signal

STOP_REQUESTED = False


def _handle_stop_signal(signum, frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True


console = Console()

def print_msg(msg):
    """Print formatted message"""
    console.log(msg)

def get_country_name(cc):
    """Get region name from country code"""
    cc_to_country = {
        "CHC": "China",
        "CHN": "China",
        "TGY": "Hong Kong",
        "KOO": "Korea",
        "EUX": "Europe",
        "INS": "India",
        "XXV": "Vietnam",
        "XAA": "USA (Unlocked)",
        "ATT": "USA (AT&T)",
        "TMB": "USA (T-Mobile)",
        "DSA": "USA (Dish)",
        "USC": "USA (US Cellular)",
        "VZW": "USA (Verizon)",

        # Canada W
        "XAC": "Canada (Unlocked)",
        "BMC": "Canada (Bell)",
        "RWC": "Canada (Rogers)",
        "TLS": "Canada (Telus)",
        "KDO": "Canada (Koodo)",
        "VTR": "Canada (Virgin)",
        "BWA": "Canada (SaskTel)",
        "PCM": "Canada (Videotron)",
    }
    return cc_to_country.get(cc, f"Region: {cc}")

# ---------- CSC blocks & special subset ----------

# User OXM subcategory: your special regions, still part of OXM
OXM_USER_SUBSET = [
    "XFE",  # South Africa
    "WWA",  # OXM multi-CSC
    "WWD",  # OXM multi-CSC
    "WWC",  # OXM multi-CSC
    "O2C",  # O2 Czech
    "CHX",  # Switzerland (OWO)
    "ZTR",  # OWO carrier / test
    "BKD",  # Bangladesh (also in OXM/ODM)
    "ANC",  # Africa / OXM
    "CPW",  # UK CPW
]

OXM_CSCS = [
    "ACR","AFG","AFR","BKD","BNG","BOG","CAC","CAU","CPW","DKR","ECT","EGY","EUX","EUY","FWD",
    "GLB","ILO","ILP","INS","KSA","LYS","MEO","MET","MID","MM1","MOB","MOT","MSR","MWD","NPB",
    "OPS","ANC","PAK","PKD","PLS","PRT","SER","SFR","SIM","SIN","SKZ","SLK","SOZ","STH","SWC","TEL",
    "THL","TOP","TUN","TUR","VAU","VIP","WWA","WWC","WWD","XFA","XFV","XME","XNZ","XSA","XSG",
    "XSP","XTC","XXV","XID",
    # ensure user-subset-only regions are also in OXM
    "XFE","O2C","CHX","ZTR",
]

ODM_CSCS = ["BKD","INS","NPL","SLK"]
OJM_CSCS = [
    "ACR","AFG","AFR","DKR","ECT","EGY","FWD","ILO","ILP","KSA","LYS",
    "MID","MWD","PAK","PKD","TUN","TUR","XFA","XFE","XFV","XSG",
]
OLE_CSCS = ["XID"]
OWO_CSCS = [
    "BVO","BVT","CHE","CHL","CHO","CHT","CHX","GTO","NBS",
    "ZTA","ZTM","ZTO","ZTR","ZVV",
]
OXE_CSCS = ["CAU","SEK","SKZ","SER"]

# Canada W block – all Canadian CSCs grouped for fallback
CANADA_W_CSCS = [
    "XAC",  # Unlocked Canada
    "BMC",  # Bell
    "RWC",  # Rogers
    "TLS",  # Telus
    "KDO",  # Koodo
    "VTR",  # Virgin
    "BWA",  # SaskTel
    "PCM",  # Videotron
]

CSC_MAP = {
    "ODM": ODM_CSCS,
    "OJM": OJM_CSCS,
    "OLE": OLE_CSCS,
    "OWO": OWO_CSCS,
    "OXE": OXE_CSCS,
    "OXM": OXM_CSCS,
    "CAN_W": CANADA_W_CSCS,
}

A73_CSCS = ODM_CSCS[:] + OJM_CSCS[:] + OLE_CSCS[:] + OWO_CSCS[:] + OXE_CSCS[:]
USA_U_CSCS  = ["ATT","VZW","TMB","CHA","CCT","DSA","DSG","GCF","XAA","USC"]
USA_U1_CSCS = ["ATT","VZW","TMB","CHA","CCT","DSA","DSG","XAA","USC","XPO","FKR","XAG","XAR","WWD","TMK","AIO","LRA"]

def get_csc_fallback_list(primary_cc: str):
    """
    Build an ordered CSC fallback list based on membership in blocks:
    - OXM / ODM / OJM / OLE / OWO / OXE / A73 / CAN_W
    - USA_U / USA_U1
    Start with the primary CSC, then all others from the same block(s).
    """
    primary_cc = primary_cc.upper()

    blocks = [
        ("OXM", OXM_CSCS),
        ("ODM", ODM_CSCS),
        ("OJM", OJM_CSCS),
        ("OLE", OLE_CSCS),
        ("OWO", OWO_CSCS),
        ("OXE", OXE_CSCS),
        ("A73", A73_CSCS),
        ("USA_U", USA_U_CSCS),
        ("USA_U1", USA_U1_CSCS),
        ("CAN_W", CANADA_W_CSCS),
    ]

    fallback = []

    # Collect all CSCs from any block that contains the primary CSC
    for name, lst in blocks:
        if primary_cc in lst:
            for c in lst:
                if c not in fallback:
                    fallback.append(c)

    # If primary doesn't belong to any known block, just return itself
    if not fallback:
        return [primary_cc]

    # Ensure primary_cc is first in the list
    if primary_cc in fallback:
        fallback.remove(primary_cc)

    return [primary_cc] + fallback

def request_xml(url, max_retries=3):
    """Request XML content from URL.
    403/404 = permanent rejection, bail immediately without retry.
    Other errors get up to max_retries attempts.
    """
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Linux; Android 9; SAMSUNG SM-T825Y) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/15.0 Chrome/90.0.4430.210 Safari/537.36",
    ]

    headers = {"User-Agent": random.choice(user_agents), "Connection": "close"}

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code in (403, 404):
                return None  # Permanent rejection — no point retrying
            response.raise_for_status()
            return response.content
        except requests.exceptions.HTTPError:
            return None
        except Exception as e:
            if attempt == max_retries:
                print_msg(f"[yellow]⚠️  Network error for {url.split('/')[-2]}: {e}[/yellow]")
            elif attempt < max_retries:
                time.sleep(0.5)

    return None


VERSION_TEST_HMAC_SHA256_KEY = b"fcjimts25@%"


def _is_hex_string(value):
    value = (value or "").strip()
    if not value:
        return False
    try:
        int(value, 16)
        return True
    except ValueError:
        return False


def is_version_test_md5_hash(value):
    value = (value or "").strip().lower()
    return len(value) == 32 and _is_hex_string(value)


def is_version_test_hmac_sha256_hash(value):
    value = (value or "").strip().lower()
    return len(value) == 64 and _is_hex_string(value)


def is_version_test_hash(value):
    return is_version_test_md5_hash(value) or is_version_test_hmac_sha256_hash(value)


def get_md5_list(model, cc):
    """Get version.test.xml hash list, including newer layouts where <latest> may itself hold a hash."""
    url = f"https://fota-cloud-dn.ospserver.net/firmware/{cc}/{model}/version.test.xml"
    content = request_xml(url)
    if content is None:
        return []

    try:
        xml_text = content.decode("utf-8", errors="ignore")
    except Exception:
        xml_text = ""

    try:
        xml = etree.fromstring(content)
    except Exception:
        xml = None

    hashes = []
    seen = set()

    def _add(value):
        hv = (value or "").strip().lower()
        if is_version_test_hash(hv) and hv not in seen:
            seen.add(hv)
            hashes.append(hv)

    if xml is not None:
        latest_nodes = xml.xpath("//latest//text()")
        if latest_nodes:
            _add(latest_nodes[0])

        for node in xml.xpath("//value//text()"):
            _add(node)

    # Regex fallback for the newer version.test.xml layout
    if xml_text:
        latest_match = re.search(
            r'<latest(?:\s+o="([^"]*)")?\s*>(.*?)</latest>|<latest(?:\s+o="([^"]*)")?\s*/>',
            xml_text,
            re.S | re.I,
        )
        if latest_match:
            latest_value = (latest_match.group(2) or "").strip()
            _add(latest_value)

        for match in re.finditer(r'<value(?:\s+[^>]*)?>(.*?)</value>', xml_text, re.S | re.I):
            _add(match.group(1))

    return hashes


def split_hashes(hash_list):
    """
    Split version.test.xml values into algorithm buckets by digest length.
    Returns: (md5_set, hmac_sha256_set, unknown_set)
    """
    md5_set = set()
    hmac_sha256_set = set()
    unknown_set = set()

    for h in hash_list:
        if not h:
            continue
        hv = h.strip().lower()
        if is_version_test_md5_hash(hv):
            md5_set.add(hv)
        elif is_version_test_hmac_sha256_hash(hv):
            hmac_sha256_set.add(hv)
        else:
            unknown_set.add(hv)

    return md5_set, hmac_sha256_set, unknown_set


def get_unresolved_hashes(hash_list, decrypted):
    """Return unresolved version.test hashes after subtracting already decrypted hits."""
    md5_set, hmac_sha256_set, _ = split_hashes(hash_list)
    already_found = {str(k).strip().lower() for k in (decrypted or {}).keys()}
    md5_set -= already_found
    hmac_sha256_set -= already_found
    return md5_set, hmac_sha256_set


def get_latest_version(model, cc):
    """Get the latest official firmware version for a specific CSC — silent, no logging."""
    url = f"https://fota-cloud-dn.ospserver.net/firmware/{cc}/{model}/version.xml"
    content = request_xml(url)
    if content is None:
        return None, None
    try:
        xml = etree.fromstring(content)
        latest_nodes = xml.xpath("//latest//text()")
        if not latest_nodes:
            return None, None
        latest_version = latest_nodes[0]
        os_version = xml.xpath("//latest//@o")[0] if xml.xpath("//latest//@o") else "Unknown"
        return latest_version, os_version
    except Exception:
        return None, None

def get_latest_version_with_fallback(model, primary_cc, max_seconds=7):
    """
    Walk through fallback CSCs briefly, then continue immediately to brute force.
    Returns: (latest_version, os_version, base_cc_used)
    """
    fallbacks = get_csc_fallback_list(primary_cc)
    start_ts = time.time()

    with console.status(
        f"[cyan]Scanning briefly for base firmware...[/cyan]",
        spinner="dots"
    ):
        for candidate_cc in fallbacks:
            if STOP_REQUESTED:
                break
            if time.time() - start_ts >= max_seconds:
                break
            latest_version, os_version = get_latest_version(model, candidate_cc)
            if latest_version:
                return latest_version, os_version, candidate_cc

    return None, None, None

def get_next_char(char, alphabet="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    """Get next character in sequence"""
    if char in alphabet:
        index = alphabet.index(char)
        return alphabet[(index + 1) % len(alphabet)]
    else:
        raise ValueError(f"Character '{char}' not in alphabet")

def get_pre_char(char, alphabet="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    """Get previous character in sequence"""
    if char in alphabet:
        index = alphabet.index(char)
        return alphabet[(index - 1) % len(alphabet)]
    else:
        raise ValueError(f"Character '{char}' not in alphabet")

def get_letters_range(start, end):
    """Return string of given range (including end character).
    If start > end (inverted range), returns from start to end of alphabet
    so the brute-force loop still runs rather than crashing."""
    letters = "0123456789" + string.ascii_uppercase
    si = letters.index(start.upper()) if start.upper() in letters else 0
    ei = letters.index(end.upper()) + 1 if end.upper() in letters else len(letters)
    if si >= ei:
        # Inverted or equal — return full tail from start so we don't skip everything
        return letters[si:]
    return letters[si:ei]

def load_checkpoint(model, cc):
    """Load checkpoint file if present."""
    output_dir = ensure_output_dir()
    path = os.path.join(output_dir, f"{model}_{cc}_checkpoint.json")
    if not os.path.exists(path):
        return path, {}, {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        entries = data.get("found_hashes", {})
        meta = data.get("meta", {})
        if isinstance(entries, dict):
            return path, entries, meta if isinstance(meta, dict) else {}
    except Exception:
        pass
    return path, {}, {}


def save_checkpoint(path, model, cc, decrypted, meta=None):
    """Save partial brute-force progress."""
    payload = {
        "model": model,
        "region": cc,
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(decrypted),
        "found_hashes": decrypted,
    }
    if meta:
        payload["meta"] = meta
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def remove_checkpoint(path):
    """Delete checkpoint file if it exists."""
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def ensure_output_dir():
    """Ensure Decrypted/ directory exists, return its path"""
    output_dir = os.path.join(os.getcwd(), "Decrypted")
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        print_msg(f"[yellow]Warning: could not create Decrypted/ directory ({e}), using current directory.[/yellow]")
        return os.getcwd()
    return output_dir

def decrypt_firmware(model, cc, hash_list, latest_version=None, checkpoint_path=None, resume_data=None, resume_meta=None):
    """Decrypt firmware versions by brute force, supporting MD5 and HMAC-SHA256 version.test hashes."""
    cc = cc.upper()

    # OWO-based CSCs use ...BOWO... codes — ignore any fallback base
    if cc in OWO_CSCS:
        latest_version = None

    # Initialize version components based on latest version or defaults
    if latest_version:
        version_parts = latest_version.split("/")
        first_code = version_parts[0][:-6]   # e.g., S938BXXS
        second_code = version_parts[1][:-5]  # e.g., S938BOXM
        third_code = version_parts[2][:-6] if len(version_parts) > 2 and version_parts[2] else ""

        # Go back 3 years from the latest version for thorough search
        latest_year_char = version_parts[0][-3]
        start_year_offset = max(0, ord(latest_year_char) - ord("A") - 3)
        start_year = chr(ord("A") + start_year_offset)

        start_bl = "0"
        start_update = "A"
        end_update = get_next_char(version_parts[0][-4])
        end_bl = get_next_char(version_parts[0][-5], "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")

        # If month is Oct/Nov/Dec, add a year
        if version_parts[0][-2] in "JKL":
            end_year = get_next_char(version_parts[0][-3], "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        else:
            end_year = version_parts[0][-3]
    else:
        # Default initialization for unknown models
        model_code = model.replace("SM-", "")

        # Common patterns from tool.py + OWO + Canada W additions
        region_defaults = {
            "CHC": ["ZC", "CHC", "ZC"],
            "CHN": ["ZC", "CHC", ""],
            "TGY": ["ZH", "OZS", "ZC"],
            # USA carrier (U-suffix): AP=SQ, CSC=OYN
            "ATT": ["SQ", "OYN", "SQ"],
            "VZW": ["SQ", "OYN", "SQ"],
            "TMB": ["SQ", "OYN", "SQ"],
            "CHA": ["SQ", "OYN", "SQ"],
            "CCT": ["SQ", "OYN", "SQ"],
            "DSA": ["SQ", "OYN", "SQ"],
            "DSG": ["SQ", "OYN", "SQ"],
            "GCF": ["SQ", "OYN", "SQ"],
            "XAA": ["SQ", "OYN", "SQ"],
            "USC": ["SQ", "OYN", "SQ"],
            "XPO": ["SQ", "OYN", "SQ"],
            "FKR": ["SQ", "OYN", "SQ"],
            "XAG": ["SQ", "OYN", "SQ"],
            "XAR": ["SQ", "OYN", "SQ"],
            "TMK": ["SQ", "OYN", "SQ"],
            "AIO": ["SQ", "OYN", "SQ"],
            "LRA": ["SQ", "OYN", "SQ"],
            "KOO": ["KS", "OKR", "KS"],
            "EUX": ["XX", "OXM", "XX"],
            "INS": ["XX", "ODM", "XX"],
            # OWO block
            "CHX": ["XX", "OWO", "XX"],
            "ZTR": ["XX", "OWO", "XX"],
            # Canada W block
            "XAC": ["VL", "OYV", "VL"],
            "BMC": ["VL", "OYV", "VL"],
            "RWC": ["VL", "OYV", "VL"],
            "TLS": ["VL", "OYV", "VL"],
            "KDO": ["VL", "OYV", "VL"],
            "VTR": ["VL", "OYV", "VL"],
            "BWA": ["VL", "OYV", "VL"],
            "PCM": ["VL", "OYV", "VL"],
        }

        cc_u = cc.upper()
        if cc_u in region_defaults:
            codes = region_defaults[cc_u]
            first_code = model_code + codes[0]
            second_code = model_code + codes[1]
            third_code = model_code + codes[2]
        else:
            # Generic fallback: infer from model suffix
            suffix = model_code[-1].upper()
            if model_code.upper().endswith("U1"):
                first_code  = model_code + "UE"
                second_code = model_code + "OYM"
                third_code  = model_code + "UE"
            elif suffix == "U":
                first_code  = model_code + "SQ"
                second_code = model_code + "OYN"
                third_code  = model_code + "SQ"
            elif suffix == "W":
                first_code  = model_code + "VL"
                second_code = model_code + "OYV"
                third_code  = model_code + "VL"
            elif suffix == "N":
                first_code  = model_code + "KS"
                second_code = model_code + "OKR"
                third_code  = model_code + "KS"
            else:
                first_code  = model_code + "XX"
                second_code = model_code + "OXM"
                third_code  = model_code + "XX"

        # Wide default ranges matching decrypp.py: 8-year lookback, full BL + update sweep
        now_year = datetime.now().year
        start_y = max(2017, now_year - 8)
        end_y   = min(2027, now_year + 2)
        def _yr_char(y):
            return chr(ord("A") + max(0, min(25, y - 2001)))
        start_year = _yr_char(start_y)
        end_year   = _yr_char(end_y)

        start_bl   = "0"
        end_bl     = "9"

        start_update = "A"
        end_update   = "Z"

    # Build update list
    update_list = get_letters_range(start_update, end_update) + "Z"

    # Store hash -> {version, year, month, algo}
    decrypted = {}
    if resume_data:
        for k, v in resume_data.items():
            if isinstance(v, dict) and "version" in v:
                decrypted[k.lower()] = v
    md5_set, hmac_sha256_set, unknown_set = split_hashes(hash_list)
    total_supported_hashes = len(md5_set) + len(hmac_sha256_set)

    # Remove hashes already found in checkpoint so resume starts from the true remaining set
    already_found = set(decrypted.keys())
    md5_set -= already_found
    hmac_sha256_set -= already_found
    all_target_hashes = md5_set | hmac_sha256_set

    if unknown_set:
        print_msg(f"[yellow]⚠️  Ignoring {len(unknown_set)} unsupported hash values (unexpected length).[/yellow]")

    if decrypted:
        print_msg(f"[cyan]Resuming with {len(decrypted)} previously found hash(es).[/cyan]")
    if resume_meta and resume_meta.get("loop_state"):
        ls = resume_meta["loop_state"]
        print_msg(
            f"[cyan]Loop resume point:[/cyan] "
            f"i1={ls.get('i1','?')} bl={ls.get('bl','?')} upd={ls.get('update','?')} "
            f"year={ls.get('year','?')} month={ls.get('month','?')} serial={ls.get('serial','?')}"
        )

    total_attempts = 0
    start_time = time.time()
    last_found = time.time()
    IDLE_TIMEOUT = 300           # stop if no hit for 5 min; much safer for sparse hashes
    cp_versions = []  # Store baseband versions
    stop_requested = False
    checkpoint_every_hits = 1
    loop_state = {}
    resume_loop_state = resume_meta.get("loop_state", {}) if isinstance(resume_meta, dict) else {}
    resume_active = bool(resume_loop_state)

    # Compute total outer iterations for the progress bar
    _list_bl     = get_letters_range(start_bl, end_bl)
    _list_year   = get_letters_range(start_year, end_year)
    _list_month  = get_letters_range("A", "L")
    _total_outer = 2 * len(_list_bl) * len(update_list) * len(_list_year) * len(_list_month)

    console.print("[dim]Press Ctrl+C once during brute force to stop cleanly and keep all hits found so far.[/dim]")
    if checkpoint_path:
        console.print(f"[dim]Checkpoint file: {checkpoint_path}[/dim]")
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]Scanning[/bold cyan]"),
        BarColumn(bar_width=32),
        TaskProgressColumn(),
        TextColumn("• [green]{task.fields[found]}[/green]/[dim]{task.fields[total_hashes]}[/dim] found"),
        TextColumn("• [dim]{task.fields[rate]}/s[/dim]"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    )

    with progress:
        task = progress.add_task(
            "brute",
            total=_total_outer,
            found=len(decrypted),
            total_hashes=total_supported_hashes,
            rate="0",
        )

        # Brute force loop
        _done = False
        _tick = 0
        for i1 in "US":  # Two variants
            if resume_active and i1 != resume_loop_state.get("i1"):
                continue
            if _done:
                break
            if STOP_REQUESTED:
                stop_requested = True
                _done = True
                break
            for bl_version in get_letters_range(start_bl, end_bl):
                if resume_active and bl_version != resume_loop_state.get("bl"):
                    continue
                if _done:
                    break
                if STOP_REQUESTED:
                    stop_requested = True
                    _done = True
                    break
                for update_version in update_list:
                    if resume_active and update_version != resume_loop_state.get("update"):
                        continue
                    if _done:
                        break
                    if STOP_REQUESTED:
                        stop_requested = True
                        _done = True
                        break
                    for year_str in get_letters_range(start_year, end_year):
                        if resume_active and year_str != resume_loop_state.get("year"):
                            continue
                        if _done:
                            break
                        if STOP_REQUESTED:
                            stop_requested = True
                            _done = True
                            break
                        for month_str in get_letters_range("A", "L"):
                            if resume_active and month_str != resume_loop_state.get("month"):
                                continue
                            # Clean stop requested by user
                            if STOP_REQUESTED:
                                stop_requested = True
                                _done = True
                                break

                            # Idle-timeout: stop if no new hit in IDLE_TIMEOUT seconds
                            if time.time() - last_found >= IDLE_TIMEOUT:
                                _done = True
                                break
                            _tick += 1
                            progress.advance(task, 1)

                            for serial_str in (string.digits + string.ascii_uppercase):
                                if resume_active:
                                    target_serial = resume_loop_state.get("serial")
                                    chars = string.digits + string.ascii_uppercase
                                    if target_serial in chars and chars.index(serial_str) < chars.index(target_serial):
                                        continue
                                    resume_active = False
                                loop_state = {
                                    "i1": i1,
                                    "bl": bl_version,
                                    "update": update_version,
                                    "year": year_str,
                                    "month": month_str,
                                    "serial": serial_str,
                                }
                                total_attempts += 1

                                # Rebuild temp_cp each serial so hits from earlier
                                # serials in the same month are immediately available
                                # as cross-CP candidates (fixes mismatched-CP misses).
                                temp_cp = cp_versions[-12:].copy()
                                if third_code != "":
                                    for i in range(1, 3):
                                        init_cp = third_code + i1 + bl_version + update_version + year_str + month_str + str(i)
                                        if init_cp not in temp_cp:
                                            temp_cp.append(init_cp)

                                # Add prev-serial CP seeds
                                init_cp1 = third_code + i1 + bl_version + update_version + year_str + month_str + get_pre_char(serial_str)
                                init_cp2 = third_code + i1 + bl_version + update_version + year_str + month_str + get_pre_char(get_pre_char(serial_str))
                                if init_cp1 not in temp_cp:
                                    temp_cp.append(init_cp1)
                                if init_cp2 not in temp_cp:
                                    temp_cp.append(init_cp2)

                                # Build base version components
                                random_version = bl_version + update_version + year_str + month_str + serial_str
                                vc2            = bl_version + "Z" + year_str + month_str + serial_str
                                temp_code      = "" if third_code == "" else third_code + i1 + random_version

                                # ── Build candidate list ──────────────────────────────────
                                # Each entry: (ap_part, csc_part, cp_part)
                                candidates = []

                                # Normal AP variants
                                ap_norm = f"{first_code}{i1}{random_version}"
                                ap_e    = f"{first_code}E{random_version}"
                                ap_zn   = f"{first_code}{i1}{vc2}"
                                ap_ze   = f"{first_code}E{vc2}"

                                csc_norm = f"{second_code}{random_version}"
                                csc_z    = f"{second_code}{vc2}"

                                # Base candidates (matching CP)
                                candidates.append((ap_norm, csc_norm, temp_code))
                                candidates.append((ap_e,    csc_norm, temp_code))
                                candidates.append((ap_zn,   csc_z,    temp_code))
                                candidates.append((ap_ze,   csc_z,    temp_code))

                                # Cross-CP candidates (different baseband)
                                if cp_versions:
                                    for cpv in temp_cp[-12:]:
                                        candidates.append((ap_norm, csc_norm, cpv))
                                        candidates.append((ap_e,    csc_norm, cpv))
                                        candidates.append((ap_zn,   csc_z,    cpv))
                                        candidates.append((ap_ze,   csc_z,    cpv))

                                # ── Expand with .DM and .DM.BIG AP suffixes ───────────────
                                # Samsung diagnostic/maintenance builds append .DM or .DM.BIG
                                # to the AP part only; CSC and CP parts are unchanged.
                                # e.g.  F971USQU0AZB1.DM/F971UOYN0AZB1/F971USQU0AZB1
                                #       F971USQU0AZB1.DM.BIG/F971UOYN0AZB1/F971USQU0AZB1
                                dm_candidates = []
                                for (ap, csc, cp) in candidates:
                                    dm_candidates.append((ap + ".DM",     csc, cp))
                                    dm_candidates.append((ap + ".DM.BIG", csc, cp))
                                candidates.extend(dm_candidates)

                                # ── Check all candidates ──────────────────────────────────
                                for (ap, csc, cp) in candidates:
                                    ver = f"{ap}/{csc}/{cp}"
                                    hit_hash = None
                                    hit_algo = None

                                    if md5_set:
                                        md5_hash = hashlib.md5(ver.encode("ascii", errors="ignore")).hexdigest()
                                        if md5_hash in md5_set and md5_hash not in decrypted:
                                            hit_hash = md5_hash
                                            hit_algo = "md5"

                                    if hit_hash is None and hmac_sha256_set:
                                        hmac_sha256_hash = hmac.new(
                                            VERSION_TEST_HMAC_SHA256_KEY,
                                            ver.encode("ascii", errors="ignore"),
                                            hashlib.sha256,
                                        ).hexdigest()
                                        if hmac_sha256_hash in hmac_sha256_set and hmac_sha256_hash not in decrypted:
                                            hit_hash = hmac_sha256_hash
                                            hit_algo = "hmac-sha256"

                                    if hit_hash is not None:
                                        year_num  = ord(year_str) - ord("A") + 2001
                                        month_num = ord(month_str) - ord("A") + 1
                                        decrypted[hit_hash] = {
                                            'version': ver,
                                            'year':    year_num,
                                            'month':   month_num,
                                            'algo':    hit_algo,
                                        }
                                        print_msg(f"[green]HIT[/green] [{hit_algo.upper()}] {hit_hash} -> {ver}")

                                        # Remove resolved hash immediately from the active target sets
                                        if hit_algo == "md5":
                                            md5_set.discard(hit_hash)
                                        else:
                                            hmac_sha256_set.discard(hit_hash)
                                        all_target_hashes = md5_set | hmac_sha256_set
                                        progress.update(task, found=len(decrypted))

                                        if checkpoint_path and (len(decrypted) % checkpoint_every_hits == 0):
                                            save_checkpoint(
                                                checkpoint_path,
                                                model,
                                                cc,
                                                decrypted,
                                                meta={"status": "running", "latest_version": latest_version, "loop_state": loop_state},
                                            )
                                        cp_part = ver.split("/")[2]
                                        if cp_part and cp_part not in cp_versions and cp_part not in temp_cp:
                                            cp_versions.append(cp_part)
                                            temp_cp.append(cp_part)
                                        last_found = time.time()
                                        if not all_target_hashes:
                                            _done = True
                                            break
                                if _done:
                                    break

                                # Update progress bar rate every ~50k ticks
                                if _tick % 50000 == 0:
                                    elapsed = time.time() - start_time
                                    rate = int(_tick / elapsed) if elapsed > 0 else 0
                                    progress.update(task, found=len(decrypted), total_hashes=total_supported_hashes, rate=f"{rate:,}")

    elapsed_time = time.time() - start_time
    rate_final = int(_tick / elapsed_time) if elapsed_time > 0 else 0
    remaining_targets = len(md5_set) + len(hmac_sha256_set)
    console.print(
        f"[dim]  {_tick:,} attempts  •  {rate_final:,}/s  •  "
        f"[green]{len(decrypted)}[/green]/[dim]{total_supported_hashes}[/dim] found  •  "
        f"[dim]{remaining_targets} remaining[/dim]  •  "
        f"{elapsed_time:.1f}s[/dim]"
    )

    remaining_md5 = sorted(md5_set)
    remaining_hmac = sorted(hmac_sha256_set)
    if remaining_md5 or remaining_hmac:
        console.print("\n[yellow]Unresolved hashes remaining:[/yellow]")
        for h in remaining_md5:
            console.print(f"  [cyan]MD5[/cyan] {h}")
        for h in remaining_hmac:
            console.print(f"  [magenta]HMAC-SHA256[/magenta] {h}")

    if checkpoint_path:
        status = "complete" if remaining_targets == 0 else ("stopped" if stop_requested else "partial")
        save_checkpoint(
            checkpoint_path,
            model,
            cc,
            decrypted,
            meta={"status": status, "latest_version": latest_version, "loop_state": loop_state},
        )

    return decrypted, stop_requested


def save_results_json(model, cc, decrypted_versions, base_cc=None):
    """Save decrypted results to a single JSON file."""
    output_dir = ensure_output_dir()
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    sorted_items = sorted(
        decrypted_versions.items(),
        key=lambda x: (x[1]['year'], x[1]['month'], x[1]['version'].split("/")[0][-4:])
    )

    entries = [
        {
            "version": info["version"],
            "hash":    md5_hash,
            "algo":    info.get("algo", "md5"),
            "date":    f"{info['year']}-{info['month']:02d}",
        }
        for md5_hash, info in sorted_items
    ]

    payload = {
        "model":        model,
        "region":       cc,
        "base_region":  base_cc,
        "generated_at": now_str,
        "count":        len(entries),
        "firmwares":    entries,
    }

    filepath = os.path.join(output_dir, f"{model}_{cc}_decrypted.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return filepath


def _box(title: str, body: str, style: str = "cyan") -> Panel:
    """Helper — titled panel with consistent styling."""
    return Panel(body, title=f"[bold {style}]{title}[/bold {style}]",
                 border_style=style, box=box.ROUNDED, padding=(0, 2))


def _rule(label: str = "") -> None:
    from rich.rule import Rule
    console.print(Rule(f"[dim]{label}[/dim]" if label else "", style="dim cyan"))


def main():
    # ── Banner ────────────────────────────────────────────────────────────────
    console.print()
    console.print(Panel(
        "[bold bright_white]Decrypter[/bold bright_white]",
        border_style="cyan",
        box=box.HEAVY,
        padding=(0, 2),
        expand=True,
    ))
    console.print()

    # ── Model input ───────────────────────────────────────────────────────────
    console.print(_box("INPUT", "[dim]Samsung model number[/dim]"))
    raw_model = input("  Model: ").strip().upper()
    if not raw_model:
        console.print("[red]  ✘  No model entered.[/red]\n")
        return
    model = raw_model if raw_model.startswith("SM-") else "SM-" + raw_model
    console.print()

    # ── Region input ──────────────────────────────────────────────────────────
    console.print(_box("REGION", "  [bold]1[/bold]  Enter CSC manually\n  [bold]2[/bold]  Choose from OXM subset"))
    choice = input("  Option [1/2]: ").strip()
    console.print()

    if choice == "2":
        rows = "\n".join(
            f"  [bold cyan]{i:>2}[/bold cyan]  {code}  [dim]{get_country_name(code)}[/dim]"
            for i, code in enumerate(OXM_USER_SUBSET, 1)
        )
        console.print(_box("OXM SUBSET", rows))
        sel = input("  Select number: ").strip()
        console.print()
        try:
            idx = int(sel)
            cc = OXM_USER_SUBSET[idx - 1] if 1 <= idx <= len(OXM_USER_SUBSET) \
                 else input("  CSC: ").strip().upper()
        except ValueError:
            cc = input("  CSC: ").strip().upper()
    else:
        cc = input("  CSC (e.g. ATT, XAA, INS): ").strip().upper()
        console.print()

    # ── Target summary ────────────────────────────────────────────────────────
    console.print(_box("TARGET",
        f"  Model   [bold bright_white]{model}[/bold bright_white]\n"
        f"  Region  [bold cyan]{cc}[/bold cyan]  [dim]{get_country_name(cc)}[/dim]"
    ))
    console.print()

    # ── Step 1: fetch hashes ──────────────────────────────────────────────────
    with console.status("[cyan]  Fetching firmware hashes…[/cyan]", spinner="dots"):
        md5_list = get_md5_list(model, cc)

    if not md5_list:
        console.print(_box("ERROR", "  [red]No firmware hashes found.[/red]  Check model / CSC.", style="red"))
        console.print()
        return

    md5_set, hmac_sha256_set, unknown_set = split_hashes(md5_list)
    console.print(_box(
        "HASHES",
        f"  [green]✓[/green]  [bold]{len(md5_list)}[/bold] hashes loaded\n"
        f"      MD5      [cyan]{len(md5_set)}[/cyan]\n"
        f"      HMAC-SHA256  [cyan]{len(hmac_sha256_set)}[/cyan]"
        + (f"\n      Other    [yellow]{len(unknown_set)}[/yellow]" if unknown_set else "")
    ))
    console.print()

    # ── Step 2: base firmware scan (silent if nothing found) ──────────────────
    with console.status("[cyan]  Scanning for base firmware…[/cyan]", spinner="dots"):
        latest_version, os_version, base_cc = get_latest_version_with_fallback(model, cc)

    if latest_version:
        console.print(_box("BASE FIRMWARE",
            f"  [green]✓[/green]  [bold]{latest_version}[/bold]\n"
            f"      Source  [cyan]{base_cc}[/cyan]"
        ))
        console.print()
    else:
        console.print(_box("BASE FIRMWARE", "  [yellow]No base firmware found quickly.[/yellow]  Continuing straight to brute force.", style="yellow"))
        console.print()

    # ── Step 3: confirm + brute force ─────────────────────────────────────────
    checkpoint_path, resume_data, checkpoint_meta = load_checkpoint(model, cc)
    if resume_data:
        extra = ""
        if checkpoint_meta.get("loop_state"):
            ls = checkpoint_meta["loop_state"]
            extra = f"\n      Loop  [cyan]{ls.get('i1','?')} / {ls.get('bl','?')} / {ls.get('update','?')} / {ls.get('year','?')} / {ls.get('month','?')} / {ls.get('serial','?')}[/cyan]"
        console.print(_box("CHECKPOINT", f"  [green]Found checkpoint[/green] with [bold]{len(resume_data)}[/bold] saved hit(s).  Resuming from saved results.{extra}"))
        console.print()

    console.print(_box("READY", "  Press [bold]Enter[/bold] to begin decryption  [dim]Press Ctrl+C once to stop cleanly[/dim]"))
    input("  > ")
    console.print()

    decrypted, stop_requested = decrypt_firmware(
        model,
        cc,
        md5_list,
        latest_version,
        checkpoint_path=checkpoint_path,
        resume_data=resume_data,
        resume_meta=checkpoint_meta,
    )
    console.print()

    if stop_requested:
        console.print(_box("STOPPED", "  [yellow]Brute force stopped cleanly by user.[/yellow]  Partial hits were preserved.", style="yellow"))
        console.print()

    # ── Step 4: results ───────────────────────────────────────────────────────
    if not decrypted:
        console.print(_box("RESULT", "  [yellow]No firmware versions decrypted.[/yellow]", style="yellow"))
        console.print()
        return

    sorted_entries = sorted(
        decrypted.items(),
        key=lambda x: (x[1]['year'], x[1]['month'], x[1]['version'].split("/")[0][-4:])
    )

    table = Table(
        box=box.SIMPLE_HEAD, border_style="dim cyan",
        show_header=True, header_style="bold cyan",
        pad_edge=True, show_edge=False,
    )
    table.add_column("#",        style="dim",          width=4,  justify="right")
    table.add_column("Firmware", style="bright_white", no_wrap=True)
    table.add_column("Date",     style="green",        width=10, no_wrap=True)

    for idx, (md5_hash, info) in enumerate(sorted_entries, 1):
        import calendar
        month_name = calendar.month_abbr[info['month']]
        table.add_row(
            str(idx),
            info['version'],
            f"{month_name} {info['year']}",
        )

    console.print(Panel(
        table,
        title=f"[bold cyan]RESULTS[/bold cyan]  [dim]{model} / {cc}  —  {len(decrypted)} decrypted[/dim]",
        border_style="cyan",
        box=box.ROUNDED,
        padding=(0, 1),
    ))
    console.print()

    # ── Step 5: save ──────────────────────────────────────────────────────────
    filepath = save_results_json(model, cc, decrypted, base_cc=base_cc)
    remaining_md5, remaining_hmac = get_unresolved_hashes(md5_list, decrypted)
    remaining_targets = len(remaining_md5) + len(remaining_hmac)

    if remaining_targets == 0:
        remove_checkpoint(checkpoint_path)
    console.print(_box("SAVED", f"  [green]✓[/green]  [bold]{filepath}[/bold]"))
    if remaining_targets > 0:
        console.print(_box("CHECKPOINT", f"  [yellow]Progress checkpoint kept[/yellow] at [bold]{checkpoint_path}[/bold]", style="yellow"))
    else:
        console.print(_box("CHECKPOINT", "  [green]All supported hashes resolved.[/green]  Checkpoint removed.", style="green"))
    console.print()


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _handle_stop_signal)
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n\n[yellow]  Stop requested.[/yellow]\n")
    except Exception as e:
        console.print(f"\n[red]  Error: {e}[/red]\n")



