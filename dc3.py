from __future__ import annotations
from InquirerPy.utils import get_style

import csv
import hashlib
import json
import os
import random
import re
import sqlite3
import string
import sys
import time
import select

try:
    import termios
    import tty
except Exception:
    termios = None
    tty = None
from collections import defaultdict
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests
from lxml import etree
from rich import box
from rich.console import Console, Group
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from rich.text import Text

try:
    from InquirerPy import inquirer
    from InquirerPy.base.control import Choice
    from InquirerPy.separator import Separator
    HAVE_INQUIRER = True
except Exception:
    HAVE_INQUIRER = False
    Choice = None
    Separator = None

console = Console()

# ===============================
#            THEME
# ===============================
P = {
    "rose": "#f2a7b8",
    "peach": "#f6c6a8",
    "mint": "#9fd8c1",
    "sky": "#9ec5fe",
    "lav": "#c4b5fd",
    "cyan": "#8bd3dd",
    "text": "#e8e6f2",
    "dim": "#9aa3b2",
    "line": "#6b7280",
    "warn": "#f7d794",
    "err": "#f5a3a3",
    "ok": "#a8e6cf",
}

STYLE = get_style(
    {
        "questionmark": f"fg:{P['lav']} bold",
        "question": f"fg:{P['text']} bold",
        "answer": f"fg:{P['mint']} bold",
        "pointer": f"fg:{P['sky']} bold",
        "selected": f"fg:{P['mint']} bold",
        "checkbox": f"fg:{P['sky']} bold",
        "separator": f"fg:{P['dim']}",
        "instruction": f"fg:{P['dim']}",
    },
    style_override=False,
)
# ===============================
#         PATHS / CONST
# ===============================
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DECRYPTED_DIR = os.path.join(BASE_PATH, "Decrypted")
DB_PATH = os.path.join(BASE_PATH, "database", "fumo_history.db")

ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
SERIALS = "123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
MONTHS = "ABCDEFGHIJKL"

OXM_CSCS = [
    "ACR","AFG","AFR","BKD","BNG","BOG","CAC","CAU","CPW","DKR","ECT","EGY","EUX","EUY","FWD",
    "GLB","ILO","ILP","INS","KSA","LYS","MEO","MET","MID","MM1","MOB","MOT","MSR","MWD","NPB",
    "OPS","ANC","PAK","PKD","PLS","PRT","SER","SFR","SIM","SIN","SKZ","SLK","SOZ","STH","SWC","TEL",
    "THL","TOP","TUN","TUR","VAU","VIP","WWA","WWC","WWD","XFA","XFV","XME","XNZ","XSA","XSG",
    "XSP","XTC","XXV","XID","XFE","O2C","CHX","ZTR",
]
ODM_CSCS = ["BKD", "INS", "NPL", "SLK"]
OJM_CSCS = ["ACR","AFG","AFR","DKR","ECT","EGY","FWD","ILO","ILP","KSA","LYS","MID","MWD","PAK","PKD","TUN","TUR","XFA","XFE","XFV","XSG"]
OLE_CSCS = ["XID"]
OWO_CSCS = ["BVO","BVT","CHE","CHL","CHO","CHT","CHX","GTO","NBS","ZTA","ZTM","ZTO","ZTR","ZVV"]
OXE_CSCS = ["CAU","SEK","SKZ","SER"]
USA_U_CSCS = ["ATT","VZW","TMB","CHA","CCT","DSA","DSG","GCF","XAA","USC"]
USA_U1_CSCS = ["ATT","VZW","TMB","CHA","CCT","DSA","DSG","XAA","USC","XPO","FKR","XAG","XAR","WWD","TMK","AIO","LRA"]

CSC_BLOCKS = {
    "OXM": OXM_CSCS,
    "ODM": ODM_CSCS,
    "OJM": OJM_CSCS,
    "OLE": OLE_CSCS,
    "OWO": OWO_CSCS,
    "OXE": OXE_CSCS,
    "USA_U": USA_U_CSCS,
    "USA_U1": USA_U1_CSCS,
}

# ===============================
#          UTILITIES
# ===============================
def ensure_output_dir() -> str:
    os.makedirs(DECRYPTED_DIR, exist_ok=True)
    return DECRYPTED_DIR


def norm_model(raw: str) -> str:
    raw = raw.strip().upper()
    return raw if raw.startswith("SM-") else f"SM-{raw}"


def norm_csc(raw: str) -> str:
    return raw.strip().upper()


def next_char(ch: str) -> str:
    return ALPHABET[(ALPHABET.index(ch) + 1) % len(ALPHABET)]


def prev_char(ch: str) -> str:
    return ALPHABET[(ALPHABET.index(ch) - 1) % len(ALPHABET)]


def letters_range(start: str, end: str) -> str:
    s = ALPHABET.index(start)
    e = ALPHABET.index(end)
    return ALPHABET[s:e + 1] if s <= e else ALPHABET[s:] + ALPHABET[:e + 1]


def country_name(cc: str) -> str:
    names = {
        "CHC": "China", "CHN": "China", "TGY": "Hong Kong", "KOO": "Korea",
        "EUX": "Europe", "INS": "India", "XXV": "Vietnam", "XAA": "USA (Unlocked)",
        "ATT": "USA (AT&T)", "TMB": "USA (T-Mobile)", "DSA": "USA (Dish)",
        "USC": "USA (US Cellular)", "VZW": "USA (Verizon)",
    }
    return names.get(cc, cc)


def print_status(msg: str, style: str = None) -> None:
    console.print(msg if style is None else f"[{style}]{msg}[/{style}]")


def classify_build(version: str, latest_version: Optional[str]) -> str:
    pda = version.split("/")[0] if version else ""
    if latest_version and version == latest_version:
        return "stable-live"
    if len(pda) >= 4 and pda[-4] == "Z":
        return "beta"
    if len(pda) >= 3 and pda[-3] == "Z":
        return "beta"
    if "/" in version:
        cp = version.split("/")[-1]
        if cp and cp != pda:
            return "variant"
    return "stable"


def parse_date_from_version(version: str) -> Tuple[int, int]:
    try:
        pda = version.split("/")[0]
        y = ord(pda[-3]) - ord("A") + 2001
        m = ord(pda[-2]) - ord("A") + 1
        if 1 <= m <= 12:
            return y, m
    except Exception:
        pass
    return 0, 0

# ===============================
#          NETWORK
# ===============================
def request_xml(url: str, retries: int = 3, timeout: int = 12) -> Optional[bytes]:
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        "Mozilla/5.0 (Linux; Android 14; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/24.0 Chrome/120 Safari/537.36",
    ]
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers={"User-Agent": random.choice(agents), "Connection": "close"}, timeout=timeout)
            if r.status_code == 200 and r.content:
                return r.content
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)
        if attempt < retries:
            time.sleep(0.5 * attempt)
    console.print(f"[bold {P['err']}]Network error[/]: {last_err}")
    return None


def get_md5_list(model: str, cc: str) -> List[str]:
    url = f"https://fota-cloud-dn.ospserver.net/firmware/{cc}/{model}/version.test.xml"
    content = request_xml(url)
    if not content:
        return []
    try:
        xml = etree.fromstring(content)
        return [x.strip() for x in xml.xpath("//value//text()") if x and x.strip()]
    except Exception as e:
        console.print(f"[bold {P['err']}]XML parse error[/]: {e}")
        return []


def get_latest_version(model: str, cc: str) -> Tuple[Optional[str], Optional[str]]:
    url = f"https://fota-cloud-dn.ospserver.net/firmware/{cc}/{model}/version.xml"
    content = request_xml(url)
    if not content:
        return None, None
    try:
        xml = etree.fromstring(content)
        latest = xml.xpath("//latest//text()")
        if not latest:
            return None, None
        osv = xml.xpath("//latest/@o")
        return latest[0], (osv[0] if osv else None)
    except Exception:
        return None, None


def get_csc_fallbacks(primary_cc: str) -> List[str]:
    primary_cc = primary_cc.upper()
    fallback: List[str] = []
    for _, members in CSC_BLOCKS.items():
        if primary_cc in members:
            for item in members:
                if item not in fallback:
                    fallback.append(item)
    return [primary_cc] + [x for x in fallback if x != primary_cc] if fallback else [primary_cc]


def get_latest_with_fallback(model: str, primary_cc: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    for csc in get_csc_fallbacks(primary_cc):
        latest, osver = get_latest_version(model, csc)
        if latest:
            return latest, osver, csc
    return None, None, None

# ===============================
#       PERSISTENCE / SAVE
# ===============================
def load_existing_decrypted_map(model: str, cc: str) -> Dict[str, dict]:
    ensure_output_dir()
    path = os.path.join(DECRYPTED_DIR, f"{model}_{cc}_full.json")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return {}
    out: Dict[str, dict] = {}
    for fw in data.get("firmwares", []):
        md5 = fw.get("md5")
        ver = fw.get("version")
        y, m = parse_date_from_version(ver)
        if md5 and ver:
            out[md5] = {"version": ver, "year": y, "month": m, "kind": fw.get("kind", "unknown")}
    return out


def save_outputs(model: str, cc: str, decrypted_map: Dict[str, dict], server_md5s: Sequence[str], base_cc: Optional[str], latest_version: Optional[str]) -> Dict[str, str]:
    ensure_output_dir()
    items = sorted(decrypted_map.items(), key=lambda x: (x[1].get("year", 0), x[1].get("month", 0), x[1].get("version", "")), reverse=True)
    unresolved = [md5 for md5 in server_md5s if md5 not in decrypted_map]

    json_path = os.path.join(DECRYPTED_DIR, f"{model}_{cc}_full.json")
    txt_path = os.path.join(DECRYPTED_DIR, f"{model}_{cc}_decrypted.txt")
    csv_path = os.path.join(DECRYPTED_DIR, f"{model}_{cc}_decrypted.csv")
    unr_path = os.path.join(DECRYPTED_DIR, f"{model}_{cc}_unresolved.txt")

    full = {
        "model": model,
        "region": cc,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "base_csc": base_cc,
        "latest_stable": latest_version,
        "total_server_md5": len(server_md5s),
        "total_found": len(items),
        "unresolved": len(unresolved),
        "firmwares": [
            {
                "version": info["version"],
                "md5": md5,
                "date": f"{info.get('year', 0)}-{info.get('month', 0):02d}",
                "kind": info.get("kind", classify_build(info["version"], latest_version)),
            }
            for md5, info in items
        ],
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(full, f, indent=2)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(f"Samsung Firmware Results\nModel: {model}\nRegion: {cc}\nBase CSC: {base_cc}\nLatest stable: {latest_version}\nGenerated: {datetime.now():%Y-%m-%d %H:%M:%S}\n\n")
        for md5, info in items:
            f.write(f"{info['version']}\n")
            f.write(f"MD5 : {md5}\n")
            f.write(f"Date: {info.get('year', 0)}-{info.get('month', 0):02d}\n")
            f.write(f"Kind: {info.get('kind', classify_build(info['version'], latest_version))}\n\n")

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["version", "md5", "date", "kind"])
        for md5, info in items:
            writer.writerow([info["version"], md5, f"{info.get('year', 0)}-{info.get('month', 0):02d}", info.get("kind", classify_build(info['version'], latest_version))])

    with open(unr_path, "w", encoding="utf-8") as f:
        for md5 in unresolved:
            f.write(md5 + "\n")

    return {"json": json_path, "txt": txt_path, "csv": csv_path, "unresolved": unr_path}

# ===============================
#        BRUTE ENGINE
# ===============================
def derive_codes(model: str, cc: str, latest_version: Optional[str]) -> Tuple[str, str, str, str, str, str, str, str]:
    cc = cc.upper()
    if latest_version:
        vp = latest_version.split("/")
        first_code = vp[0][:-6]
        second_code = vp[1][:-5]
        third_code = vp[2][:-6] if len(vp) > 2 else ""

        latest_year = vp[0][-3]
        start_year = chr(ord("A") + max(0, ord(latest_year) - ord("A") - 4))
        end_year = next_char(latest_year) if vp[0][-2] in "JKL" else latest_year
        start_bl = "0"
        end_bl = next_char(vp[0][-5])
        start_upd = "A"
        end_upd = next_char(vp[0][-4])
        return first_code, second_code, third_code, start_year, end_year, start_bl, end_bl, start_upd + ":" + end_upd

    model_code = model.replace("SM-", "")
    suffix = "U1" if model_code.endswith("U1") else model_code[-1]

    ap_tag, csc_tag, cp_tag = "XX", "OXM", "XX"
    if cc in ("CHC", "CHN"):
        ap_tag, csc_tag, cp_tag = "ZC", "CHC", ("" if cc == "CHN" else "ZC")
    elif cc == "TGY":
        ap_tag, csc_tag, cp_tag = "ZH", "OZS", "ZC"
    elif suffix == "U":
        ap_tag, csc_tag, cp_tag = "SQ", "OYN", "SQ"
    elif suffix == "U1":
        ap_tag, csc_tag, cp_tag = "UE", "OYM", "UE"
    elif suffix == "W":
        ap_tag, csc_tag, cp_tag = "VL", "OYV", "VL"
    elif suffix == "N":
        ap_tag, csc_tag, cp_tag = "NK", "OKR", "NK"
    elif suffix == "0":
        ap_tag = "ZH" if cc in ["TGY", "BRI"] else "ZC"
        csc_tag = cc
        cp_tag = ap_tag
    elif cc in ["EUX", "EUY"]:
        ap_tag, csc_tag, cp_tag = "XX", "OXM", "XX"
    elif cc in ["INS", "NPL", "SLK"]:
        ap_tag, csc_tag, cp_tag = "XX", "ODM", "XX"
    elif cc in ["CHX", "ZTR"]:
        ap_tag, csc_tag, cp_tag = "XX", "OWO", "XX"

    first_code = model_code + ap_tag
    second_code = model_code + csc_tag
    third_code = model_code + cp_tag if cp_tag else ""

    start_year = chr(max(ord("A"), datetime.now().year - 2001 + ord("A") - 3))
    end_year = next_char(next_char(start_year))
    start_bl = "0"
    end_bl = "9"
    start_upd = "A"
    end_upd = "Z"
    return first_code, second_code, third_code, start_year, end_year, start_bl, end_bl, start_upd + ":" + end_upd


def decrypt_firmware(
    model: str,
    cc: str,
    md5_list: Iterable[str],
    latest_version: Optional[str],
    full_brute: bool = True,
    progress_callback: Optional[Callable[[str, int, int, str], None]] = None,
) -> Dict[str, dict]:
    md5_set: Set[str] = set(md5_list)
    if not md5_set:
        return {}

    first_code, second_code, third_code, start_year, end_year, start_bl, end_bl, upd_range = derive_codes(model, cc, latest_version)
    start_upd, end_upd = upd_range.split(":", 1)

    # Keep dc2-like broad search, including Z beta and E-prefixed AP variants.
    years = letters_range(start_year, end_year)
    bls = letters_range(start_bl, end_bl)
    updates = letters_range(start_upd, end_upd)
    if "Z" not in updates:
        updates += "Z"

    cp_versions: List[str] = []
    decrypted: Dict[str, dict] = {}
    seen_versions: Set[str] = set()

    outer_total = len("US") * len(bls) * len(updates) * len(years) * len(MONTHS)
    progress_label = f"{model}/{cc}"
    last_reported_percent = -1

    def register(ver: str, md5: str, ych: str, mch: str) -> None:
        if md5 in md5_set and md5 not in decrypted and ver not in seen_versions:
            y = ord(ych) - ord("A") + 2001
            m = ord(mch) - ord("A") + 1
            decrypted[md5] = {"version": ver, "year": y, "month": m, "kind": classify_build(ver, latest_version)}
            seen_versions.add(ver)

    with Progress(
        SpinnerColumn(style=f"bold {P['sky']}"),
        TextColumn(f"[bold {P['lav']}]Decrypting[/] [bold {P['text']}]{{task.fields[target]}}[/]"),
        BarColumn(bar_width=34, complete_style=f"bold {P['mint']}", finished_style=f"bold {P['ok']}"),
        TextColumn("[bold] {task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task("scan", total=outer_total, target=progress_label)
        if progress_callback:
            progress_callback("decrypt", 0, outer_total, progress_label)
        for flavor in "US":
            for bl in bls:
                for upd in updates:
                    for ych in years:
                        for mch in MONTHS:
                            progress.advance(task)
                            if progress_callback:
                                completed = int(progress.tasks[task].completed)
                                percent = int((completed / outer_total) * 100) if outer_total else 100
                                if percent != last_reported_percent:
                                    last_reported_percent = percent
                                    progress_callback("decrypt", completed, outer_total, progress_label)
                            local_cp = cp_versions[-16:].copy()
                            if third_code:
                                for i in range(1, 4):
                                    seed = third_code + flavor + bl + upd + ych + mch + str(i)
                                    if seed not in local_cp:
                                        local_cp.append(seed)

                            for serial in SERIALS:
                                rnd = bl + upd + ych + mch + serial
                                beta_rnd = bl + "Z" + ych + mch + serial
                                tcode = "" if not third_code else third_code + flavor + rnd
                                btcode = "" if not third_code else third_code + flavor + beta_rnd

                                # learn nearby CP candidates like dc2
                                if third_code:
                                    for nearby in [serial, prev_char(serial), prev_char(prev_char(serial))]:
                                        cpv = third_code + flavor + bl + upd + ych + mch + nearby
                                        if cpv not in local_cp:
                                            local_cp.append(cpv)

                                normal_versions = [
                                    f"{first_code}{flavor}{rnd}/{second_code}{rnd}/{tcode}",
                                    f"{first_code}E{rnd}/{second_code}{rnd}/{tcode}",
                                    f"{first_code}{flavor}{beta_rnd}/{second_code}{beta_rnd}/{btcode}",
                                    f"{first_code}E{beta_rnd}/{second_code}{beta_rnd}/{btcode}",
                                ]
                                for ver in normal_versions:
                                    register(ver, hashlib.md5(ver.encode()).hexdigest(), ych, mch)

                                # CP variants / mismatched CP can expose more server lines
                                for cpv in local_cp:
                                    if cpv:
                                        ver2 = f"{first_code}{flavor}{rnd}/{second_code}{rnd}/{cpv}"
                                        ver2e = f"{first_code}E{rnd}/{second_code}{rnd}/{cpv}"
                                        ver4 = f"{first_code}{flavor}{beta_rnd}/{second_code}{beta_rnd}/{cpv}"
                                        ver4e = f"{first_code}E{beta_rnd}/{second_code}{beta_rnd}/{cpv}"
                                        register(ver2, hashlib.md5(ver2.encode()).hexdigest(), ych, mch)
                                        register(ver2e, hashlib.md5(ver2e.encode()).hexdigest(), ych, mch)
                                        register(ver4, hashlib.md5(ver4.encode()).hexdigest(), ych, mch)
                                        register(ver4e, hashlib.md5(ver4e.encode()).hexdigest(), ych, mch)

                                if tcode and tcode not in cp_versions and any(v.endswith("/" + tcode) for v in seen_versions):
                                    cp_versions.append(tcode)

                                if not full_brute and len(decrypted) == len(md5_set):
                                    if progress_callback:
                                        progress_callback("decrypt", outer_total, outer_total, progress_label)
                                    return decrypted
    if progress_callback:
        progress_callback("decrypt", outer_total, outer_total, progress_label)
    return decrypted

# ===============================
#          TARGETS
# ===============================
def get_family_targets(main_cc: str) -> List[str]:
    main_cc = main_cc.upper()
    for members in CSC_BLOCKS.values():
        if main_cc in members:
            # keep it practical; front-load requested CSC, then nearby siblings
            siblings = [x for x in members if x != main_cc]
            return [main_cc] + siblings[:8]
    return [main_cc]

# ===============================
#           DB UI
# ===============================
def get_grouped_db_entries() -> Dict[str, List[dict]]:
    if not os.path.exists(DB_PATH):
        return {}
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            SELECT device_model, csc, COUNT(*) AS hits
            FROM firmware_hits
            WHERE device_model IS NOT NULL AND device_model != ''
              AND csc IS NOT NULL AND csc != ''
              AND found_pda IS NOT NULL AND found_pda != ''
            GROUP BY device_model, csc
            ORDER BY device_model ASC, hits DESC
        """)
        rows = cur.fetchall()
        conn.close()
    except Exception:
        return {}

    best: Dict[str, Tuple[str, str, int]] = {}
    for model, csc, hits in rows:
        model = str(model).upper()
        csc = str(csc).upper()
        if model not in best or hits > best[model][2]:
            best[model] = (model, csc, hits)

    grouped = defaultdict(list)
    for model, csc, hits in sorted(best.values(), key=lambda x: x[0]):
        prefix = model.replace("SM-", "")[:1]
        grouped[prefix].append({"model": model, "csc": csc, "hits": hits})
    return dict(grouped)

# ===============================
#           RENDERING
# ===============================
def pda_only(v: str) -> str:
    if not v:
        return "Unknown"
    return v.split("/")[0]


def render_header() -> None:
    info = Table.grid(expand=True)
    info.add_column(style=f"bold {P['lav']}", justify="left", ratio=1)
    info.add_column(style=P["text"], justify="left", ratio=3)
    info.add_row("Decrypter", "DC3 Standalone")
    info.add_row("Mode", "Full server enumeration")
    info.add_row("Style", "Pastel mobile UI")
    info.add_row("Time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    console.print(Panel(info, title=f"[bold {P['sky']}]Decrypter Info[/]", border_style=P["line"], box=box.ROUNDED, padding=(0, 1)))


def render_target_header(model: str, cc: str, targets: list[str]) -> None:
    body = Table.grid(expand=True)
    body.add_column(justify="left", ratio=1)
    body.add_column(justify="left", ratio=3)
    body.add_row("Target", f"{model} / {cc}")
    body.add_row("Regions", ", ".join(targets))
    console.print(Panel(body, title=f"[bold {P['mint']}]Scan[/]", border_style=P["line"], box=box.ROUNDED, padding=(0,1)))


def render_compact_summary(result: dict) -> None:
    t = Table.grid(expand=True)
    t.add_column(style=f"bold {P['sky']}", ratio=1)
    t.add_column(style=P["text"], ratio=3)
    t.add_row("Model", result["model"])
    t.add_row("CSC", result["region"])
    t.add_row("Stable", result.get("latest_stable") or "Unknown")
    t.add_row("Android", result.get("android") or "Unknown")
    t.add_row("Base CSC", result.get("base_csc") or result["region"])
    t.add_row("MD5s", str(result.get("server_md5s", 0)))
    t.add_row("Resolved", str(result.get("resolved_count", 0)))
    t.add_row("Unresolved", str(result.get("unresolved_count", 0)))
    t.add_row("Elapsed", f"{result.get('elapsed', 0.0):.1f}s")
    console.print(Panel(t, title=f"[bold {P['lav']}]Summary[/]", border_style=P["line"], box=box.ROUNDED, padding=(0,1)))


def render_latest_builds(items: list[dict], limit: int = 25) -> None:
    total = len(items)
    items = items[:limit]
    table = Table(box=box.SIMPLE_HEAVY, expand=True, padding=(0,1), show_lines=False)
    table.add_column("Date", style=P["mint"], no_wrap=True, width=7)
    table.add_column("Kind", style=P["lav"], no_wrap=True, width=11)
    table.add_column("Triplet", style=P["text"], ratio=1, overflow="fold")
    for item in items:
        table.add_row(
            f"{item.get('year', 0)}-{item.get('month', 0):02d}",
            item.get("kind", "unknown"),
            item.get("version", ""),
        )
    console.print(Panel(table, title=f"[bold {P['sky']}]Latest Builds ({len(items)}/{total})[/]", border_style=P["line"], box=box.ROUNDED, padding=(0,0)))


def render_saved_files(paths: dict) -> None:
    lines = [
        f"[bold {P['lav']}]JSON[/]  {paths['json']}",
        f"[bold {P['lav']}]TXT[/]   {paths['txt']}",
        f"[bold {P['lav']}]CSV[/]   {paths['csv']}",
        f"[bold {P['lav']}]UNR[/]   {paths['unresolved']}",
    ]
    console.print(
        Panel(
            '\n'.join(lines),
            title=f"[bold {P['mint']}]Saved Files[/]",
            border_style=P["line"],
            box=box.ROUNDED,
            padding=(0,1),
        )
    )

def wait_for_return() -> None:
    console.print(f"\n[{P['dim']}]Press Enter to return to menu... [extra keys Enter works too][/{P['dim']}]")
    if sys.stdin.isatty() and termios and tty:
        try:
            fd = sys.stdin.fileno()
            old = termios.tcgetattr(fd)
            tty.setraw(fd)
            while True:
                ch = sys.stdin.read(1)
                if ch in ('\r', '\n'):
                    break
        except Exception:
            try:
                input()
            except EOFError:
                pass
        finally:
            try:
                termios.tcsetattr(fd, termios.TCSADRAIN, old)
            except Exception:
                pass
    else:
        try:
            input()
        except EOFError:
            pass

def terminal_reset(full: bool = True) -> None:
    if not sys.stdout.isatty():
        console.clear()
        return
    try:
        # Full RIS reset plus scrollback clear is the strongest practical reset Termux honors.
        if full:
            sys.stdout.write("c")
        sys.stdout.write("[3J[2J[H")
        sys.stdout.flush()
    except Exception:
        pass
    console.clear()


def clear_screen(hard: bool = False) -> None:
    if hard:
        terminal_reset(full=True)
    else:
        console.clear()

# ===============================
#          CORE RUNNER
# ===============================
def run_region(
    model: str,
    cc: str,
    family: bool = False,
    force: bool = False,
    progress_callback: Optional[Callable[[str, int, int, str], None]] = None,
) -> List[dict]:
    model = norm_model(model)
    cc = norm_csc(cc)
    targets = get_family_targets(cc) if family else [cc]
    session_results: List[dict] = []

    for region in targets:
        console.rule(f"[bold {P['sky']}]Scanning {model} / {region}[/]", style=P["line"])
        start = time.time()
        existing = {} if force else load_existing_decrypted_map(model, region)
        if progress_callback:
            progress_callback("prepare", 0, 1, f"{model}/{region}")
        server_md5s = get_md5_list(model, region)
        if not server_md5s:
            console.print(Panel(f"No MD5 list for {region}", border_style=P["warn"], box=box.ROUNDED))
            continue
        latest_version, osver, base_cc = get_latest_with_fallback(model, region)
        md5_set = set(server_md5s)
        todo = md5_set if force else (md5_set - set(existing.keys()))
        subtitle = f"server={len(md5_set)}  new={len(todo)}  base={base_cc or region}"
        console.print(f"[{P['dim']}]" + subtitle + f"[/{P['dim']}]")
        if latest_version:
            console.print(f"[{P['mint']}]stable={latest_version}[/{P['mint']}]")

        found = (
            decrypt_firmware(
                model,
                region,
                todo,
                latest_version,
                full_brute=True,
                progress_callback=progress_callback,
            )
            if todo
            else {}
        )
        combined = dict(existing)
        combined.update(found)
        for info in combined.values():
            info["kind"] = info.get("kind") or classify_build(info["version"], latest_version)

        elapsed = time.time() - start
        paths = save_outputs(model, region, combined, server_md5s, base_cc, latest_version)
        sorted_items = sorted(combined.values(), key=lambda x: (x.get("year", 0), x.get("month", 0), x.get("version", "")), reverse=True)
        session_results.append({
            "model": model,
            "region": region,
            "latest_stable": latest_version,
            "android": osver,
            "base_csc": base_cc,
            "server_md5s": len(server_md5s),
            "resolved_count": len(combined),
            "unresolved_count": max(0, len(server_md5s) - len(combined)),
            "elapsed": elapsed,
            "items": sorted_items,
            "paths": paths,
        })
        if progress_callback:
            progress_callback("finalize", 1, 1, f"{model}/{region}")
    return session_results

# ===============================
#          INTERACTIVE
# ===============================
def ask_text(message: str, default: str = "") -> str:
    if HAVE_INQUIRER:
        return inquirer.text(message=message, default=default, style=STYLE).execute().strip()
    raw = input(f"{message} ").strip()
    return raw or default


def ask_confirm(message: str, default: bool = True) -> bool:
    if HAVE_INQUIRER:
        return inquirer.confirm(message=message, default=default, style=STYLE).execute()
    raw = input(f"{message} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
    if not raw:
        return default
    return raw in ("y", "yes", "1", "true")


def menu_select(message: str, choices: list):
    if HAVE_INQUIRER:
        iq_choices = []
        for item in choices:
            if isinstance(item, tuple):
                iq_choices.append(Choice(value=item[0], name=item[1]))
            else:
                iq_choices.append(Choice(value=item, name=str(item)))
        return inquirer.select(message=message, choices=iq_choices, style=STYLE, cycle=True).execute()
    for idx, item in enumerate(choices, 1):
        label = item[1] if isinstance(item, tuple) else str(item)
        print(f"{idx}. {label}")
    raw = input(f"{message} #: ").strip()
    try:
        i = int(raw) - 1
        item = choices[i]
        return item[0] if isinstance(item, tuple) else item
    except Exception:
        return None


def show_results_screen(results: List[dict]) -> None:
    if not results:
        clear_screen(hard=True)
        render_header()
        console.print(Panel("No results to show.", border_style=P["warn"], box=box.ROUNDED))
        wait_for_return()
        return

    for idx, result in enumerate(results, 1):
        clear_screen(hard=True)
        render_header()
        if len(results) > 1:
            console.print(Panel(f"[bold {P['sky']}]Result {idx} of {len(results)}[/]", border_style=P["line"], box=box.ROUNDED, padding=(0,1)))
        render_compact_summary(result)
        render_latest_builds(result["items"], limit=25)
        render_saved_files(result["paths"])
        wait_for_return()


def run_single_ui() -> None:

    clear_screen()
    render_header()
    model = norm_model(ask_text("Model", "SM-S938B"))
    csc = norm_csc(ask_text("CSC", "EUX"))
    family = ask_confirm(f"Scan family / sibling CSCs for {csc}?", True)
    force = ask_confirm("Full brute recheck even if JSON already has entries?", False)
    clear_screen()
    render_header()
    render_target_header(model, csc, get_family_targets(csc) if family else [csc])
    results = run_region(model, csc, family=family, force=force)
    show_results_screen(results)


def run_batch_ui() -> None:
    clear_screen()
    render_header()
    grouped = get_grouped_db_entries()
    if not grouped:
        console.print(f"[bold {P['err']}]No database entries found at {DB_PATH}[/]")
        wait_for_return()
        return
    prefixes = sorted(grouped.keys())
    if HAVE_INQUIRER:
        fam_choices = [Choice(value=p, name=f"{p} ({len(grouped[p])} models)") for p in prefixes]
        selected = inquirer.checkbox(message="Select device groups", choices=fam_choices, style=STYLE, instruction="Space to toggle").execute()
    else:
        selected = prefixes
    if not selected:
        return
    entries: List[dict] = []
    for p in selected:
        entries.extend(grouped[p])
    family = ask_confirm("Enable family scan for each item?", False)
    force = ask_confirm("Force full brute for each item?", False)
    all_results: List[dict] = []
    for item in entries:
        clear_screen(hard=True)
        render_header()
        render_target_header(item["model"], item["csc"], get_family_targets(item["csc"]) if family else [item["csc"]])
        all_results.extend(run_region(item["model"], item["csc"], family=family, force=force))
    show_results_screen(all_results)


def show_help() -> None:
    clear_screen()
    render_header()
    body = Group(
        Text("What this does", style=f"bold {P['sky']}"),
        Text("- pulls version.test.xml as authoritative MD5 source", style=P["text"]),
        Text("- brute-enumerates stable + E + Z/beta-style patterns", style=P["text"]),
        Text("- saves full results and unresolved MD5s", style=P["text"]),
        Text("", style=P["text"]),
        Text("Result screen", style=f"bold {P['lav']}"),
        Text("- shows up to last 25 builds on screen", style=P["text"]),
        Text("- keeps full triplets copyable on screen", style=P["text"]),
        Text("- full list remains in *_full.json", style=P["text"]),
    )
    console.print(Panel(body, border_style=P["line"], box=box.ROUNDED))
    wait_for_return()


def interactive_main() -> None:

    while True:
        clear_screen(hard=True)
        render_header()
        choices = [
            ("single", "Single Decrypt"),
            ("batch", "Batch Decrypt (Database)"),
            ("help", "About / Saved Files"),
            ("quit", "Quit"),
        ]
        op = menu_select("Menu", choices)
        if op == "single":
            run_single_ui()
        elif op == "batch":
            run_batch_ui()
        elif op == "help":
            show_help()
        else:
            break


def cli_main(argv: List[str]) -> int:
    # light compatibility so it can still be scripted when needed
    if len(argv) >= 3:
        model = argv[1]
        csc = argv[2]
        family = "--family" in argv
        force = "--force" in argv
        results = run_region(model, csc, family=family, force=force)
        show_results_screen(results)
        return 0
    interactive_main()
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(cli_main(sys.argv))
    except KeyboardInterrupt:
        console.print(f"\n[bold {P['warn']}]Stopped by user.[/]")
        raise SystemExit(130)
