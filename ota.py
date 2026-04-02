#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VALHALLA OTA FETCHER - OVERHAULED UI EDITION
"""

import os
import sys
import time
import random
import requests
import zipfile
import hashlib
import base64
import ssl
import urllib3
import re
import json
import xml.etree.ElementTree as ET
import string
import math
import subprocess
import glob
from urllib.parse import quote, urlparse
from datetime import datetime, timedelta

# Rich imports
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, DownloadColumn, TransferSpeedColumn
    from rich.columns import Columns
    from rich.live import Live
    from rich.align import Align
    from rich.text import Text
    from rich import box
    from rich.console import Group
except ImportError:
    print("Rich library not found. Please install it using: pip install rich")
    sys.exit(3)

console = Console()

# Cross-platform raw keyboard input handling
if os.name == 'nt':
    import msvcrt
else:
    import tty
    import termios
    import select

# --- GOOGLE DRIVE IMPORTS ---
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
except ImportError:
    print("\n[!] Error: Missing Google libraries.")
    print("Run: pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib requests")
    sys.exit(3)

# Disable Warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURATION ---
DOWNLOAD_DIR = "Downloads"
USER_AGENT = "SAMSUNG-Android"
DEVICES_FILE = "devices.json"
DEVICES_SOURCE_FILE = "devicesIMEI.json"
LIBRARY_FILE = "library.json"
LINKS_FILE = "saved_links.json"
CLOUD_LOGS_FILE = "cloud_logs.json"
IMEI_DB_DIR = "IMEI_Database"
DECRYPTED_DIR = "Decrypted"
SEARCH_DEPTH_DAYS = 30
SEARCH_DEPTH_MONTHS = 120
MAX_RESULTS = 50
SCOPES = ['https://www.googleapis.com/auth/drive.file']

TG_ENABLED = True
TG_TOKEN = "8508463124:AAEUj_DY2DzdfUbbnMBqL8lo9Gum6UHk8B4" # bot token
TG_CHAT_ID = "-5202902271"  # user id 

if not os.path.exists(DECRYPTED_DIR): os.makedirs(DECRYPTED_DIR)

# --- THEME CONFIGURATION ---
class Colors:
    RESET   = "\033[0m"
    BOLD    = "\033[1m"
    DIM     = "\033[2m"      
    RED     = "\033[31m"
    GREEN   = "\033[32m"
    YELLOW  = "\033[33m"
    BLUE    = "\033[34m"
    MAGENTA = "\033[35m"     
    CYAN    = "\033[36m"
    WHITE   = "\033[37m"

def clear_screen():
    console.clear()

def get_key():
    """Captures single key presses across OS platforms"""
    if os.name == 'nt':
        while True:
            key = msvcrt.getch()
            if key in (b'\x00', b'\xe0'): 
                key = msvcrt.getch()
                if key == b'H': return 'UP'
                if key == b'P': return 'DOWN'
            elif key in (b'\r', b'\n'):
                return 'ENTER'
            elif key == b'\x03':  
                raise KeyboardInterrupt
            else:
                try: return key.decode('utf-8').lower()
                except: pass
    else:
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
            if ch == '\x1b':  
                ch2 = sys.stdin.read(1)
                if ch2 == '[':
                    ch3 = sys.stdin.read(1)
                    if ch3 == 'A': return 'UP'
                    if ch3 == 'B': return 'DOWN'
            elif ch in ('\r', '\n'):
                return 'ENTER'
            elif ch == '\x03':  
                raise KeyboardInterrupt
            else:
                return ch.lower()
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        return None

def get_key_timeout(timeout=0.25):
    """Non-blocking key capture with a timeout to allow live screen updates."""
    if os.name == 'nt':
        start = time.time()
        while time.time() - start < timeout:
            if msvcrt.kbhit():
                return get_key()
            time.sleep(0.01)
        return None
    else:
        # Uses select to check if stdin has data waiting before we block on read
        r, _, _ = select.select([sys.stdin], [], [], timeout)
        if r:
            return get_key()
        return None

def loading_animation(target_name):
    """Cyberpunk transition loader using Rich"""
    clear_screen()
    
    console.print(Panel(
        Align.center(f"[bold white]ACCESSING DATABANKS: {target_name.upper()}[/bold white]"),
        style="blue",
        box=box.ROUNDED
    ))
    print("\n")
    
    with Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(style="blue", complete_style="cyan", pulse_style="white"),
        TextColumn("[cyan]{task.percentage:>3.0f}%[/cyan]"),
        console=console
    ) as progress:
        task = progress.add_task("[white]Establishing uplink...", total=100)
        for i in range(101):
            time.sleep(0.005)
            progress.advance(task)
            
    print("\n")
    time.sleep(0.2)

def readable_size(size_in_bytes):
    if size_in_bytes < 1024: return f"{size_in_bytes} B"
    elif size_in_bytes < 1024**2: return f"{size_in_bytes/1024:.2f} KB"
    elif size_in_bytes < 1024**3: return f"{size_in_bytes/(1024**2):.2f} MB"
    else: return f"{size_in_bytes/(1024**3):.2f} GB"

def load_json(filepath, default=[]):
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r') as f: return json.load(f)
        except json.JSONDecodeError: return default
    return default

def save_json(filepath, data):
    with open(filepath, 'w') as f: json.dump(data, f, indent=4)

# ==============================================================================
# TELEGRAM SERVICE (ADAPTED FROM LOG.PY)
# ==============================================================================

class TelegramService:
    SIGIL = (
        "+-------------+\n"
        "|   Y ᛟ ᚦ ᛋ   |\n"
        "|   V A L H   |\n"
        "|   A L L A   |\n"
        "+-------------+"
    )
    R = {"header": "ᚱ", "device": "ᛟ", "pack": "ᛒ", "guard": "ᛉ", "path": "ᛏ", "cycle": "ᛃ", "link": "ᚱ", "local": "ᚦ", "world": "ᛜ"}

    @staticmethod
    def _clean_html(text: str) -> str:
        return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    @staticmethod
    def send_message(text: str) -> bool:
        if not TG_ENABLED or not TG_TOKEN: return False
        try:
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data={"chat_id": TG_CHAT_ID, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
            return True
        except: return False

    @staticmethod
    def send_batch(file_list):
        if not TG_ENABLED or not TG_TOKEN or not file_list: return False
        MAX_LEN = 3900

        def _region_label(model: str) -> str:
            m = (model or "").upper()
            if m.endswith("U") or m.endswith("U1"): return "USA"
            if m.endswith("W"): return "CAN"
            if m.endswith("0"): return "CHINA/HK"
            if m.endswith("N"): return "KOR"
            return "GLOBAL"

        def _parse_versions_from_fname(fname: str):
            base = (fname or "").replace(".zip", "").replace(".DM", "")
            parts = base.split("_")
            if len(parts) >= 2:
                new_v = parts[-1]
                old_v = parts[-2]
                if len(old_v) > 10: old_v = "---"
                return old_v, new_v
            return "---", "LATEST"

        def build_block(item) -> str:
            model = TelegramService._clean_html(item.get("model", "UNKNOWN"))
            fname = TelegramService._clean_html(item.get("fname", "UNKNOWN"))
            size  = TelegramService._clean_html(item.get("size", "N/A"))
            link  = item.get("link")
            old_v, new_v = _parse_versions_from_fname(fname)
            region = _region_label(model)

            line1 = f"<b>{TelegramService.R['device']} {model}</b>\n"
            line2 = f"{TelegramService.R['world']} <i>REGION: {region}</i>\n"
            line3 = f"{TelegramService.R['pack']} <code>{size}</code>\n"
            line4 = f"{TelegramService.R['path']} <code>{old_v}</code> → <code>{new_v}</code>\n"
            line5 = f"{TelegramService.R['cycle']} <code>{fname}</code>\n"
            line6 = f"{TelegramService.R['link']} <a href=\"{TelegramService._clean_html(link)}\">GDRIVE LINK</a>\n" if link else f"{TelegramService.R['local']} LOCAL ONLY\n"
            return line1 + line2 + line3 + line4 + line5 + line6 + "\n"

        header = f"<pre>{TelegramService.SIGIL}</pre>\n<b>{TelegramService.R['header']} NEW BUILD DETECTED</b>\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        footer = f"{TelegramService.R['guard']} <i>Captured by ᚳᛁᚾᛋᚩᚳᚢ</i>"
        
        messages = []; current = header
        for item in file_list:
            blk = build_block(item)
            if len(current) + len(blk) + len(footer) + 10 > MAX_LEN:
                current += footer; messages.append(current); current = header
            current += blk
        current += footer; messages.append(current)
        for msg in messages: TelegramService.send_message(msg)
        return True # Added to confirm success for UI

# ==============================================================================
# LOCAL LOGGING UTILS
# ==============================================================================

def save_cloud_log(log_text, zip_name, model="UNKNOWN", link="", size="N/A"):
    logs = []
    if os.path.exists(CLOUD_LOGS_FILE):
        try:
            with open(CLOUD_LOGS_FILE, "r", encoding="utf-8") as f:
                logs = json.load(f)
        except: logs = []
    
    logs.append({
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "name": zip_name,
        "content": log_text,
        "model": model,  # Added for Resend feature
        "link": link,    # Added for Resend feature
        "size": size     # Added for Resend feature
    })
    
    with open(CLOUD_LOGS_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, indent=4)

def load_cloud_logs():
    if not os.path.exists(CLOUD_LOGS_FILE): return []
    try:
        with open(CLOUD_LOGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except: return []

# ==============================================================================
# PART 0: GOOGLE DRIVE ENGINE
# ==============================================================================

def get_drive_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print(f"  {Colors.DIM}>> Refreshing expired Google Drive token...{Colors.RESET}")
            creds.refresh(Request())
        else:
            if not os.path.exists('credentials.json'):
                # Upgraded error UI
                print(f"\n  {Colors.RED}>> CRITICAL AUTH ERROR: credentials.json missing from root directory.{Colors.RESET}")
                return None
                
            # Upgraded prompt for manual auth
            print(f"\n  {Colors.YELLOW}>> INITIATING LOCAL OAUTH SERVER: Manual authorization required.{Colors.RESET}")
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0, open_browser=False)
            
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            
        # Upgraded success UI
        print(f"  {Colors.GREEN}>> UPLINK ESTABLISHED: Google Drive authentication secured.{Colors.RESET}")
        
    return build('drive', 'v3', credentials=creds)

def upload_to_drive(file_path, file_name, model, display_size):
    # --- CHECK IF ALREADY UPLOADED ---
    existing_logs = load_cloud_logs()
    
    if any(log.get('name') == file_name for log in existing_logs):
        # Upgraded warning UI
        print(f"\n  {Colors.YELLOW}>> NOTICE: Build already exists in Cloud Library.{Colors.RESET}")
        reup = input(f"  {Colors.BLUE}>> Force re-deployment? (Y/N): {Colors.RESET}").lower()
        if reup != 'y':
            print(f"  {Colors.DIM}>> Deployment aborted by user.{Colors.RESET}")
            return None

    try:
        service = get_drive_service()
        if not service: return
        
        # Upgraded 96-Width Stat Box for Upload Start
        width = 96
        print(f"\n{Colors.BLUE}┌{'─' * width}┐{Colors.RESET}")
        print(f"{Colors.BLUE}│{Colors.WHITE}{Colors.BOLD}{f' UPLOADING DATABANK: {file_name} '.center(width)}{Colors.BLUE}│{Colors.RESET}")
        print(f"{Colors.BLUE}└{'─' * width}┘{Colors.RESET}\n")
        
        file_metadata = {'name': file_name}
        media = MediaFileUpload(file_path, resumable=True, chunksize=5*1024*1024)
        request = service.files().create(body=file_metadata, media_body=media, fields='id, webViewLink')
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                # Cyberpunk Progress Bar Implementation
                percent = status.progress() * 100
                bar_width = 60
                filled = int(bar_width * percent / 100)
                bar = '█' * filled + '░' * (bar_width - filled)
                sys.stdout.write(f"\r  {Colors.CYAN}[{Colors.WHITE}{percent:>5.1f}%{Colors.CYAN}]{Colors.RESET} {Colors.BLUE}{bar}{Colors.RESET}  UPLOADING")
                sys.stdout.flush()
        
        drive_link = response.get('webViewLink')
        
        # Upgraded success UI
        print(f"\n\n  {Colors.GREEN}>> UPLOAD COMPLETE: Secure link established.{Colors.RESET}")
        
        # --- TELEGRAM DEPLOYMENT ---
        print(f"  {Colors.DIM}>> Transmitting deployment notification via Telegram...{Colors.RESET}")
        
        item_data = {
            "model": model,
            "fname": file_name,
            "size": display_size,
            "link": drive_link
        }
        
        # Send to Telegram using the class from log.py
        tg_success = TelegramService.send_batch([item_data])
        
        if tg_success:
            print(f"  {Colors.GREEN}>> Telegram broadcast deployed successfully.{Colors.RESET}")
        else:
            print(f"  {Colors.RED}>> Warning: Telegram broadcast failed. Check network/token.{Colors.RESET}")
        
        # Save simple local log for history (Updated with metadata for Resend)
        save_cloud_log(f"Deployed: {file_name} | Link: {drive_link}", file_name, model=model, link=drive_link, size=display_size)
        
        return drive_link
        
    except Exception as e:
        # Upgraded error UI to match the Night City aesthetic
        print(f"\n  {Colors.RED}>> CRITICAL UPLOAD ERROR: Failed to sync with Google Drive.{Colors.RESET}")
        print(f"  {Colors.DIM}>> Details: {e}{Colors.RESET}")
        return None

# ==============================================================================
# PART 1: HTTP & SSL (Engine Preserved)
# ==============================================================================
class LegacySSLAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)
        kwargs['ssl_context'] = context
        return super().init_poolmanager(*args, **kwargs)

session = requests.Session()
session.mount("https://", LegacySSLAdapter())

# ==============================================================================
# PART 2: DATA MANAGEMENT (Engine Preserved)
# ==============================================================================

def load_json_devices():
    default_structure = {
        "SM-S93xB (Europe)": [
            {"name": "Galaxy S25 (Europe)", "model": "SM-S931B", "csc": "EUX", "imei": "352688830087983"},
            {"name": "Galaxy S25+ (Europe)", "model": "SM-S936B", "csc": "EUX", "imei": "351232090011358"},
            {"name": "Galaxy S25 Ultra (Europe)", "model": "SM-S938B", "csc": "EUX", "imei": "354222650031430"}
        ],
        "SM-S93xUx (USA)": [
            {"name": "Galaxy S25 (USA)", "model": "SM-S931U1", "csc": "XAA", "imei": "350135000012039"},
            {"name": "SM-S938U", "model": "SM-S938U", "csc": "ATT", "imei": "352512140059054"}
        ]
    }
    if not os.path.exists(DEVICES_FILE):
        with open(DEVICES_FILE, "w", encoding="utf-8") as f:
            json.dump(default_structure, f, indent=4)
        return default_structure
    try:
        with open(DEVICES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                new_data = {"Misc / Imported": data}
                save_json_devices(new_data)
                return new_data
            return data
    except: return default_structure

def save_json_devices(devices_dict):
    try:
        with open(DEVICES_FILE, "w", encoding="utf-8") as f:
            json.dump(devices_dict, f, indent=4)
        
        # Added a thematic success confirmation
        print(f"  {Colors.GREEN}>> DATABANKS SYNCED: {DEVICES_FILE} updated successfully.{Colors.RESET}")
        
    except Exception as e:
        # Upgraded error UI to match the Night City aesthetic
        print(f"\n  {Colors.RED}>> CRITICAL FILE SYSTEM ERROR: Could not write to {DEVICES_FILE}.{Colors.RESET}")
        print(f"  {Colors.DIM}>> Details: {e}{Colors.RESET}")

def get_model_imei_file(model):
    clean_model = "".join(x for x in model if x.isalnum() or x in ['-', '_'])
    return os.path.join(IMEI_DB_DIR, f"{clean_model}.json")

def load_imei_for_model(model):
    file_path = get_model_imei_file(model)
    if not os.path.exists(file_path): return None
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list): return data
            return []
    except: return []

def save_imei_for_model(model, data):
    if not os.path.exists(IMEI_DB_DIR): 
        os.makedirs(IMEI_DB_DIR)
        
    file_path = get_model_imei_file(model)
    
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
            
        # Added a thematic success confirmation for the specific model
        print(f"  {Colors.GREEN}>> IMEI DATABANKS SYNCED: {model} records updated successfully.{Colors.RESET}")
        
    except Exception as e:
        # Upgraded error UI to match the Night City aesthetic
        print(f"\n  {Colors.RED}>> CRITICAL FILE SYSTEM ERROR: Could not write IMEI data for {model}.{Colors.RESET}")
        print(f"  {Colors.DIM}>> Details: {e}{Colors.RESET}")

def load_library():
    if not os.path.exists(LIBRARY_FILE): return {}
    try:
        with open(LIBRARY_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except: return {}

def save_to_library(model, triplet):
    lib = load_library()
    if model not in lib: lib[model] = []
    if triplet not in lib[model]:
        lib[model].append(triplet)
        try:
            with open(LIBRARY_FILE, "w", encoding="utf-8") as f:
                json.dump(lib, f, indent=4)
        except Exception as e: pass

def load_saved_links():
    if not os.path.exists(LINKS_FILE): return {}
    try:
        with open(LINKS_FILE, "r", encoding="utf-8") as f: return json.load(f)
    except: return {}

def save_link_entry(data):
    links_db = load_saved_links()
    model = data.get('model')
    
    if not model: return
    
    if model not in links_db: 
        links_db[model] = []
        
    exists = any(entry.get('url') == data.get('url') for entry in links_db[model])
    
    if not exists:
        links_db[model].append(data)
        try:
            with open(LINKS_FILE, "w", encoding="utf-8") as f:
                json.dump(links_db, f, indent=4)
                
            # Upgraded success UI
            print(f"  {Colors.GREEN}>> DATABANKS SYNCED: Link saved to ON DESK library.{Colors.RESET}")
            
        except Exception as e:
            # Upgraded error UI to match the Night City aesthetic
            print(f"\n  {Colors.RED}>> CRITICAL FILE SYSTEM ERROR: Could not write link data for {model}.{Colors.RESET}")
            print(f"  {Colors.DIM}>> Details: {e}{Colors.RESET}")
    else:
        # Upgraded warning UI
        print(f"  {Colors.YELLOW}>> NOTICE: Link already exists in local databanks.{Colors.RESET}")

# --- DOWNLOAD LOGIC ---
def fetch_latest_version(model, csc):
    # Upgraded thematic startup message
    print(f"  {Colors.CYAN}>> INITIATING AUTO-FETCH: Querying regional databanks for {model} [{csc}]...{Colors.RESET}")
    candidate_urls = []
    if csc in ["CHC", "CHM", "CTC"]:
        # Chinese region specific endpoint
        candidate_urls.append(f"https://cn-fota-cloud-dn.ospserver.net/firmware/{csc}/{model}/version.xml")
    candidate_urls.append(f"https://fota-cloud-dn.ospserver.net/firmware/{csc}/{model}/version.xml")
    for url in candidate_urls:
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10, verify=False)
            if r.status_code == 200:
                root = ET.fromstring(r.content)
                latest = root.find(".//latest")
                if latest is not None and latest.text: 
                    print(f"  {Colors.GREEN}>> DATABANK HIT: Remote latest version data acquired.{Colors.RESET}")
                    return latest.text.strip()
                ver = root.find(".//version")
                if ver is not None and ver.text: 
                    print(f"  {Colors.GREEN}>> DATABANK HIT: Remote version data acquired.{Colors.RESET}")
                    return ver.text.strip()
        except: 
            continue
            
    # Thematic warning if both URLs fail or return nothing
    print(f"  {Colors.YELLOW}>> AUTO-FETCH FAILED: No baseline version found on OSPServers.{Colors.RESET}")
    return None

def readable_size(size):
    try:
        s = int(size)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if s < 1024: return f"{s:.2f} {unit}"
            s /= 1024
        return f"{s:.2f} PB"
    except: return "0 B"

def verify_zip(path):
    # Upgraded thematic startup message
    print(f"\n  {Colors.YELLOW}>> INITIATING INTEGRITY SCAN: {os.path.basename(path)}...{Colors.RESET}")
    
    try:
        with zipfile.ZipFile(path) as z:
            if z.testzip() is None:
                # Upgraded success UI
                print(f"  {Colors.GREEN}>> SCAN COMPLETE: Databank integrity verified.{Colors.RESET}")
                return True
                
    except Exception as e:
        # Upgraded error UI to match the Night City aesthetic
        print(f"  {Colors.RED}>> CRITICAL CORRUPTION DETECTED: Archive integrity compromised.{Colors.RESET}")
        print(f"  {Colors.DIM}>> Details: {e}{Colors.RESET}")
        
    return False

def download_file(item):
    def get_short_ver(v_str):
        if not v_str: return "00000"
        clean = v_str.split('/')[0] if "/" in v_str else v_str
        return clean[-5:] if len(clean) >= 5 else clean

    src_ver = get_short_ver(item.get('base', ''))
    tgt_ver = get_short_ver(item.get('ver', ''))
    real_csc = item['csc']
    try:
        if "/" in item['ver']:
            middle = item['ver'].split('/')[1]
            for grp in ["OXM", "OYN", "OYM", "OWO", "OXE", "OJM", "OLM", "IND"]:
                if grp in middle: real_csc = grp; break
    except: pass

    total_size = int(r.headers.get('content-length', 0))
    if total_size == 0:
        console.print(f"\n[bold yellow]  >> WARNING: File size unknown.[/bold yellow]")
    fname = f"{item['model']}_{real_csc}_{src_ver}_{tgt_ver}.zip"
    url = item['url'].replace("&amp;", "&") + "&px-nb=Xero&px-rmtime=Xero"
    if ".DM" in url.upper(): fname = fname.replace(".zip", ".DM.zip")
    
    path = os.path.join(DOWNLOAD_DIR, item['model'], fname)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    
    display_size = readable_size(item.get('size', 0))

    if os.path.exists(path):
        console.print(f"\n[bold yellow]  [!] File already exists locally : {fname}[/bold yellow]")
        choice = console.input(f"[bold magenta]  >> Upload to Drive anyway? (Y/N): [/bold magenta]").lower()
        if choice == 'y': upload_to_drive(path, fname, item['model'], display_size)
        return path

    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
    console.print(Panel(
        Align.center(f"[bold white]DOWNLOADING: {fname}[/bold white]"),
        style="blue",
        box=box.ROUNDED
    ))
    try:
        headers = {"User-Agent": USER_AGENT}
        with requests.get(url, headers=headers, stream=True, verify=False) as r:
            r.raise_for_status()
            total = int(r.headers.get('content-length', 0))
            with Progress(
                SpinnerColumn(style="cyan"),
                TextColumn("[bold cyan]{task.description}"),
                BarColumn(style="magenta", complete_style="cyan", pulse_style="white"),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeElapsedColumn(),
                console=console
            ) as progress:
                task = progress.add_task("Transmitting...", total=total)
                
                with open(path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            progress.update(task, advance=len(chunk))

        console.print(f"\n[bold green]  [+] Download Complete.[/bold green]")
        
        if verify_zip(path):
            choice = console.input(f"[bold magenta]  >> Upload to Google Drive? (Y/N): [/bold magenta]").lower()
            if choice == 'y':
                upload_to_drive(path, fname, item['model'], display_size)
        
        return path
                
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        return None

# ==============================================================================
# PART 3: CRYPTO (FV1 ENGINE) - STRICTLY PRESERVED
# ==============================================================================
class DesCrypt:
    MAX_CRYPT_BITS_SIZE = 64
    FP = [40, 8, 48, 16, 56, 24, 64, 32, 39, 7, 47, 15, 55, 23, 63, 31, 38, 6, 46, 14, 54, 22, 62, 30, 37, 5, 45, 13, 53, 21, 61, 29, 36, 4, 44, 12, 52, 20, 60, 28, 35, 3, 43, 11, 51, 19, 59, 27, 34, 2, 42, 10, 50, 18, 58, 26, 33, 1, 41, 9, 49, 17, 57, 25]
    IP = [58, 50, 42, 34, 26, 18, 10, 2, 60, 52, 44, 36, 28, 20, 12, 4, 62, 54, 46, 38, 30, 22, 14, 6, 64, 56, 48, 40, 32, 24, 16, 8, 57, 49, 41, 33, 25, 17, 9, 1, 59, 51, 43, 35, 27, 19, 11, 3, 61, 53, 45, 37, 29, 21, 13, 5, 63, 55, 47, 39, 31, 23, 15, 7]
    P = [16, 7, 20, 21, 29, 12, 28, 17, 1, 15, 23, 26, 5, 18, 31, 10, 2, 8, 24, 14, 32, 27, 3, 9, 19, 13, 30, 6, 22, 11, 4, 25]
    PC1_C = [57, 49, 41, 33, 25, 17, 9, 1, 58, 50, 42, 34, 26, 18, 10, 2, 59, 51, 43, 35, 27, 19, 11, 3, 60, 52, 44, 36]
    PC1_D = [63, 55, 47, 39, 31, 23, 15, 7, 62, 54, 46, 38, 30, 22, 14, 6, 61, 53, 45, 37, 29, 21, 13, 5, 28, 20, 12, 4]
    PC2_C = [14, 17, 11, 24, 1, 5, 3, 28, 15, 6, 21, 10, 23, 19, 12, 4, 26, 8, 16, 7, 27, 20, 13, 2]
    PC2_D = [41, 52, 31, 37, 47, 55, 30, 40, 51, 45, 33, 48, 44, 49, 39, 56, 34, 53, 46, 42, 50, 36, 29, 32]
    S = [[14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7, 0, 15, 7, 4, 14, 2, 13, 1, 10, 6, 12, 11, 9, 5, 3, 8, 4, 1, 14, 8, 13, 6, 2, 11, 15, 12, 9, 7, 3, 10, 5, 0, 15, 12, 8, 2, 4, 9, 1, 7, 5, 11, 3, 14, 10, 0, 6, 13], [15, 1, 8, 14, 6, 11, 3, 4, 9, 7, 2, 13, 12, 0, 5, 10, 3, 13, 4, 7, 15, 2, 8, 14, 12, 0, 1, 10, 6, 9, 11, 5, 0, 14, 7, 11, 10, 4, 13, 1, 5, 8, 12, 6, 9, 3, 2, 15, 13, 8, 10, 1, 3, 15, 4, 2, 11, 6, 7, 12, 0, 5, 14, 9], [10, 0, 9, 14, 6, 3, 15, 5, 1, 13, 12, 7, 11, 4, 2, 8, 13, 7, 0, 9, 3, 4, 6, 10, 2, 8, 5, 14, 12, 11, 15, 1, 13, 6, 4, 9, 8, 15, 3, 0, 11, 1, 2, 12, 5, 10, 14, 7, 1, 10, 13, 0, 6, 9, 8, 7, 4, 15, 14, 3, 11, 5, 2, 12], [7, 13, 14, 3, 0, 6, 9, 10, 1, 2, 8, 5, 11, 12, 4, 15, 13, 8, 11, 5, 6, 15, 0, 3, 4, 7, 2, 12, 1, 10, 14, 9, 10, 6, 9, 0, 12, 11, 7, 13, 15, 1, 3, 14, 5, 2, 8, 4, 3, 15, 0, 6, 10, 1, 13, 8, 9, 4, 5, 11, 12, 7, 2, 14], [2, 12, 4, 1, 7, 10, 11, 6, 8, 5, 3, 15, 13, 0, 14, 9, 14, 11, 2, 12, 4, 7, 13, 1, 5, 0, 15, 10, 3, 9, 8, 6, 4, 2, 1, 11, 10, 13, 7, 8, 15, 9, 12, 5, 6, 3, 0, 14, 11, 8, 12, 7, 1, 14, 2, 13, 6, 15, 0, 9, 10, 4, 5, 3], [12, 1, 10, 15, 9, 2, 6, 8, 0, 13, 3, 4, 14, 7, 5, 11, 10, 15, 4, 2, 7, 12, 9, 5, 6, 1, 13, 14, 0, 11, 3, 8, 9, 14, 15, 5, 2, 8, 12, 3, 7, 0, 4, 10, 1, 13, 11, 6, 4, 3, 2, 12, 9, 5, 15, 10, 11, 14, 1, 7, 6, 0, 8, 13], [4, 11, 2, 14, 15, 0, 8, 13, 3, 12, 9, 7, 5, 10, 6, 1, 13, 0, 11, 7, 4, 9, 1, 10, 14, 3, 5, 12, 2, 15, 8, 6, 1, 4, 11, 13, 12, 3, 7, 14, 10, 15, 6, 8, 0, 5, 9, 2, 6, 11, 13, 8, 1, 4, 10, 7, 9, 5, 0, 15, 14, 2, 3, 12], [13, 2, 8, 4, 6, 15, 11, 1, 10, 9, 3, 14, 5, 0, 12, 7, 1, 15, 13, 8, 10, 3, 7, 4, 12, 5, 6, 11, 0, 14, 9, 2, 7, 11, 4, 1, 9, 12, 14, 2, 0, 6, 10, 13, 15, 3, 5, 8, 2, 1, 14, 7, 4, 10, 8, 13, 15, 12, 9, 0, 3, 5, 6, 11]]
    E2 = [32, 1, 2, 3, 4, 5, 4, 5, 6, 7, 8, 9, 8, 9, 10, 11, 12, 13, 12, 13, 14, 15, 16, 17, 16, 17, 18, 19, 20, 21, 20, 21, 22, 23, 24, 25, 24, 25, 26, 27, 28, 29, 28, 29, 30, 31, 32, 1]
    SHIFTS = [1, 1, 2, 2, 2, 2, 2, 2, 1, 2, 2, 2, 2, 2, 2, 1]
    def __init__(self): self._c = [0]*28; self._d = [0]*28; self._ks = [[0]*48 for _ in range(16)]; self._e = [0]*48; self._pre_s = [0]*48; self._crypt_crypt_byte = [0]*16
    def _init_password(self, b_arr, b_arr2):
        i = 0; i2 = 0
        while i < len(b_arr) and b_arr[i] != 0 and i2 < self.MAX_CRYPT_BITS_SIZE:
            for i3 in range(6, -1, -1): b_arr2[i2] = (b_arr[i] >> i3) & 1; i2 += 1
            i += 1; b_arr2[i2] = 0; i2 += 1
        while i2 < self.MAX_CRYPT_BITS_SIZE + 2: b_arr2[i2] = 0; i2 += 1
        return b_arr2
    def _zero_password(self, b_arr):
        for i in range(self.MAX_CRYPT_BITS_SIZE + 2): b_arr[i] = 0
        return b_arr
    def _set_key(self, b_arr):
        for i in range(28): self._c[i] = b_arr[self.PC1_C[i] - 1]; self._d[i] = b_arr[self.PC1_D[i] - 1]
        for i2 in range(16):
            for _ in range(self.SHIFTS[i2]):
                b = self._c[0]; 
                for i4 in range(27): self._c[i4] = self._c[i4 + 1]
                self._c[27] = b; b2 = self._d[0]
                for i6 in range(27): self._d[i6] = self._d[i6 + 1]
                self._d[27] = b2
            for i8 in range(24): self._ks[i2][i8] = self._c[self.PC2_C[i8] - 1]; self._ks[i2][i8 + 24] = self._d[self.PC2_D[i8] - 28 - 1]
        for i9 in range(48): self._e[i9] = self.E2[i9]
    def _e_expandsion(self, b_arr):
        i = 0; i2 = 0
        while i < 2:
            i3 = i2 + 1; b = b_arr[i2]; self._crypt_crypt_byte[i] = b; b2 = b - 59 if b > 90 else (b - 53 if b > 57 else b - 46)
            for i4 in range(6):
                if ((b2 >> i4) & 1) != 0: i5 = (i * 6) + i4; b3 = self._e[i5]; i6 = i5 + 24; self._e[i5] = self._e[i6]; self._e[i6] = b3
            i += 1; i2 = i3
    def _des_encrypt(self, b_arr):
        b_arr2 = [0]*32; b_arr3 = [0]*32; b_arr4 = [0]*32; b_arr5 = [0]*32; i = 0
        while i < 32: b_arr2[i] = b_arr[self.IP[i] - 1]; i += 1
        while i < 64: b_arr3[i - 32] = b_arr[self.IP[i] - 1]; i += 1
        for i2 in range(16):
            for i3 in range(32): b_arr4[i3] = b_arr3[i3]
            for i4 in range(48): self._pre_s[i4] = b_arr3[self._e[i4] - 1] ^ self._ks[i2][i4]
            for i5 in range(8):
                b = i5 * 6
                b2 = self.S[i5][(self._pre_s[b] << 5) + (self._pre_s[b + 1] << 3) + (self._pre_s[b + 2] << 2) + (self._pre_s[b + 3] << 1) + self._pre_s[b + 4] + (self._pre_s[b + 5] << 4)]
                b3 = i5 * 4; b_arr5[b3] = (b2 >> 3) & 1; b_arr5[b3 + 1] = (b2 >> 2) & 1; b_arr5[b3 + 2] = (b2 >> 1) & 1; b_arr5[b3 + 3] = b2 & 1
            for i6 in range(32): b_arr3[i6] = b_arr2[i6] ^ b_arr5[self.P[i6] - 1]
            for i7 in range(32): b_arr2[i7] = b_arr4[i7]
        for i8 in range(32): b4 = b_arr2[i8]; b_arr2[i8] = b_arr3[i8]; b_arr3[i8] = b4
        for i9 in range(64):
            if self.FP[i9] < 33: b_arr[i9] = b_arr2[self.FP[i9] - 1]
            else: b_arr[i9] = b_arr3[self.FP[i9] - 33]
        return b_arr
    def _encrypt(self, b_arr):
        for _ in range(25): b_arr = self._des_encrypt(b_arr)
        i2 = 0
        while i2 < 11:
            b = 0; 
            for i3 in range(6): b = (b << 1) | b_arr[(i2 * 6) + i3]
            b2 = b + 46; 
            if b2 > 57: b2 += 7
            if b2 > 90: b2 += 6
            self._crypt_crypt_byte[i2 + 2] = b2; i2 += 1
        self._crypt_crypt_byte[i2 + 2] = 0
        if self._crypt_crypt_byte[1] == 0: self._crypt_crypt_byte[1] = self._crypt_crypt_byte[0]
    def generate(self, s, b_arr):
        init_pwd = self._init_password(s.encode('utf-8'), [0] * (self.MAX_CRYPT_BITS_SIZE + 2))
        if init_pwd: self._set_key(init_pwd); zero_pwd = self._zero_password(init_pwd); self._e_expandsion(b_arr); self._encrypt(zero_pwd)
        result_bytes = bytes(self._crypt_crypt_byte)
        try: null_index = result_bytes.index(0); return result_bytes[:null_index].decode('utf-8')
        except: return result_bytes.decode('utf-8', errors='ignore').strip('\x00')

DICT = [1, 15, 5, 11, 19, 28, 23, 47, 35, 44, 2, 14, 6, 10, 18, 13, 22, 26, 32, 47, 3, 13, 7, 9, 17, 30, 21, 25, 33, 45, 4, 12, 8, 63, 16, 31, 20, 24, 34, 46]
HEX_TABLE = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']

def _adp_encode_hex(b_arr):
    c_arr = [''] * (len(b_arr) * 2); i = 0
    for b in b_arr:
        i2 = i + 1; c_arr[i] = HEX_TABLE[b & 15]; c_arr[i2] = HEX_TABLE[(b >> 4) & 15]; i = i2 + 1
    return c_arr

def _adp_shuffle(var0):
    var1 = len(var0); var2 = var1 % 2; var3 = var1 // 2
    if var2 != 0: var3 += 1
    char_list = list(var0)
    while var3 < var1:
        var4 = char_list.pop(var3); var5 = var1 - var3
        if var2 == 0: var5 -= 1
        char_list.insert(var5, var4); var3 += 1
    return "".join(char_list)

def generate_client_password(str_val, str2):
    try:
        if ':' not in str_val: return None
        substring = str_val.split(':', 1)[1]
        if not substring: return None
        c_arr = [char for char in substring if char.isalnum()]
        i = len(c_arr); j = 0; j2 = 0
        for i3 in range(i - 1):
            j3 = ord(c_arr[i3]); b_arr = DICT; j += j3 * b_arr[i3]; j2 += ord(c_arr[i3]) * ord(c_arr[(i - i3) - 1]) * b_arr[i3]
        dev_pwd_key = f"{j}{j2}"
        if not dev_pwd_key: return None
        data_to_hash = (str2 + dev_pwd_key + str_val).encode('utf-8')
        md5_hash = hashlib.md5(data_to_hash).digest()
        hex_chars = _adp_encode_hex(md5_hash)
        str_val_bytes = str_val.encode('utf-8')
        salt_bytes = bytes([str_val_bytes[len(str_val_bytes) - 2], str_val_bytes[len(str_val_bytes) - 1]])
        descrypt = DesCrypt(); des_part = descrypt.generate(str_val, salt_bytes)
        concat = "".join([hex_chars[1], hex_chars[4], hex_chars[5], hex_chars[7]]) + des_part
        string_buffer = concat
        for _ in range(3): string_buffer = _adp_shuffle(string_buffer)
        return string_buffer
    except: return ""

TOKENS_SYNCML = {'SyncML': b'\x6d', 'SyncHdr': b'\x6c', 'SyncBody': b'\x6b', 'VerDTD': b'\x71', 'VerProto': b'\x72', 'SessionID': b'\x65', 'MsgID': b'\x5b', 'Target': b'\x6e', 'Source': b'\x67', 'LocURI': b'\x57', 'LocName': b'\x56', 'Cred': b'\x4e', 'Meta': b'\x5a', 'Data': b'\x4f', 'Alert': b'\x46', 'CmdID': b'\x4b', 'Item': b'\x54', 'Status': b'\x69', 'Results': b'\x62', 'Cmd': b'\x4a', 'CmdRef': b'\x4c', 'MsgRef': b'\x5c', 'TargetRef': b'\x6f', 'SourceRef': b'\x68', 'Final': b'\x12', 'Replace': b'\x60'}
TOKENS_METINF = {'Format': b'\x47', 'Type': b'\x53', 'MaxMsgSize': b'\x4c', 'MaxObjSize': b'\x55', 'Size': b'\x52'}
SWITCH_PAGE = b'\x00'; END = b'\x01'; STR_I = b'\x03'; CP_SYNCML = 0; CP_METINF = 1

class SyncML:
    def __init__(self):
        self.next_cmd_id = 1; self.wbxml = bytearray(); self._write_header()
        self._start_element(TOKENS_SYNCML['SyncML']); self._start_header("1.2", "DM/1.2")
        self.body_started = False
    def _write_header(self):
        public_id = b'-//SYNCML//DTD SyncML 1.2//EN'; self.wbxml.extend(b'\x02\x00\x00j')
        self.wbxml.append(len(public_id)); self.wbxml.extend(public_id)
    def _switch_page(self, page): self.wbxml.extend(SWITCH_PAGE); self.wbxml.append(page)
    def _start_element(self, token): self.wbxml.extend(token)
    def _end_element(self): self.wbxml.extend(END)
    def _add_leaf(self, token, text=None):
        self.wbxml.extend(token)
        if text is not None: self.wbxml.extend(STR_I); self.wbxml.extend(text.encode('utf-8')); self.wbxml.append(0)
        self.wbxml.extend(END)
    def _start_header(self, ver_dtd, ver_proto):
        self._start_element(TOKENS_SYNCML['SyncHdr']); self._add_leaf(TOKENS_SYNCML['VerDTD'], ver_dtd); self._add_leaf(TOKENS_SYNCML['VerProto'], ver_proto)
    def add_header(self, session_id, msg_id, target_uri, source_uri, cred_data, max_msg_size=5120, max_obj_size=1048576):
        self._add_leaf(TOKENS_SYNCML['SessionID'], session_id); self._add_leaf(TOKENS_SYNCML['MsgID'], str(msg_id))
        self._start_element(TOKENS_SYNCML['Target']); self._add_leaf(TOKENS_SYNCML['LocURI'], target_uri); self._end_element()
        self._start_element(TOKENS_SYNCML['Source']); self._add_leaf(TOKENS_SYNCML['LocURI'], source_uri); self._add_leaf(TOKENS_SYNCML['LocName'], source_uri); self._end_element()
        if cred_data:
            self._start_element(TOKENS_SYNCML['Cred']); self._start_element(TOKENS_SYNCML['Meta']); self._switch_page(CP_METINF)
            self._add_leaf(TOKENS_METINF['Format'], "b64"); self._add_leaf(TOKENS_METINF['Type'], "syncml:auth-md5"); self._switch_page(CP_SYNCML); self._end_element()
            self._add_leaf(TOKENS_SYNCML['Data'], cred_data); self._end_element()
        self._start_element(TOKENS_SYNCML['Meta']); self._switch_page(CP_METINF); self._add_leaf(TOKENS_METINF['MaxMsgSize'], str(max_msg_size)); self._add_leaf(TOKENS_METINF['MaxObjSize'], str(max_obj_size)); self._switch_page(CP_SYNCML); self._end_element()
        self._end_element(); self._start_element(TOKENS_SYNCML['SyncBody']); self.body_started = True
    def add_alert(self, data, item_uri=None, item_data=None, item_type=None, item_format="chr"):
        self._start_element(TOKENS_SYNCML['Alert']); self._add_leaf(TOKENS_SYNCML['CmdID'], str(self.next_cmd_id)); self._add_leaf(TOKENS_SYNCML['Data'], data)
        if item_uri:
            self._start_element(TOKENS_SYNCML['Item']); self._start_element(TOKENS_SYNCML['Source']); self._add_leaf(TOKENS_SYNCML['LocURI'], item_uri); self._end_element()
            if item_type: self._start_element(TOKENS_SYNCML['Meta']); self._switch_page(CP_METINF); self._add_leaf(TOKENS_METINF['Format'], item_format); self._add_leaf(TOKENS_METINF['Type'], item_type); self._switch_page(CP_SYNCML); self._end_element()
            if item_data is not None: self._add_leaf(TOKENS_SYNCML['Data'], item_data)
            self._end_element()
        self._end_element(); self.next_cmd_id += 1
    def add_replace(self, items):
        self._start_element(TOKENS_SYNCML['Replace']); self._add_leaf(TOKENS_SYNCML['CmdID'], str(self.next_cmd_id))
        for uri, data in items.items():
            self._start_element(TOKENS_SYNCML['Item']); self._start_element(TOKENS_SYNCML['Source']); self._add_leaf(TOKENS_SYNCML['LocURI'], uri); self._end_element(); self._add_leaf(TOKENS_SYNCML['Data'], data); self._end_element()
        self._end_element(); self.next_cmd_id += 1
    def add_status(self, data, msg_ref=None, cmd_ref=None, cmd=None, target_ref=None, source_ref=None):
        self._start_element(TOKENS_SYNCML['Status']); self._add_leaf(TOKENS_SYNCML['CmdID'], str(self.next_cmd_id)); self._add_leaf(TOKENS_SYNCML['Data'], data)
        if msg_ref is not None: self._add_leaf(TOKENS_SYNCML['MsgRef'], str(msg_ref))
        if cmd_ref is not None: self._add_leaf(TOKENS_SYNCML['CmdRef'], str(cmd_ref))
        if cmd: self._add_leaf(TOKENS_SYNCML['Cmd'], cmd)
        if target_ref: self._add_leaf(TOKENS_SYNCML['TargetRef'], target_ref)
        if source_ref: self._add_leaf(TOKENS_SYNCML['SourceRef'], source_ref)
        self._end_element(); self.next_cmd_id += 1
    def add_results(self, loc_uri, data, msg_ref=None, cmd_ref=None, data_type="text/plain", data_format="chr"):
        self._start_element(TOKENS_SYNCML['Results']); self._add_leaf(TOKENS_SYNCML['CmdID'], str(self.next_cmd_id))
        self._start_element(TOKENS_SYNCML['Item']); self._start_element(TOKENS_SYNCML['Source']); self._add_leaf(TOKENS_SYNCML['LocURI'], loc_uri); self._end_element()
        self._start_element(TOKENS_SYNCML['Meta']); self._switch_page(CP_METINF); self._add_leaf(TOKENS_METINF['Format'], data_format); self._add_leaf(TOKENS_METINF['Type'], data_type); self._add_leaf(TOKENS_METINF['Size'], str(len(data))); self._switch_page(CP_SYNCML); self._end_element()
        self._add_leaf(TOKENS_SYNCML['Data'], data); self._end_element()
        if msg_ref is not None: self._add_leaf(TOKENS_SYNCML['MsgRef'], str(msg_ref))
        if cmd_ref is not None: self._add_leaf(TOKENS_SYNCML['CmdRef'], str(cmd_ref))
        self._end_element(); self.next_cmd_id += 1
    def get(self): self._add_leaf(TOKENS_SYNCML['Final']); self._end_element(); return bytes(self.wbxml)

def send_wbxml(url: str, wbxml: bytes, device_model: str) -> bytes:
    headers = {"User-Agent": f"Samsung {device_model} SyncML_DM Client", "Accept": "application/vnd.syncml.dm+wbxml", "Content-Type": "application/vnd.syncml.dm+wbxml"}
    response = session.post(url, data=wbxml, headers=headers)
    if not response.ok: raise requests.HTTPError(f"HTTP {response.status_code}: {response.reason}", response=response)
    return response.content

def parse_descripter(url, sourceFwV):
    try:
        response = session.get(url); response.raise_for_status(); xml_data = response.text
        root = ET.fromstring(xml_data)
        objectURI = root.findtext(".//{http://www.openmobilealliance.org/xmlns/dd}objectURI")
        installParam = root.findtext(".//{http://www.openmobilealliance.org/xmlns/dd}installParam")
        param_dict = dict(item.split("=", 1) for item in installParam.split(";") if "=" in item)
        size = 0
        try:
            ns = {'dd': 'http://www.openmobilealliance.org/xmlns/dd'}
            size_elem = root.find(".//dd:size", namespaces=ns)
            if size_elem is not None and size_elem.text: size = int(size_elem.text)
        except: pass
        if size == 0:
            match = re.search(r'<size>(\d+)</size>', response.text)
            if match: size = int(match.group(1))
        
        os_ver = param_dict.get("updateFwOsv", "")
        if os_ver: os_ver = os_ver.replace("B(", "").replace(")", "")

        return {"sourceFwV": sourceFwV, "updateFwV": param_dict.get("updateFwV"), "size": size, "url": objectURI.split('&px-wid')[0], "oneUiVersion": param_dict.get("updateOneUiVersion"), "os_ver": os_ver, "security": param_dict.get("securityPatchVersion")}
    except Exception as e: return None

class Client:
    def __init__(self, data: dict):
        self.Model = data.get("Model", ""); self.DeviceId = data.get("DeviceId", ""); self.CustomerCode = data.get("CustomerCode", "")
        self.SerialNumber = data.get("SerialNumber", ""); self.FirmwareVersion = data.get("FirmwareVersion", "")
        self.Mcc = data.get("Mcc", "001"); self.Mnc = data.get("Mnc", "01"); self.FotaClientVersion = data.get("FotaClientVersion", "4.4.14")
        self.Registered = data.get("Registered", False)
        now = datetime.fromtimestamp(time.time()); self.ssid = format(now.minute, 'X') + format(now.second, 'X')
        self.nonce = b''; self.CurrentMessageId = 1
        self.generate_password()
    def compute_md5_auth(self):
        if not self.nonce or self.CurrentMessageId == 1: self.nonce = base64.b64decode(base64.b64encode((str(random.randint(0, 2**31 - 1)) + "SSNextNonce").encode('utf-8')).decode('utf-8'))
        concat_str = f"{self.DeviceId}:{self.ClientPassword}"; concat2_str = f"{base64.b64encode(hashlib.md5(concat_str.encode('utf-8')).digest()).decode('utf-8')}:"
        combined_b_arr = concat2_str.encode('utf-8') + self.nonce
        return base64.b64encode(hashlib.md5(combined_b_arr).digest()).decode('utf-8')
    def set_server_nonce(self, nonce_b64): self.nonce = base64.b64decode(nonce_b64); self.CurrentMessageId += 1
    def generate_password(self): self.ClientPassword = generate_client_password(self.DeviceId, "x6g1q14r75")
    def build_device_request(self, url):
        b = SyncML()
        b.add_header(self.ssid, self.CurrentMessageId, url, self.DeviceId, self.compute_md5_auth())
        b.add_alert("1201")
        b.add_replace({
            "./DevInfo/DevId": self.DeviceId, "./DevInfo/Man": "Samsung", "./DevInfo/Mod": self.Model, "./DevInfo/DmV": "1.2", "./DevInfo/Lang": "en-US", 
            "./DevInfo/Ext/DevNetworkConnType": "WIFI", 
            "./DevInfo/Ext/TelephonyMcc": self.Mcc, "./DevInfo/Ext/TelephonyMnc": self.Mnc, 
            "./DevInfo/Ext/OmcCode": self.CustomerCode, "./DevInfo/Ext/FotaClientVer": self.FotaClientVersion, 
            "./DevInfo/Ext/DMClientVer": self.FotaClientVersion, "./DevInfo/Ext/ModemZeroBilling": "1", 
            "./DevInfo/Ext/SIMCardMcc": self.Mcc, "./DevInfo/Ext/SIMCardMnc": self.Mnc, 
            "./DevInfo/Ext/AidCode": self.CustomerCode, "./DevInfo/Ext/CountryISOCode": "sk"
        })
        b.add_alert("1226", "./FUMO/DownloadAndUpdate", "0", "org.openmobilealliance.dm.firmwareupdate.devicerequest")
        return b.get()
    def build_update_request(self, url, fwv):
        b = SyncML(); 
        b.add_header(self.ssid, self.CurrentMessageId, url, self.DeviceId, "")
        ref = self.CurrentMessageId - 1
        b.add_status("212", ref, 0, "SyncHdr", self.DeviceId, url.split('?')[0])
        b.add_status("200", ref, 5, "Get", "./DevDetail/FwV"); b.add_results("./DevDetail/FwV", fwv, ref, 5)
        b.add_status("200", ref, 6, "Get", "./DevInfo/Ext/DevNetworkConnType"); b.add_results("./DevInfo/Ext/DevNetworkConnType", "WIFI", ref, 6)
        return b.get()
    def do_auth(self):
        url = "https://dms.ospserver.net/v1/device/magicsync/mdm"
        for _ in range(5):
            resp = send_wbxml(url, self.build_device_request(url), self.Model)
            ns = b'SyncHdr\x00'; ni = resp.find(ns)
            if ni != -1:
                ni += len(ns); nei = resp.find(b'\x00', ni); sn = resp[ni:nei].decode('utf-8')
                cs = nei + 1; ce = resp.find(b'\x00', cs); sc = resp[cs:ce].decode('utf-8')
            else: sn = None; sc = None
            end = b'\x00b64'; ei = resp.find(end)
            if ei != -1:
                si = resp.rfind(b'\x00', 0, ei - 1)
                r_url = resp[si+1:ei].decode('utf-8') if si != -1 else None
            else: r_url = None
            url = r_url
            if sn == "425": return "auth_failed_banned"
            elif sn and sn != "401":
                self.set_server_nonce(sn)
                if sc != "401": return url
        return "auth_failed"
    def check_update(self, fvw):
        url = self.do_auth()
        if not url or "http" not in url: return f"Error: {url}"
        resp = send_wbxml(url, self.build_update_request(url, fvw), self.Model)
        if b'DevInfo/Ext/DeviceRegistrationStatus' in resp: return "Error: bad_csc"
        ps = b'chr\x00'; pe = b'\x00'; pi = resp.find(ps)
        if pi == -1: return "Error: no_pkg_marker"
        pi += len(ps); pei = resp.find(pe, pi)
        ret = resp[pi:pei].decode('utf-8')
        if ret == "260": return "Status: 260 (No Update)"
        if ret == "261": return "Status: 261 (Unknown Error)"
        if ret == "220": return "Status: 220 (Unknown Firmware)"
        return ret

# ==============================================================================
# DECRYPTER ENGINE (MERGED FROM SCAN.PY)
# ==============================================================================

class SamsungDecrypter:
    @staticmethod
    def get_md5_list(model, csc):
        # NOTE: Using global session to handle LegacySSL
        url = f"https://fota-cloud-dn.ospserver.net/firmware/{csc}/{model}/version.test.xml"
        try:
            r = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=10)
            if r.status_code != 200: return []
            root = ET.fromstring(r.content)
            return [elem.text for elem in root.findall(".//value") if elem.text]
        except: return []

    @staticmethod
    def get_next_char(char):
        alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        idx = alphabet.find(char)
        if idx == -1: return char
        return alphabet[(idx + 1) % len(alphabet)]

    @staticmethod
    def get_letters_range(start, end):
        letters = "0123456789" + string.ascii_uppercase
        try:
            s = letters.index(start)
            e = letters.index(end) + 1
            return letters[s:e]
        except: return []

    @staticmethod
    def brute_force_search(model, csc, target_short=None, months_limit=SEARCH_DEPTH_MONTHS):
        """
        Search for valid firmwares via MD5.
        target_short: if provided (5 chars), searches only for this build (complete triplet).
        months_limit: ignored if target_short is present.
        """
        md5_list = set(SamsungDecrypter.get_md5_list(model, csc))
        if not md5_list: return []

        # Basic configuration for brute force
        clean_model = model.replace("SM-", "")
        # Simplification of codes for OXM/EUX etc (Common pattern)
        first_code = clean_model + "XX"
        second_code = clean_model + "OXM"
        third_code = clean_model + "XX"

        # For US U/U1 models, simple adjustment
        if clean_model.endswith("U1") or csc == "XAA":
            first_code = clean_model + "UEU"
            second_code = clean_model + "OYM"
            third_code = clean_model + "UEU"
        elif clean_model.endswith("U"):
            first_code = clean_model + "SQ"
            second_code = clean_model + "OYN"
            third_code = clean_model + "SQ"

        # --- MODIFICATION: IF SHORT CODE IS PRESENT, SKIP THE DATE LOOP ---
        if target_short and len(target_short) == 5:
            # Direct construction with the 5 chars
            full_short = target_short
            
            triplets = []
            
            # Candidate 1 (Standard EUX/Global)
            t1 = f"{first_code}U{full_short}/{second_code}{full_short}/{third_code}U{full_short}"
            triplets.append(t1)
            
            # Candidate 2 (Security S instead of U)
            t2 = f"{first_code}S{full_short}/{second_code}{full_short}/{third_code}S{full_short}"
            triplets.append(t2)
            
            # Candidate 3 (Beta Z)
            t3 = f"{first_code}Z{full_short}/{second_code}{full_short}/{third_code}Z{full_short}"
            triplets.append(t3)

            for t in triplets:
                h = hashlib.md5(t.encode()).hexdigest()
                if h in md5_list:
                    return t  # Returns the first valid match found in the MD5 list
            
            return None # Not found

        # --- DATE LOOP (ACTIVATED IF target_short IS NONE/EMPTY) ---
        now = datetime.now()
        found_items = []
        
        # Upgraded thematic startup message
        print(f"  {Colors.CYAN}>> INITIATING DECRYPT ENGINE: Scanning depth {months_limit} months | {len(md5_list)} hashes...{Colors.RESET}")
        
        for m_offset in range(months_limit):
            curr_date = now - timedelta(days=30 * m_offset)
            year_char = chr(curr_date.year - 2001 + ord("A"))
            month_char = chr(curr_date.month - 1 + ord("A")) 
            
            bl_range = "123456789ABC"
            upd_range = "ABCDE"
            rev_range = "123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

            iter_bl = bl_range
            iter_upd = upd_range
            iter_yr = year_char
            iter_m = month_char
            iter_rev = rev_range

            for bl in iter_bl:
                for upd in iter_upd:
                    short = f"{bl}{upd}{iter_yr}{iter_m}"
                    for rev in iter_rev:
                        full_short = short + rev
                        
                        triplets = []
                        t1 = f"{first_code}U{full_short}/{second_code}{full_short}/{third_code}U{full_short}"
                        triplets.append(t1)
                        t2 = f"{first_code}S{full_short}/{second_code}{full_short}/{third_code}S{full_short}"
                        triplets.append(t2)
                        if 'Z' in upd:
                            t3 = f"{first_code}Z{full_short}/{second_code}{full_short}/{third_code}Z{full_short}"
                            triplets.append(t3)

                        for t in triplets:
                            h = hashlib.md5(t.encode()).hexdigest()
                            if h in md5_list:
                                if t not in [x['ver'] for x in found_items]:
                                    # Store with date for potential display if needed
                                    found_items.append({'ver': t, 'date': f"20{ord(iter_yr)-ord('A')+1:02d}-{ord(iter_m)-ord('A')+1:02d}"})
                                    
                                    # Added thematic real-time match feedback
                                    sys.stdout.write(f"\r  {Colors.GREEN}>> HASH MATCH SECURED: {t}{Colors.RESET}\n")
                                    sys.stdout.flush()
        
        found_items.sort(key=lambda x: x['ver'], reverse=True)
        # Return the complete list of dictionaries to retain date info in the menu
        return found_items

# ==============================================================================
# MAIN LOGIC WRAPPERS
# ==============================================================================

def get_firmware_type(version_string):
    if not version_string: return "UNK"
    ap = version_string.split('/')[0] if '/' in version_string else version_string
    if ap.endswith(".DM"): return "DM Build"
    if len(ap) >= 6:
        t = ap[-6]
        if t == 'S': return "Security"
        if t == 'U': return "Stable"
        if t == 'Z': return "Beta"
    return "UNK"

def get_bootloader_type(version_string):
    if not version_string: return "UNK"
    ap = version_string.split('/')[2] if '/' in version_string else version_string
    if len(ap) >= 5:
        t = ap[-5]
    return f"V{t}"

def check_update_wrapper(model, csc, imei, manual_base):
    try:
        mcc, mnc = ("460", "01") if csc in ["CHC","CHM"] else ("310", "410")
        client = Client({
            "Model": model, "DeviceId": f"IMEI:{imei}", "CustomerCode": csc, 
            "FirmwareVersion": manual_base, "Registered": True, "Mcc": mcc, "Mnc": mnc
        })
        ret = client.check_update(manual_base)
        if ret and ret.startswith("http"):
            desc = parse_descripter(ret, manual_base)
            if desc: 
                found_ver = desc.get("updateFwV", "Unknown")
                if found_ver and "/" in found_ver:
                    if '.DM' in found_ver.upper():
                        return {"ver": found_ver, "dm": True}
                if found_ver != "Unknown": save_to_library(model, found_ver)
                return {
                    "model": model, "csc": csc, "ver": found_ver,
                    "base": manual_base, "size": desc.get("size", 0), "url": desc.get("url"), "oneUiVersion": desc.get("oneUiVersion"), "os_ver": desc.get("os_ver"), "security": desc.get("security")
                }
            return "Error: Failed to parse update descriptor"
        else: return ret
    except Exception as e: return f"Exception: {e}"
    return "Error: Unknown failure in update check"

def smart_build_and_check(preset, short_code):
    if len(short_code) == 5:
        decrypted_triplet = SamsungDecrypter.brute_force_search(preset['model'], preset['csc'], target_short=short_code)
        if decrypted_triplet:
            return check_update_wrapper(preset['model'], preset['csc'], preset['imei'], decrypted_triplet)
        return None
    raw_model, csc, imei = preset["model"], preset["csc"], preset["imei"]
    clean_model = raw_model.replace("SM-", "")
    if clean_model.endswith("U1") or csc == "XAA":
        candidates = [f"{clean_model}UEU{short_code}/{clean_model}OYM{short_code}/{clean_model}UEU{short_code}", f"{clean_model}UES{short_code}/{clean_model}OYM{short_code}/{clean_model}UES{short_code}"]
    else:
        candidates = [f"{clean_model}XXU{short_code}/{clean_model}OXM{short_code}/{clean_model}XXU{short_code}", f"{clean_model}XXS{short_code}/{clean_model}OXM{short_code}/{clean_model}XXS{short_code}"]
    for candidate in candidates:
        res = check_update_wrapper(raw_model, csc, imei, candidate)
        if isinstance(res, dict): return res
    return None

# ==============================================================================
# INTERFACE
# ==============================================================================

def get_header_panel():
    now_str = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v1.03[/dim]  |  [dim]SESSION[/dim] [yellow]0A571D861E67[/yellow]  [dim]SERVER[/dim] [bold green]ONLINE[/bold green]  |  {now_str} [bold red]ᚳ[/bold red]"
    return Panel(Align.center(header_text), box=box.ROUNDED, style="dim white")

def get_footer_panel():
    footer_text = "[white on red] ⬍ [/white on red] [dim]Navigate[/dim] | [white on red] ⬌ [/white on red] [dim]Page[/dim] | [white on red] Enter [/white on red] [dim]Select[/dim] | [white on red] Q [/white on red] [dim]Back/Quit[/dim]"
    return Panel(Align.center(footer_text), box=box.ROUNDED, border_style="dim white")

def navigate_menu(header_title, sub_title, items, actions, extra_text=None, columns=None, side_text=None):
    """Universal interactive stat-box menu renderer"""
    selected = 0
    total_options = len(items) + len(actions)
    
    clear_screen()
    with Live(refresh_per_second=4, screen=False) as live:
        while True:
            
            # --- Construct Content ---
            table = Table(
                box=box.ROUNDED if columns is not None else None,
                show_header=(columns is not None),
                expand=True,
                header_style="bold cyan",
                show_lines=(columns is not None)
            )
            
            table.add_column("Sel", justify="center", width=4)
            
            if columns:
                for col in columns:
                    table.add_column(col)
            else:
                table.add_column("Option", justify="left")

            for i, text in enumerate(items):
                is_sel = (i == selected)
                marker = "[cyan]►[/cyan]" if is_sel else " "
                style = "bold bright_white" if is_sel else "white"
                
                if columns:
                    row_cells = []
                    for cell in text:
                        row_cells.append(f"[{style}]{str(cell)}[/{style}]")
                    table.add_row(marker, *row_cells)
                else:
                    clean_text = re.sub(r'\033\[[0-9;]*m', '', str(text))
                    table.add_row(marker, f"[{style}]{clean_text}[/{style}]")
                
            if items and actions:
                if columns:
                    table.add_row("", *[""] * len(columns))
                else:
                    table.add_row("", "")

            action_panel = None
            if actions:
                action_grid = Table.grid(expand=True, padding=(1, 1))
                for _ in actions:
                    action_grid.add_column(justify="center", ratio=1)
                    
                row_cells = []
                for i, text in enumerate(actions):
                    actual_idx = i + len(items)
                    is_sel = (actual_idx == selected)
                    
                    b_box = box.HEAVY if is_sel else box.ROUNDED
                    b_color = "cyan" if is_sel else "dim"
                    t_style = "bold cyan" if is_sel else "white"
                    marker = "► " if is_sel else ""
                    
                    btn = Panel(
                        Align.center(f"[{t_style}]{marker}{text}[/{t_style}]", vertical="middle"),
                        box=b_box,
                        border_style=b_color,
                        padding=(0, 1)
                    )
                    row_cells.append(btn)
                
                action_grid.add_row(*row_cells)
                action_panel = action_grid

            # --- Assemble Layout ---
            header = get_header_panel()
            
            # Title Panel (Global Context)
            title_panel = Panel(
                Align.center(f"[bold white]{header_title}[/bold white]"),
                style="blue",
                box=box.ROUNDED
            )
            
            main_content = None
            
            if extra_text is not None:
                info_content = Text(extra_text, style="white") if isinstance(extra_text, str) else extra_text
                
                info_panel = Panel(
                    info_content,
                    title=f"[bold bright_white]{sub_title}[/bold bright_white]",
                    border_style="cyan" if selected < len(items) else "blue",
                    box=box.ROUNDED,
                    padding=(1, 2)
                )
                
                content_group = Group(info_panel, action_panel) if action_panel else info_panel
                    
                if side_text:
                    grid = Table.grid(expand=True)
                    grid.add_column(ratio=7)
                    grid.add_column(ratio=3)
                    
                    side_panel = Panel(
                        Align.center(side_text.strip(), vertical="middle"),
                        title="[bold white]STATS[/bold white]",
                        border_style="green",
                        box=box.ROUNDED,
                        padding=(1, 2)
                    )
                    grid.add_row(content_group, side_panel)
                    main_content = grid
                else:
                    main_content = content_group
                    
            else:
                menu_panel = Panel(
                    table,
                    title=f"[bold bright_white]{sub_title}[/bold bright_white]",
                    border_style="cyan" if selected < len(items) else "blue",
                    box=box.ROUNDED,
                    padding=(1, 2)
                )

                if action_panel:
                    content_group = Group(menu_panel, action_panel)
                else:
                    content_group = menu_panel

                if side_text:
                    grid = Table.grid(expand=True)
                    grid.add_column(ratio=7)
                    grid.add_column(ratio=3)
                    
                    side_panel = Panel(
                        Align.center(side_text.strip(), vertical="middle"),
                        title="[bold white]STATS[/bold white]",
                        border_style="green",
                        box=box.ROUNDED,
                        padding=(1, 2)
                    )
                    grid.add_row(content_group, side_panel)
                    main_content = grid
                else:
                    main_content = content_group
            
            footer = get_footer_panel()
            
            # Full View
            view = Group(header, title_panel, main_content, footer)
            live.update(view)
            
            # Input
            key = get_key_timeout(0.25)
            if key:
                if key == 'UP': selected = (selected - 1) % total_options if total_options else 0
                elif key == 'DOWN': selected = (selected + 1) % total_options if total_options else 0
                elif key == 'ENTER': return selected
                elif key == 'q': return -1
                elif key == 'LEFT': return -2
                elif key == 'RIGHT': return -3

def view_device_vault():
    if not os.path.exists(IMEI_DB_DIR):
        os.makedirs(IMEI_DB_DIR)

    json_files = glob.glob(os.path.join(IMEI_DB_DIR, "*.json"))
    items = []
    
    for file_path in json_files:
        model = os.path.basename(file_path).replace(".json", "")
        csc = "UNK"
        imeis = []
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            # Safely handle the JSON whether it's a Dictionary or a direct List of IMEIs
            if isinstance(data, dict):
                csc = data.get("csc", "UNK")
                imeis = data.get("imeis", [])
            elif isinstance(data, list):
                imeis = data
                
            items.append({
                "model": model,
                "csc": csc,
                "imeis": imeis
            })
        except Exception:
            pass

    page_size = 15
    pages = [items[i:i + page_size] for i in range(0, max(1, len(items)), page_size)]
    current_page = 0
    
    while True:
        if not items:
            sel = navigate_menu("VALHALLA - DEVICE VAULT", "INVENTORY", ["[dim]Vault is completely empty.[/dim]"], ["BACK"])
            if sel == -1 or sel == 0:
                return

        page_items = pages[current_page]
        page_actions = []
        if current_page < len(pages) - 1: page_actions.append("NEXT PAGE")
        if current_page > 0: page_actions.append("PREV PAGE")
        page_actions.append("BACK")
        
        display_items = []
        for d in page_items:
            display_items.append([f"[bold cyan]{d['model']}[/bold cyan]", f"[yellow]{d['csc']}[/yellow]", f"[dim]{len(d['imeis'])} IMEIs[/dim]"])
        
        title = f"INVENTORY (Page {current_page+1}/{len(pages)})"
        r_sel = navigate_menu("VALHALLA - DEVICE VAULT", title, display_items, page_actions, columns=["Model", "CSC", "IMEIs"])
        
        if r_sel == -1: return
        if r_sel == -2: # LEFT ARROW
            if current_page > 0: current_page -= 1
            continue
        if r_sel == -3: # RIGHT ARROW
            if current_page < len(pages) - 1: current_page += 1
            continue

        if r_sel < len(page_items):
            target_dev = page_items[r_sel]
            while True:
                # Format full IMEIs list cleanly
                imei_str = "\n".join([f"  - {i}" for i in target_dev['imeis']]) if target_dev['imeis'] else "  - No IMEIs saved."
                
                info_text = (
                    f"ᛟ Model No.:      {target_dev['model']}\n"
                    f"ᛜ CSC:            {target_dev['csc']}\n"
                    f"ᚹ Saved IMEIs ({len(target_dev['imeis'])}):\n{imei_str}"
                )
                
                sel_action = navigate_menu("VALHALLA - DEVICE VAULT", "DEVICE INFO", [], ["BACK"], extra_text=info_text)
                if sel_action == 0 or sel_action == -1:
                    break
        else:
            act_idx = r_sel - len(display_items)
            if act_idx >= 0 and act_idx < len(page_actions):
                act = page_actions[act_idx]
                if act == "NEXT PAGE": current_page += 1
                elif act == "PREV PAGE": current_page -= 1
                elif act == "BACK": return

def main():
    while True:
        items = ["OTA CAPTURE", "DEVICE VAULT"]
        actions = ["EXIT"]
        
        # Calculate brief stats to display on the side menu
        vault_count = len(glob.glob(os.path.join(IMEI_DB_DIR, "*.json"))) if os.path.exists(IMEI_DB_DIR) else 0
        stats = f"\n[bold blue]VAULT FILES:[/bold blue]\n[white]{vault_count}[/white]\n"
        
        sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", "MAIN MENU", items, actions, side_text=stats)
        
        if sel == 0:
            # Route to your original Category code
            run_ota_capture()
        elif sel == 1:
            # Route to the newly built Vault UI
            view_device_vault()
        elif sel == 2 or sel == -1:
            clear_screen()
            sys.exit(0)

def run_ota_capture():
    while True:
        all_presets = load_json_devices()
        categories = list(all_presets.keys())
        
        # 1. Main Category Menu
        items = [f"{cat} {Colors.DIM}({len(all_presets[cat])} devices){Colors.RESET}" for cat in categories]
        actions = ["ADD CATEGORY", "DELETE CATEGORY", "VALHALLA LIBRARIES (CLOUDS/DESK)", "REFRESH", "EXIT"]
        
        sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", "CATEGORY SELECTION", items, actions)
        
        if sel < len(items):
            current_cat_name = categories[sel]
            chosen = None
            
            # 2. Target Device Menu
            while True:
                devs = all_presets[current_cat_name]
                d_items = []
                for p in devs:
                    n = str(p.get('name', 'Unknown'))[:22].ljust(22)
                    m = str(p.get('model', 'Unknown'))[:12].ljust(12)
                    c = str(p.get('csc', 'UNK'))[:5].ljust(5)
                    i = str(p.get('imei', 'Unknown'))[:15].ljust(15)
                    d_items.append(f"{n} │ {m} │ {c} │ {i}")
                    
                d_actions = ["ADD DEVICE", "DELETE DEVICE", "BACK"]
                title = f"TARGET DEVICE : {current_cat_name.upper()}"
                
                d_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", title, d_items, d_actions)
                
                if d_sel < len(d_items):
                    chosen = devs[d_sel]
                    break  # Break out of device menu loop to proceed to IMEI Wizard
                else:
                    act = d_actions[d_sel - len(d_items)]
                    if act == "BACK": break
                    
                    elif act == "ADD DEVICE":
                        add_actions = ["MANUAL INPUT", "SCAN DATABASE", "BACK"]
                        a_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", "ADD NEW DEVICE", [], add_actions)
                        a_act = add_actions[a_sel]
                        
                        if a_act == "MANUAL INPUT":
                            clear_screen()
                            print(f"\n{Colors.BLUE}┌{'─' * 96}┐{Colors.RESET}")
                            print(f"{Colors.BLUE}│{Colors.WHITE}{Colors.BOLD}{' MANUAL DEVICE ENTRY '.center(96)}{Colors.BLUE}│{Colors.RESET}")
                            print(f"{Colors.BLUE}└{'─' * 96}┘{Colors.RESET}\n")
                            n = input(f"  {Colors.BLUE}>> Name:{Colors.RESET} ").strip()
                            m = input(f"  {Colors.BLUE}>> Model (e.g. SM-S911B):{Colors.RESET} ").strip().upper()
                            c = input(f"  {Colors.BLUE}>> CSC:{Colors.RESET} ").strip().upper()
                            i = input(f"  {Colors.BLUE}>> IMEI:{Colors.RESET} ").strip()
                            all_presets[current_cat_name].append({"name": n, "model": m, "csc": c, "imei": i})
                            save_json_devices(all_presets)
                            
                        elif a_act == "SCAN DATABASE":
                            if os.path.exists(DEVICES_SOURCE_FILE):
                                with open(DEVICES_SOURCE_FILE, "r") as f: sdb = json.load(f)
                                flat = [d for c in sdb.values() for d in c]
                                scan_items = [f"{item['name']} {Colors.DIM}({item['model']}){Colors.RESET}" for item in flat]
                                
                                sm_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", "SELECT MODEL FROM DATABASE", scan_items, ["BACK"])
                                if sm_sel < len(scan_items):
                                    sm = flat[sm_sel]
                                    iopts = load_imei_for_model(sm['model'])
                                    if iopts:
                                        imei_items = [f"IMEI: {it['imei']} {Colors.DIM}({it['csc']}){Colors.RESET}" for it in iopts]
                                        i_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", f"SELECT IMEI FOR {sm['model']}", imei_items, ["BACK"])
                                        if i_sel < len(imei_items):
                                            final = iopts[i_sel]
                                            all_presets[current_cat_name].append({"name": sm['name'], "model": sm['model'], "csc": final['csc'], "imei": final['imei']})
                                            save_json_devices(all_presets)
                                            
                    elif act == "DELETE DEVICE":
                        if not devs: continue
                        del_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", "SELECT DEVICE TO DELETE", d_items, ["BACK"])
                        if del_sel < len(d_items):
                            clear_screen()
                            conf = input(f"\n  {Colors.RED}>> CONFIRM DELETION OF {devs[del_sel]['model']}? (Y/N): {Colors.RESET}").lower()
                            if conf == 'y':
                                del all_presets[current_cat_name][del_sel]
                                save_json_devices(all_presets)
            
            # --- BREAKOUT SEQUENCE: EXECUTE UPON DEVICE SELECTION ---
            if chosen:
                loading_animation(f"ACCESSING DATABANKS: {chosen['model']}")
                
                # ======================================================================
                # --- STEP 1: IMEI CONFIGURATION WIZARD ---
                # ======================================================================
                width = 96
                clear_screen()
                print(f"\n{Colors.BLUE}┌{'─' * width}┐{Colors.RESET}")
                print(f"{Colors.BLUE}│{Colors.WHITE}{Colors.BOLD}{' IMEI CONFIGURATION '.center(width)}{Colors.BLUE}│{Colors.RESET}")
                print(f"{Colors.BLUE}└{'─' * width}┘{Colors.RESET}\n")
                print(f"  {Colors.DIM}Target Model: {chosen['model']} | Current IMEI: {chosen['imei']}{Colors.RESET}")
                
                if input(f"\n  {Colors.BLUE}>> Modify target IMEI? (Y/N): {Colors.RESET}").strip().lower() == 'y':
                    imei_list = load_imei_for_model(chosen['model'])
                    
                    if imei_list:
                        imei_items = [f"{d['imei']} {Colors.DIM}({d.get('csc', '')}){Colors.RESET}" if isinstance(d, dict) else str(d) for d in imei_list]
                        imei_actions = ["MANUAL INPUT", "BACK"]
                        
                        i_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", f"IMEI LIBRARY: {chosen['model']}", imei_items, imei_actions)
                        
                        if i_sel < len(imei_items):
                            val = imei_list[i_sel]
                            chosen['imei'] = val['imei'] if isinstance(val, dict) else val
                        else:
                            act = imei_actions[i_sel - len(imei_items)]
                            if act == "MANUAL INPUT":
                                new_i = input(f"\n  {Colors.BLUE}>> Enter Manual IMEI:{Colors.RESET} ").strip()
                                if new_i: chosen['imei'] = new_i
                    else:
                        new_i = input(f"\n  {Colors.BLUE}>> Enter Manual IMEI:{Colors.RESET} ").strip()
                        if new_i: chosen['imei'] = new_i
                
                # Persist IMEI update locally just in case it was changed
                save_json_devices(all_presets)

                # ======================================================================
                # --- STEP 2: VERSION QUERY & DECRYPT MENU ---
                # ======================================================================
                v_actions = ["AUTO-FETCH", "LOCAL LIBRARY", "MANUAL INPUT", "DECRYPT SEARCH", "BACK"]
                v_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", f"VERSION QUERY: {chosen['model']} [{chosen['csc']}]", [], v_actions)
                v_act = v_actions[v_sel]
                
                target_ver = ""
                
                if v_act == "BACK": 
                    continue # Skips query and loops back to category selection
                    
                elif v_act == "AUTO-FETCH":
                    target_ver = fetch_latest_version(chosen['model'], chosen['csc'])
                    
                elif v_act == "LOCAL LIBRARY":
                    lib = load_library().get(chosen['model'], [])
                    if not lib:
                        print(f"\n  {Colors.YELLOW}>> NOTICE: Local library is empty for {chosen['model']}.{Colors.RESET}")
                        time.sleep(1.5)
                    else:
                        l_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", f"LOCAL LIBRARY: {chosen['model']}", lib, ["BACK"])
                        if l_sel < len(lib): 
                            target_ver = lib[l_sel]
                            
                elif v_act == "MANUAL INPUT":
                    clear_screen()
                    print(f"\n{Colors.BLUE}┌{'─' * width}┐{Colors.RESET}")
                    print(f"{Colors.BLUE}│{Colors.WHITE}{Colors.BOLD}{' MANUAL VERSION ENTRY '.center(width)}{Colors.BLUE}│{Colors.RESET}")
                    print(f"{Colors.BLUE}└{'─' * width}┘{Colors.RESET}\n")
                    target_ver = input(f"  {Colors.BLUE}>> Enter Target Version: {Colors.RESET}").strip()
                    
                elif v_act == "DECRYPT SEARCH":
                    while True:
                        clear_screen()
                        print(f"\n{Colors.BLUE}┌{'─' * width}┐{Colors.RESET}")
                        print(f"{Colors.BLUE}│{Colors.WHITE}{Colors.BOLD}{f' DECRYPT SEARCH: {chosen['model']} [{chosen['csc']}] '.center(width)}{Colors.BLUE}│{Colors.RESET}")
                        print(f"{Colors.BLUE}└{'─' * width}┘{Colors.RESET}\n")
                        print(f"  {Colors.DIM}>> Input 5 characters (e.g. 3AWF7) for targeted search.{Colors.RESET}")
                        print(f"  {Colors.DIM}>> Leave empty and press ENTER for deep background decryption.{Colors.RESET}")
                        
                        short = input(f"\n  {Colors.BLUE}>> Input or 'B' to back: {Colors.RESET}").strip().upper()
                        if short == 'B': break
                        
                        # --- CASE 1: FULL JSON DECRYPTION ENGINE ---
                        if short == '':
                            print(f"\n  {Colors.CYAN}>> INITIATING EXTERNAL DECRYPTION ENGINE...{Colors.RESET}")
                            
                            try:
                                input_str = f"1\n{chosen['model']}\n{chosen['csc']}\n"
                                subprocess.run([sys.executable, "decrypt.py"], input=input_str.encode(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                            except Exception as e:
                                print(f"  {Colors.RED}>> CRITICAL ENGINE ERROR: {e}{Colors.RESET}")
                                time.sleep(2)
                                continue

                            found_list = []
                            json_pattern = os.path.join(DECRYPTED_DIR, f"{chosen['model']}_*_full.json")
                            matching_files = glob.glob(json_pattern)
                            
                            if matching_files:
                                try:
                                    with open(matching_files[0], 'r', encoding='utf-8') as f:
                                        json_data = json.load(f)
                                        for fw_entry in json_data.get('firmwares', []):
                                            found_list.append({
                                                'ver': fw_entry.get('version'),
                                                'date': fw_entry.get('date')
                                            })
                                except Exception as e:
                                    print(f"  {Colors.RED}>> FILE SYSTEM ERROR: {e}{Colors.RESET}")
                            
                            if found_list:
                                # JSON PAGINATION
                                page = 0
                                while True:
                                    page_size = 20
                                    total_items = len(found_list)
                                    total_pages = math.ceil(total_items / page_size)
                                    
                                    start_idx = page * page_size
                                    end_idx = start_idx + page_size
                                    current_slice = found_list[start_idx:end_idx]
                                    
                                    fw_items = [f"{item['ver']} {Colors.DIM}(~{item['date']}){Colors.RESET}" for item in current_slice]
                                    
                                    fw_actions = []
                                    if page < total_pages - 1: fw_actions.append("NEXT PAGE")
                                    if page > 0: fw_actions.append("PREV PAGE")
                                    fw_actions.append("BACK")
                                    
                                    title = f"AVAILABLE BUILDS (Page {page+1}/{total_pages} | Total: {total_items})"
                                    f_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", title, fw_items, fw_actions)
                                    
                                    if f_sel < len(fw_items):
                                        target_ver = current_slice[f_sel]['ver']
                                        print(f"\n  {Colors.GREEN}>> TARGET SECURED: {target_ver}{Colors.RESET}")
                                        time.sleep(1)
                                        break
                                    else:
                                        act = fw_actions[f_sel - len(fw_items)]
                                        if act == "BACK": break
                                        elif act == "NEXT PAGE": page += 1
                                        elif act == "PREV PAGE": page -= 1
                                        
                                if target_ver: break
                            else:
                                print(f"  {Colors.RED}>> SCAN COMPLETE: No builds recovered.{Colors.RESET}")
                                time.sleep(1.5)

                        # --- CASE 2: TARGETED 5-CHARACTER SEARCH ---
                        elif len(short) == 5:
                            found = SamsungDecrypter.brute_force_search(chosen['model'], chosen['csc'], target_short=short)
                            if found:
                                print(f"\n  {Colors.GREEN}>> MATCH ACQUIRED: {found}{Colors.RESET}")
                                if input(f"  {Colors.BLUE}>> Lock target? (Y/N): {Colors.RESET}").strip().lower() == 'y':
                                    target_ver = found
                                    break
                            else:
                                print(f"\n  {Colors.RED}>> SEARCH FAILED: Pattern '{short}' not found.{Colors.RESET}")
                                if input(f"  {Colors.DIM}>> Retry? (Y/N): {Colors.RESET}").strip().lower() != 'y':
                                    break
                        else:
                            print(f"  {Colors.RED}>> INVALID SYNTAX: Expected exactly 5 characters.{Colors.RESET}")
                            time.sleep(1)

                # ======================================================================
                # --- 3. POST-QUERY EXECUTION ---
                # ======================================================================
                if target_ver:
                    clear_screen()
                    print(f"\n{Colors.BLUE}┌{'─' * width}┐{Colors.RESET}")
                    print(f"{Colors.BLUE}│{Colors.WHITE}{Colors.BOLD}{' DATABANK RESPONSE VERIFICATION '.center(width)}{Colors.BLUE}│{Colors.RESET}")
                    print(f"{Colors.BLUE}└{'─' * width}┘{Colors.RESET}\n")
                    
                    res = smart_build_and_check(chosen, target_ver) if len(target_ver) <= 15 and "/" not in target_ver else check_update_wrapper(chosen['model'], chosen['csc'], chosen['imei'], target_ver)
                    
                    if isinstance(res, dict):
                        save_link_entry(res)
                        print(f"  {Colors.GREEN}>> DATABANKS SYNCED: UPDATE FOUND & SAVED TO DESK{Colors.RESET}")
                        print(f"  {Colors.WHITE}>> Target Build: {res['ver']} {Colors.DIM}({readable_size(res['size'])}){Colors.RESET}")
                        
                        if input(f"\n  {Colors.BLUE}>> INITIATE DOWNLOAD? (Y/N): {Colors.RESET}").strip().lower() == 'y': 
                            download_file(res)
                    else: 
                        print(f"\n  {Colors.RED}>> OPERATION FAILED / STATUS: {res}{Colors.RESET}")
                        
                input(f"\n  {Colors.DIM}>> Press Enter to return to Category Selection...{Colors.RESET}")
                
        # 3. Main Actions
        else:
            act = actions[sel - len(items)]
            if act == "EXIT": 
                clear_screen()
                sys.exit(0)
            elif act == "REFRESH": 
                continue
                
            elif act == "ADD CATEGORY":
                clear_screen()
                new_cat = input(f"\n  {Colors.BLUE}>> Enter New Category Name:{Colors.RESET} ").strip()
                if new_cat: 
                    all_presets[new_cat] = []
                    save_json_devices(all_presets)
                    
            elif act == "DELETE CATEGORY":
                if not categories: continue
                del_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", "SELECT CATEGORY TO DELETE", items, ["BACK"])
                if del_sel < len(items):
                    del_cat = categories[del_sel]
                    clear_screen()
                    if input(f"\n  {Colors.RED}>> CONFIRM DELETION OF CATEGORY '{del_cat}'? (Y/N):{Colors.RESET} ").lower() == 'y':
                        del all_presets[del_cat]
                        save_json_devices(all_presets)
                        
            elif act == "VALHALLA LIBRARIES (CLOUDS/DESK)":
                while True:
                    lib_actions = ["ON CLOUD (Deployment Logs)", "ON DESK (Offline Links)", "BACK"]
                    lib_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", "VALHALLA LIBRARIES", [], lib_actions)
                    lib_act = lib_actions[lib_sel]
                    
                    if lib_act == "BACK": break
                    
                    elif lib_act == "ON CLOUD (Deployment Logs)":
                        page = 0
                        while True:
                            cloud_data = load_cloud_logs()
                            total = len(cloud_data)
                            pages = math.ceil(total / 10) if total else 1
                            
                            start = page * 10
                            items_slice = cloud_data[::-1][start:start+10]
                            log_items = [f"{it['name']} {Colors.DIM}({it['timestamp']}){Colors.RESET}" for it in items_slice]
                            
                            c_actions = []
                            if page < pages - 1: c_actions.append("NEXT PAGE")
                            if page > 0: c_actions.append("PREV PAGE")
                            c_actions.extend(["DELETE ALL LOGS", "BACK"])
                            
                            title = f"ON CLOUD LOGS (Page {page+1}/{pages} | Total: {total})"
                            c_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", title, log_items, c_actions)
                            
                            if c_sel < len(log_items):
                                selected_log = items_slice[c_sel]
                                detail_actions = ["RESEND ON TELEGRAM", "BACK"]
                                d_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", "LOG DETAILS", [], detail_actions, extra_text=selected_log['content'])
                                
                                if detail_actions[d_sel] == "RESEND ON TELEGRAM":
                                    print(f"\n  {Colors.CYAN}>> Routing telegram packet...{Colors.RESET}")
                                    item_data = {
                                        "model": selected_log.get('model', 'UNKNOWN'),
                                        "fname": selected_log.get('name', 'UNKNOWN'),
                                        "size": selected_log.get('size', 'N/A'),
                                        "link": selected_log.get('link', '')
                                    }
                                    if TelegramService.send_batch([item_data]):
                                        print(f"  {Colors.GREEN}>> Notification Deployed!{Colors.RESET}")
                                    else:
                                        print(f"  {Colors.RED}>> Deployment Failed.{Colors.RESET}")
                                    time.sleep(1.5)
                            else:
                                ca_act = c_actions[c_sel - len(log_items)]
                                if ca_act == "BACK": break
                                elif ca_act == "NEXT PAGE": page += 1
                                elif ca_act == "PREV PAGE": page -= 1
                                elif ca_act == "DELETE ALL LOGS":
                                    clear_screen()
                                    if input(f"\n  {Colors.RED}>> WARNING: PERMANENTLY DELETE ALL CLOUD LOGS? (Y/N): {Colors.RESET}").lower() == 'y':
                                        if os.path.exists(CLOUD_LOGS_FILE): os.remove(CLOUD_LOGS_FILE)
                                        break
                                        
                    elif lib_act == "ON DESK (Offline Links)":
                        while True:
                            links_db = load_saved_links()
                            models = list(links_db.keys())
                            desk_items = [f"{mod} {Colors.DIM}({len(links_db[mod])} items){Colors.RESET}" for mod in models]
                            
                            dk_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", "ON DESK (LOCAL DATABANKS)", desk_items, ["BACK"])
                            
                            if dk_sel < len(desk_items):
                                sel_model = models[dk_sel]
                                while True:
                                    m_links = []
                                    for link in links_db[sel_model]:
                                        base_s = link.get('base', '')
                                        if "/" in base_s: base_s = base_s.split('/')[0][-5:]
                                        else: base_s = base_s[-5:] if len(base_s) >= 5 else base_s
                                        
                                        ver_s = link.get('ver', '')
                                        if "/" in ver_s: ver_s = ver_s.split('/')[0][-5:]
                                        else: ver_s = ver_s[-5:] if len(ver_s) >= 5 else ver_s
                                        
                                        m_links.append(f"{base_s} -> {ver_s} | {readable_size(link.get('size'))}")
                                        
                                    lk_sel = navigate_menu("VALHALLA - KINZOKU OTA SYSTEM", f"OFFLINE LINKS: {sel_model}", m_links, ["BACK"])
                                    if lk_sel < len(m_links):
                                        download_file(links_db[sel_model][lk_sel])
                                    else:
                                        break
                            else:
                                break

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        clear_screen()
        sys.exit(0)