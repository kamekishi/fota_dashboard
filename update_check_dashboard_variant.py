#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VALHALLA UPDATE NOTIFIER - KINZOKU
"""

import os
import sys
import time
import json
import re
import socket
import subprocess
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta
import zipfile

# Fix: Rename local http.py if present to avoid shadowing standard library http module
_local_http = os.path.join(os.path.dirname(os.path.abspath(__file__)), "http.py")
_new_http = os.path.join(os.path.dirname(os.path.abspath(__file__)), "http_transport.py")
if os.path.exists(_local_http):
    try:
        os.rename(_local_http, _new_http)
    except OSError:
        pass

import requests
import xml.etree.ElementTree as ET

import test_firmware_decrypt

try:
    import paramiko
except ImportError:
    paramiko = None

try:
    from osp_http_client import OspHttpClient, OspDevice
except ImportError:
    pass

# Rich imports
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, DownloadColumn, TransferSpeedColumn
    from rich.columns import Columns
    from rich.layout import Layout
    from rich.align import Align
    from rich.text import Text
    from rich.console import Group
    from rich.live import Live
    from rich import box
except ImportError:
    print("Rich library not found. Please install it using: pip install rich")
    sys.exit(1)


console = Console()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ACTIVITY_LOCK = threading.Lock()
BOT_PROCESS_LOCK = threading.Lock()
BOT_PROCESSES = {}

# Cross-platform raw keyboard input handling
if os.name == 'nt':
    import msvcrt
else:
    import tty
    import termios
    import select

# Attempt to import sibling modules
try:
    import ota
    import scan
    import otadl
    import gui
except ImportError:
    console.print("\n[bold red][!] Error: 'ota.py', 'scan.py' or 'otadl.py' not found in the current directory.[/bold red]")
    sys.exit(1)

# --- CONFIGURATION ---
DEVICES_FILE = "devices.json"
LOG_FILE = "update_logs.json"
TERMUX_SSH_HOST = "127.0.0.1"
TERMUX_SSH_PORT = 8022
TERMUX_SSH_USERNAME = "u0_a619"
TERMUX_SSH_PASSWORD = "KinZoKu95"
OUTPUT_DIR = "ScanResults"
IMEI_SCAN_THREAD_LIMIT = 4
IMEI_MULTI_DEVICE_LIMIT = 3

# --- GLOBAL STATE ---
VERSION = "1.06"
START_TIME = time.time()
ACTIVITY_LOG = []
SYSTEM_STATUS = {
    'ssh': "[dim]Checking...[/dim]",
    'network': "[dim]Checking...[/dim]",
    'integrations': {
        'fota': "[dim]Checking...[/dim]",
        'sak': "[dim]Checking...[/dim]",
        'fumo': "[dim]Checking...[/dim]",
        'dms': "[dim]Checking...[/dim]",
        'tg': "[dim]Checking...[/dim]"
    }
}

def add_activity(level, message):
    """Adds an entry to the global activity feed."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    
    # Color code levels
    if level == "INFO":
        lvl_str = "[cyan]INFO[/cyan]"
    elif level == "WARN":
        lvl_str = "[yellow]WARN[/yellow]"
    elif level == "ERROR":
        lvl_str = "[red]ERROR[/red]"
    elif level == "HIT":
        lvl_str = "[green]HIT [/green]"
    elif level == "OK":
        lvl_str = "[green]OK  [/green]"
    else:
        lvl_str = f"[{level}]"
        
    entry = f"{timestamp} \\[{lvl_str}] {message}"
    with ACTIVITY_LOCK:
        ACTIVITY_LOG.insert(0, entry)
        
        # Keep only last 10 activities
        if len(ACTIVITY_LOG) > 10:
            ACTIVITY_LOG.pop()

# Populate some initial dummy data
add_activity("INFO", f"Valhalla Engine v{VERSION} initialized.")
add_activity("OK", "Database loaded successfully.")

def clear_screen():
    console.clear()

def show_transition(text="Loading...", duration=0.4):
    """Displays a brief loading spinner as a menu transition."""
    clear_screen()
    with console.status(f"[bold cyan]{text}[/bold cyan]", spinner="dots"):
        time.sleep(duration)

def sanitize_subprocess_output(line):
    """Strip terminal control sequences before adding a line to the feed."""
    if not line:
        return ""
    clean = re.sub(r'\x1b\[[0-9;?]*[A-Za-z]', '', str(line))
    clean = clean.replace('\r', ' ').replace('\n', ' ').strip()
    return clean

def monitor_bot_process(script_name, display_name, process):
    """Stream bot output into the dashboard activity feed until the process exits."""
    try:
        if process.stdout is not None:
            for raw_line in process.stdout:
                clean_line = sanitize_subprocess_output(raw_line)
                if clean_line:
                    add_activity("INFO", f"{display_name}: {clean_line}")

        exit_code = process.wait()
        level = "OK" if exit_code == 0 else "WARN"
        add_activity(level, f"{display_name} stopped with exit code {exit_code}.")
    except Exception as e:
        add_activity("ERROR", f"{display_name} monitor failed: {e}")
    finally:
        try:
            if process.stdout is not None:
                process.stdout.close()
        except Exception:
            pass

        with BOT_PROCESS_LOCK:
            current = BOT_PROCESSES.get(script_name)
            if current and current.get("process") is process:
                BOT_PROCESSES.pop(script_name, None)

def launch_background_script(script_name, display_name):
    """Launch a sibling bot script and mirror its output into the activity feed."""
    script_path = os.path.join(BASE_DIR, script_name)
    if not os.path.exists(script_path):
        add_activity("ERROR", f"{display_name} not found: {script_name}")
        return False

    with BOT_PROCESS_LOCK:
        existing = BOT_PROCESSES.get(script_name)
        if existing and existing.get("process") and existing["process"].poll() is None:
            add_activity("WARN", f"{display_name} is already running.")
            return False

    try:
        process = subprocess.Popen(
            [sys.executable, "-u", script_path],
            cwd=BASE_DIR,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            universal_newlines=True,
        )
        monitor_thread = threading.Thread(
            target=monitor_bot_process,
            args=(script_name, display_name, process),
            daemon=True,
        )
        with BOT_PROCESS_LOCK:
            BOT_PROCESSES[script_name] = {
                "process": process,
                "thread": monitor_thread,
                "display_name": display_name,
            }
        monitor_thread.start()
        add_activity("OK", f"Launched {display_name}.")
        return True
    except Exception as e:
        add_activity("ERROR", f"Failed to launch {display_name}: {e}")
        return False

def readable_size(size_in_bytes):
    try:
        size = float(size_in_bytes or 0)
    except (TypeError, ValueError):
        return "0 B"

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024 or unit == "TB":
            return f"{size:.2f} {unit}"
        size /= 1024

    return "0 B"

def normalize_navigation_key(key):
    if key is None:
        return None

    key = key.lower()
    navigation_map = {
        "w": "UP",
        "s": "DOWN",
        "a": "LEFT",
        "d": "RIGHT",
    }
    return navigation_map.get(key, key)

def get_key():
    """Captures single key presses across OS platforms (Blocking)"""
    if os.name == 'nt':
        while True:
            key = msvcrt.getch()
            if key in (b'\x00', b'\xe0'): 
                key = msvcrt.getch()
                if key == b'H': return 'UP'
                if key == b'P': return 'DOWN'
                if key == b'K': return 'LEFT'
                if key == b'M': return 'RIGHT'
            elif key in (b'\r', b'\n'):
                return 'ENTER'
            elif key == b'\x03':  
                raise KeyboardInterrupt
            else:
                try: return normalize_navigation_key(key.decode('utf-8'))
                except: pass
    else:
        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(sys.stdin.fileno())
            ch = sys.stdin.read(1)
            if ch == '\x1b':  
                escape_buffer = ""
                while True:
                    ready, _, _ = select.select([sys.stdin], [], [], 0.03)
                    if not ready:
                        break
                    escape_buffer += sys.stdin.read(1)

                if escape_buffer:
                    if escape_buffer[0] in {"[", "O"}:
                        final_char = escape_buffer[-1]
                        if final_char == 'A': return 'UP'
                        if final_char == 'B': return 'DOWN'
                        if final_char == 'D': return 'LEFT'
                        if final_char == 'C': return 'RIGHT'
            elif ch in ('\r', '\n'):
                return 'ENTER'
            elif ch == '\x03':  
                raise KeyboardInterrupt
            else:
                return normalize_navigation_key(ch)
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

def check_stop_key():
    """Non-blocking check for 'r' key during loops"""
    if os.name == 'nt':
        if msvcrt.kbhit():
            try:
                if msvcrt.getch().decode('utf-8').lower() == 'r': return True
            except: pass
    else:
        if select.select([sys.stdin], [], [], 0)[0]:
            try:
                if sys.stdin.read(1).lower() == 'r': return True
            except: pass
    return False

def refresh_status_checks():
    """Ping servers and endpoints to update the system cache once."""

    # 1. SSH Status
    try:
        if paramiko is None:
            SYSTEM_STATUS['ssh'] = "[bold yellow]⚠️ UNAVAILABLE (paramiko missing)[/bold yellow]"
        else:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=TERMUX_SSH_HOST,
                port=TERMUX_SSH_PORT,
                username=TERMUX_SSH_USERNAME,
                password=TERMUX_SSH_PASSWORD,
                timeout=2,
                auth_timeout=2,
                banner_timeout=2,
                look_for_keys=False,
                allow_agent=False,
            )
            client.close()
            SYSTEM_STATUS['ssh'] = "[bold green]🟢 ONLINE[/bold green]"
    except paramiko.AuthenticationException:
        SYSTEM_STATUS['ssh'] = "[bold yellow]⛔AUTH FAILED[/bold yellow]"
    except (socket.timeout, TimeoutError, OSError):
        SYSTEM_STATUS['ssh'] = "[bold red]🔴 OFFLINE[/bold red]"
    except Exception:
        SYSTEM_STATUS['ssh'] = "[bold red]🔴 OFFLINE[/bold red]"

    # 2. Network Status
    try:
        requests.get('https://1.1.1.1', timeout=1)
        SYSTEM_STATUS['network'] = "[bold green]🟢 ONLINE[/bold green]"
    except:
        SYSTEM_STATUS['network'] = "[bold red]🔴 OFFLINE[/bold red]"
        
    # 3. Integrations (Using placeholders for now as requested)
    try:
        requests.get('https://fota-cloud-dn.ospserver.net/firmware/EUX/SM-S938B/version.xml', timeout=3)
        SYSTEM_STATUS['integrations']['fota'] = "[bold green]🟢 ACTIVE[/bold green]"
    except:
        SYSTEM_STATUS['integrations']['fota'] = "[bold red]🔴 OFFLINE[/bold red]"

    # SAK Integration Check
    try:
        # Dummy device for SAK check
        sak_device = OspDevice(
            model="SM-S918B",
            device_id="IMEI:350000000000001",
            customer_code="EUX",
            serial_number="R3CT000000Z",
            firmware_version="S918BXXU1AWA1",
            mcc=262,
            mnc=1,
            fota_client_version="4.4.14"
        )
        # Default credentials from FumoServiceConfig
        sak_client = OspHttpClient(timeout=5)
        is_sak_active = sak_client.send_fumo_register(
            sak_device, 
            "dz7680f4t7", 
            "4BE4F2C346C6F8831A480E14FD4DE276"
        )
        
        if is_sak_active:
            SYSTEM_STATUS['integrations']['sak'] = "[bold green]🟢 ACTIVE[/bold green]"
        else:
            SYSTEM_STATUS['integrations']['sak'] = "[bold red]⚠️ ERROR[/bold red]"
    except Exception:
        SYSTEM_STATUS['integrations']['sak'] = "[bold red]🔴 OFFLINE[/bold red]"

    SYSTEM_STATUS['integrations']['fumo'] = "[bold green]🟢 ACTIVE[/bold green]"
    
    # DMS Integration Check
    try:
        requests.get('https://dms.ospserver.net/v1/device/magicsync/mdm', timeout=5)
        SYSTEM_STATUS['integrations']['dms'] = "[bold green]🟢 ACTIVE[/bold green]"
    except:
        SYSTEM_STATUS['integrations']['dms'] = "[bold red]🔴 OFFLINE[/bold red]"

    try:
        requests.get('https://api.telegram.org/bot8508463124:AAEUj_DY2DzdfUbbnMBqL8lo9Gum6UHk8B4/getMe', timeout=5)
        SYSTEM_STATUS['integrations']['tg'] = "[bold green]🟢 ACTIVE[/bold green]"
    except:
        SYSTEM_STATUS['integrations']['tg'] = "[bold red]🔴 OFFLINE[/bold red]"

    add_activity("INFO", "Integration status updated.")

def load_devices():
    if os.path.exists(DEVICES_FILE):
        try:
            with open(DEVICES_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_devices(data):
    try:
        with open(DEVICES_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
    except: pass

def save_log(message):
    entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "message": message
    }
    logs = []
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, 'r') as f:
                logs = json.load(f)
        except: pass
    logs.append(entry)
    with open(LOG_FILE, 'w') as f:
        json.dump(logs, f, indent=4)

def get_bootloader_type_local(base):
    if not base or base == 'Not Set' or base == 'UNK': return "UNK"
    parts = base.split('/')
    pda = parts[2] if len(parts) > 2 else base
    if len(pda) >= 5:
        return f"V{pda[-5]}"
    return "UNK"

def get_firmware_type_local(model, csc, base):
    if not base or base == 'Not Set' or base == 'UNK':
        return "UNK", "UNK"
        
    pda = base.split('/')[0] if '/' in base else base
    if len(pda) < 6:
        return "UNK", "UNK"

    build_type = "Unknown"
    if len(pda) >= 4 and pda[-4].upper() == 'Z':
        build_type = "Beta"
    elif len(pda) >= 6 and pda[-6].upper() == 'S':
        build_type = "Security"
    elif len(pda) >= 6 and pda[-6].upper() == 'U':
        build_type = "Stable"
    elif len(pda) >= 3 and pda[-3:].upper() == '.DM':
        build_type = "DM Build"

    if not model or not csc or model == 'Unknown' or csc == 'UNK':
        return "Unknown", build_type

    release_status = "Internal"
    try:
        url = f"https://fota-cloud-dn.ospserver.net/firmware/{csc}/{model}/version.xml"
        resp = requests.get(url, timeout=3)
        if resp.status_code == 200:
            root = ET.fromstring(resp.content)
            
            latest_pda = ""
            upgrade_pda = ""
            
            latest = root.find('.//latest')
            if latest is not None and latest.text:
                latest_pda = latest.text.split('/')[0]
                
            upgrade = root.find('.//upgrade')
            if upgrade is not None and upgrade.text:
                upgrade_pda = upgrade.text.split('/')[0]
                
            if pda == latest_pda or pda == upgrade_pda:
                release_status = "Official"
    except Exception:
        pass 

    return release_status, build_type

def markup_to_text(value):
    """Best-effort conversion of rich markup into a Text object."""
    if isinstance(value, Text):
        return value.copy()

    text_value = str(value)
    try:
        return Text.from_markup(text_value)
    except Exception:
        return Text(text_value)

def add_activity_message(level, message):
    """Push a plain-text status line into the activity feed."""
    plain = markup_to_text(message).plain.replace("\n", " ").strip()
    if plain:
        add_activity(level, plain)

def navigate_menu(header_title, sub_title, items, actions, extra_text=None, columns=None, side_text=None):
    selected = 0
    total_options = len(items) + len(actions)
    
    while True:
        clear_screen()
        
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
            marker = Text("►" if is_sel else " ")
            if is_sel:
                marker.stylize("bold white on blue")
            
            if columns:
                row_cells = []
                for cell in text:
                    cell_text = markup_to_text(cell)
                    if is_sel:
                        cell_text.stylize("bold white on blue")
                    row_cells.append(cell_text)
                table.add_row(marker, *row_cells)
            else:
                clean_text = markup_to_text(re.sub(r'\033\[[0-9;]*m', '', str(text)))
                if is_sel:
                    clean_text.stylize("bold white on blue")
                else:
                    clean_text.stylize("white")
                table.add_row(marker, clean_text)
            
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
                b_color = "blue" if is_sel else "dim"
                btn_text = markup_to_text(text)
                if is_sel:
                    btn_text.stylize("bold white on blue")
                else:
                    btn_text.stylize("white")
                
                btn = Panel(
                    Align.center(btn_text, vertical="middle"),
                    box=b_box,
                    border_style=b_color,
                    padding=(0, 1)
                )
                row_cells.append(btn)
            
            action_grid.add_row(*row_cells)
            action_panel = action_grid

        console.print(Panel(
            Align.center(f"[bold white]{header_title}[/bold white]"),
            style="blue",
            box=box.ROUNDED
        ))
        
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
                console.print(grid)
            else:
                console.print(content_group)
                
        else:
            menu_panel = Panel(
                table,
                title=f"[bold bright_white]{sub_title}[/bold bright_white]",
                border_style="cyan" if selected < len(items) else "blue",
                box=box.ROUNDED,
                padding=(1, 2),
            )

            if action_panel:
                console.print(Group(menu_panel, action_panel))
            else:
                console.print(menu_panel)
            
        footer_text = "[white on red] ⬍ / WASD [/white on red] [dim]Navigate[/dim] | [white on red] ⬌ / AD [/white on red] [dim]Page[/dim] | [white on red] Enter [/white on red] [dim]Select[/dim] | [white on red] Q [/white on red] [dim]Back[/dim]"
        console.print(Panel(Align.center(footer_text), box=box.ROUNDED, border_style="dim white"))
        
        key = get_key()
        if key == 'UP': selected = (selected - 1) % total_options if total_options else 0
        elif key == 'DOWN': selected = (selected + 1) % total_options if total_options else 0
        elif key == 'ENTER': return selected
        elif key == 'q': return -1
        elif key == 'LEFT': return -2
        elif key == 'RIGHT': return -3

def renotify(updates):
    if not updates: return
    header = "<u><b>ᛒ NEW BUILD REMINDER!:</b></u>\n\n"
    messages = []
    current_msg = header
    MAX_LEN = 4000 
    
    for up in updates:
        def clean(s):
            return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            
        name = clean(up['name'])
        model = clean(up['model'])
        csc = clean(up['csc'])
        new_v = clean(up['new_ver'])
        
        block = f"<u>{name} ({model}) [{csc}]</u>\nᚿ {new_v}\n\n"
        if len(current_msg) + len(block) > MAX_LEN:
            messages.append(current_msg)
            current_msg = header + block 
        else:
            current_msg += block
            
    if current_msg != header:
        messages.append(current_msg)
        
    for msg in messages:
        ota.TelegramService.send_message(msg)

def send_consolidated_notification(updates):
    if not updates: return
    header = "<u><b>ᛒ NEW BUILD FOUND!:</b></u>\n\n"
    messages = []
    current_msg = header
    MAX_LEN = 4000 
    
    for up in updates:
        def clean(s):
            return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            
        name = clean(up['name'])
        model = clean(up['model'])
        csc = clean(up['csc'])
        old_v = clean(up.get('old_ver', 'Unknown'))
        new_v = clean(up['new_ver'])
        oneui_val = up.get('oneUiVersion')
        oneui_str = f" (OneUI {clean(oneui_val)})" if oneui_val else ""
        
        block = f"<u>{name} ({model}) [{csc}]</u>\nᛟ {old_v}\nᚿ {new_v}{oneui_str}\n\n"
        if len(current_msg) + len(block) > MAX_LEN:
            messages.append(current_msg)
            current_msg = header + block 
        else:
            current_msg += block
            
    if current_msg != header:
        messages.append(current_msg)
        
    for msg in messages:
        ota.TelegramService.send_message(msg)

def get_region_label(model):
    m = (model or "").upper()
    if m.endswith("U") or m.endswith("U1"): return "USA"
    if m.endswith("W"): return "Canada"
    if m.endswith("0"): return "China/HK"
    if m.endswith("N"): return "Korea"
    return "International"

def get_device_info_renderable(device):
    model = device.get('model', 'Unknown')
    name = device.get('name', 'Unknown')
    csc = device.get('csc', 'UNK')
    base = device.get('base', 'Not Set')
    category = device.get('category', 'Unknown')
    imei = device.get('imei', 'Unknown')

    grid = Table.grid(padding=(0, 3), expand=True)
    grid.add_column(style="bold cyan", justify="left", ratio=1)
    grid.add_column(style="bright_white", justify="left", ratio=3)

    grid.add_row("Name", f"Galaxy {name}")
    grid.add_row("Category", category)
    grid.add_row("Model", model)
    grid.add_row("CSC", csc)
    grid.add_row("IMEI", imei)
    grid.add_row("Base Firmware", base)

    bl = get_bootloader_type_local(base)
    rel, bld = get_firmware_type_local(model, csc, base)

    grid.add_row("Release", rel)
    grid.add_row("Build", bld)
    grid.add_row("Bootloader", bl)

    return grid

def perform_update_check(device_entry):
    model = device_entry.get('model')
    csc = device_entry.get('csc')
    imei = device_entry.get('imei')
    base = device_entry.get('base') or device_entry.get('firmware_base')
    
    if not model or not csc or not imei:
        return "[red]Invalid Config[/red]", "UNK", "UNK", None

    if not base:
        base = ota.fetch_latest_version(model, csc)
        if not base:
            return "[yellow]No Base Found[/yellow]", "UNK", "UNK", None
    
    res = ota.check_update_wrapper(model, csc, imei, base)

    if isinstance(res, dict):
        dm_flag = bool(res.get('dm', False))
        if dm_flag is True:
            add_activity("WARN", f"Only DM build found for {model} - {res['ver']}")
            device_entry['base'] = base
            rel, bld = get_firmware_type_local(model, csc, res['ver'])
            return f"[yellow]Only DM Build Available ({res['ver']})[/yellow]", rel, bld, None
        
        elif dm_flag is False:
            ota.save_link_entry(res)
            save_log(f"Update found for {model} ({csc}): {res['ver']}")
            add_activity("HIT", f"Update found for {model} - {res['ver']}")
            device_entry['base'] = res['ver']
            rel, bld = get_firmware_type_local(model, csc, res['ver'])
            
            update_info = {
                "name": device_entry.get('name', 'Unknown'),
                "model": model,
                "csc": csc,
                "old_ver": base,
                "new_ver": res['ver'],
                "oneUiVersion": res.get('oneUiVersion')
            }
            return f"[green]UPDATE FOUND: {res['ver']}[/green]", rel, bld, update_info

    elif isinstance(res, str):
        rel, bld = get_firmware_type_local(model, csc, base)
        if "260" in res:
            device_entry['base'] = base
            add_activity("INFO", f"Device ({model}) is Up To Date - {base}")
            return f"[dim]Up to Date ({base})[/dim]", rel, bld, None
        else:
            return f"[red]{res}[/red]", "UNK", "UNK", None
            
    return "[red]Unknown Error[/red]", "UNK", "UNK", None

def run_task_with_progress(title, description, func, *args, **kwargs):
    clear_screen()
    console.print(Panel(Align.center(title), box=box.ROUNDED, style="dim white"))

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        progress_value = 0

        with Progress(
            SpinnerColumn(style="cyan"),
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(style="magenta", complete_style="cyan", pulse_style="white"),
            TextColumn("[cyan]{task.percentage:>3.0f}%[/cyan]"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task(description, total=100)
            while not future.done():
                progress_value = min(94, progress_value + 4)
                progress.update(task_id, completed=progress_value)
                time.sleep(0.08)
            progress.update(task_id, completed=100)

        return future.result()

def format_security_patch(value):
    if not value:
        return "Unknown"

    value = str(value).strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y.%m.%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return value

def build_download_filename(item):
    def get_short_ver(version_value):
        if not version_value:
            return "00000"
        clean = version_value.split("/")[0] if "/" in version_value else version_value
        return clean[-5:] if len(clean) >= 5 else clean

    source_version = get_short_ver(item.get("old_ver") or item.get("base"))
    target_version = get_short_ver(item.get("new_ver") or item.get("ver"))
    real_csc = item.get("csc", "UNK")

    try:
        ver_value = item.get("new_ver") or item.get("ver") or ""
        if "/" in ver_value:
            middle = ver_value.split("/")[1]
            for group in ["OXM", "OYN", "OYM", "OWO", "OXE", "OJM", "OLM", "IND"]:
                if group in middle:
                    real_csc = group
                    break
    except Exception:
        pass

    filename = f"{item['model']}_{real_csc}_{source_version}_{target_version}.zip"
    url = (item.get("url") or "").replace("&amp;", "&")
    if ".DM" in url.upper():
        filename = filename.replace(".zip", ".DM.zip")

    return filename, readable_size(item.get("size", 0))

def build_update_status_payload(device_entry):
    model = device_entry.get("model")
    csc = device_entry.get("csc")
    imei = device_entry.get("imei")
    base = device_entry.get("base") or device_entry.get("firmware_base")

    if not model or not csc or not imei:
        return {
            "kind": "error",
            "status": "INVALID CONFIG",
            "recordable": False,
            "model": model or "Unknown",
            "csc": csc or "UNK",
            "old_ver": base or "Unknown",
            "new_ver": "Unknown",
            "one_ui": "Unknown",
            "security_patch": "Unknown",
            "release_type": "Unknown",
            "build_type": "Unknown",
            "bootloader": "Unknown",
            "size": 0,
            "url": "",
        }

    if not base:
        base = ota.fetch_latest_version(model, csc)
        if not base:
            return {
                "kind": "error",
                "status": "NO BASE FOUND",
                "recordable": False,
                "model": model,
                "csc": csc,
                "old_ver": "Unknown",
                "new_ver": "Unknown",
                "one_ui": "Unknown",
                "security_patch": "Unknown",
                "release_type": "Unknown",
                "build_type": "Unknown",
                "bootloader": "Unknown",
                "size": 0,
                "url": "",
            }

    result = ota.check_update_wrapper(model, csc, imei, base)

    payload = {
        "model": model,
        "name": device_entry.get("name", "Unknown"),
        "csc": csc,
        "old_ver": base,
        "base": base,
        "size": 0,
        "url": "",
        "one_ui": "Unknown",
        "security_patch": "Unknown",
        "release_type": "Unknown",
        "build_type": "Unknown",
        "bootloader": "Unknown",
        "recordable": False,
    }

    if isinstance(result, dict):
        if result.get("dm"):
            payload.update(
                {
                    "kind": "dm",
                    "status": "DM BUILD ONLY",
                    "new_ver": result.get("ver", "Unknown"),
                }
            )
            payload["release_type"], payload["build_type"] = get_firmware_type_local(model, csc, payload["new_ver"])
            payload["bootloader"] = ota.get_bootloader_type(payload["new_ver"])
            add_activity("WARN", f"DM build detected for {model} - {payload['new_ver']}.")
            device_entry["base"] = base
            return payload

        payload.update(
            {
                "kind": "update",
                "status": "UPDATE FOUND!",
                "new_ver": result.get("ver", "Unknown"),
                "size": result.get("size", 0),
                "url": result.get("url", ""),
                "one_ui": result.get("oneUiVersion") or result.get("os_ver") or "Unknown",
                "security_patch": format_security_patch(result.get("security")),
                "recordable": True,
            }
        )
        payload["release_type"], payload["build_type"] = get_firmware_type_local(model, csc, payload["new_ver"])
        payload["bootloader"] = ota.get_bootloader_type(payload["new_ver"])

        ota.save_link_entry(result)
        save_log(f"Update found for {model} ({csc}): {payload['new_ver']}")
        add_activity("HIT", f"Update found for {model} - {payload['new_ver']}.")
        device_entry["base"] = payload["new_ver"]
        return payload

    if isinstance(result, str):
        if "260" in result:
            payload.update(
                {
                    "kind": "uptodate",
                    "status": "UP TO DATE",
                    "new_ver": base,
                }
            )
            payload["release_type"], payload["build_type"] = get_firmware_type_local(model, csc, base)
            payload["bootloader"] = ota.get_bootloader_type(base)
            device_entry["base"] = base
            add_activity("INFO", f"Device ({model}) is up to date - {base}.")
            return payload

        payload.update(
            {
                "kind": "error",
                "status": markup_to_text(f"[red]{result}[/red]").plain,
                "new_ver": "Unknown",
            }
        )
        add_activity("ERROR", f"Update check failed for {model}: {result}")
        return payload

    payload.update({"kind": "error", "status": "UNKNOWN ERROR", "new_ver": "Unknown"})
    add_activity("ERROR", f"Unknown update check state for {model}.")
    return payload

def build_update_status_renderable(payload):
    result_text = Text()
    result_text.append("Scanner Result: ", style="bold cyan")

    status_style = "green" if payload["kind"] == "update" else ("yellow" if payload["kind"] in {"dm", "uptodate"} else "red")
    result_text.append(payload["status"], style=f"bold {status_style}")
    result_text.append("\n\n")
    result_text.append(f"ᛃ {payload.get('old_ver', 'Unknown')}\n", style="white")
    result_text.append(f"ᛏ {payload.get('new_ver', 'Unknown')}\n", style="white")
    result_text.append(f"ᛉ OneUI: {payload.get('one_ui', 'Unknown')}\n", style="white")
    result_text.append(f"ᚹ Security Patch: {payload.get('security_patch', 'Unknown')}\n", style="white")
    result_text.append(f"ᚱ Release Type: {payload.get('release_type', 'Unknown')}\n", style="white")
    result_text.append(f"ᛒ Build Type: {payload.get('build_type', 'Unknown')}\n", style="white")
    result_text.append(f"ᛢ Bootloader: {payload.get('bootloader', 'Unknown')}", style="white")

    return result_text

def notify_update_payload(payload, link=""):
    filename, display_size = build_download_filename(payload)
    item_data = {
        "model": payload["model"],
        "fname": filename,
        "size": display_size,
        "link": link,
    }
    return ota.TelegramService.send_batch([item_data])

def verify_downloaded_zip(path):
    try:
        with zipfile.ZipFile(path) as archive:
            return archive.testzip() is None
    except Exception:
        return False

def get_drive_service_with_reauth():
    token_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "token.json")

    try:
        return ota.get_drive_service()
    except Exception as exc:
        if "invalid_grant" not in str(exc):
            raise

        add_activity("WARN", "Google Drive token expired or revoked. Reauthenticating.")

        try:
            if os.path.exists(token_path):
                os.remove(token_path)
        except OSError as remove_exc:
            raise RuntimeError(f"Failed to reset stale Google token: {remove_exc}") from exc

        try:
            return ota.get_drive_service()
        except Exception as retry_exc:
            raise RuntimeError(
                "Google Drive reauthentication failed. Complete the OAuth login again."
            ) from retry_exc

def upload_update_to_drive(file_path, file_name, payload, display_size):
    service = get_drive_service_with_reauth()
    if not service:
        raise RuntimeError("Google Drive service is unavailable.")

    clear_screen()
    console.print(Panel(Align.center(get_imei_header("Upload to Drive").replace("IMEI Scanner", "FOTA Scanner")), box=box.ROUNDED, style="dim white"))
    console.print(Panel(Align.center(f"[bold white]UPLOADING: {file_name}[/bold white]"), style="blue", box=box.ROUNDED))

    file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
    metadata = {"name": file_name}
    media = ota.MediaFileUpload(file_path, resumable=True, chunksize=5 * 1024 * 1024)
    request = service.files().create(body=metadata, media_body=media, fields="id, webViewLink")

    response = None
    with Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(style="magenta", complete_style="cyan", pulse_style="white"),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task_id = progress.add_task("Uploading...", total=max(file_size, 1))
        while response is None:
            status, response = request.next_chunk()
            if status:
                progress.update(task_id, completed=int(status.progress() * max(file_size, 1)))
        progress.update(task_id, completed=max(file_size, 1))

    drive_link = response.get("webViewLink")
    if not drive_link:
        raise RuntimeError("Google Drive did not return a webViewLink.")

    ota.save_cloud_log(
        f"Deployed: {file_name} | Link: {drive_link}",
        file_name,
        model=payload["model"],
        link=drive_link,
        size=display_size,
    )
    return drive_link

def download_update_to_root(payload):
    filename, display_size = build_download_filename(payload)
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    url = (payload.get("url") or "").replace("&amp;", "&")

    if not url:
        raise RuntimeError("Update URL is unavailable.")

    if os.path.exists(path):
        add_activity("INFO", f"Using existing package in root folder: {filename}.")
        return path, filename, display_size

    clear_screen()
    console.print(Panel(Align.center(get_imei_header("Fetch Update").replace("IMEI Scanner", "FOTA Scanner")), box=box.ROUNDED, style="dim white"))
    console.print(Panel(Align.center(f"[bold white]DOWNLOADING: {filename}[/bold white]"), style="blue", box=box.ROUNDED))

    headers = {"User-Agent": getattr(ota, "USER_AGENT", "SAMSUNG-Android")}
    with requests.get(url, headers=headers, stream=True, verify=False, timeout=60) as response:
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))
        with Progress(
            SpinnerColumn(style="cyan"),
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(style="magenta", complete_style="cyan", pulse_style="white"),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task_id = progress.add_task("Downloading...", total=total or None)
            with open(path, "wb") as handle:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        handle.write(chunk)
                        progress.update(task_id, advance=len(chunk))

    if filename.lower().endswith(".zip") and not verify_downloaded_zip(path):
        raise RuntimeError("Downloaded package failed zip verification.")

    add_activity("OK", f"Downloaded update package to root folder: {filename}.")
    return path, filename, display_size

def execute_update_result_action(action_name, payload):
    downloaded = None
    filename, display_size = build_download_filename(payload)
    result = {"go_to_main_menu": False, "drive_link": "", "filename": filename}

    def notify_with_progress(link=""):
        return run_task_with_progress(
            get_imei_header("Notify Result").replace("IMEI Scanner", "FOTA Scanner"),
            "Sending Telegram notification...",
            notify_update_payload,
            payload,
            link,
        )

    if action_name in {"FETCH", "FETCH + NOTIFY", "FETCH + NOTIFY + UPLOAD"}:
        add_activity("INFO", f"Fetching update package for {payload['model']}.")
        downloaded = download_update_to_root(payload)

    if action_name == "NOTIFY":
        add_activity("INFO", f"Sending Telegram notification for {payload['model']}.")
        if notify_with_progress():
            add_activity("OK", f"Telegram notification sent for {payload['model']}.")
        else:
            add_activity("ERROR", f"Telegram notification failed for {payload['model']}.")

    elif action_name == "FETCH + NOTIFY":
        add_activity("INFO", f"Sending Telegram notification for {payload['model']}.")
        if notify_with_progress():
            add_activity("OK", f"Telegram notification sent for {payload['model']}.")
        else:
            add_activity("ERROR", f"Telegram notification failed for {payload['model']}.")

    elif action_name == "FETCH + NOTIFY + UPLOAD":
        if downloaded is None:
            downloaded = download_update_to_root(payload)
        file_path, filename, display_size = downloaded
        add_activity("INFO", f"Uploading {filename} to Google Drive.")
        try:
            drive_link = upload_update_to_drive(file_path, filename, payload, display_size)
            add_activity("OK", f"Uploaded {filename} to Google Drive.")
            add_activity("INFO", f"Sending Telegram notification for {payload['model']} with Drive link.")
            if notify_with_progress(drive_link):
                add_activity("OK", f"Telegram notification sent for {payload['model']}.")
            else:
                add_activity("ERROR", f"Telegram notification failed for {payload['model']}.")
            result["go_to_main_menu"] = True
            result["drive_link"] = drive_link
        except Exception as exc:
            add_activity("WARN", f"Drive upload failed for {filename}: {exc}")
            add_activity("INFO", f"Sending Telegram notification for {payload['model']} without Drive link.")
            if notify_with_progress():
                add_activity("OK", f"Telegram notification sent for {payload['model']}.")
            else:
                add_activity("ERROR", f"Telegram notification failed for {payload['model']}.")

    result["downloaded"] = downloaded
    return result

def show_drive_upload_success(filename, drive_link):
    clear_screen()
    console.print(
        Panel(
            Align.center(get_imei_header("Upload Complete").replace("IMEI Scanner", "FOTA Scanner")),
            box=box.ROUNDED,
            style="dim white",
        )
    )

    body = Text()
    body.append("Upload Successful\n\n", style="bold green")
    body.append(f"File: {filename}\n\n", style="white")
    body.append("URL:\n", style="bold cyan")
    body.append(drive_link, style="underline blue")

    console.print(
        Panel(
            body,
            title="[bold white]GOOGLE DRIVE[/bold white]",
            border_style="green",
            box=box.ROUNDED,
        )
    )
    console.print("\n[dim]Press Enter to return to Main Menu...[/dim]")
    input()

def run_single_update_status_flow(target_dev, devices_dict):
    add_activity("INFO", f"Single check initiated for {target_dev.get('model')}.")
    payload = run_task_with_progress(
        get_imei_header("Check for Update").replace("IMEI Scanner", "FOTA Scanner"),
        "Searching for update...",
        build_update_status_payload,
        target_dev,
    )
    save_devices(devices_dict)

    actions = ["BACK"]
    if payload.get("kind") == "update" and payload.get("build_type") != "DM Build":
        actions = ["FETCH", "NOTIFY", "FETCH + NOTIFY", "FETCH + NOTIFY + UPLOAD", "BACK"]

    while True:
        selection = navigate_menu(
            get_imei_header("Check for Update").replace("IMEI Scanner", "FOTA Scanner"),
            "UPDATE STATUS",
            [],
            actions,
            extra_text=build_update_status_renderable(payload),
        )

        if selection < 0 or selection == len(actions) - 1:
            return

        action_name = actions[selection]
        try:
            action_result = execute_update_result_action(action_name, payload)
            if action_result.get("go_to_main_menu"):
                show_drive_upload_success(action_result["filename"], action_result["drive_link"])
                return "root"
        except Exception as exc:
            add_activity("ERROR", f"{action_name} failed for {payload['model']}: {exc}")
            clear_screen()
            console.print(Panel(Align.center(get_imei_header("Action Error").replace("IMEI Scanner", "FOTA Scanner")), box=box.ROUNDED, style="dim white"))
            console.print(Panel(f"[bold red]{exc}[/bold red]", title="[bold white]ERROR[/bold white]", border_style="red", box=box.ROUNDED))
            console.print("\n[dim]Press Enter to return...[/dim]")
            input()
        return

def run_batch_check():
    show_transition("Preparing Batch Check...", 0.5)
    devices_dict = load_devices()
    flat_list = []
    for cat, devs in devices_dict.items():
        for d in devs:
            d['category'] = cat
            flat_list.append(d)
            
    total = len(flat_list)
    results = []
    found_updates = []
    
    clear_screen()
    
    add_activity("INFO", "Initiated Batch FOTA Check.")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console
    ) as progress:
        task = progress.add_task("[cyan]Checking devices...", total=total)
        
        for i, dev in enumerate(flat_list):
            if check_stop_key():
                add_activity("WARN", "Batch scan stopped by user.")
                time.sleep(1)
                break

            model = dev.get('model', 'Unknown')
            name = dev.get('name', 'Unknown')
            csc = dev.get('csc', 'UNK')
            
            progress.update(task, description=f"[cyan]Checking: {name} ({model})[/cyan]")
            
            status, rel, bld, update_info = perform_update_check(dev)
            
            base = dev.get('base', 'Not Set')
            results.append([name, model, csc, base, status])
            
            if update_info:
                found_updates.append(update_info)
            
            progress.advance(task)
            time.sleep(0.5)
        
    save_devices(devices_dict)
    
    if found_updates:
        send_consolidated_notification(found_updates)
    
    add_activity("OK", "Batch check completed.")
    time.sleep(1)
    return results

def run_single_check():
    show_transition("Loading Single Check...", 0.5)
    devices_dict = load_devices()
    flat_list = []
    for cat, devs in devices_dict.items():
        for d in devs:
            d['category'] = cat
            flat_list.append(d)
            
    items = []
    for d in flat_list:
        model = d.get('model', 'Unknown')
        csc = d.get('csc', 'UNK')
        base = d.get('base', 'Not Set')
        items.append([d.get('name', 'Unknown'), model, csc, base])
        
    page_size = 10
    pages = [items[i:i + page_size] for i in range(0, len(items), page_size)]
    
    current_page = 0
    while True:
        if not pages:
            header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]FOTA Scanner - Single Check[/yellow]"
                
            navigate_menu(header_text, "ᛞ DEVICE LIST", ["[yellow]No devices found.[/yellow]"], ["BACK"])
            break
            
        page_items = pages[current_page]
        page_actions = []
        
        title = f"SELECT DEVICE (Page {current_page+1}/{len(pages)})"

        header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]FOTA Scanner - Single Check[/yellow]"
    
        r_sel = navigate_menu(header_text, title, page_items, page_actions, columns=["Name", "Model", "CSC", "Base Firmware"])
        
        if r_sel == -1: return
        if r_sel == -2: 
            if current_page > 0: current_page -= 1
            continue
        if r_sel == -3: 
            if current_page < len(pages) - 1: current_page += 1
            continue

        if r_sel < len(page_items):
            show_transition("Loading Device Info...", 0.3)
            real_idx = (current_page * page_size) + r_sel
            target_dev = flat_list[real_idx]
            
            while True:
                info_renderable = get_device_info_renderable(target_dev)
                actions = ["CHECK FOR UPDATE", "OTA SCAN", "EDIT IMEI", "EDIT FIRMWARE"]

                header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]FOTA Scanner - Single Check[/yellow]"
                
                sel_action = navigate_menu(header_text, "ᚻ DEVICE INFO", [], actions, extra_text=info_renderable)
                
                if sel_action == 0:
                    next_menu = run_single_update_status_flow(target_dev, devices_dict)
                    if next_menu == "root":
                        return "root"
                    break
                elif sel_action == 1: # OTA SCAN
                    depth_opts = gui._DECRYPT_DEPTH_OPTIONS
                    depth_labels = [opt[0] for opt in depth_opts]

                    header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]FOTA Scanner - Single Check[/yellow]  |  [green]OTA Scan[/green]"

                    d_sel = navigate_menu(header_text, "SELECT DEPTH", depth_labels, ["BACK"])
                    
                    if d_sel >= 0 and d_sel < len(depth_opts):
                        selected_depth = depth_opts[d_sel][1]
                        clear_screen()
                        console.print(Panel(f"[bold white]SCANNING: {target_dev.get('model')} ({selected_depth})[/bold white]", style="blue", box=box.ROUNDED))
                        
                        try:
                            results = test_firmware_decrypt.decrypt_version_test_md5s(target_dev.get('model'), target_dev.get('csc'), depth=selected_depth, progress=lambda msg: console.print(f"[dim]{msg}[/dim]"))
                            if results:
                                versions = test_firmware_decrypt.sort_firmware_versions(list(set(results.values())))
                                last_10 = versions[-10:]
                                
                                scan_table = Table(box=box.ROUNDED, expand=True, header_style="bold cyan")
                                scan_table.add_column("Base Firmware", justify="left")
                                scan_table.add_column("Update Firmware", justify="left")
                                scan_table.add_column("Release Type", justify="center")
                                scan_table.add_column("Build Type", justify="center")
                                
                                with Progress(
                                    SpinnerColumn(),
                                    TextColumn("[progress.description]{task.description}"),
                                    BarColumn(),
                                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                                    console=console
                                ) as progress:
                                    task = progress.add_task("[cyan]Checking updates...", total=len(last_10))
                                    
                                    for base_fw in last_10:
                                        progress.update(task, description=f"[cyan]Checking: {base_fw}[/cyan]")
                                        res = ota.check_update_wrapper(target_dev.get('model'), target_dev.get('csc'), target_dev.get('imei'), base_fw)
                                        
                                        upd_fw = "Unknown"
                                        rel = "-"
                                        bld = "-"
                                        
                                        if isinstance(res, dict):
                                            upd_fw = f"[green]{res['ver']}[/green]"
                                            rel, bld = get_firmware_type_local(target_dev.get('model'), target_dev.get('csc'), res['ver'])
                                        elif isinstance(res, str):
                                            if "260" in res:
                                                upd_fw = "[dim]Up to Date[/dim]"
                                            elif "DM_BUILD_ONLY" in res:
                                                upd_fw = f"[yellow]{res['ver']}[/yellow]"
                                                rel, bld = get_firmware_type_local(target_dev.get('model'), target_dev.get('csc'), res['ver'])
                                            else:
                                                upd_fw = f"[red]Error[/red]"
                                        
                                        scan_table.add_row(base_fw, upd_fw, rel, bld)
                                        progress.advance(task)
                                
                                console.print(scan_table)
                                footer_text = "[white on red] ⬍ / WASD [/white on red] [dim]Navigate[/dim] | [white on red] ⬌ / AD [/white on red] [dim]Page[/dim] | [white on red] Enter [/white on red] [dim]Select[/dim] | [white on red] Q [/white on red] [dim]Back[/dim]"
                                console.print(Panel(Align.center(footer_text), box=box.ROUNDED, border_style="dim white"))
                                console.print("\n[dim]Press Enter to return...[/dim]")
                                input()
                            else:
                                add_activity("WARN", f"No firmware found for {target_dev.get('model')}.")
                                time.sleep(1)
                        except Exception as e:
                            add_activity("ERROR", f"OTA scan failed for {target_dev.get('model')}: {e}")
                            time.sleep(1)

                elif sel_action == 2: # EDIT IMEI
                    clear_screen()

                    header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]FOTA Scanner - Single Check[/yellow]  |  [green]Edit IMEI[/green]"
                    header_panel = Panel(Align.center(header_text), box=box.ROUNDED, style="dim white")
                    
                    console.print(header_panel)
                    console.print(Panel(f"[bold white]EDIT IMEI: {target_dev.get('model')}[/bold white]", style="blue", box=box.ROUNDED))
                    console.print(f"Current IMEI: {target_dev.get('imei')}")
                    new_val = console.input("[bold yellow]Enter New IMEI:[/bold yellow] ").strip()
                    if new_val:
                        target_dev['imei'] = new_val
                        save_devices(devices_dict)
                        add_activity("OK", f"Updated IMEI for {target_dev.get('model')}.")
                        time.sleep(1)
                elif sel_action == 3: # EDIT FIRMWARE
                    while True:
                        header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]FOTA Scanner - Single Check[/yellow]  |  [green]Edit Firmware[/green]"

                        fw_actions = ["INPUT MANUALLY", "DECRYPT FIRMWARE", "BACK"]
                        fw_sel = navigate_menu(header_text, "CHOOSE METHOD", [], fw_actions, extra_text=f"Current Base: {target_dev.get('base')}")
                        
                        if fw_sel == 0: # INPUT MANUALLY
                            clear_screen()

                            header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]FOTA Scanner - Single Check[/yellow]  |  [green]Edit Firmware[/green]"
                            header_panel = Panel(Align.center(header_text), box=box.ROUNDED, style="dim white")

                            console.print(header_panel)
                            console.print(Panel(f"[bold white]INPUT MANUALLY: {target_dev.get('model')}[/bold white]", style="blue", box=box.ROUNDED))
                            console.print(f"Current Base: {target_dev.get('base')}")
                            new_val = console.input("[bold yellow]Enter New Firmware:[/bold yellow] ").strip()
                            if new_val:
                                target_dev['base'] = new_val
                                save_devices(devices_dict)
                                add_activity("OK", f"Updated Base FW for {target_dev.get('model')}.")
                                time.sleep(1)
                            break
                        elif fw_sel == 1: # DECRYPT FIRMWARE
                            header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]FOTA Scanner - Single Check[/yellow]  |  [green]Edit Firmware[/green]"

                            depth_opts = gui._DECRYPT_DEPTH_OPTIONS
                            depth_labels = [opt[0] for opt in depth_opts]
                            d_sel = navigate_menu(header_text, "SELECT DEPTH", depth_labels, ["BACK"])
                            
                            if d_sel >= 0 and d_sel < len(depth_opts):
                                selected_depth = depth_opts[d_sel][1]
                                clear_screen()
                                console.print(Panel(f"[bold white]DECRYPTING: {target_dev.get('model')} ({selected_depth})[/bold white]", style="blue", box=box.ROUNDED))
                                add_activity("INFO", f"Decrypting firmware for {target_dev.get('model')}")
                                
                                try:
                                    results = test_firmware_decrypt.decrypt_version_test_md5s(target_dev.get('model'), target_dev.get('csc'), depth=selected_depth, progress=lambda msg: console.print(f"[dim]{msg}[/dim]"))
                                    if results:
                                        versions = test_firmware_decrypt.sort_firmware_versions(list(set(results.values())))
                                        
                                        page_size = 10
                                        version_items = [[v] for v in versions]
                                        pages = [version_items[i:i + page_size] for i in range(0, len(version_items), page_size)]
                                        current_page = 0
                                        
                                        while True:
                                            if not pages: break
                                            page_items = pages[current_page]
                                            
                                            page_actions = []
                                            if current_page < len(pages) - 1: page_actions.append("NEXT PAGE")
                                            if current_page > 0: page_actions.append("PREV PAGE")
                                            page_actions.append("BACK")
                                            
                                            header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]FOTA Scanner - Single Check[/yellow]  |  [green]Edit Firmware[/green]"
                                            
                                            title = f"SELECT FIRMWARE (Page {current_page+1}/{len(pages)})"
                                            v_sel = navigate_menu(header_text, title, page_items, page_actions, columns=["Version"])
                                            
                                            if v_sel == -1: break
                                            if v_sel == -2: 
                                                if current_page > 0: current_page -= 1
                                                continue
                                            if v_sel == -3: 
                                                if current_page < len(pages) - 1: current_page += 1
                                                continue
                                                
                                            if v_sel < len(page_items):
                                                target_dev['base'] = page_items[v_sel][0]
                                                save_devices(devices_dict)
                                                add_activity("OK", f"Updated Base FW for {target_dev.get('model')}.")
                                                time.sleep(1)
                                                break
                                            else:
                                                act_idx = v_sel - len(page_items)
                                                if act_idx >= 0 and act_idx < len(page_actions):
                                                    act = page_actions[act_idx]
                                                    if act == "NEXT PAGE": current_page += 1
                                                    elif act == "PREV PAGE": current_page -= 1
                                                    elif act == "BACK": break
                                    else:
                                        add_activity("WARN", f"No firmware found for {target_dev.get('model')}.")
                                        time.sleep(1)
                                except Exception as e:
                                    add_activity("ERROR", f"Decryption failed: {e}")
                                    time.sleep(1)
                            break
                        elif fw_sel == 2 or fw_sel == -1: # BACK
                            break
                elif sel_action == -1:
                    break
        else:
            act_idx = r_sel - len(page_items)
            if act_idx >= 0 and act_idx < len(page_actions):
                act = page_actions[act_idx]
                if act == "BACK": return

def view_device_status():
    show_transition("Opening Device Vault...", 0.5)
    devices_dict = load_devices()
    flat_list = []
    for cat, devs in devices_dict.items():
        for d in devs:
            d['category'] = cat
            flat_list.append(d)
            
    items = []
    
    for d in flat_list:
        name = d.get('name', 'Unknown')
        model = d.get('model', 'Unknown')
        csc = d.get('csc', 'UNK')
        base = d.get('base', 'Not Set')
        items.append([name, model, csc, base])
        
    page_size = 10
    pages = [items[i:i + page_size] for i in range(0, len(items), page_size)]
    
    current_page = 0
    while True:
        if not pages:
            header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]Device Vault[/yellow]"
            header_panel = Panel(Align.center(header_text), box=box.ROUNDED, style="dim white")

            navigate_menu(header_text, "INVENTORY", ["[yellow]Looks like your Vault is empty.[/yellow]"], [])
            break
            
        page_items = pages[current_page]
        page_actions = []
        
        header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]Device Vault[/yellow]"

        title = f"INVENTORY (Page {current_page+1}/{len(pages)})"
        r_sel = navigate_menu(header_text, title, page_items, page_actions, columns=["Name", "Model", "CSC", "Base Firmware"])
        
        if r_sel == -1: break
        if r_sel == -2: 
            if current_page > 0: current_page -= 1
            continue
        if r_sel == -3: 
            if current_page < len(pages) - 1: current_page += 1
            continue

        if r_sel < len(page_items):
            show_transition("Loading Device Info...", 0.3)
            real_idx = (current_page * page_size) + r_sel
            target_dev = flat_list[real_idx]
            
            while True:
                info_renderable = get_device_info_renderable(target_dev)
                header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]Device Vault[/yellow]"

                actions = ["EDIT IMEI", "EDIT FIRMWARE"]
                sel_action = navigate_menu(header_text, "ᚻ DEVICE INFO", [], actions, extra_text=info_renderable)
                
                if sel_action == 0: # EDIT IMEI
                    clear_screen()
                    header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]Device Vault[/yellow]"
                    header_panel = Panel(Align.center(header_text), box=box.ROUNDED, style="dim white")
                    
                    console.print(header_panel)
                    console.print(Panel(f"[bold white]EDIT IMEI: {target_dev.get('model')}[/bold white]", style="blue", box=box.ROUNDED))
                    console.print(f"Current IMEI: {target_dev.get('imei')}")
                    new_val = console.input("[bold yellow]Enter New IMEI:[/bold yellow] ").strip()
                    if new_val:
                        target_dev['imei'] = new_val
                        save_devices(devices_dict)
                        add_activity("OK", f"Updated IMEI for {target_dev.get('model')}.")
                        time.sleep(1)
                elif sel_action == 1: # EDIT FIRMWARE
                    pass 
                elif sel_action == -1:
                    break
        else:
            act_idx = r_sel - len(page_items)
            if act_idx >= 0 and act_idx < len(page_actions):
                act = page_actions[act_idx]
                if act == "BACK": break

def get_system_stats():
    # Uptime calculates dynamically against the script's START_TIME
    uptime_sec = int(time.time() - START_TIME)
    uptime_str = str(timedelta(seconds=uptime_sec))
    
    # Vault Count
    devices = load_devices()
    total_devices = sum([len(devices[cat]) for cat in devices])
    
    # Last Discovery
    last_check = "Never"
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, 'r') as f:
                logs = json.load(f)
            if logs:
                last_check = logs[-1]['timestamp']
        except: pass
        
    # Disk Space Stub (Local)
    try:
        total, used, free = shutil.disk_usage("/")
        free_gb = free // 1073741824
        disk_str = f"{free_gb} GB"
    except:
        disk_str = "Unknown" 
        
    return uptime_str, total_devices, last_check, disk_str

def draw_main_dashboard(selected_idx, menu_items, operations_title="MAIN MENU", allow_back=False):
    # 1. HEADER - The time ticks visually due to 'seconds' format
    now_str = datetime.now().strftime("%Y-%m-%d  %H:%M:%S")
    
    # Read from cache instead of pinging live
    ssh_status = SYSTEM_STATUS['ssh']
    
    header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [dim]SESSION[/dim] [yellow]0A571D861E67[/yellow]  [dim]SERVER[/dim] {ssh_status}  |  {now_str} [bold red]ᚳ[/bold red]"
    header_panel = Panel(Align.center(header_text), box=box.ROUNDED, style="dim white")

    # 2. LEFT BOX: OPTIONS
    opt_lines = []
    for i, opt in enumerate(menu_items):
        prefix = f"[{i+1}]  "
        clean_opt = markup_to_text(opt).plain
        if i == selected_idx:
            opt_lines.append(f"[bold white on blue]{prefix}{clean_opt:<27}[/bold white on blue]")
        else:
            opt_lines.append(f"[white]{prefix}{clean_opt:<27}[/white]")
            
    left_panel = Panel(
        "\n\n".join(opt_lines) if opt_lines else "[dim]No options available.[/dim]",
        title=f"[bold red]ᛟ {operations_title}[/bold red]", 
        box=box.HEAVY, 
        border_style="red", 
        height=18
    )

    # 3. MIDDLE BOX: SYSTEM
    up_s, vault_c, last_d, disk_s = get_system_stats()
    net_s = SYSTEM_STATUS['network'] # Pull from static cache
    
    sys_grid = Table.grid(padding=(0, 2), expand=True)
    sys_grid.add_column(style="dim white", ratio=1)
    sys_grid.add_column(justify="right", ratio=1)
    
    sys_grid.add_row("• Uptime", f"[white]{up_s}[/white]")
    sys_grid.add_row("• Server", ssh_status)
    sys_grid.add_row("• Network", net_s)
    sys_grid.add_row("• DB Records", f"[yellow]{vault_c}[/yellow]")
    sys_grid.add_row("• Last Disc.", f"[dim]{last_d.split(' ')[-1] if ' ' in last_d else last_d}[/dim]")
    sys_grid.add_row("• Disk Free", f"[white]{disk_s}[/white]")
    
    mid_panel = Panel(
        sys_grid, 
        title="[bold white]ᛘ SYSTEM[/bold white]", 
        box=box.ROUNDED, 
        border_style="dim white",
        height=18
    )

    # 4. RIGHT BOX: INTEGRATIONS
    # Pulling from static cache
    int_cache = SYSTEM_STATUS['integrations']
    int_grid = Table.grid(padding=(0, 2), expand=True)
    int_grid.add_column(style="dim white", ratio=1)
    int_grid.add_column(justify="right", ratio=1)
    
    int_grid.add_row("FOTA Engine", int_cache['fota'])
    int_grid.add_row("SAK Bypass", int_cache['sak'])
    int_grid.add_row("FUMO Client", int_cache['fumo'])
    int_grid.add_row("DMS Metadata", int_cache['dms'])
    int_grid.add_row("Telegram Bot", int_cache['tg'])
    
    right_panel = Panel(
        int_grid, 
        title="[bold white]ᛋ INTEGRATIONS[/bold white]", 
        box=box.ROUNDED, 
        border_style="dim white",
        height=18
    )

    if operations_title == "IMEI SCANNER":
        top_columns = left_panel
    else:
        top_columns = Columns([left_panel, mid_panel, right_panel], expand=True)

    # 5. BOTTOM BOX: ACTIVITY FEED
    feed_text = "\n".join(ACTIVITY_LOG) if ACTIVITY_LOG else "[dim]No recent activity.[/dim]"
    feed_panel = Panel(
        feed_text, 
        title="[bold white]∿ ACTIVITY FEED[/bold white]", 
        box=box.ROUNDED, 
        border_style="dim white",
        height=12
    )
    
    # 6. FOOTER
    back_text = "Back" if allow_back else "Quit"
    footer_text = f"[white on red] ⬍ / WASD [/white on red] [dim]Navigate[/dim]   |   [white on red] Enter [/white on red] [dim]Select[/dim]   |   [white on red] Q [/white on red] [dim]{back_text}[/dim]"
    # Placed the footer inside a rounded panel box to match the header
    footer_panel = Panel(Align.center(footer_text), box=box.ROUNDED, border_style="dim white")

    return Group(header_panel, top_columns, feed_panel, footer_panel)

def show_batch_results(results):
    if not results:
        add_activity("WARN", "Batch check returned no results.")
        return

    show_transition("Formatting Results...", 0.4)
    page_size = 10
    pages = [results[i:i + page_size] for i in range(0, len(results), page_size)]
    current_page = 0

    while True:
        if not pages:
            break

        page_items = pages[current_page]
        title = f"RESULTS (Page {current_page+1}/{len(pages)})"
        header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]FOTA Scanner - Batch Check[/yellow]"
        r_sel = navigate_menu(header_text, title, page_items, [], columns=["Name", "Model", "CSC", "Base Firmware", "Status"])

        if r_sel == -1:
            break
        if r_sel == -2:
            if current_page > 0:
                current_page -= 1
            continue
        if r_sel == -3:
            if current_page < len(pages) - 1:
                current_page += 1
            continue

def get_imei_header(section):
    return (
        f"[bold red]ᚳ Project Valhalla[/bold red]  "
        f"[bold cyan]- KinZoKu[/bold cyan]  "
        f"[dim]v{VERSION}[/dim]  |  "
        f"[yellow]IMEI Scanner[/yellow]  |  [green]{section}[/green]"
    )

def flatten_devices(devices_dict):
    flat_list = []
    for category, devices in devices_dict.items():
        for device in devices:
            device["category"] = category
            flat_list.append(device)
    return flat_list

def get_device_key(device):
    return f"{device.get('model', 'UNK')}|{device.get('csc', 'UNK')}|{device.get('name', 'Unknown')}"

def get_device_label(device):
    return f"{device.get('name', 'Unknown')} ({device.get('model', 'UNK')}/{device.get('csc', 'UNK')})"

def sort_library_entries(entries):
    def date_key(entry):
        try:
            return datetime.strptime(entry.get("date", ""), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.min

    return sorted(entries, key=date_key, reverse=True)

def select_device_entry(header_text, title, devices_dict, excluded_keys=None):
    excluded_keys = excluded_keys or set()
    flat_list = [device for device in flatten_devices(devices_dict) if get_device_key(device) not in excluded_keys]

    if not flat_list:
        navigate_menu(header_text, title, ["[yellow]No devices available.[/yellow]"], [])
        return None

    items = [
        [
            device.get("name", "Unknown"),
            device.get("model", "Unknown"),
            device.get("csc", "UNK"),
            device.get("base", "Not Set"),
        ]
        for device in flat_list
    ]

    page_size = 10
    pages = [items[i:i + page_size] for i in range(0, len(items), page_size)]
    current_page = 0

    while True:
        page_items = pages[current_page]
        page_actions = []
        if current_page < len(pages) - 1:
            page_actions.append("NEXT PAGE")
        if current_page > 0:
            page_actions.append("PREV PAGE")
        page_actions.append("BACK")

        page_title = f"{title} (Page {current_page + 1}/{len(pages)})"
        selection = navigate_menu(
            header_text,
            page_title,
            page_items,
            page_actions,
            columns=["Name", "Model", "CSC", "Base Firmware"],
        )

        if selection == -1:
            return None
        if selection == -2:
            if current_page > 0:
                current_page -= 1
            continue
        if selection == -3:
            if current_page < len(pages) - 1:
                current_page += 1
            continue
        if selection < len(page_items):
            real_idx = current_page * page_size + selection
            return flat_list[real_idx]

        action_idx = selection - len(page_items)
        if action_idx < 0 or action_idx >= len(page_actions):
            continue
        action = page_actions[action_idx]
        if action == "NEXT PAGE" and current_page < len(pages) - 1:
            current_page += 1
        elif action == "PREV PAGE" and current_page > 0:
            current_page -= 1
        else:
            return None

def prompt_imei_scan_settings(title, thread_limit=None):
    clear_screen()
    console.print(Panel(Align.center(get_imei_header(title)), box=box.ROUNDED, style="dim white"))
    console.print(Panel("[bold white]Configure scan settings[/bold white]", style="blue", box=box.ROUNDED))

    count_input = console.input("[bold yellow]Amount to scan[/bold yellow] [dim](default 100, B to back)[/dim]: ").strip()
    if count_input.upper() == "B":
        return None

    try:
        amount = max(1, int(count_input or "100"))
    except ValueError:
        amount = 100

    threads = thread_limit
    if thread_limit is None:
        threads_input = console.input(
            f"[bold yellow]Threads[/bold yellow] [dim](1-{IMEI_SCAN_THREAD_LIMIT}, default {IMEI_SCAN_THREAD_LIMIT})[/dim]: "
        ).strip()
        try:
            threads = min(IMEI_SCAN_THREAD_LIMIT, max(1, int(threads_input or str(IMEI_SCAN_THREAD_LIMIT))))
        except ValueError:
            threads = IMEI_SCAN_THREAD_LIMIT

    direction = console.input(
        "[bold yellow]Direction[/bold yellow] [dim](press Enter for next, type P for previous)[/dim]: "
    ).strip().upper()

    return {
        "amount": amount,
        "threads": threads,
        "step": -1 if direction == "P" else 1,
    }

def edit_scan_device_imei(target_dev, devices_dict):
    model = target_dev.get("model", "Unknown")
    last_imeis = scan.load_last_imeis()

    while True:
        library_entries = sort_library_entries(scan.load_library(model))
        items = [
            [
                entry.get("imei", "Unknown"),
                entry.get("csc", "UNK"),
                entry.get("fw", "Unknown"),
                entry.get("date", "-"),
            ]
            for entry in library_entries
        ]

        actions = ["MANUAL INPUT"]
        if model in last_imeis:
            actions.append("RESUME LAST IMEI")
        actions.append("BACK")

        selection = navigate_menu(
            get_imei_header("Edit IMEI"),
            "SELECT IMEI SOURCE",
            items,
            actions,
            extra_text=f"Current IMEI: {target_dev.get('imei', 'Unknown')}",
            columns=["IMEI", "CSC", "Firmware", "Date"],
        )

        if selection < 0:
            return
        if selection < len(items):
            target_dev["imei"] = library_entries[selection].get("imei", target_dev.get("imei"))
            save_devices(devices_dict)
            add_activity("OK", f"Loaded IMEI for {get_device_label(target_dev)} from library.")
            time.sleep(1)
            return

        action_idx = selection - len(items)
        if action_idx < 0 or action_idx >= len(actions):
            continue

        action = actions[action_idx]
        if action == "MANUAL INPUT":
            clear_screen()
            console.print(Panel(Align.center(get_imei_header("Edit IMEI")), box=box.ROUNDED, style="dim white"))
            new_imei = console.input("[bold yellow]Enter new IMEI[/bold yellow]: ").strip()
            if not re.fullmatch(r"\d{15}", new_imei):
                add_activity("WARN", f"Rejected invalid IMEI input for {model}.")
                time.sleep(1)
                continue
            target_dev["imei"] = new_imei
            save_devices(devices_dict)
            add_activity("OK", f"Updated IMEI for {get_device_label(target_dev)}.")
            time.sleep(1)
            return

        if action == "RESUME LAST IMEI":
            target_dev["imei"] = last_imeis[model]
            save_devices(devices_dict)
            add_activity("OK", f"Resumed last IMEI for {get_device_label(target_dev)}.")
            time.sleep(1)
            return

        return

def decrypt_scan_device_firmware(target_dev):
    depth_opts = gui._DECRYPT_DEPTH_OPTIONS
    depth_labels = [opt[0] for opt in depth_opts]

    depth_selection = navigate_menu(
        get_imei_header("Decrypt Firmware"),
        "SELECT DEPTH",
        depth_labels,
        ["BACK"],
    )

    if depth_selection < 0 or depth_selection >= len(depth_opts):
        return None

    selected_depth = depth_opts[depth_selection][1]
    clear_screen()
    console.print(Panel(Align.center(get_imei_header("Decrypt Firmware")), box=box.ROUNDED, style="dim white"))
    console.print(
        Panel(
            f"[bold white]Decrypting {target_dev.get('model')} ({target_dev.get('csc')})[/bold white]\n[dim]Depth: {selected_depth}[/dim]",
            style="blue",
            box=box.ROUNDED,
        )
    )
    add_activity("INFO", f"Decrypting firmware for {get_device_label(target_dev)}.")

    try:
        results = test_firmware_decrypt.decrypt_version_test_md5s(
            target_dev.get("model"),
            target_dev.get("csc"),
            depth=selected_depth,
            progress=lambda msg: console.print(f"[dim]{msg}[/dim]"),
        )
    except Exception as exc:
        add_activity("ERROR", f"Decryption failed for {target_dev.get('model')}: {exc}")
        time.sleep(1)
        return None

    if not results:
        add_activity("WARN", f"No decrypted firmware found for {target_dev.get('model')}.")
        time.sleep(1)
        return None

    versions = list(reversed(test_firmware_decrypt.sort_firmware_versions(list(set(results.values())))))
    items = [[version] for version in versions]
    page_size = 10
    pages = [items[i:i + page_size] for i in range(0, len(items), page_size)]
    current_page = 0

    while True:
        page_items = pages[current_page]
        page_actions = []
        if current_page < len(pages) - 1:
            page_actions.append("NEXT PAGE")
        if current_page > 0:
            page_actions.append("PREV PAGE")
        page_actions.append("BACK")

        selection = navigate_menu(
            get_imei_header("Decrypt Firmware"),
            f"SELECT FIRMWARE (Page {current_page + 1}/{len(pages)})",
            page_items,
            page_actions,
            columns=["Version"],
        )

        if selection == -1:
            return None
        if selection == -2:
            if current_page > 0:
                current_page -= 1
            continue
        if selection == -3:
            if current_page < len(pages) - 1:
                current_page += 1
            continue
        if selection < len(page_items):
            return page_items[selection][0]

        action_idx = selection - len(page_items)
        if action_idx < 0 or action_idx >= len(page_actions):
            continue
        action = page_actions[action_idx]
        if action == "NEXT PAGE" and current_page < len(pages) - 1:
            current_page += 1
        elif action == "PREV PAGE" and current_page > 0:
            current_page -= 1
        else:
            return None

def edit_scan_device_firmware(target_dev, devices_dict):
    model = target_dev.get("model", "Unknown")

    while True:
        library_firmwares = list(reversed(scan.load_fw_library().get(model, [])))
        items = [[fw] for fw in library_firmwares]
        actions = ["MANUAL INPUT", "DECRYPT FIRMWARE", "BACK"]

        selection = navigate_menu(
            get_imei_header("Edit Firmware Base"),
            "SELECT FIRMWARE BASE",
            items,
            actions,
            extra_text=f"Current Base: {target_dev.get('base', 'Not Set')}",
            columns=["Base Firmware"],
        )

        if selection < 0:
            return
        if selection < len(items):
            target_dev["base"] = library_firmwares[selection]
            save_devices(devices_dict)
            add_activity("OK", f"Updated base firmware for {get_device_label(target_dev)} from library.")
            time.sleep(1)
            return

        action_idx = selection - len(items)
        if action_idx < 0 or action_idx >= len(actions):
            continue

        action = actions[action_idx]
        if action == "MANUAL INPUT":
            clear_screen()
            console.print(Panel(Align.center(get_imei_header("Edit Firmware Base")), box=box.ROUNDED, style="dim white"))
            new_base = console.input("[bold yellow]Enter new firmware base[/bold yellow]: ").strip()
            if not new_base:
                continue
            target_dev["base"] = new_base
            save_devices(devices_dict)
            add_activity("OK", f"Updated base firmware for {get_device_label(target_dev)}.")
            time.sleep(1)
            return

        if action == "DECRYPT FIRMWARE":
            selected_firmware = decrypt_scan_device_firmware(target_dev)
            if selected_firmware:
                target_dev["base"] = selected_firmware
                save_devices(devices_dict)
                add_activity("OK", f"Selected decrypted firmware for {get_device_label(target_dev)}.")
                time.sleep(1)
                return
            continue

        return

def manage_scan_device_library(target_dev):
    model = target_dev.get("model", "Unknown")

    while True:
        library_entries = sort_library_entries(scan.load_library(model))
        items = [
            [
                entry.get("imei", "Unknown"),
                entry.get("csc", "UNK"),
                entry.get("fw", "Unknown"),
                entry.get("date", "-"),
            ]
            for entry in library_entries
        ]

        selection = navigate_menu(
            get_imei_header("IMEI Library"),
            f"{model} LIBRARY",
            items if items else ["[yellow]No IMEI records found for this model.[/yellow]"],
            ["BACK"] if items else [],
            columns=["IMEI", "CSC", "Firmware", "Date"] if items else None,
        )

        if not items or selection < 0 or selection == len(items):
            return

        if selection < len(items):
            entry = library_entries[selection]
            info_grid = Table.grid(padding=(0, 2))
            info_grid.add_column(style="bold cyan")
            info_grid.add_column(style="white")
            info_grid.add_row("IMEI", entry.get("imei", "Unknown"))
            info_grid.add_row("CSC", entry.get("csc", "UNK"))
            info_grid.add_row("Firmware", entry.get("fw", "Unknown"))
            info_grid.add_row("Date", entry.get("date", "-"))

            action = navigate_menu(
                get_imei_header("IMEI Library"),
                "ENTRY ACTION",
                [],
                ["USE THIS IMEI", "DELETE ENTRY", "BACK"],
                extra_text=info_grid,
            )

            if action == 0:
                target_dev["imei"] = entry.get("imei", target_dev.get("imei"))
                devices_dict = load_devices()
                for device in flatten_devices(devices_dict):
                    if get_device_key(device) == get_device_key(target_dev):
                        device["imei"] = target_dev["imei"]
                        break
                save_devices(devices_dict)
                add_activity("OK", f"Loaded library IMEI for {get_device_label(target_dev)}.")
                time.sleep(1)
                return

            if action == 1:
                remaining_entries = [item for idx, item in enumerate(library_entries) if idx != selection]
                with open(scan.get_library_path(model), "w", encoding="utf-8") as handle:
                    json.dump(remaining_entries, handle, indent=4)
                add_activity("OK", f"Deleted IMEI library entry for {model}.")
                time.sleep(1)
                continue

def resolve_scan_network_codes(csc):
    csc = (csc or "").upper()
    if csc in {"CHC", "CHM"}:
        return "460", "01"
    if csc == "EUX":
        return "208", "01"
    return "310", "410"

def create_scan_log_markup(result_type, imei, message):
    color = {
        "HIT": "green",
        "VALID": "cyan",
        "INVALID": "red",
        "INFO": "white",
    }.get(result_type, "white")
    timestamp = datetime.now().strftime("%H:%M:%S")
    return f"[dim]{timestamp}[/dim] [{color}]{result_type:<7}[/{color}] {imei} | {message}"

def build_scan_runtime(selected_devices, amount, step, thread_allocations, mode_name):
    runtime = {
        "mode": mode_name,
        "amount": amount,
        "step": step,
        "started_at": time.time(),
        "stop_event": threading.Event(),
        "lock": threading.Lock(),
        "fatal_error": None,
        "totals": {"HIT": 0, "VALID": 0, "INVALID": 0, "processed": 0, "requested": amount * len(selected_devices)},
        "devices": [],
    }

    for index, device in enumerate(selected_devices):
        model = device.get("model")
        csc = device.get("csc")
        base = device.get("base")
        imei = device.get("imei")
        runtime["devices"].append(
            {
                "key": get_device_key(device),
                "name": device.get("name", "Unknown"),
                "model": model,
                "csc": csc,
                "base": base,
                "seed_imei": imei,
                "current_imei": imei,
                "next_imei": scan.increment_imei(imei, step),
                "step": step,
                "threads": thread_allocations[index],
                "target": amount,
                "submitted": 0,
                "processed": 0,
                "active": 0,
                "logs": [],
                "stats": {"HIT": 0, "VALID": 0, "INVALID": 0},
                "status": "Queued",
                "output_file": os.path.join(OUTPUT_DIR, f"IMEI_SCAN_{model}_{csc}.txt"),
            }
        )

    return runtime

def render_scan_device_panel(device_state, runtime):
    elapsed = str(timedelta(seconds=int(time.time() - runtime["started_at"])))
    overall_status = (
        "[red]Fatal 403 detected[/red]"
        if runtime["fatal_error"]
        else ("[yellow]Stopping[/yellow]" if runtime["stop_event"].is_set() else "[green]Running[/green]")
    )

    grid = Table.grid(padding=(0, 2), expand=True)
    grid.add_column(style="bold cyan", ratio=1)
    grid.add_column(style="white", ratio=3)
    grid.add_row("Elapsed", elapsed)
    grid.add_row("Status", overall_status)
    grid.add_row("Device", f"{device_state['name']} ({device_state['model']}/{device_state['csc']})")
    grid.add_row("Base", device_state.get("base", "Not Set") or "Not Set")
    grid.add_row("Current IMEI", device_state.get("current_imei", "Unknown"))
    grid.add_row("Progress", f"{device_state['processed']}/{device_state['target']}")
    grid.add_row("Threads", f"{device_state['active']}/{device_state['threads']}")
    grid.add_row(
        "Results",
        (
            f"[green]HIT {device_state['stats']['HIT']}[/green]   "
            f"[cyan]VALID {device_state['stats']['VALID']}[/cyan]   "
            f"[red]INVALID {device_state['stats']['INVALID']}[/red]"
        ),
    )
    grid.add_row("State", device_state.get("status", "Queued"))

    logs = device_state["logs"][-3:]
    logs_renderable = "\n".join(logs) if logs else "[dim]Waiting for worker activity...[/dim]"

    return Panel(
        Group(
            grid,
            Panel(logs_renderable, title="[bold white]LIVE FEED[/bold white]", border_style="dim white", box=box.ROUNDED),
        ),
        title=f"[bold white]{device_state['model']}[/bold white]",
        border_style="cyan",
        box=box.ROUNDED,
    )

def render_imei_scan_live(runtime):
    header = Panel(Align.center(get_imei_header(runtime["mode"])), box=box.ROUNDED, style="dim white")
    device_panels = [render_scan_device_panel(device_state, runtime) for device_state in runtime["devices"]]
    feed_text = "\n".join(ACTIVITY_LOG) if ACTIVITY_LOG else "[dim]No recent activity.[/dim]"

    return Group(
        header,
        *device_panels,
        Panel(feed_text, title="[bold white]ᛇ ACTIVITY FEED[/bold white]", border_style="dim white", box=box.ROUNDED),
        Panel(
            Align.center("[white on red] R [/white on red] [dim]Stop Scan[/dim]   |   [white on red] Ctrl+C [/white on red] [dim]Abort[/dim]"),
            box=box.ROUNDED,
            border_style="dim white",
        ),
    )

def update_runtime_after_result(runtime, device_state, imei, result_type, message):
    with runtime["lock"]:
        device_state["active"] = max(0, device_state["active"] - 1)
        device_state["processed"] += 1
        device_state["current_imei"] = imei
        device_state["status"] = message
        device_state["stats"][result_type] += 1
        device_state["logs"].append(create_scan_log_markup(result_type, imei, message))
        device_state["logs"] = device_state["logs"][-3:]

        runtime["totals"][result_type] += 1
        runtime["totals"]["processed"] += 1

def run_single_imei_attempt(runtime, device_state, imei):
    if runtime["stop_event"].is_set():
        return

    model = device_state["model"]
    csc = device_state["csc"]
    base = device_state["base"]
    mcc, mnc = resolve_scan_network_codes(csc)

    result_type = "INVALID"
    message = "Unknown failure"

    try:
        scan.save_last_run_imei(model, imei)
        client = scan.Client(
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

        if result == "FATAL_403":
            runtime["fatal_error"] = "FATAL_403"
            runtime["stop_event"].set()
            message = f"{csc} : fatal 403"
            add_activity("ERROR", f"Fatal 403 detected while scanning {get_device_label(device_state)}.")
        elif result in {"VALID_260", "VALID_220", "VALID_261"}:
            result_type = "VALID"
            status_code = result.replace("VALID_", "")
            message = f"{csc} : {status_code} (VALID IMEI but No Update)"
            scan.save_result(device_state["output_file"], f"VALID | {imei} | {csc} | {status_code}")
        elif str(result).startswith("FUMO"):
            result_type = "HIT"
            url = result.split("|", 1)[1]
            details = scan.parse_descripter(url, base)
            found_version = details.get("updateFwV", "Unknown") if details else "Unknown"
            message = f"{csc} : UPDATE FOUND {found_version}"
            scan.save_result(device_state["output_file"], f"HIT | {imei} | {csc} | {found_version} | {url}")
            scan.save_hit_to_json(model, imei, csc, found_version, url)
            add_activity("HIT", f"Update hit for {model} ({csc}) on IMEI {imei}.")
        elif result == "BAD_CSC":
            message = f"{csc} : CSC Rejected"
        elif result == "BAN":
            message = f"{csc} : Auth Banned"
        elif str(result).startswith("HTTP_"):
            message = f"{csc} : {result}"
        elif result == "ERR_AUTH":
            message = f"{csc} : Auth Error"
        else:
            message = f"{csc} : {result}"
    except Exception as exc:
        message = f"{csc} : worker error ({exc})"

    update_runtime_after_result(runtime, device_state, imei, result_type, message)

def submit_next_imei_job(executor, runtime, device_state, futures):
    if runtime["stop_event"].is_set():
        return
    if device_state["submitted"] >= device_state["target"]:
        return

    imei = device_state["next_imei"]
    device_state["next_imei"] = scan.increment_imei(imei, device_state["step"])
    device_state["submitted"] += 1
    device_state["active"] += 1
    device_state["current_imei"] = imei
    device_state["status"] = "Scanning"

    futures.add(executor.submit(run_single_imei_attempt, runtime, device_state, imei))

def finalize_imei_scan(runtime):
    add_activity(
        "OK",
        (
            f"{runtime['mode']} completed: "
            f"HIT {runtime['totals']['HIT']} | "
            f"VALID {runtime['totals']['VALID']} | "
            f"INVALID {runtime['totals']['INVALID']}"
        ),
    )

    for device_state in runtime["devices"]:
        add_activity(
            "INFO",
            (
                f"{device_state['model']} ({device_state['csc']}): "
                f"HIT {device_state['stats']['HIT']} | "
                f"VALID {device_state['stats']['VALID']} | "
                f"INVALID {device_state['stats']['INVALID']}"
            ),
        )

def run_imei_scan(selected_devices, amount, step, thread_allocations, mode_name):
    valid_devices = []
    for device in selected_devices:
        if not device.get("model") or not device.get("csc") or not device.get("base") or not device.get("imei"):
            add_activity("WARN", f"Skipped invalid device config for {get_device_label(device)}.")
            continue
        valid_devices.append(dict(device))

    if not valid_devices:
        add_activity("ERROR", "No valid devices available for IMEI scanning.")
        time.sleep(1)
        return

    runtime = build_scan_runtime(valid_devices, amount, step, thread_allocations, mode_name)
    add_activity("INFO", f"{mode_name} started for {len(valid_devices)} device(s).")

    futures = set()

    with ThreadPoolExecutor(max_workers=IMEI_SCAN_THREAD_LIMIT) as executor:
        for device_state in runtime["devices"]:
            for _ in range(min(device_state["threads"], device_state["target"])):
                submit_next_imei_job(executor, runtime, device_state, futures)

        with Live(render_imei_scan_live(runtime), refresh_per_second=6, screen=False, console=console) as live:
            try:
                while futures:
                    if check_stop_key() and not runtime["stop_event"].is_set():
                        runtime["stop_event"].set()
                        add_activity("WARN", "IMEI scan stop requested by user.")

                    done, _ = wait(futures, timeout=0.15)
                    for future in done:
                        futures.discard(future)
                        try:
                            future.result()
                        except Exception as exc:
                            add_activity("ERROR", f"IMEI scan worker failed: {exc}")

                    if not runtime["stop_event"].is_set():
                        for device_state in runtime["devices"]:
                            while (
                                device_state["active"] < device_state["threads"]
                                and device_state["submitted"] < device_state["target"]
                            ):
                                submit_next_imei_job(executor, runtime, device_state, futures)

                    live.update(render_imei_scan_live(runtime))
            except KeyboardInterrupt:
                runtime["stop_event"].set()
                add_activity("WARN", "IMEI scan interrupted by keyboard.")
                wait(futures)

            live.update(render_imei_scan_live(runtime))

    finalize_imei_scan(runtime)
    clear_screen()
    console.print(render_imei_scan_live(runtime))
    console.print("[dim]Press Enter to return to the IMEI Scanner menu...[/dim]")
    input()

def allocate_multi_scan_threads(device_count):
    base_threads = IMEI_SCAN_THREAD_LIMIT // device_count
    remainder = IMEI_SCAN_THREAD_LIMIT % device_count
    allocations = []
    for index in range(device_count):
        allocations.append(base_threads + (1 if index < remainder else 0))
    return allocations

def run_single_imei_scanner():
    devices_dict = load_devices()
    target_dev = select_device_entry(get_imei_header("Single Scan"), "SELECT DEVICE", devices_dict)
    if not target_dev:
        return

    while True:
        info_renderable = get_device_info_renderable(target_dev)
        selection = navigate_menu(
            get_imei_header("Single Scan"),
            "DEVICE INFO",
            [],
            ["EDIT IMEI", "EDIT FIRMWARE BASE", "START SCANNING", "IMEI LIBRARY", "BACK"],
            extra_text=info_renderable,
        )

        if selection < 0 or selection == 4:
            return
        if selection == 0:
            edit_scan_device_imei(target_dev, devices_dict)
        elif selection == 1:
            edit_scan_device_firmware(target_dev, devices_dict)
        elif selection == 2:
            settings = prompt_imei_scan_settings("Single Scan")
            if settings:
                run_imei_scan([target_dev], settings["amount"], settings["step"], [settings["threads"]], "Single Scan")
        elif selection == 3:
            manage_scan_device_library(target_dev)

def run_multi_imei_scanner():
    devices_dict = load_devices()
    selected_devices = []
    excluded_keys = set()

    while len(selected_devices) < IMEI_MULTI_DEVICE_LIMIT:
        target_dev = select_device_entry(
            get_imei_header("Multi Scan"),
            f"SELECT DEVICE {len(selected_devices) + 1}/{IMEI_MULTI_DEVICE_LIMIT}",
            devices_dict,
            excluded_keys=excluded_keys,
        )

        if not target_dev:
            return

        selected_devices.append(dict(target_dev))
        excluded_keys.add(get_device_key(target_dev))
        add_activity("INFO", f"Added {get_device_label(target_dev)} to multi-scan queue.")

        if len(selected_devices) >= IMEI_MULTI_DEVICE_LIMIT:
            break

        selected_text = "\n".join(f"• {get_device_label(device)}" for device in selected_devices)
        selection = navigate_menu(
            get_imei_header("Multi Scan"),
            "QUEUE READY",
            [],
            ["ADD ANOTHER DEVICE", "START MULTI SCAN", "CLEAR SELECTION", "BACK"],
            extra_text=selected_text,
        )

        if selection < 0:
            return
        if selection == 0:
            continue
        if selection == 1:
            break
        if selection == 2:
            selected_devices = []
            excluded_keys = set()
            add_activity("INFO", "Cleared multi-scan device queue.")
            continue
        return

    settings = prompt_imei_scan_settings("Multi Scan", thread_limit=IMEI_SCAN_THREAD_LIMIT)
    if not settings:
        return

    allocations = allocate_multi_scan_threads(len(selected_devices))
    allocation_text = ", ".join(
        f"{selected_devices[idx].get('model')}={allocations[idx]}T" for idx in range(len(selected_devices))
    )
    add_activity("INFO", f"Multi-scan thread allocation: {allocation_text}.")
    run_imei_scan(selected_devices, settings["amount"], settings["step"], allocations, "Multi Scan")

def show_recent_revelations():
    add_activity("INFO", "Accessing System Logs.")
    show_transition("Fetching Logs...", 0.5)

    header_text = f"[bold red]ᚳ Project Valhalla[/bold red]  [bold cyan]- KinZoKu[/bold cyan]  [dim]v{VERSION}[/dim]  |  [yellow]Recent Revelations[/yellow]"

    if not os.path.exists(LOG_FILE):
        navigate_menu(header_text, "RECENTLY REVEALED LIST", ["[yellow]You havent checked any devices yet.[/yellow]"], [])
        return

    try:
        with open(LOG_FILE, 'r') as f:
            logs = json.load(f)

        logs.sort(key=lambda x: x['timestamp'], reverse=True)

        devices_dict = load_devices()
        flat_list = []
        for cat, devs in devices_dict.items():
            flat_list.extend(devs)

        log_items = []
        for l in logs:
            msg = l['message']
            log_items.append([f"[dim]{l['timestamp']}[/dim]", msg])

        page_size = 10
        pages = [log_items[i:i + page_size] for i in range(0, len(log_items), page_size)]
        current_page = 0

        while True:
            if not pages:
                navigate_menu(header_text, "RECENTLY REVEALED LIST", ["[yellow]No logs found.[/yellow]"], [])
                break

            page_items = pages[current_page]
            title = f"RECENTLY REVEALED (Page {current_page+1}/{len(pages)})"
            r_sel = navigate_menu(header_text, title, page_items, [], columns=["Time", "Message"])

            if r_sel == -1:
                break
            if r_sel == -2:
                if current_page > 0:
                    current_page -= 1
                continue
            if r_sel == -3:
                if current_page < len(pages) - 1:
                    current_page += 1
                continue

            if r_sel < len(page_items):
                show_transition("Loading Entry...", 0.3)
                real_idx = (current_page * page_size) + r_sel
                selected_log = logs[real_idx]

                m = re.search(r"Update found for (.*?) \((.*?)\): (.*)", selected_log['message'])
                target_dev = None
                model, csc, ver = "Unknown", "UNK", "Unknown"

                if m:
                    model, csc, ver = m.groups()
                    for d in flat_list:
                        if d.get('model') == model and d.get('csc') == csc:
                            target_dev = d
                            break

                if not target_dev:
                    target_dev = {'name': 'Unknown', 'model': model, 'csc': csc, 'imei': 'Unknown', 'base': 'Unknown'}

                info_renderable = get_device_info_renderable(target_dev)
                log_time_text = Text(f"Log Time: {selected_log['timestamp']}\n\n", style="dim", justify="center")
                full_info = Group(log_time_text, info_renderable)

                while True:
                    actions = ["RENOTIFY ON TELEGRAM"] if m else []
                    sel_action = navigate_menu(header_text, "LOG INFO", [], actions, extra_text=full_info)

                    if sel_action == 0 and m:
                        show_transition("Sending Notification...", 0.5)
                        update_info = {
                            "name": target_dev.get('name', 'Unknown'),
                            "model": model,
                            "csc": csc,
                            "new_ver": ver,
                        }
                        renotify([update_info])
                        add_activity("OK", f"Resent Telegram notification for {model}.")
                        time.sleep(1)
                        break
                    if sel_action == -1:
                        break
    except Exception as e:
        navigate_menu(header_text, "RECENTLY REVEALED LIST", [f"[red]Error reading history: {e}[/red]"], [])

def get_dashboard_menus():
    return {
        "root": {
            "title": "MAIN MENU",
            "options": [
                ("FOTA Scanner", "open_fota"),
                ("IMEI Scanner", "open_imei"),
                ("Recent Revelations (Logs)", "open_logs"),
                ("Device Vault", "open_vault"),
                ("Integration Status Check", "open_status"),
                ("Launch Telegram Bots", "open_telegram_bots"),
                ("Exit", "open_exit"),
            ],
        },
        "fota": {
            "title": "FOTA SCANNER",
            "options": [
                ("Single Check", "run_single_check"),
                ("Batch Check", "run_batch_check"),
                ("Back to Main Menu", "back_to_root"),
            ],
        },
        "imei": {
            "title": "IMEI SCANNER",
            "options": [
                ("Single Scan", "run_single_imei_scanner"),
                ("Multi Scan", "run_multi_imei_scanner"),
                ("Back to Main Menu", "back_to_root"),
            ],
        },
        "logs": {
            "title": "RECENT REVELATIONS",
            "options": [
                ("Open Logs", "show_recent_revelations"),
                ("Back to Main Menu", "back_to_root"),
            ],
        },
        "vault": {
            "title": "DEVICE VAULT",
            "options": [
                ("Open Vault", "show_device_vault"),
                ("Back to Main Menu", "back_to_root"),
            ],
        },
        "status": {
            "title": "INTEGRATION STATUS",
            "options": [
                ("Refresh Status Check", "refresh_status_checks"),
                ("Back to Main Menu", "back_to_root"),
            ],
        },
        "telegram_bots": {
            "title": "LAUNCH TELEGRAM BOTS",
            "options": [
                ("Update Check Bot", "launch_update_check_bot"),
                ("OTA Fetching Bot", "launch_ota_fetching_bot"),
                ("Back to Main Menu", "back_to_root"),
            ],
        },
        "exit": {
            "title": "EXIT",
            "options": [
                ("Exit Program", "exit_program"),
                ("Back to Main Menu", "back_to_root"),
            ],
        },
    }

def main():
    menus = get_dashboard_menus()
    current_menu = "root"
    selected = 0

    show_transition("Initialising System...", 3)
    add_activity("INFO", "Dashboard menu loaded. Please run status checks to initiate the system check.")

    while True:
        clear_screen()
        action_triggered = None
        menu_config = menus[current_menu]
        menu_labels = [label for label, _ in menu_config["options"]]

        with Live(
            draw_main_dashboard(selected, menu_labels, operations_title=menu_config["title"], allow_back=(current_menu != "root")),
            refresh_per_second=4,
            screen=False
        ) as live:
            while True:
                live.update(draw_main_dashboard(selected, menu_labels, operations_title=menu_config["title"], allow_back=(current_menu != "root")))
                key = get_key_timeout(0.25)

                if key:
                    if key == 'UP':
                        selected = (selected - 1) % len(menu_labels)
                    elif key == 'DOWN':
                        selected = (selected + 1) % len(menu_labels)
                    elif key == 'q':
                        action_triggered = 'BACK' if current_menu != "root" else 'QUIT'
                        break
                    elif key == 'ENTER':
                        action_triggered = 'SELECT'
                        break

        if action_triggered == 'QUIT':
            clear_screen()
            sys.exit(0)

        if action_triggered == 'BACK':
            current_menu = "root"
            selected = 0
            add_activity("INFO", "Returned to main menu.")
            continue

        if action_triggered != 'SELECT':
            continue

        action = menu_config["options"][selected][1]

        if action == "open_fota":
            current_menu = "fota"
            selected = 0
            add_activity("INFO", "Opened FOTA Scanner menu.")
        elif action == "open_imei":
            current_menu = "imei"
            selected = 0
            add_activity("INFO", "Opened IMEI Scanner menu.")
        elif action == "open_logs":
            current_menu = "logs"
            selected = 0
            add_activity("INFO", "Opened Recent Revelations menu.")
        elif action == "open_vault":
            current_menu = "vault"
            selected = 0
            add_activity("INFO", "Opened Device Vault menu.")
        elif action == "open_status":
            current_menu = "status"
            selected = 0
            add_activity("INFO", "Opened Integration Status menu.")
        elif action == "open_telegram_bots":
            current_menu = "telegram_bots"
            selected = 0
            add_activity("INFO", "Opened Telegram Bots launcher.")
        elif action == "open_exit":
            current_menu = "exit"
            selected = 0
        elif action == "back_to_root":
            current_menu = "root"
            selected = 0
            add_activity("INFO", "Returned to main menu.")
        elif action == "run_single_check":
            add_activity("INFO", "Opening Targeted Scan Menu.")
            next_menu = run_single_check()
            if next_menu == "root":
                current_menu = "root"
                selected = 0
        elif action == "run_batch_check":
            add_activity("INFO", "Opening FOTA Scanner Mode.")
            show_batch_results(run_batch_check())
        elif action == "run_single_imei_scanner":
            add_activity("INFO", "Opening IMEI Single Scan.")
            run_single_imei_scanner()
        elif action == "run_multi_imei_scanner":
            add_activity("INFO", "Opening IMEI Multi Scan.")
            run_multi_imei_scanner()
        elif action == "show_recent_revelations":
            show_recent_revelations()
        elif action == "show_device_vault":
            add_activity("INFO", "Opening Device Vault.")
            view_device_status()
        elif action == "refresh_status_checks":
            add_activity("INFO", "Refreshing system and integration statuses...")
            show_transition("Pinging Servers...", 8)
            refresh_status_checks()
            add_activity("OK", "Status checks updated successfully.")
        elif action == "launch_update_check_bot":
            add_activity("INFO", "Launching Update Check Bot...")
            show_transition("Launching Update Check Bot...", 0.5)
            launch_background_script("telegram_update_check_bot.py", "Update Check Bot")
        elif action == "launch_ota_fetching_bot":
            add_activity("INFO", "Launching OTA Fetching Bot...")
            show_transition("Launching OTA Fetching Bot...", 0.5)
            launch_background_script("telegram_bot.py", "OTA Fetching Bot")
        elif action == "exit_program":
            clear_screen()
            sys.exit(0)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        clear_screen()
        sys.exit(0)
