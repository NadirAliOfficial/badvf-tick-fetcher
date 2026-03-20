#!/usr/bin/env python3
"""
BADVF Tick Fetcher — Desktop App
Trades via Polygon.io + Bid/Ask via IBKR TWS
"""

import csv
import json
import os
import threading
import time
import requests
import tkinter as tk
from tkinter import ttk, scrolledtext
from datetime import datetime, timezone, timedelta

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

# ── Config ───────────────────────────────────────────────────────────────────
SYMBOL     = "BADVF"
HOST       = "127.0.0.1"
PORT       = 4001
OUTPUT_DIR = "output"
BATCH_SIZE = 1000
DAYS_BACK  = 30
CONFIG_FILE = "config.json"
# ─────────────────────────────────────────────────────────────────────────────

DARK_BG    = "#0f1117"
CARD_BG    = "#1a1d27"
BORDER     = "#2a2d3a"
ACCENT     = "#4f8ef7"
GREEN      = "#22c55e"
RED        = "#ef4444"
YELLOW     = "#f59e0b"
TEXT       = "#e2e8f0"
MUTED      = "#64748b"
FONT_MAIN  = ("Segoe UI", 10)
FONT_BOLD  = ("Segoe UI", 10, "bold")
FONT_TITLE = ("Segoe UI", 13, "bold")
FONT_MONO  = ("Consolas", 9)


# ══════════════════════════════════════════════════════════════════════════════
#  Config persistence
# ══════════════════════════════════════════════════════════════════════════════

def load_config() -> dict:
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"polygon_api_key": ""}


def save_config(cfg: dict):
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)


# ══════════════════════════════════════════════════════════════════════════════
#  IBKR
# ══════════════════════════════════════════════════════════════════════════════

class IBKRApp(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.batch = []
        self._done = threading.Event()
        self._req_id = 1

    def nextValidId(self, _orderId):
        pass

    def historicalTicksBidAsk(self, _reqId, ticks, done):
        for t in ticks:
            self.batch.append({
                "timestamp": datetime.fromtimestamp(t.time).strftime("%Y-%m-%d %H:%M:%S"),
                "bid_price": t.priceBid,
                "ask_price": t.priceAsk,
                "bid_size":  t.sizeBid,
                "ask_size":  t.sizeAsk,
            })
        if done:
            self._done.set()

    def error(self, _reqId, errorCode, _errorString, advancedOrderRejectJson=""):
        if errorCode in (2104, 2106, 2158):
            return
        if errorCode not in (162,):
            self._done.set()


def fetch_ibkr_bidask(cutoff_utc, log, port=PORT) -> list:
    client_id = int(time.time()) % 9000 + 1000
    app = IBKRApp()
    try:
        app.connect(HOST, port, client_id)
    except Exception:
        log("  Could not connect to TWS. Make sure TWS is open and logged in.", "error")
        return []

    threading.Thread(target=app.run, daemon=True).start()
    time.sleep(2)

    contract = Contract()
    contract.symbol      = SYMBOL
    contract.secType     = "STK"
    contract.exchange    = "SMART"
    contract.currency    = "USD"
    contract.primaryExch = "PINK"

    all_ticks = []
    end_dt    = datetime.now(timezone.utc)

    while end_dt > cutoff_utc:
        app.batch = []
        app._done.clear()
        app._req_id += 1

        app.reqHistoricalTicks(
            reqId=app._req_id, contract=contract,
            startDateTime="", endDateTime=end_dt.strftime("%Y%m%d-%H:%M:%S"),
            numberOfTicks=BATCH_SIZE, whatToShow="BID_ASK",
            useRth=0, ignoreSize=False, miscOptions=[],
        )
        app._done.wait(timeout=60)

        if not app.batch:
            break

        cutoff_str = cutoff_utc.strftime("%Y-%m-%d %H:%M:%S")
        valid      = [r for r in app.batch if r["timestamp"] >= cutoff_str]
        all_ticks  = valid + all_ticks

        earliest_ts = min(r["timestamp"] for r in app.batch)
        earliest_dt = datetime.strptime(earliest_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

        if earliest_dt <= cutoff_utc or len(app.batch) < BATCH_SIZE:
            break

        end_dt = earliest_dt - timedelta(seconds=1)
        time.sleep(0.5)

    app.disconnect()
    return all_ticks


# ══════════════════════════════════════════════════════════════════════════════
#  Polygon
# ══════════════════════════════════════════════════════════════════════════════

def fetch_polygon_trades(cutoff_utc, api_key, log) -> list:
    if not api_key:
        log("  No Polygon API key set. Go to Settings and add your key.", "error")
        return []

    all_trades = []
    start_ns   = int(cutoff_utc.timestamp() * 1_000_000_000)
    end_ns     = int(datetime.now(timezone.utc).timestamp() * 1_000_000_000)
    url        = f"https://api.polygon.io/v3/trades/{SYMBOL}"

    params = {
        "timestamp.gte": start_ns,
        "timestamp.lte": end_ns,
        "limit":         50000,
        "sort":          "timestamp",
        "order":         "asc",
        "apiKey":        api_key,
    }

    while url:
        try:
            resp = requests.get(url, params=params, timeout=30)
        except requests.ConnectionError:
            log("  No internet connection. Check your network and try again.", "error")
            break
        except requests.Timeout:
            log("  Polygon request timed out. Try again in a moment.", "error")
            break

        if resp.status_code == 403:
            log("  Polygon API key does not have access to OTC data.", "error")
            log("  Make sure you are on the Starter plan or higher.", "warn")
            break
        elif resp.status_code == 401:
            log("  Invalid Polygon API key. Go to Settings and update it.", "error")
            break
        elif resp.status_code != 200:
            log(f"  Polygon returned an unexpected error ({resp.status_code}). Try again later.", "error")
            break

        data    = resp.json()
        results = data.get("results", [])

        for t in results:
            ts_ns = t.get("participant_timestamp") or t.get("sip_timestamp", 0)
            ts    = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
            all_trades.append({
                "timestamp":  ts.strftime("%Y-%m-%d %H:%M:%S"),
                "price":      t.get("price"),
                "size":       t.get("size"),
                "exchange":   t.get("exchange"),
                "conditions": ",".join(str(c) for c in t.get("conditions", [])),
            })

        next_url = data.get("next_url")
        if next_url:
            url    = next_url
            params = {"apiKey": api_key}
        else:
            break

    return all_trades


# ══════════════════════════════════════════════════════════════════════════════
#  CSV
# ══════════════════════════════════════════════════════════════════════════════

def save_csv(rows, filepath):
    if not rows:
        return
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def append_to_master(rows, filepath):
    if not rows:
        return
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    file_exists = os.path.isfile(filepath)
    with open(filepath, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


# ══════════════════════════════════════════════════════════════════════════════
#  GUI
# ══════════════════════════════════════════════════════════════════════════════

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("BADVF Tick Fetcher")
        self.geometry("640x600")
        self.resizable(False, False)
        self.configure(bg=DARK_BG)
        self.cfg = load_config()
        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # ── UI build ──────────────────────────────────────────────────────────────

    def _build_ui(self):
        # Header
        hdr = tk.Frame(self, bg=CARD_BG, pady=14)
        hdr.pack(fill="x")
        tk.Label(hdr, text="BADVF  Tick Fetcher", font=FONT_TITLE,
                 bg=CARD_BG, fg=TEXT).pack()
        tk.Label(hdr, text="Trades · Bid/Ask · 30-Day History · CSV Export",
                 font=FONT_MAIN, bg=CARD_BG, fg=MUTED).pack(pady=(2, 0))

        # Stat cards
        cards = tk.Frame(self, bg=DARK_BG, pady=10)
        cards.pack(fill="x", padx=20)
        self._trade_var  = tk.StringVar(value="—")
        self._bidask_var = tk.StringVar(value="—")
        self._make_card(cards, "Trade Ticks",   self._trade_var).pack(side="left", expand=True, fill="x", padx=(0, 6))
        self._make_card(cards, "Bid/Ask Ticks", self._bidask_var).pack(side="left", expand=True, fill="x", padx=(6, 0))

        # Settings panel
        cfg_frame = tk.Frame(self, bg=CARD_BG, padx=16, pady=10)
        cfg_frame.pack(fill="x", padx=20, pady=(0, 8))

        tk.Label(cfg_frame, text="Polygon API Key", font=FONT_BOLD,
                 bg=CARD_BG, fg=MUTED).pack(anchor="w")

        key_row = tk.Frame(cfg_frame, bg=CARD_BG)
        key_row.pack(fill="x", pady=(4, 0))

        self.key_var = tk.StringVar(value=self.cfg.get("polygon_api_key", ""))
        self.key_entry = tk.Entry(
            key_row, textvariable=self.key_var, font=FONT_MONO,
            bg="#0f1117", fg=TEXT, insertbackground=TEXT,
            relief="flat", bd=0, show="•",
        )
        self.key_entry.pack(side="left", fill="x", expand=True,
                            ipady=6, ipadx=8)

        self.show_btn = tk.Button(
            key_row, text="Show", font=FONT_MAIN,
            bg=BORDER, fg=MUTED, relief="flat", cursor="hand2",
            padx=10, command=self._toggle_key_visibility,
        )
        self.show_btn.pack(side="left", padx=(6, 0))

        save_btn = tk.Button(
            key_row, text="Save", font=FONT_BOLD,
            bg=ACCENT, fg="white", relief="flat", cursor="hand2",
            padx=12, command=self._save_key,
        )
        save_btn.pack(side="left", padx=(6, 0))

        self.key_status = tk.Label(cfg_frame, text="", font=("Segoe UI", 9),
                                   bg=CARD_BG, fg=GREEN)
        self.key_status.pack(anchor="w", pady=(4, 0))

        # TWS Port
        port_row = tk.Frame(cfg_frame, bg=CARD_BG)
        port_row.pack(fill="x", pady=(10, 0))

        tk.Label(port_row, text="TWS Port", font=FONT_BOLD,
                 bg=CARD_BG, fg=MUTED, width=12, anchor="w").pack(side="left")

        self.port_var = tk.StringVar(value=str(self.cfg.get("tws_port", PORT)))
        port_entry = tk.Entry(
            port_row, textvariable=self.port_var, font=FONT_MONO,
            bg="#0f1117", fg=TEXT, insertbackground=TEXT,
            relief="flat", bd=0, width=8,
        )
        port_entry.pack(side="left", ipady=6, ipadx=8)

        port_save = tk.Button(
            port_row, text="Save", font=FONT_BOLD,
            bg=ACCENT, fg="white", relief="flat", cursor="hand2",
            padx=12, command=self._save_port,
        )
        port_save.pack(side="left", padx=(6, 0))

        self.port_status = tk.Label(cfg_frame, text="", font=("Segoe UI", 9),
                                    bg=CARD_BG, fg=GREEN)
        self.port_status.pack(anchor="w", pady=(4, 0))

        # Log
        log_frame = tk.Frame(self, bg=DARK_BG)
        log_frame.pack(fill="both", expand=True, padx=20)
        tk.Label(log_frame, text="Activity Log", font=FONT_BOLD,
                 bg=DARK_BG, fg=MUTED, anchor="w").pack(fill="x", pady=(0, 4))
        self.log_box = scrolledtext.ScrolledText(
            log_frame, height=12, font=FONT_MONO,
            bg=CARD_BG, fg=TEXT, insertbackground=TEXT,
            relief="flat", borderwidth=0, state="disabled", wrap="word",
        )
        self.log_box.pack(fill="both", expand=True)
        self.log_box.tag_config("ok",    foreground=GREEN)
        self.log_box.tag_config("error", foreground=RED)
        self.log_box.tag_config("warn",  foreground=YELLOW)
        self.log_box.tag_config("muted", foreground=MUTED)

        # Progress + bottom bar
        pb_frame = tk.Frame(self, bg=DARK_BG)
        pb_frame.pack(fill="x", padx=20, pady=(8, 0))
        self.progress = ttk.Progressbar(pb_frame, mode="indeterminate", length=600)
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TProgressbar", troughcolor=CARD_BG, background=ACCENT, thickness=4)
        self.progress.pack(fill="x")

        bottom = tk.Frame(self, bg=DARK_BG, pady=12)
        bottom.pack(fill="x", padx=20)
        self._status_var = tk.StringVar(value="Ready")
        self.status_lbl = tk.Label(bottom, textvariable=self._status_var,
                                   font=FONT_MAIN, bg=DARK_BG, fg=MUTED, anchor="w")
        self.status_lbl.pack(side="left")
        self.run_btn = tk.Button(
            bottom, text="  Run Now  ", font=FONT_BOLD,
            bg=ACCENT, fg="white", relief="flat", cursor="hand2",
            activebackground="#3a7bd5", activeforeground="white",
            padx=16, pady=6, command=self.start_run,
        )
        self.run_btn.pack(side="right")

    def _make_card(self, parent, label, var):
        f = tk.Frame(parent, bg=CARD_BG, padx=16, pady=10)
        tk.Label(f, text=label, font=FONT_MAIN, bg=CARD_BG, fg=MUTED).pack(anchor="w")
        tk.Label(f, textvariable=var, font=("Segoe UI", 20, "bold"),
                 bg=CARD_BG, fg=ACCENT).pack(anchor="w")
        return f

    # ── Settings actions ──────────────────────────────────────────────────────

    def _toggle_key_visibility(self):
        if self.key_entry.cget("show") == "•":
            self.key_entry.config(show="")
            self.show_btn.config(text="Hide")
        else:
            self.key_entry.config(show="•")
            self.show_btn.config(text="Show")

    def _save_key(self):
        key = self.key_var.get().strip()
        self.cfg["polygon_api_key"] = key
        save_config(self.cfg)
        self.key_status.config(text="✓ API key saved", fg=GREEN)
        self.after(3000, lambda: self.key_status.config(text=""))

    def _save_port(self):
        port = self.port_var.get().strip()
        if not port.isdigit():
            self.port_status.config(text="✗ Port must be a number", fg=RED)
            self.after(3000, lambda: self.port_status.config(text=""))
            return
        self.cfg["tws_port"] = int(port)
        save_config(self.cfg)
        self.port_status.config(text="✓ Port saved", fg=GREEN)
        self.after(3000, lambda: self.port_status.config(text=""))

    # ── Log helpers ───────────────────────────────────────────────────────────

    def log(self, msg, tag=None):
        self.log_box.configure(state="normal")
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_box.insert("end", f"[{ts}] {msg}\n", tag or "")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def set_status(self, msg, color=MUTED):
        self._status_var.set(msg)
        self.status_lbl.configure(fg=color)

    # ── Run ───────────────────────────────────────────────────────────────────

    def start_run(self):
        self.run_btn.configure(state="disabled", bg=MUTED)
        self.progress.start(12)
        self.set_status("Running...", ACCENT)
        threading.Thread(target=self._run_fetch, daemon=True).start()

    def _run_fetch(self):
        today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cutoff  = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
        api_key = self.cfg.get("polygon_api_key", "").strip()
        port    = int(self.cfg.get("tws_port", PORT))

        self.log(f"Starting fetch for {SYMBOL} — last {DAYS_BACK} days", "muted")

        # Polygon trades
        self.log("Fetching trade ticks from Polygon.io...")
        trades = fetch_polygon_trades(cutoff, api_key, self.log)
        self._trade_var.set(f"{len(trades):,}")
        if trades:
            self.log(f"  {len(trades):,} trade ticks received", "ok")
        else:
            self.log("  0 trade ticks — see above for details", "warn")

        # IBKR bid/ask
        self.log("Fetching bid/ask ticks from IBKR TWS...")
        bidasks = fetch_ibkr_bidask(cutoff, self.log, port)
        self._bidask_var.set(f"{len(bidasks):,}")
        if bidasks:
            self.log(f"  {len(bidasks):,} bid/ask ticks received", "ok")
        else:
            self.log("  0 bid/ask ticks — make sure TWS is open and API is enabled on port 4001", "warn")

        # Save
        self.log("Saving CSV files...")
        save_csv(trades,  f"{OUTPUT_DIR}/{SYMBOL}_trades_{today}.csv")
        save_csv(bidasks, f"{OUTPUT_DIR}/{SYMBOL}_bidask_{today}.csv")
        append_to_master(trades,  f"{OUTPUT_DIR}/{SYMBOL}_trades_ALL.csv")
        append_to_master(bidasks, f"{OUTPUT_DIR}/{SYMBOL}_bidask_ALL.csv")

        if trades or bidasks:
            self.log(f"Files saved to output/ folder", "ok")
            self.log(f"  {SYMBOL}_trades_ALL.csv  →  import into Google Sheets", "muted")
            self.log(f"  {SYMBOL}_bidask_ALL.csv  →  import into Google Sheets", "muted")
        self.log("Done!", "ok")

        self.progress.stop()
        self.run_btn.configure(state="normal", bg=ACCENT)
        self.set_status(f"Last run: {datetime.now().strftime('%H:%M:%S')}", GREEN)

    def on_close(self):
        self.destroy()


if __name__ == "__main__":
    app = App()
    app.mainloop()
