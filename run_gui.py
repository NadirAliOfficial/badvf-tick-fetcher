#!/usr/bin/env python3
"""
BADVF Tick Fetcher — Desktop App
Trades via Polygon.io (or yfinance fallback) + Bid/Ask via IBKR TWS
"""

import csv
import json
import os
import platform
import subprocess
import sys
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
SYMBOL_CSE = "BAD.CN"
HOST       = "127.0.0.1"
PORT       = 4001
OUTPUT_DIR = "output"
BATCH_SIZE = 1000
DAYS_BACK  = 30
CONFIG_FILE = "config.json"
IS_MAC     = platform.system() == "Darwin"
# ─────────────────────────────────────────────────────────────────────────────

# Cross-platform fonts
if IS_MAC:
    FONT_MAIN  = ("SF Pro Text", 11)
    FONT_BOLD  = ("SF Pro Text", 11, "bold")
    FONT_TITLE = ("SF Pro Display", 14, "bold")
    FONT_MONO  = ("Menlo", 10)
    FONT_SM    = ("SF Pro Text", 10)
else:
    FONT_MAIN  = ("Segoe UI", 10)
    FONT_BOLD  = ("Segoe UI", 10, "bold")
    FONT_TITLE = ("Segoe UI", 13, "bold")
    FONT_MONO  = ("Consolas", 9)
    FONT_SM    = ("Segoe UI", 9)

DARK_BG = "#0f1117"
CARD_BG = "#1a1d27"
BORDER  = "#2a2d3a"
ACCENT  = "#4f8ef7"
GREEN   = "#22c55e"
RED     = "#ef4444"
YELLOW  = "#f59e0b"
TEXT    = "#e2e8f0"
MUTED   = "#64748b"


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
#  IBKR — Bid/Ask
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
#  Polygon — Trade ticks
# ══════════════════════════════════════════════════════════════════════════════

def fetch_polygon_trades(cutoff_utc, api_key, log) -> list:
    if not api_key:
        return []

    all_trades = []
    start_date = cutoff_utc.strftime("%Y-%m-%d")
    end_date   = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # 1-minute bars for the full 30-day range
    log("  Fetching 1-minute bars from Polygon...", "muted")
    url = f"https://api.polygon.io/v2/aggs/ticker/{SYMBOL}/range/1/minute/{start_date}/{end_date}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": api_key}

    try:
        resp = requests.get(url, params=params, timeout=30)
    except (requests.ConnectionError, requests.Timeout):
        log("  Polygon: network error, trying yfinance fallback...", "warn")
        return []

    if resp.status_code in (401, 403):
        log("  Polygon: access denied, trying yfinance fallback...", "warn")
        return []
    elif resp.status_code != 200:
        log(f"  Polygon: error {resp.status_code}, trying yfinance fallback...", "warn")
        return []

    data    = resp.json()
    results = data.get("results", [])

    for bar in results:
        ts = datetime.fromtimestamp(bar["t"] / 1000, tz=timezone.utc)
        all_trades.append({
            "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
            "open":      bar.get("o"),
            "high":      bar.get("h"),
            "low":       bar.get("l"),
            "close":     bar.get("c"),
            "volume":    bar.get("v"),
            "vwap":      bar.get("vw"),
            "trades":    bar.get("n"),
        })

    if not all_trades:
        # Fall back to daily bars
        log("  No 1-min data, trying daily bars...", "muted")
        url = f"https://api.polygon.io/v2/aggs/ticker/{SYMBOL}/range/1/day/{start_date}/{end_date}"
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                for bar in resp.json().get("results", []):
                    ts = datetime.fromtimestamp(bar["t"] / 1000, tz=timezone.utc)
                    all_trades.append({
                        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                        "open":      bar.get("o"),
                        "high":      bar.get("h"),
                        "low":       bar.get("l"),
                        "close":     bar.get("c"),
                        "volume":    bar.get("v"),
                        "vwap":      bar.get("vw"),
                        "trades":    bar.get("n"),
                    })
        except Exception:
            pass

    return all_trades


# ══════════════════════════════════════════════════════════════════════════════
#  yfinance fallback — 1-min bars (free, no key needed)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_yfinance_trades(log) -> list:
    try:
        import yfinance as yf
    except ImportError:
        log("  Installing yfinance (one time only)...", "muted")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance", "-q"])
        import yfinance as yf

    ticker = yf.Ticker(SYMBOL)
    rows = []

    # 1-min bars: yfinance allows max 8 days per request
    log("  Fetching 1-min bars (last 8 days)...", "muted")
    try:
        df = ticker.history(period="7d", interval="1m")
    except Exception:
        df = None

    if df is not None and not df.empty:
        for ts, row in df.iterrows():
            rows.append({
                "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "open":      float(round(row.get("Open", 0), 6)),
                "high":      float(round(row.get("High", 0), 6)),
                "low":       float(round(row.get("Low", 0), 6)),
                "close":     float(round(row.get("Close", 0), 6)),
                "volume":    int(row.get("Volume", 0)),
            })

    # Daily bars for the full 30 days
    log("  Fetching daily bars (last 30 days)...", "muted")
    try:
        df_daily = ticker.history(period="30d", interval="1d")
    except Exception:
        df_daily = None

    if df_daily is not None and not df_daily.empty:
        existing_dates = {r["timestamp"][:10] for r in rows}
        for ts, row in df_daily.iterrows():
            date_str = ts.strftime("%Y-%m-%d")
            if date_str not in existing_dates:
                rows.append({
                    "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "open":      float(round(row.get("Open", 0), 6)),
                    "high":      float(round(row.get("High", 0), 6)),
                    "low":       float(round(row.get("Low", 0), 6)),
                    "close":     float(round(row.get("Close", 0), 6)),
                    "volume":    int(row.get("Volume", 0)),
                })

    rows.sort(key=lambda r: r["timestamp"])

    if not rows:
        log("  yfinance: no data available for BADVF", "warn")

    return rows


# ══════════════════════════════════════════════════════════════════════════════
#  CSE (BAD.CN) — via yfinance
# ══════════════════════════════════════════════════════════════════════════════

def fetch_cse_trades(log) -> list:
    try:
        import yfinance as yf
    except ImportError:
        log("  Installing yfinance (one time only)...", "muted")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "yfinance", "-q"])
        import yfinance as yf

    ticker = yf.Ticker(SYMBOL_CSE)
    rows = []

    # 1-min bars (max 8 days)
    log(f"  Fetching {SYMBOL_CSE} 1-min bars (last 7 days)...", "muted")
    try:
        df = ticker.history(period="7d", interval="1m")
    except Exception:
        df = None

    if df is not None and not df.empty:
        for ts, row in df.iterrows():
            rows.append({
                "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "open":      float(round(row.get("Open", 0), 6)),
                "high":      float(round(row.get("High", 0), 6)),
                "low":       float(round(row.get("Low", 0), 6)),
                "close":     float(round(row.get("Close", 0), 6)),
                "volume":    int(row.get("Volume", 0)),
            })

    # Daily bars for full 30 days
    log(f"  Fetching {SYMBOL_CSE} daily bars (last 30 days)...", "muted")
    try:
        df_daily = ticker.history(period="30d", interval="1d")
    except Exception:
        df_daily = None

    if df_daily is not None and not df_daily.empty:
        existing_dates = {r["timestamp"][:10] for r in rows}
        for ts, row in df_daily.iterrows():
            date_str = ts.strftime("%Y-%m-%d")
            if date_str not in existing_dates:
                rows.append({
                    "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "open":      float(round(row.get("Open", 0), 6)),
                    "high":      float(round(row.get("High", 0), 6)),
                    "low":       float(round(row.get("Low", 0), 6)),
                    "close":     float(round(row.get("Close", 0), 6)),
                    "volume":    int(row.get("Volume", 0)),
                })

    rows.sort(key=lambda r: r["timestamp"])

    if not rows:
        log(f"  yfinance: no data available for {SYMBOL_CSE}", "warn")

    return rows


# ══════════════════════════════════════════════════════════════════════════════
#  CSV helpers
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
        self.geometry("680x720")
        self.resizable(False, False)
        self.configure(bg=DARK_BG)
        self.cfg = load_config()
        self._setup_styles()
        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _setup_styles(self):
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TProgressbar", troughcolor=CARD_BG, background=ACCENT, thickness=4)
        style.configure("Accent.TButton", font=FONT_BOLD, background=ACCENT,
                         foreground="white", padding=(16, 8))
        style.map("Accent.TButton",
                  background=[("active", "#3a7bd5"), ("disabled", MUTED)])
        style.configure("Card.TButton", font=FONT_MAIN, background=CARD_BG,
                         foreground=TEXT, padding=(8, 6))
        style.map("Card.TButton",
                  background=[("active", BORDER)])
        style.configure("Small.TButton", font=FONT_SM, background=BORDER,
                         foreground=TEXT, padding=(10, 6))
        style.map("Small.TButton",
                  background=[("active", CARD_BG)])

    def _build_ui(self):
        # Header
        hdr = tk.Frame(self, bg=CARD_BG, pady=14)
        hdr.pack(fill="x")
        tk.Label(hdr, text="BADVF  Tick Fetcher", font=FONT_TITLE,
                 bg=CARD_BG, fg=TEXT).pack()
        tk.Label(hdr, text="OTC + CSE  ·  Trades  ·  Bid/Ask  ·  30-Day History  ·  CSV",
                 font=FONT_MAIN, bg=CARD_BG, fg=MUTED).pack(pady=(2, 0))

        # Stat cards
        cards = tk.Frame(self, bg=DARK_BG, pady=10)
        cards.pack(fill="x", padx=20)
        self._trade_var  = tk.StringVar(value="—")
        self._bidask_var = tk.StringVar(value="—")
        self._cse_var    = tk.StringVar(value="—")
        self._make_card(cards, "OTC Trades",    self._trade_var).pack(side="left", expand=True, fill="x", padx=(0, 4))
        self._make_card(cards, "Bid/Ask",       self._bidask_var).pack(side="left", expand=True, fill="x", padx=(4, 4))
        self._make_card(cards, "CSE Trades",    self._cse_var).pack(side="left", expand=True, fill="x", padx=(4, 0))

        # Settings panel
        cfg_frame = tk.Frame(self, bg=CARD_BG, padx=16, pady=12)
        cfg_frame.pack(fill="x", padx=20, pady=(0, 8))

        # API Key row
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
        self.key_entry.pack(side="left", fill="x", expand=True, ipady=6, ipadx=8)

        ttk.Button(key_row, text="Show", style="Small.TButton",
                   command=self._toggle_key_visibility).pack(side="left", padx=(6, 0))
        ttk.Button(key_row, text="Save", style="Accent.TButton",
                   command=self._save_key).pack(side="left", padx=(6, 0))

        self.key_status = tk.Label(cfg_frame, text="", font=FONT_SM, bg=CARD_BG, fg=GREEN)
        self.key_status.pack(anchor="w", pady=(2, 0))

        # Port row
        port_row = tk.Frame(cfg_frame, bg=CARD_BG)
        port_row.pack(fill="x", pady=(8, 0))

        tk.Label(port_row, text="TWS Port", font=FONT_BOLD,
                 bg=CARD_BG, fg=MUTED, width=12, anchor="w").pack(side="left")

        self.port_var = tk.StringVar(value=str(self.cfg.get("tws_port", PORT)))
        tk.Entry(port_row, textvariable=self.port_var, font=FONT_MONO,
                 bg="#0f1117", fg=TEXT, insertbackground=TEXT,
                 relief="flat", bd=0, width=8).pack(side="left", ipady=6, ipadx=8)

        ttk.Button(port_row, text="Save", style="Accent.TButton",
                   command=self._save_port).pack(side="left", padx=(6, 0))

        self.port_status = tk.Label(cfg_frame, text="", font=FONT_SM, bg=CARD_BG, fg=GREEN)
        self.port_status.pack(anchor="w", pady=(2, 0))

        # ── Bottom section (packed first with side=BOTTOM so they stay visible) ──

        # Run Now + status
        bottom = tk.Frame(self, bg=DARK_BG, pady=12)
        bottom.pack(side="bottom", fill="x", padx=20)
        self._status_var = tk.StringVar(value="Ready")
        self.status_lbl = tk.Label(bottom, textvariable=self._status_var,
                                   font=FONT_MAIN, bg=DARK_BG, fg=MUTED, anchor="w")
        self.status_lbl.pack(side="left")
        if IS_MAC:
            self.run_btn = tk.Button(
                bottom, text="  Run Now  ", font=FONT_BOLD,
                highlightbackground=ACCENT, fg="white",
                padx=16, pady=6, command=self.start_run,
            )
        else:
            self.run_btn = tk.Button(
                bottom, text="  Run Now  ", font=FONT_BOLD,
                bg=ACCENT, fg="white", relief="flat", cursor="hand2",
                activebackground="#3a7bd5", activeforeground="white",
                padx=16, pady=6, command=self.start_run,
            )
        self.run_btn.pack(side="right")

        # Progress bar
        pb_frame = tk.Frame(self, bg=DARK_BG)
        pb_frame.pack(side="bottom", fill="x", padx=20, pady=(0, 4))
        self.progress = ttk.Progressbar(pb_frame, mode="indeterminate", length=640)
        self.progress.pack(fill="x")

        # File buttons
        files_frame = tk.Frame(self, bg=DARK_BG)
        files_frame.pack(side="bottom", fill="x", padx=20, pady=(8, 4))

        tk.Label(files_frame, text="Output Files", font=FONT_BOLD,
                 bg=DARK_BG, fg=MUTED, anchor="w").pack(fill="x", pady=(0, 4))

        btn_row = tk.Frame(files_frame, bg=DARK_BG)
        btn_row.pack(fill="x")

        for label, filename in [
            ("OTC Trades",   f"{SYMBOL}_trades_ALL.csv"),
            ("Bid/Ask",      f"{SYMBOL}_bidask_ALL.csv"),
            ("CSE Trades",   "BAD_CSE_trades_ALL.csv"),
            ("Open Folder",  "__folder__"),
        ]:
            ttk.Button(
                btn_row, text=label, style="Card.TButton",
                command=lambda f=filename: self._open_file(f),
            ).pack(side="left", expand=True, fill="x", padx=(0, 4))

        # ── Log (fills remaining middle space) ──
        log_frame = tk.Frame(self, bg=DARK_BG)
        log_frame.pack(fill="both", expand=True, padx=20, pady=(8, 0))
        tk.Label(log_frame, text="Activity Log", font=FONT_BOLD,
                 bg=DARK_BG, fg=MUTED, anchor="w").pack(fill="x", pady=(0, 4))
        self.log_box = scrolledtext.ScrolledText(
            log_frame, height=8, font=FONT_MONO,
            bg=CARD_BG, fg=TEXT, insertbackground=TEXT,
            relief="flat", borderwidth=0, state="disabled", wrap="word",
        )
        self.log_box.pack(fill="both", expand=True)
        self.log_box.tag_config("ok",    foreground=GREEN)
        self.log_box.tag_config("error", foreground=RED)
        self.log_box.tag_config("warn",  foreground=YELLOW)
        self.log_box.tag_config("muted", foreground=MUTED)

    def _make_card(self, parent, label, var):
        f = tk.Frame(parent, bg=CARD_BG, padx=16, pady=10)
        tk.Label(f, text=label, font=FONT_MAIN, bg=CARD_BG, fg=MUTED).pack(anchor="w")
        tk.Label(f, textvariable=var, font=(FONT_TITLE[0], 20, "bold"),
                 bg=CARD_BG, fg=ACCENT).pack(anchor="w")
        return f

    # ── Settings actions ──────────────────────────────────────────────────────

    def _toggle_key_visibility(self):
        if self.key_entry.cget("show") == "•":
            self.key_entry.config(show="")
        else:
            self.key_entry.config(show="•")

    def _save_key(self):
        key = self.key_var.get().strip()
        self.cfg["polygon_api_key"] = key
        save_config(self.cfg)
        self.key_status.config(text="API key saved", fg=GREEN)
        self.after(3000, lambda: self.key_status.config(text=""))

    def _save_port(self):
        port = self.port_var.get().strip()
        if not port.isdigit():
            self.port_status.config(text="Port must be a number", fg=RED)
            self.after(3000, lambda: self.port_status.config(text=""))
            return
        self.cfg["tws_port"] = int(port)
        save_config(self.cfg)
        self.port_status.config(text="Port saved", fg=GREEN)
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
        self.run_btn.configure(state="disabled", text="  Running...  ")
        self.progress.start(12)
        self.set_status("Running...", ACCENT)
        threading.Thread(target=self._run_fetch, daemon=True).start()

    def _run_fetch(self):
        today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cutoff  = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)
        api_key = self.cfg.get("polygon_api_key", "").strip()
        port    = int(self.cfg.get("tws_port", PORT))

        self.log(f"Starting fetch for {SYMBOL} — last {DAYS_BACK} days", "muted")

        # Trades: try Polygon first, fall back to yfinance
        self.log("Fetching trade data from Polygon.io...")
        trades = fetch_polygon_trades(cutoff, api_key, self.log)

        if not trades:
            self.log("Fetching trade data from Yahoo Finance (free)...", "muted")
            trades = fetch_yfinance_trades(self.log)

        self._trade_var.set(f"{len(trades):,}")
        if trades:
            self.log(f"  {len(trades):,} trade records received", "ok")
        else:
            self.log("  No trade data available for this period", "warn")

        # IBKR bid/ask
        self.log("Fetching bid/ask ticks from IBKR TWS...")
        bidasks = fetch_ibkr_bidask(cutoff, self.log, port)
        self._bidask_var.set(f"{len(bidasks):,}")
        if bidasks:
            self.log(f"  {len(bidasks):,} bid/ask ticks received", "ok")
        else:
            self.log("  No bid/ask data — make sure TWS is open and API is enabled", "warn")

        # CSE trades (BAD.CN)
        self.log(f"Fetching CSE trade data for {SYMBOL_CSE}...")
        cse_trades = fetch_cse_trades(self.log)
        self._cse_var.set(f"{len(cse_trades):,}")
        if cse_trades:
            self.log(f"  {len(cse_trades):,} CSE trade records received", "ok")
        else:
            self.log("  No CSE trade data available", "warn")

        # Save
        self.log("Saving CSV files...")
        save_csv(trades,     f"{OUTPUT_DIR}/{SYMBOL}_trades_{today}.csv")
        save_csv(bidasks,    f"{OUTPUT_DIR}/{SYMBOL}_bidask_{today}.csv")
        save_csv(cse_trades, f"{OUTPUT_DIR}/BAD_CSE_trades_{today}.csv")
        append_to_master(trades,     f"{OUTPUT_DIR}/{SYMBOL}_trades_ALL.csv")
        append_to_master(bidasks,    f"{OUTPUT_DIR}/{SYMBOL}_bidask_ALL.csv")
        append_to_master(cse_trades, f"{OUTPUT_DIR}/BAD_CSE_trades_ALL.csv")

        if trades or bidasks or cse_trades:
            self.log("Files saved to output/ folder", "ok")
        self.log("Done!", "ok")

        self.progress.stop()
        self.run_btn.configure(state="normal", text="  Run Now  ")
        self.set_status(f"Last run: {datetime.now().strftime('%H:%M:%S')}", GREEN)

    def _open_file(self, filename):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if filename == "__folder__":
            path = os.path.abspath(OUTPUT_DIR)
            os.makedirs(path, exist_ok=True)
        else:
            path = os.path.abspath(
                os.path.join(OUTPUT_DIR, filename.replace("{today}", today))
            )
        if filename != "__folder__" and not os.path.exists(path):
            self.log(f"  File not found: {os.path.basename(path)} — run the fetch first", "warn")
            return
        if IS_MAC:
            subprocess.Popen(["open", path])
        else:
            os.startfile(path)

    def on_close(self):
        self.destroy()


if __name__ == "__main__":
    app = App()
    app.mainloop()
