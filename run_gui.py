#!/usr/bin/env python3
"""
BADVF Tick Fetcher — Desktop App
Trades via Polygon.io + Bid/Ask via IBKR TWS
"""

import csv
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
SYMBOL          = "BADVF"
HOST            = "127.0.0.1"
PORT            = 4001
OUTPUT_DIR      = "output"
BATCH_SIZE      = 1000
DAYS_BACK       = 30
POLYGON_API_KEY = "bIqgwroBoIiRcM5soBBmH1faxA2DZ8NI"
# ─────────────────────────────────────────────────────────────────────────────

DARK_BG    = "#0f1117"
CARD_BG    = "#1a1d27"
ACCENT     = "#4f8ef7"
GREEN      = "#22c55e"
RED        = "#ef4444"
YELLOW     = "#f59e0b"
TEXT       = "#e2e8f0"
MUTED      = "#64748b"
FONT_MAIN  = ("Segoe UI", 10)
FONT_BOLD  = ("Segoe UI", 10, "bold")
FONT_TITLE = ("Segoe UI", 14, "bold")
FONT_MONO  = ("Consolas", 9)


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

    def nextValidId(self, orderId):
        pass

    def historicalTicksBidAsk(self, reqId, ticks, done):
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

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode in (2104, 2106, 2158):
            return
        if errorCode not in (162,):
            self._done.set()


def fetch_ibkr_bidask(cutoff_utc, log):
    client_id = int(time.time()) % 9000 + 1000
    app = IBKRApp()
    try:
        app.connect(HOST, PORT, client_id)
    except Exception as e:
        log(f"  IBKR connection failed: {e}", "error")
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

def fetch_polygon_trades(cutoff_utc, log):
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
        "apiKey":        POLYGON_API_KEY,
    }

    while url:
        try:
            resp = requests.get(url, params=params, timeout=30)
        except Exception as e:
            log(f"  Polygon request failed: {e}", "error")
            break

        if resp.status_code != 200:
            log(f"  Polygon error {resp.status_code}: {resp.json().get('message','')}", "error")
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
            params = {"apiKey": POLYGON_API_KEY}
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
        self.geometry("620x540")
        self.resizable(False, False)
        self.configure(bg=DARK_BG)
        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self):
        # ── Header ──────────────────────────────────────────────────────────
        hdr = tk.Frame(self, bg=CARD_BG, pady=16)
        hdr.pack(fill="x")

        tk.Label(hdr, text="BADVF  Tick Fetcher", font=FONT_TITLE,
                 bg=CARD_BG, fg=TEXT).pack()
        tk.Label(hdr, text="Trades · Bid/Ask · 30-Day History · CSV Export",
                 font=FONT_MAIN, bg=CARD_BG, fg=MUTED).pack(pady=(2, 0))

        # ── Status cards ────────────────────────────────────────────────────
        cards = tk.Frame(self, bg=DARK_BG, pady=12)
        cards.pack(fill="x", padx=20)

        self._trade_var  = tk.StringVar(value="—")
        self._bidask_var = tk.StringVar(value="—")
        self._status_var = tk.StringVar(value="Ready")

        self._make_card(cards, "Trade Ticks",   self._trade_var,  ACCENT).pack(side="left", expand=True, fill="x", padx=(0,6))
        self._make_card(cards, "Bid/Ask Ticks", self._bidask_var, ACCENT).pack(side="left", expand=True, fill="x", padx=(6,0))

        # ── Log ─────────────────────────────────────────────────────────────
        log_frame = tk.Frame(self, bg=DARK_BG)
        log_frame.pack(fill="both", expand=True, padx=20)

        tk.Label(log_frame, text="Activity Log", font=FONT_BOLD,
                 bg=DARK_BG, fg=MUTED, anchor="w").pack(fill="x", pady=(0,4))

        self.log_box = scrolledtext.ScrolledText(
            log_frame, height=14, font=FONT_MONO,
            bg=CARD_BG, fg=TEXT, insertbackground=TEXT,
            relief="flat", borderwidth=0, state="disabled",
            wrap="word",
        )
        self.log_box.pack(fill="both", expand=True)
        self.log_box.tag_config("ok",    foreground=GREEN)
        self.log_box.tag_config("error", foreground=RED)
        self.log_box.tag_config("warn",  foreground=YELLOW)
        self.log_box.tag_config("muted", foreground=MUTED)

        # ── Progress bar ────────────────────────────────────────────────────
        pb_frame = tk.Frame(self, bg=DARK_BG)
        pb_frame.pack(fill="x", padx=20, pady=(8,0))

        self.progress = ttk.Progressbar(pb_frame, mode="indeterminate", length=580)
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TProgressbar", troughcolor=CARD_BG, background=ACCENT, thickness=4)
        self.progress.pack(fill="x")

        # ── Bottom ──────────────────────────────────────────────────────────
        bottom = tk.Frame(self, bg=DARK_BG, pady=14)
        bottom.pack(fill="x", padx=20)

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

    def _make_card(self, parent, label, var, color):
        f = tk.Frame(parent, bg=CARD_BG, padx=16, pady=12)
        tk.Label(f, text=label, font=FONT_MAIN, bg=CARD_BG, fg=MUTED).pack(anchor="w")
        tk.Label(f, textvariable=var, font=("Segoe UI", 20, "bold"),
                 bg=CARD_BG, fg=color).pack(anchor="w")
        return f

    def log(self, msg, tag=None):
        self.log_box.configure(state="normal")
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_box.insert("end", f"[{ts}] {msg}\n", tag or "")
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def set_status(self, msg, color=MUTED):
        self._status_var.set(msg)
        self.status_lbl.configure(fg=color)

    def start_run(self):
        self.run_btn.configure(state="disabled", bg=MUTED)
        self.progress.start(12)
        self.set_status("Running...", ACCENT)
        threading.Thread(target=self._run_fetch, daemon=True).start()

    def _run_fetch(self):
        today  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

        self.log(f"Starting fetch for {SYMBOL} — last {DAYS_BACK} days", "muted")

        # Polygon trades
        self.log("Fetching trade ticks from Polygon.io...")
        trades = fetch_polygon_trades(cutoff, self.log)
        self._trade_var.set(f"{len(trades):,}")
        if trades:
            self.log(f"  {len(trades):,} trade ticks received", "ok")
        else:
            self.log("  No trade data (check Polygon plan)", "warn")

        # IBKR bid/ask
        self.log("Fetching bid/ask ticks from IBKR TWS...")
        bidasks = fetch_ibkr_bidask(cutoff, self.log)
        self._bidask_var.set(f"{len(bidasks):,}")
        if bidasks:
            self.log(f"  {len(bidasks):,} bid/ask ticks received", "ok")
        else:
            self.log("  No bid/ask data (is TWS open and logged in?)", "warn")

        # Save
        self.log("Saving CSV files...")
        save_csv(trades,  f"{OUTPUT_DIR}/{SYMBOL}_trades_{today}.csv")
        save_csv(bidasks, f"{OUTPUT_DIR}/{SYMBOL}_bidask_{today}.csv")
        append_to_master(trades,  f"{OUTPUT_DIR}/{SYMBOL}_trades_ALL.csv")
        append_to_master(bidasks, f"{OUTPUT_DIR}/{SYMBOL}_bidask_ALL.csv")

        self.log(f"Saved to output/ folder", "ok")
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
