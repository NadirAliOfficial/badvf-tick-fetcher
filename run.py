#!/usr/bin/env python3
"""
BADVF Tick Fetcher
- Bid/Ask quotes  → IBKR (via TWS on port 4001)
- Trade ticks     → Polygon.io

SETUP (one time only):
  1. Install TWS, log in, enable API on port 4001
  2. pip install ibapi requests
  3. python run.py

DAILY USE:
  Open TWS, log in, then run:  python run.py
  CSVs saved to output/ — import into Google Sheets.
"""

import csv
import os
import threading
import time
import requests
from datetime import datetime, timezone, timedelta

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

# ── Config ───────────────────────────────────────────────────────────────────
SYMBOL          = "BADVF"
HOST            = "127.0.0.1"
PORT            = 4001
CLIENT_ID       = int(time.time()) % 9000 + 1000  # unique ID each run
OUTPUT_DIR      = "output"
BATCH_SIZE      = 1000
DAYS_BACK       = 30
POLYGON_API_KEY = "bIqgwroBoIiRcM5soBBmH1faxA2DZ8NI"
# ─────────────────────────────────────────────────────────────────────────────


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

    def nextValidId(self, orderId):
        print("  Connected to TWS ✓")

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
        print(f"  [IBKR] {errorCode}: {errorString}")
        if errorCode not in (162,):
            self._done.set()


def fetch_ibkr_bidask(cutoff_utc: datetime) -> list[dict]:
    app = IBKRApp()
    app.connect(HOST, PORT, CLIENT_ID)
    threading.Thread(target=app.run, daemon=True).start()
    time.sleep(2)

    contract = Contract()
    contract.symbol      = SYMBOL
    contract.secType     = "STK"
    contract.exchange    = "SMART"
    contract.currency    = "USD"
    contract.primaryExch = "PINK"

    all_ticks = []
    end_dt = datetime.now(timezone.utc)

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
        valid = [r for r in app.batch if r["timestamp"] >= cutoff_str]
        all_ticks = valid + all_ticks

        earliest_ts = min(r["timestamp"] for r in app.batch)
        earliest_dt = datetime.strptime(earliest_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)

        if earliest_dt <= cutoff_utc or len(app.batch) < BATCH_SIZE:
            break

        end_dt = earliest_dt - timedelta(seconds=1)
        time.sleep(0.5)

    app.disconnect()
    return all_ticks


# ══════════════════════════════════════════════════════════════════════════════
#  Polygon.io — Trades
# ══════════════════════════════════════════════════════════════════════════════

def fetch_polygon_trades(cutoff_utc: datetime) -> list[dict]:
    """Fetch tick-by-tick trades from Polygon.io (paginated)."""
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
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"  [Polygon] HTTP {resp.status_code}: {resp.text[:200]}")
            break

        data = resp.json()
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

        # Pagination
        next_url = data.get("next_url")
        if next_url:
            url    = next_url
            params = {"apiKey": POLYGON_API_KEY}  # next_url already has other params
        else:
            break

    return all_trades


# ══════════════════════════════════════════════════════════════════════════════
#  CSV helpers
# ══════════════════════════════════════════════════════════════════════════════

def save_csv(rows: list[dict], filepath: str):
    if not rows:
        print(f"  No data — {os.path.basename(filepath)}")
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved {len(rows):,} rows → {filepath}")


def append_to_master(rows: list[dict], filepath: str):
    if not rows:
        return
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    file_exists = os.path.isfile(filepath)
    with open(filepath, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    print(f"  Appended {len(rows):,} rows → {filepath}")


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    today   = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cutoff  = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

    print(f"\n{'='*52}")
    print(f"  BADVF Tick Fetcher — {today}")
    print(f"  Last {DAYS_BACK} days  |  Trades + Bid/Ask")
    print(f"{'='*52}")

    # ── 1. Polygon trades ────────────────────────────────────────────────────
    print("\n[1/4] Fetching trade ticks from Polygon.io...")
    trades = fetch_polygon_trades(cutoff)
    print(f"  Trades collected: {len(trades):,}")

    # ── 2. IBKR bid/ask ──────────────────────────────────────────────────────
    print("\n[2/4] Connecting to TWS for bid/ask data...")
    bidasks = fetch_ibkr_bidask(cutoff)
    print(f"  Bid/Ask ticks collected: {len(bidasks):,}")

    # ── 3. Save CSVs ─────────────────────────────────────────────────────────
    print("\n[3/4] Saving daily snapshots...")
    save_csv(trades,  f"{OUTPUT_DIR}/{SYMBOL}_trades_{today}.csv")
    save_csv(bidasks, f"{OUTPUT_DIR}/{SYMBOL}_bidask_{today}.csv")

    print("\n[4/4] Updating master files...")
    append_to_master(trades,  f"{OUTPUT_DIR}/{SYMBOL}_trades_ALL.csv")
    append_to_master(bidasks, f"{OUTPUT_DIR}/{SYMBOL}_bidask_ALL.csv")

    print(f"\n{'='*52}")
    print(f"  Done! Import these into Google Sheets:")
    print(f"  → output/{SYMBOL}_trades_ALL.csv  (all trades)")
    print(f"  → output/{SYMBOL}_bidask_ALL.csv  (all bid/ask)")
    print(f"{'='*52}\n")


if __name__ == "__main__":
    main()
