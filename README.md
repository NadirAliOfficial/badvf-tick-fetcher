<div align="center">

# BADVF Tick Fetcher

**Professional-grade tick-by-tick market data for BADVF (Naughty Ventures Corp)**
*Powered by Interactive Brokers TWS + Polygon.io*

[![Build Windows EXE](https://github.com/NadirAliOfficial/badvf-tick-fetcher/actions/workflows/build.yml/badge.svg)](https://github.com/NadirAliOfficial/badvf-tick-fetcher/actions/workflows/build.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![Platform](https://img.shields.io/badge/Platform-Windows-blue?logo=windows&logoColor=white)
![License](https://img.shields.io/badge/License-Private-red)

</div>

---

## What It Does

Every time you click **Run Now**, the app:

| Step | Source | Data |
|---|---|---|
| Fetches trade ticks | Polygon.io | Price, size, exchange, conditions |
| Fetches bid/ask quotes | IBKR TWS | Bid price, ask price, bid/ask size |
| Exports to CSV | Local `output/` folder | Daily snapshot + rolling master file |

All data is ready to drag-and-drop into **Google Sheets**.

---

## Download (Windows)

> **[Download Latest .exe →](../../releases/latest)**

No Python installation required. Just download and run.

---

## Prerequisites

Before running the app:

1. **Interactive Brokers Account** — live or paper trading
2. **TWS (Trader Workstation)** — must be open and logged in
3. **TWS API enabled** — follow the setup steps below

---

## TWS API Setup (One Time Only)

1. Open TWS
2. Go to **Edit → Global Configuration → API → Settings**
3. Check **"Enable ActiveX and Socket Clients"**
4. Set **Socket port** to `4001`
5. Uncheck **"Read-Only API"**
6. Add `127.0.0.1` to **Trusted IP Addresses**
7. Click **OK** — done

---

## Daily Usage

```
1. Open TWS and log in
2. Double-click  BADVF Tick Fetcher.exe
3. Click  Run Now
4. Wait ~30 seconds
5. Open the output/ folder → import CSVs into Google Sheets
```

---

## Output Files

```
output/
├── BADVF_trades_YYYY-MM-DD.csv     ← today's trade ticks
├── BADVF_trades_ALL.csv            ← all trade ticks (append daily)
├── BADVF_bidask_YYYY-MM-DD.csv     ← today's bid/ask quotes
└── BADVF_bidask_ALL.csv            ← all bid/ask quotes (append daily)
```

### Trade Columns
| Column | Description |
|---|---|
| timestamp | Date & time of trade |
| price | Trade price (USD) |
| size | Number of shares |
| exchange | Exchange where trade occurred |
| conditions | Trade condition codes |

### Bid/Ask Columns
| Column | Description |
|---|---|
| timestamp | Date & time of quote |
| bid_price | Best bid price |
| ask_price | Best ask price |
| bid_size | Bid size (shares) |
| ask_size | Ask size (shares) |

---

## Google Sheets Import

1. Open Google Sheets
2. **File → Import → Upload**
3. Select `BADVF_trades_ALL.csv` or `BADVF_bidask_ALL.csv`
4. Choose **"Replace spreadsheet"** or **"Append to current sheet"**

---

## Build From Source

```bash
git clone https://github.com/NadirAliOfficial/badvf-tick-fetcher.git
cd badvf-tick-fetcher
pip install ibapi requests pyinstaller
pyinstaller --onefile --windowed --name "BADVF Tick Fetcher" run_gui.py
```

The `.exe` will appear in the `dist/` folder.

---

## Troubleshooting

| Issue | Fix |
|---|---|
| `Connection refused` | TWS is not running, or port is not 4001 |
| `0 bid/ask ticks` | Check TWS API is enabled and not Read-Only |
| `0 trade ticks` | Verify Polygon.io plan includes OTC data |
| App won't open | Run as Administrator, allow through Windows Defender |

---

<div align="center">
Built with Interactive Brokers API + Polygon.io
</div>
