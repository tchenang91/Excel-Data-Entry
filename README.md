# UT County Expired Listings – Lead Enrichment Scraper

Automates the manual workflow of looking up **parcel owners** and **LLC principal information** for every expired commercial listing in Utah County.

---

## What it does

```
Excel (Tax IDs)
      │
      ▼
Utah County Parcel Map          → Owner Name
      │
      ▼
Utah Business Entity Search     → LLC Principal Title / Name / Address / Last Updated
      │
      ▼
results.csv  +  scraper_YYYYMMDD_HHMMSS.log
```

For every unique Tax ID in the spreadsheet the script:

1. Opens **https://maps.utahcounty.gov/ParcelMap/ParcelMap.html**, types the Tax ID into the *Search by Parcel Serial* field, and reads back the Owner name.
2. Opens **https://businessregistration.utah.gov/EntitySearch/OnlineEntitySearch**, searches for that Owner name, selects the first result whose name ends with **LLC**, and extracts every row in the *Principal Information* table (Title · Name · Address · Last Updated).
3. Writes one CSV row per principal (or one empty row when no data was found).
4. Logs every action with timestamp to both the console and a log file.

---

## Requirements

| Requirement | Version |
|---|---|
| Python | 3.10 or newer |
| pip packages | see `requirements.txt` |
| Chromium | installed via Playwright |

---

## Installation

```bash
# 1. Clone / download this folder, then enter it
cd ut_leads_scraper

# 2. (Recommended) Create a virtual environment
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
.venv\Scripts\activate           # Windows

# 3. Install Python packages
pip install -r requirements.txt

# 4. Install the Playwright browser
playwright install chromium
```

---

## Usage

### Basic (all defaults)

```bash
python main.py
```

The script expects the Excel file named  
`UT_County_Expired_Listings_(UTMLS)_4_16_2026.xlsx`  
in the same directory.  Results go to `results.csv`.

### Custom paths

```bash
python main.py \
  --input  "path/to/UT_County_Expired_Listings_(UTMLS)_4_16_2026.xlsx" \
  --output  my_leads.csv \
  --logfile  run1.log
```

### Watch the browser (non-headless)

Useful for debugging or understanding why a selector is not matching:

```bash
python main.py --no-headless
```

### All options

| Flag | Default | Description |
|---|---|---|
| `--input` | `UT_County_Expired_Listings_(UTMLS)_4_16_2026.xlsx` | Path to UTMLS Excel file |
| `--output` | `results.csv` | Path for the output CSV |
| `--logfile` | `scraper_YYYYMMDD_HHMMSS.log` | Path for the log file |
| `--headless` / `--no-headless` | headless on | Run browser invisibly or visibly |
| `--delay` | `2.0` | Seconds to pause between requests |
| `--resume` / `--no-resume` | resume on | Skip Tax IDs already in the CSV |

---

## Output

### results.csv

```
Tax ID,Owner Name,Principal Title,Principal Name,Principal Address,Last Updated,Status
18-047-0085,ACME PROPERTIES LLC,Manager,John Smith,"123 Main St, Provo UT 84601",2024-01-15,ok
38-101-0013,,,,,,,owner_not_found
```

**Status values:**

| Status | Meaning |
|---|---|
| `ok` | Full data found |
| `no_principals` | Owner found but no LLC / principal data |
| `owner_not_found` | Parcel map returned no owner |

### Log file

Every significant action is timestamped and written to both the console (INFO level) and the log file (DEBUG level):

```
2025-04-16 09:00:01  INFO     UT County Leads Scraper – started
2025-04-16 09:00:05  INFO     [1/46] Processing Tax ID: 18-047-0085
2025-04-16 09:00:07  INFO     [Parcel] Tax ID 18-047-0085 → Owner: ACME PROPERTIES LLC
2025-04-16 09:00:12  INFO     [BizSearch] ACME PROPERTIES LLC → 2 principal(s) found
...
2025-04-16 09:04:33  INFO     SCRAPE COMPLETE
2025-04-16 09:04:33  INFO       Total Tax IDs processed : 44
2025-04-16 09:04:33  INFO       ✓ Full data found       : 31
2025-04-16 09:04:33  INFO       ⚠ No principal data     : 8
2025-04-16 09:04:33  INFO       ✗ Owner not found       : 5
```

---

## Resume / fault tolerance

The script is designed to be safe to re-run:

- **Duplicate Tax IDs** in the Excel file are automatically deduplicated before processing.
- **Resume mode** (on by default): if the script is interrupted, re-run it and it will skip every Tax ID that already has a row in `results.csv`.
- **Per-record error handling**: a network timeout or missing element on one record does not crash the whole run — it logs a warning and moves to the next record.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `PlaywrightError: Executable doesn't exist` | Run `playwright install chromium` |
| Parcel map never loads | Try `--no-headless` to watch; the site may require a manual CAPTCHA on first visit |
| Owner found but no LLC results | The parcel owner may not be a registered LLC in Utah; check the `no_principals` rows manually |
| Very slow | Increase `--delay 3` to be polite; reduce it to `--delay 1` if speed is needed |
| Wrong column name in Excel | Open the Excel file and make sure there is a column header exactly spelling **Tax ID** |

---

## Project structure

```
ut_leads_scraper/
├── main.py           ← single-file script (all logic)
├── requirements.txt  ← pip dependencies
└── README.md         ← this file
```

---

## Legal notice

This tool performs automated web requests to publicly accessible government portals.  Use responsibly:

- Do not set `--delay` below 1 second.
- Do not run multiple instances simultaneously.
- Respect the sites' Terms of Service.
