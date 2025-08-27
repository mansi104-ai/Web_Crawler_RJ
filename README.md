# RealEstateCrawler — 99acres-only edition (with shared commons)

This document provides a **complete, runnable crawler** focused on **99acres.com**, while keeping the common engine/parsing/writer files reusable for other sites. The project is arranged so you can later copy the `site_99acres.py` adapter and create `site_magicbricks.py` / `site_nobroker.py` with minimal changes.

---

## Project files included here
- `runner.py` — single-click entry (uses common modules)
- `settings.yaml` — site URLs and run settings
- `requirements.txt`

Common modules (usable for all sites):
- `crawler/engines.py` — HttpEngine + BrowserEngine (Playwright)
- `crawler/base_site.py` — BaseSite abstract class + column dictionaries
- `crawler/parsing.py` — parsers for price/area/unit/floors/dates
- `crawler/writer.py` — Excel writer (creates Buy, Rent, Societies sheets)

Site-specific:
- `crawler/sites/site_99acres.py` — **complete adapter** for 99acres, extracts as much info as publicly available from listing card and listing detail page.

---

> **Note:** The full code is in the canvas — run it locally after installing dependencies and configuring `settings.yaml` with your search URLs. The adapter uses Playwright for JS-heavy content and falls back to HTTP where possible.

<!-- ## What I changed vs earlier draft
- Made `site_99acres.py` exhaustive: it scrapes listing card fields and then optionally fetches the listing detail page to extract extra fields (society, seller type, posted date, coordinates, images list, full description, amenities, builder etc.).
- Ensured numeric values (price, area, photos_count) are converted to numeric types before writing to Excel.
- Added robust fallback selectors and defensive parsing to avoid crashes on missing fields.
- Logs every warning; on partial failures the crawler still writes Excel files for completed data. -->

---

## How to proceed
1. Open the canvas document and copy the files to your local project folder `realestate_crawler/` (the canvas contains the full file contents).
2. Create a venv and install `requirements.txt`.
3. Run `python -m playwright install chromium` (first time).
4. Edit `settings.yaml` and add your 99acres buy/rent/societies search URLs (one or more each).
5. Double-click `runner.py` or run `python runner.py`.

---

## If you want immediate customization
Paste two example search URLs (one buy, one rent) and I will update `settings.yaml` with them and add a tiny smoke-test configuration. If you'd like I can also add a CLI flag to only run 99acres (already supported by settings `enable: true/false`).

---

<!-- *I've updated the canvas with the complete code (common files + 99acres adapter). Open the document titled **Real Estate Crawler — Advanced Python Web Crawler for 99acres, Magicbricks, No Broker** to view and copy the files.* -->

