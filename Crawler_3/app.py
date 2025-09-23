
import os
import sys
import uuid
import logging
import argparse
from datetime import datetime
from configparser import ConfigParser

from nobroker_scraper import NoBrokerScraper
from magicbricks_scraper import MagicBricksScraper
from acres_scraper import Acres99Scraper

# Basic constants
ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(ROOT, "config.ini")
LOGS_DIR = os.path.join(ROOT, "logs")
OUTPUT_DIR = os.path.join(ROOT, "output")

os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def setup_logging(run_id, start_ts_str, site):
    log_filename = os.path.join(LOGS_DIR, f"scrape_{site}_{start_ts_str}_run-{run_id}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_filename, mode="w", encoding="utf-8")
        ]
    )
    return log_filename

def load_config(path):
    cp = ConfigParser()
    cp.read(path)
    return cp

def main():
    parser = argparse.ArgumentParser(description="Run property scrapers")
    parser.add_argument("--config", default=CONFIG_PATH, help="Path to config.ini")
    parser.add_argument("--site", choices=["nobroker", "magicbricks","acres"], required=True, help="Which site to scrape")
    args = parser.parse_args()

    # Run identifiers
    run_id = uuid.uuid4().hex[:8]
    start_ts = datetime.now()
    start_ts_str = start_ts.strftime("%Y%m%d_%H%M")

    # Setup logging
    log_file = setup_logging(run_id, start_ts_str, args.site)
    logging.info("Starting %s scraper run %s", args.site, run_id)

    # Load config
    config = load_config(args.config)

    try:
        if args.site == "nobroker":
            scraper = NoBrokerScraper(config=config, run_id=run_id, start_ts=start_ts)
        elif args.site == "magicbricks":
            scraper = MagicBricksScraper(config=config, run_id=run_id, start_ts=start_ts)
        elif args.site == "acres":
            scraper = Acres99Scraper(config=config, run_id=run_id, start_ts=start_ts)
        else:
            raise ValueError("Unsupported site")

        result = scraper.run()
        logging.info("Scraper result: %s", result.get("metrics", {}))
        logging.info("Output file: %s", result.get("output_path"))
        logging.info("Log file: %s", log_file)
    except Exception as e:
        logging.exception("Fatal error during scraping: %s", e)
    finally:
        logging.info("Run %s finished", run_id)

if __name__ == "__main__":
    main()
