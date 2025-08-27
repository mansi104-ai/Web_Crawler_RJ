#!/usr/bin/env python3
"""
Advanced Real Estate Web Crawler
Scrapes listing data from 99acres, Magicbricks, and NoBroker
"""

import asyncio
import aiohttp
import json
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse
import uuid

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import yaml


class RealEstateCrawler:
    """Main crawler class for real estate platforms"""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_config(config_path)
        self.run_id = str(uuid.uuid4())[:8]
        self.ist_tz = timezone(timedelta(hours=5, minutes=30))
        self.session_timeout = 30
        self.request_delay = 2.0  # Polite delay between requests
        
        # Setup logging
        self._setup_logging()
        
        # Data containers
        self.listings_data = []
        self.societies_data = []
        
        # Site-specific parsers
        self.site_parsers = {
            '99acres': self._parse_99acres,
            'magicbricks': self._parse_magicbricks,
            'nobroker': self._parse_nobroker
        }
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            # Create default config if not exists
            default_config = {
                'sites': {
                    '99acres': {
                        'urls': ['https://www.99acres.com/search/property/buy/residential-all/delhi-ncr-all'],
                        'max_pages': 10,
                        'use_selenium': True
                    },
                    'magicbricks': {
                        'urls': ['https://www.magicbricks.com/property-for-sale/residential-real-estate?proptype=Multistorey-Apartment,Builder-Floor,Penthouse,Studio-Apartment&cityName=New-Delhi'],
                        'max_pages': 10,
                        'use_selenium': True
                    },
                    'nobroker': {
                        'urls': ['https://www.nobroker.in/property/sale/delhi/'],
                        'max_pages': 10,
                        'use_selenium': True
                    }
                },
                'output': {
                    'directory': './output',
                    'logs_directory': './logs'
                },
                'crawler': {
                    'delay_seconds': 2.0,
                    'timeout_seconds': 30,
                    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
            }
            
            os.makedirs(os.path.dirname(config_path), exist_ok=True)
            with open(config_path, 'w') as f:
                yaml.dump(default_config, f, default_flow_style=False)
            
            return default_config
    
    def _setup_logging(self):
        """Setup logging configuration"""
        logs_dir = Path(self.config['output']['logs_directory'])
        logs_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now(self.ist_tz).strftime('%Y%m%d_%H%M')
        log_file = logs_dir / f"crawl_{timestamp}_run-{self.run_id}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Starting crawler run {self.run_id}")
    
    def _get_selenium_driver(self) -> webdriver.Chrome:
        """Create and configure Selenium WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument(f"--user-agent={self.config['crawler']['user_agent']}")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        return webdriver.Chrome(options=chrome_options)
    
    async def _fetch_with_session(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        """Fetch URL content with aiohttp session"""
        try:
            headers = {'User-Agent': self.config['crawler']['user_agent']}
            async with session.get(url, headers=headers, timeout=self.session_timeout) as response:
                if response.status == 200:
                    return await response.text()
                else:
                    self.logger.warning(f"HTTP {response.status} for URL: {url}")
                    return None
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {str(e)}")
            return None
    
    def _fetch_with_selenium(self, driver: webdriver.Chrome, url: str) -> Optional[str]:
        """Fetch URL content with Selenium"""
        try:
            driver.get(url)
            WebDriverWait(driver, self.session_timeout).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(2)  # Additional wait for JS content
            return driver.page_source
        except TimeoutException:
            self.logger.error(f"Timeout loading URL: {url}")
            return None
        except Exception as e:
            self.logger.error(f"Selenium error for {url}: {str(e)}")
            return None
    
    def _extract_number(self, text: str) -> Optional[float]:
        """Extract numeric value from text"""
        if not text:
            return None
        
        # Remove commas and extract numbers
        numbers = re.findall(r'[\d,]+\.?\d*', str(text).replace(',', ''))
        if numbers:
            try:
                return float(numbers[0])
            except ValueError:
                return None
        return None
    
    def _parse_99acres(self, html: str, url: str) -> Tuple[List[Dict], List[Dict]]:
        """Parse 99acres listings and society data"""
        listings = []
        societies = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Parse listing cards
            listing_cards = soup.find_all(['div'], class_=re.compile(r'srpTuple|listingCard|property-card'))
            
            for card in listing_cards:
                try:
                    listing_data = {
                        'source': '99acres',
                        'page_url': url,
                        'site_listing_id': '',
                        'title': '',
                        'description_short': '',
                        'price_or_rent': None,
                        'area_value': None,
                        'area_unit': '',
                        'unit_type': '',
                        'furnishing': '',
                        'society_or_project': '',
                        'locality': '',
                        'city': '',
                        'floor_no': '',
                        'total_floors': '',
                        'posted_date': '',
                        'last_updated_on_site': '',
                        'seller_type': '',
                        'photos_count': None,
                        'amenities_listed': '',
                        'captured_timestamp': datetime.now(self.ist_tz).isoformat(),
                        'run_id': self.run_id
                    }
                    
                    # Extract listing ID
                    id_element = card.find(attrs={'data-id': True}) or card.find('a', href=True)
                    if id_element:
                        if 'data-id' in id_element.attrs:
                            listing_data['site_listing_id'] = id_element['data-id']
                        elif id_element.get('href'):
                            listing_data['site_listing_id'] = re.search(r'/(\d+)', id_element['href'])
                            listing_data['site_listing_id'] = listing_data['site_listing_id'].group(1) if listing_data['site_listing_id'] else ''
                    
                    # Extract title
                    title_elem = card.find(['h2', 'h3', 'h4', 'a'], class_=re.compile(r'title|heading|name'))
                    if title_elem:
                        listing_data['title'] = title_elem.get_text(strip=True)
                    
                    # Extract price
                    price_elem = card.find(text=re.compile(r'[₹\d,]+\s*(Lakh|Crore|K)', re.I))
                    if price_elem:
                        price_text = price_elem.strip()
                        listing_data['price_or_rent'] = self._extract_number(price_text)
                    
                    # Extract area
                    area_elem = card.find(text=re.compile(r'\d+\s*(sq\.?\s*ft|sqft)', re.I))
                    if area_elem:
                        area_match = re.search(r'(\d+)\s*(sq\.?\s*ft|sqft)', area_elem, re.I)
                        if area_match:
                            listing_data['area_value'] = float(area_match.group(1))
                            listing_data['area_unit'] = 'sqft'
                    
                    # Extract unit type (BHK)
                    bhk_elem = card.find(text=re.compile(r'\d+\s*(bhk|rk)', re.I))
                    if bhk_elem:
                        listing_data['unit_type'] = re.search(r'\d+\s*(bhk|rk)', bhk_elem, re.I).group(0).upper()
                    
                    # Extract location
                    location_elem = card.find(['span', 'div'], class_=re.compile(r'location|locality|address'))
                    if location_elem:
                        location_text = location_elem.get_text(strip=True)
                        listing_data['locality'] = location_text
                        if 'Delhi' in location_text or 'NCR' in location_text:
                            listing_data['city'] = 'Delhi'
                    
                    listings.append(listing_data)
                    
                except Exception as e:
                    self.logger.warning(f"Error parsing 99acres listing card: {str(e)}")
                    continue
            
            self.logger.info(f"Parsed {len(listings)} listings from 99acres")
            
        except Exception as e:
            self.logger.error(f"Error parsing 99acres page: {str(e)}")
        
        return listings, societies
    
    def _parse_magicbricks(self, html: str, url: str) -> Tuple[List[Dict], List[Dict]]:
        """Parse Magicbricks listings and society data"""
        listings = []
        societies = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Parse listing cards
            listing_cards = soup.find_all(['div'], class_=re.compile(r'mb-srp__card|listing|property-card'))
            
            for card in listing_cards:
                try:
                    listing_data = {
                        'source': 'magicbricks',
                        'page_url': url,
                        'site_listing_id': '',
                        'title': '',
                        'description_short': '',
                        'price_or_rent': None,
                        'area_value': None,
                        'area_unit': '',
                        'unit_type': '',
                        'furnishing': '',
                        'society_or_project': '',
                        'locality': '',
                        'city': '',
                        'floor_no': '',
                        'total_floors': '',
                        'posted_date': '',
                        'last_updated_on_site': '',
                        'seller_type': '',
                        'photos_count': None,
                        'amenities_listed': '',
                        'captured_timestamp': datetime.now(self.ist_tz).isoformat(),
                        'run_id': self.run_id
                    }
                    
                    # Extract title
                    title_elem = card.find(['h2', 'a'], class_=re.compile(r'mb-srp__card--title|title'))
                    if title_elem:
                        listing_data['title'] = title_elem.get_text(strip=True)
                    
                    # Extract price
                    price_elem = card.find(['span', 'div'], class_=re.compile(r'mb-srp__card__price|price'))
                    if price_elem:
                        price_text = price_elem.get_text(strip=True)
                        listing_data['price_or_rent'] = self._extract_number(price_text)
                    
                    # Extract configuration and area
                    config_elem = card.find(text=re.compile(r'\d+\s*BHK.*\d+\s*sq\.?ft', re.I))
                    if config_elem:
                        bhk_match = re.search(r'(\d+)\s*BHK', config_elem, re.I)
                        if bhk_match:
                            listing_data['unit_type'] = f"{bhk_match.group(1)}BHK"
                        
                        area_match = re.search(r'(\d+)\s*sq\.?ft', config_elem, re.I)
                        if area_match:
                            listing_data['area_value'] = float(area_match.group(1))
                            listing_data['area_unit'] = 'sqft'
                    
                    # Extract location
                    location_elem = card.find(['span', 'div'], class_=re.compile(r'mb-srp__card--addr|location|locality'))
                    if location_elem:
                        location_text = location_elem.get_text(strip=True)
                        listing_data['locality'] = location_text
                        if 'Delhi' in location_text or 'New Delhi' in location_text:
                            listing_data['city'] = 'New Delhi'
                    
                    listings.append(listing_data)
                    
                except Exception as e:
                    self.logger.warning(f"Error parsing Magicbricks listing card: {str(e)}")
                    continue
            
            self.logger.info(f"Parsed {len(listings)} listings from Magicbricks")
            
        except Exception as e:
            self.logger.error(f"Error parsing Magicbricks page: {str(e)}")
        
        return listings, societies
    
    def _parse_nobroker(self, html: str, url: str) -> Tuple[List[Dict], List[Dict]]:
        """Parse NoBroker listings and society data"""
        listings = []
        societies = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Parse listing cards
            listing_cards = soup.find_all(['div'], class_=re.compile(r'card|listing|property'))
            
            for card in listing_cards:
                try:
                    listing_data = {
                        'source': 'nobroker',
                        'page_url': url,
                        'site_listing_id': '',
                        'title': '',
                        'description_short': '',
                        'price_or_rent': None,
                        'area_value': None,
                        'area_unit': '',
                        'unit_type': '',
                        'furnishing': '',
                        'society_or_project': '',
                        'locality': '',
                        'city': '',
                        'floor_no': '',
                        'total_floors': '',
                        'posted_date': '',
                        'last_updated_on_site': '',
                        'seller_type': '',
                        'photos_count': None,
                        'amenities_listed': '',
                        'captured_timestamp': datetime.now(self.ist_tz).isoformat(),
                        'run_id': self.run_id
                    }
                    
                    # Extract title
                    title_elem = card.find(['h3', 'h4', 'a'], class_=re.compile(r'heading|title|propertyTitle'))
                    if title_elem:
                        listing_data['title'] = title_elem.get_text(strip=True)
                    
                    # Extract price
                    price_elem = card.find(['span', 'div'], class_=re.compile(r'price|rent|amount'))
                    if price_elem:
                        price_text = price_elem.get_text(strip=True)
                        listing_data['price_or_rent'] = self._extract_number(price_text)
                    
                    # Extract configuration
                    config_text = card.get_text()
                    bhk_match = re.search(r'(\d+)\s*(BHK|RK)', config_text, re.I)
                    if bhk_match:
                        listing_data['unit_type'] = bhk_match.group(0).upper()
                    
                    # Extract area
                    area_match = re.search(r'(\d+)\s*sq\.?\s*ft', config_text, re.I)
                    if area_match:
                        listing_data['area_value'] = float(area_match.group(1))
                        listing_data['area_unit'] = 'sqft'
                    
                    # Extract location
                    location_elem = card.find(['span', 'div'], text=re.compile(r'Delhi|Gurgaon|Noida', re.I))
                    if location_elem:
                        location_text = location_elem.get_text(strip=True)
                        listing_data['locality'] = location_text
                        listing_data['city'] = 'Delhi'
                    
                    listings.append(listing_data)
                    
                except Exception as e:
                    self.logger.warning(f"Error parsing NoBroker listing card: {str(e)}")
                    continue
            
            self.logger.info(f"Parsed {len(listings)} listings from NoBroker")
            
        except Exception as e:
            self.logger.error(f"Error parsing NoBroker page: {str(e)}")
        
        return listings, societies
    
    async def crawl_site(self, site_name: str) -> None:
        """Crawl a specific site"""
        site_config = self.config['sites'][site_name]
        urls = site_config['urls']
        max_pages = site_config.get('max_pages', 5)
        use_selenium = site_config.get('use_selenium', True)
        
        self.logger.info(f"Starting crawl of {site_name} with {len(urls)} URLs")
        
        driver = None
        if use_selenium:
            driver = self._get_selenium_driver()
        
        try:
            session = None
            if not use_selenium:
                session = aiohttp.ClientSession()
            
            for base_url in urls:
                for page in range(1, max_pages + 1):
                    try:
                        # Construct page URL
                        if '?' in base_url:
                            page_url = f"{base_url}&page={page}"
                        else:
                            page_url = f"{base_url}?page={page}"
                        
                        self.logger.info(f"Fetching {site_name} page {page}: {page_url}")
                        
                        # Fetch page content
                        html_content = None
                        if use_selenium and driver:
                            html_content = self._fetch_with_selenium(driver, page_url)
                        elif session:
                            html_content = await self._fetch_with_session(session, page_url)
                        
                        if not html_content:
                            self.logger.warning(f"Failed to fetch content from {page_url}")
                            continue
                        
                        # Parse the content
                        parser = self.site_parsers.get(site_name)
                        if parser:
                            listings, societies = parser(html_content, page_url)
                            self.listings_data.extend(listings)
                            self.societies_data.extend(societies)
                        
                        # Polite delay
                        await asyncio.sleep(self.request_delay)
                        
                    except Exception as e:
                        self.logger.error(f"Error crawling {site_name} page {page}: {str(e)}")
                        continue
            
            if session:
                await session.close()
                
        except Exception as e:
            self.logger.error(f"Critical error crawling {site_name}: {str(e)}")
        finally:
            if driver:
                driver.quit()
    
    def save_to_excel(self) -> str:
        """Save collected data to Excel file"""
        timestamp = datetime.now(self.ist_tz).strftime('%Y%m%d_%H%M')
        site_codes = '_'.join(self.config['sites'].keys())
        filename = f"scrape_{site_codes}_{timestamp}_run-{self.run_id}.xlsx"
        
        output_dir = Path(self.config['output']['directory'])
        output_dir.mkdir(exist_ok=True)
        output_path = output_dir / filename
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Listings sheet
                if self.listings_data:
                    listings_df = pd.DataFrame(self.listings_data)
                    # Convert numeric columns
                    numeric_columns = ['price_or_rent', 'area_value', 'photos_count']
                    for col in numeric_columns:
                        if col in listings_df.columns:
                            listings_df[col] = pd.to_numeric(listings_df[col], errors='coerce')
                    
                    listings_df.to_excel(writer, sheet_name='listings_raw', index=False)
                    self.logger.info(f"Saved {len(listings_df)} listings to Excel")
                
                # Societies sheet
                if self.societies_data:
                    societies_df = pd.DataFrame(self.societies_data)
                    societies_df.to_excel(writer, sheet_name='societies_raw', index=False)
                    self.logger.info(f"Saved {len(societies_df)} societies to Excel")
                else:
                    # Create empty societies sheet with headers
                    empty_societies = pd.DataFrame(columns=[
                        'source', 'page_url', 'society_name', 'address', 'locality', 'city',
                        'builder_name', 'towers', 'total_units', 'typical_unit_sizes',
                        'amenities', 'avg_price_or_rent_displayed', 'coordinates',
                        'captured_timestamp', 'run_id'
                    ])
                    empty_societies.to_excel(writer, sheet_name='societies_raw', index=False)
            
            self.logger.info(f"Excel file saved: {output_path}")
            return str(output_path)
            
        except Exception as e:
            self.logger.error(f"Error saving Excel file: {str(e)}")
            raise
    
    async def run(self):
        """Main crawler execution"""
        start_time = time.time()
        
        self.logger.info("="*60)
        self.logger.info(f"Real Estate Crawler Run ID: {self.run_id}")
        self.logger.info(f"Start Time: {datetime.now(self.ist_tz).strftime('%Y-%m-%d %H:%M:%S %Z')}")
        self.logger.info("="*60)
        
        try:
            # Crawl each site
            for site_name in self.config['sites'].keys():
                try:
                    await self.crawl_site(site_name)
                except Exception as e:
                    self.logger.error(f"Failed to crawl {site_name}: {str(e)}")
                    continue
            
            # Save results
            output_file = self.save_to_excel()
            
            # Final summary
            end_time = time.time()
            duration = end_time - start_time
            
            self.logger.info("="*60)
            self.logger.info("CRAWL SUMMARY")
            self.logger.info("="*60)
            self.logger.info(f"Run ID: {self.run_id}")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            self.logger.info(f"Total Listings: {len(self.listings_data)}")
            self.logger.info(f"Total Societies: {len(self.societies_data)}")
            self.logger.info(f"Output File: {output_file}")
            
            # Per-site breakdown
            for site in self.config['sites'].keys():
                site_listings = [l for l in self.listings_data if l['source'] == site]
                self.logger.info(f"{site}: {len(site_listings)} listings")
            
            self.logger.info("="*60)
            
        except Exception as e:
            self.logger.error(f"Critical crawler error: {str(e)}")
            raise


def main():
    """Main entry point"""
    try:
        crawler = RealEstateCrawler()
        asyncio.run(crawler.run())
        print("\n✅ Crawl completed successfully!")
        print(f"📁 Check the 'output' folder for Excel files")
        print(f"📄 Check the 'logs' folder for detailed logs")
        
    except KeyboardInterrupt:
        print("\n⚠️  Crawl interrupted by user")
    except Exception as e:
        print(f"\n❌ Crawl failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()