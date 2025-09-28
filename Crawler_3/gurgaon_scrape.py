#!/usr/bin/env python3
"""
Enhanced NoBroker scraper with automated locality discovery and sequential scraping
for comprehensive Gurgaon property data extraction
"""

import os
import time
import logging
import random
import pandas as pd
from datetime import datetime
import json
from typing import List, Dict, Optional, Set
import configparser
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, ElementNotInteractableException
)
import undetected_chromedriver as uc
import re

class GurgaonLocalityDiscoverer:
    """Class to discover and manage all Gurgaon localities for automated scraping"""
    
    def __init__(self, config):
        self.config = config
        self.driver = None
        self.wait = None
        
        # Comprehensive Gurgaon localities database
        self.known_localities = {
            # Sectors (Major ones)
            "sectors": [
                "Sector 1", "Sector 2", "Sector 3", "Sector 4", "Sector 5", "Sector 6", "Sector 7", 
                "Sector 8", "Sector 9", "Sector 10", "Sector 11", "Sector 12", "Sector 13", "Sector 14", 
                "Sector 15", "Sector 16", "Sector 17", "Sector 18", "Sector 19", "Sector 20", "Sector 21", 
                "Sector 22", "Sector 23", "Sector 24", "Sector 25", "Sector 26", "Sector 27", "Sector 28", 
                "Sector 29", "Sector 30", "Sector 31", "Sector 32", "Sector 33", "Sector 34", "Sector 35",
                "Sector 36", "Sector 37", "Sector 38", "Sector 39", "Sector 40", "Sector 41", "Sector 42",
                "Sector 43", "Sector 44", "Sector 45", "Sector 46", "Sector 47", "Sector 48", "Sector 49",
                "Sector 50", "Sector 51", "Sector 52", "Sector 53", "Sector 54", "Sector 55", "Sector 56",
                "Sector 57", "Sector 58", "Sector 59", "Sector 60", "Sector 61", "Sector 62", "Sector 63",
                "Sector 64", "Sector 65", "Sector 66", "Sector 67", "Sector 68", "Sector 69", "Sector 70",
                "Sector 71", "Sector 72", "Sector 73", "Sector 74", "Sector 75", "Sector 76", "Sector 77",
                "Sector 78", "Sector 79", "Sector 80", "Sector 81", "Sector 82", "Sector 83", "Sector 84",
                "Sector 85", "Sector 86", "Sector 87", "Sector 88", "Sector 89", "Sector 90", "Sector 91",
                "Sector 92", "Sector 93", "Sector 94", "Sector 95", "Sector 96", "Sector 97", "Sector 98",
                "Sector 99", "Sector 100", "Sector 101", "Sector 102", "Sector 103", "Sector 104", 
                "Sector 105", "Sector 106", "Sector 107", "Sector 108", "Sector 109", "Sector 110",
                "Sector 111", "Sector 112", "Sector 113", "Sector 114", "Sector 115"
            ],
            
            # Named Areas/Localities
            "named_areas": [
                "DLF Phase 1", "DLF Phase 2", "DLF Phase 3", "DLF Phase 4", "DLF Phase 5",
                "Golf Course Road", "Golf Course Extension Road", "Sohna Road", "MG Road",
                "Cyber City", "Udyog Vihar", "IMT Manesar", "Palam Vihar", "South City 1",
                "South City 2", "Nirvana Country", "Central Park 1", "Central Park 2",
                "Vatika City", "Emerald Hills", "Ardee City", "Sun City", "Rosewood City",
                "Palm Court", "Emaar MGF", "IREO", "Ansal API", "Unitech", "Bestech",
                
                # Older Gurgaon areas
                "Old Gurgaon", "Sadar Bazaar", "Jacobpura", "Shivaji Nagar", "Railway Road",
                "Civil Lines", "Model Town", "Laxman Vihar", "Shakti Nagar", "Arjun Nagar",
                "Rajiv Nagar", "Krishna Nagar", "Ram Nagar", "Shyam Nagar", "New Colony",
                "Mahendra Colony", "Surya Colony", "Jyoti Colony", "Gandhi Nagar", "Nehru Nagar",
                
                # Major residential areas
                "New Palam Vihar", "Uppal Southend", "Highland Park", "Greenwood City",
                "Pioneer Park", "Malibu Town", "Beverly Park", "New Town", "Ashok Vihar",
                "Mayfield Garden", "Sushant Lok 1", "Sushant Lok 2", "Sushant Lok 3",
                "DLF City", "Chakkarpur", "Nathupur", "Bhondsi", "Wazirabad", "Badshahpur",
                
                # IT/Corporate hubs
                "Cyber Hub", "Unitech Cyber Park", "DLF Cyber City", "Udyog Vihar Phase 1",
                "Udyog Vihar Phase 2", "Udyog Vihar Phase 3", "Udyog Vihar Phase 4",
                "Udyog Vihar Phase 5", "Industrial Area", "Manesar", "IMT Manesar",
                
                # Emerging areas
                "New Gurgaon", "Dwarka Expressway", "NH 8", "SPR", "Southern Peripheral Road",
                "NPR", "Northern Peripheral Road", "Faridabad Road", "Pataudi Road",
                
                # Villages/Surrounding areas
                "Garhi Harsaru", "Sukhrali", "Rampura", "Carterpuri", "Khandsa",
                "Ghata", "Sultanpur", "Kadipur", "Fazilpur", "Bhim Nagar", "Sirhaul",
                "Daultabad", "Tikri Kalan", "Tikri Khurd", "Jharsa", "Khoh"
            ]
        }
        
        self.all_localities = self.known_localities["sectors"] + self.known_localities["named_areas"]
        self.discovered_localities = set()
        self.failed_localities = set()
        self.scraped_localities = set()

    def setup_driver(self):
        """Setup Chrome WebDriver for locality discovery"""
        try:
            options = uc.ChromeOptions()
            options.add_argument("--start-maximized")
            options.add_argument("--disable-blink-features=AutomationControlled")
            
            self.driver = uc.Chrome(options=options)
            self.wait = WebDriverWait(self.driver, 10)
            
            logging.info("Driver setup completed for locality discovery")
            
        except Exception as e:
            logging.error(f"Driver setup failed: {e}")
            raise

    def discover_localities_from_website(self) -> Set[str]:
        """Discover localities directly from NoBroker's autocomplete/suggestion system"""
        try:
            self.setup_driver()
            
            # Navigate to NoBroker search page
            self.driver.get("https://www.nobroker.in/")
            time.sleep(3)
            
            # Look for location search input
            search_selectors = [
                "input[placeholder*='locality']",
                "input[placeholder*='city']", 
                "input[placeholder*='area']",
                "input[id*='location']",
                "input[class*='search']",
                ".search-input input"
            ]
            
            search_input = None
            for selector in search_selectors:
                try:
                    search_input = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if search_input.is_displayed():
                        break
                except:
                    continue
            
            if not search_input:
                logging.warning("Could not find search input, using predefined localities")
                return set(self.all_localities)
            
            discovered = set()
            
            # Try different search patterns to trigger autocomplete
            search_patterns = [
                "Gurgaon Sector",
                "Gurgaon DLF", 
                "Gurgaon Phase",
                "Gurgaon Udyog",
                "Gurgaon Sohna",
                "Gurgaon Golf",
                "Gurgaon Palam",
                "Gurgaon Manesar"
            ]
            
            for pattern in search_patterns:
                try:
                    search_input.clear()
                    search_input.send_keys(pattern)
                    time.sleep(2)
                    
                    # Look for autocomplete suggestions
                    suggestion_selectors = [
                        ".autocomplete-suggestion",
                        ".suggestion-item",
                        ".dropdown-item",
                        "[class*='suggestion']",
                        "[class*='dropdown'] li",
                        ".search-results li"
                    ]
                    
                    for selector in suggestion_selectors:
                        try:
                            suggestions = self.driver.find_elements(By.CSS_SELECTOR, selector)
                            for suggestion in suggestions:
                                text = suggestion.text.strip()
                                if "gurgaon" in text.lower() and len(text) > 5:
                                    # Extract locality name
                                    locality = self._extract_locality_name(text)
                                    if locality:
                                        discovered.add(locality)
                        except:
                            continue
                    
                except Exception as e:
                    logging.debug(f"Search pattern '{pattern}' failed: {e}")
                    continue
            
            logging.info(f"Discovered {len(discovered)} additional localities from website")
            return discovered
            
        except Exception as e:
            logging.error(f"Locality discovery from website failed: {e}")
            return set()
        
        finally:
            if self.driver:
                self.driver.quit()

    def _extract_locality_name(self, text: str) -> Optional[str]:
        """Extract clean locality name from suggestion text"""
        try:
            # Remove common prefixes/suffixes
            text = text.replace("Gurgaon", "").replace("Gurugram", "").strip()
            text = re.sub(r'^[,\-\s]+|[,\-\s]+$', '', text)
            
            # Filter out too short or invalid names
            if len(text) < 3 or text.isdigit():
                return None
            
            # Remove parenthetical information
            text = re.sub(r'\([^)]*\)', '', text).strip()
            
            return text if text else None
            
        except Exception:
            return None

    def get_all_localities(self) -> List[str]:
        """Get comprehensive list of all Gurgaon localities"""
        try:
            # Start with known localities
            all_localities = set(self.all_localities)
            
            # Add discovered localities from website
            discovered = self.discover_localities_from_website()
            all_localities.update(discovered)
            
            # Convert to sorted list for consistent processing
            locality_list = sorted(list(all_localities))
            
            logging.info(f"Total localities identified: {len(locality_list)}")
            
            return locality_list
            
        except Exception as e:
            logging.error(f"Failed to get all localities: {e}")
            return self.all_localities

    def create_locality_batches(self, localities: List[str], batch_size: int = 3) -> List[List[str]]:
        """Create batches of localities for NoBroker's 3-locality limit"""
        batches = []
        
        for i in range(0, len(localities), batch_size):
            batch = localities[i:i + batch_size]
            batches.append(batch)
        
        logging.info(f"Created {len(batches)} batches from {len(localities)} localities")
        return batches


class AutomatedGurgaonScraper:
    """Main scraper class that handles automated locality-based scraping"""
    
    def __init__(self, config, base_scraper_class):
        self.config = config
        self.base_scraper_class = base_scraper_class
        self.locality_discoverer = GurgaonLocalityDiscoverer(config)
        
        # Tracking
        self.all_extracted_data = []
        self.processing_stats = {
            "total_localities": 0,
            "successful_localities": 0,
            "failed_localities": 0,
            "total_listings": 0,
            "start_time": datetime.now()
        }
        
        # Output management
        self.output_dir = config.get("output", "output_dir", fallback="automated_output")
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    def build_search_url(self, localities: List[str], property_type: str = "1bhk", transaction_type: str = "sale") -> str:
        """Build NoBroker search URL with specific localities"""
        try:
            base_url = "https://www.nobroker.in"
            
            # URL encode localities
            from urllib.parse import quote_plus
            locality_params = []
            
            for locality in localities:
                encoded_locality = quote_plus(f"{locality}, Gurgaon")
                locality_params.append(encoded_locality)
            
            # Create search URL
            locality_string = ",".join(locality_params)
            
            if transaction_type.lower() == "sale":
                url = f"{base_url}/{property_type}-flats-for-sale-in-{locality_string.lower().replace(' ', '-')}"
            else:
                url = f"{base_url}/{property_type}-flats-for-rent-in-{locality_string.lower().replace(' ', '-')}"
            
            # Fallback to general search if URL construction fails
            if len(url) > 2000:  # URLs too long may fail
                return f"{base_url}/property/{transaction_type}/gurgaon?searchParam={locality_string[:100]}"
            
            return url
            
        except Exception as e:
            logging.error(f"URL building failed: {e}")
            # Fallback URL
            return "https://www.nobroker.in/property/sale/gurgaon"

    def scrape_locality_batch(self, localities: List[str], batch_index: int) -> List[Dict]:
        """Scrape a specific batch of localities"""
        try:
            logging.info(f"Starting batch {batch_index + 1}: {localities}")
            
            # Build search URL
            search_url = self.build_search_url(localities)
            logging.info(f"Search URL: {search_url}")
            
            # Initialize scraper instance
            scraper = self.base_scraper_class(
                config=self.config,
                run_id=f"{self.run_id}_batch_{batch_index + 1}",
                start_ts=datetime.now()
            )
            
            # Run extraction
            extracted_data = []
            try:
                scraper._setup_enhanced_webdriver()
                
                if scraper.navigate_and_setup(search_url):
                    # Reduce manual wait time for automation
                    scraper.manual_wait_time = 3
                    
                    extracted_data = scraper.extract_all_listings()
                    
                    # Add locality batch info to each record
                    for record in extracted_data:
                        record['locality_batch'] = ', '.join(localities)
                        record['batch_index'] = batch_index + 1
                        record['search_url'] = search_url
                
            except Exception as extraction_e:
                logging.error(f"Extraction failed for batch {batch_index + 1}: {extraction_e}")
                
            finally:
                try:
                    if scraper.driver:
                        scraper.driver.quit()
                except:
                    pass
            
            if extracted_data:
                self.processing_stats["successful_localities"] += len(localities)
                self.processing_stats["total_listings"] += len(extracted_data)
                logging.info(f"Batch {batch_index + 1} completed: {len(extracted_data)} listings extracted")
            else:
                self.processing_stats["failed_localities"] += len(localities)
                logging.warning(f"Batch {batch_index + 1} failed: No data extracted")
            
            return extracted_data
            
        except Exception as e:
            logging.error(f"Batch {batch_index + 1} processing failed: {e}")
            self.processing_stats["failed_localities"] += len(localities)
            return []

    def run_automated_scraping(self) -> Optional[str]:
        """Main method to run automated scraping across all Gurgaon localities"""
        try:
            logging.info("Starting automated Gurgaon-wide scraping...")
            
            # Get all localities
            all_localities = self.locality_discoverer.get_all_localities()
            self.processing_stats["total_localities"] = len(all_localities)
            
            # Create batches for NoBroker's locality limit
            locality_batches = self.locality_discoverer.create_locality_batches(all_localities, batch_size=3)
            
            logging.info(f"Will process {len(locality_batches)} batches covering {len(all_localities)} localities")
            
            # Process each batch
            for batch_index, batch_localities in enumerate(locality_batches):
                try:
                    logging.info(f"\n{'='*60}")
                    logging.info(f"Processing Batch {batch_index + 1}/{len(locality_batches)}")
                    logging.info(f"Localities: {batch_localities}")
                    logging.info(f"{'='*60}")
                    
                    batch_data = self.scrape_locality_batch(batch_localities, batch_index)
                    
                    if batch_data:
                        self.all_extracted_data.extend(batch_data)
                        
                        # Save intermediate results every 5 batches
                        if (batch_index + 1) % 5 == 0:
                            self._save_intermediate_results(batch_index + 1)
                    
                    # Delay between batches to be respectful
                    if batch_index < len(locality_batches) - 1:
                        delay = random.uniform(5, 10)
                        logging.info(f"Waiting {delay:.1f}s before next batch...")
                        time.sleep(delay)
                
                except KeyboardInterrupt:
                    logging.info("User interrupted scraping")
                    break
                    
                except Exception as batch_e:
                    logging.error(f"Batch {batch_index + 1} failed: {batch_e}")
                    continue
            
            # Save final results
            if self.all_extracted_data:
                output_path = self._save_final_results()
                self._print_automation_summary()
                return output_path
            else:
                logging.warning("No data was extracted from any locality")
                return None
                
        except Exception as e:
            logging.error(f"Automated scraping failed: {e}")
            return None

    def _save_intermediate_results(self, batch_count: int):
        """Save intermediate results during processing"""
        try:
            if not self.all_extracted_data:
                return
            
            df = pd.DataFrame(self.all_extracted_data)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"gurgaon_automated_intermediate_batch{batch_count}_{timestamp}.xlsx"
            filepath = os.path.join(self.output_dir, filename)
            
            df.to_excel(filepath, index=False)
            logging.info(f"Intermediate results saved: {filepath}")
            
        except Exception as e:
            logging.error(f"Failed to save intermediate results: {e}")

    def _save_final_results(self) -> str:
        """Save final comprehensive results with analysis"""
        try:
            df = pd.DataFrame(self.all_extracted_data)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"gurgaon_1bhk_automated_complete_{timestamp}.xlsx"
            filepath = os.path.join(self.output_dir, filename)
            
            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                # Main data
                df.to_excel(writer, sheet_name="All_Properties", index=False)
                
                # Locality-wise summary
                if 'locality_batch' in df.columns:
                    locality_summary = df.groupby('locality_batch').agg({
                        'listing_index': 'count',
                        'price': lambda x: x.notna().sum(),
                        'nearby_places_count': 'mean'
                    }).round(2)
                    locality_summary.columns = ['Total_Listings', 'With_Price', 'Avg_Nearby_Places']
                    locality_summary.to_excel(writer, sheet_name="Locality_Summary")
                
                # Automation statistics
                stats_data = {
                    "Metric": [
                        "Total Localities Targeted",
                        "Successful Localities", 
                        "Failed Localities",
                        "Total Properties Found",
                        "Properties with Price Info",
                        "Processing Duration (hours)",
                        "Average Properties per Locality",
                        "Success Rate (%)"
                    ],
                    "Value": [
                        self.processing_stats["total_localities"],
                        self.processing_stats["successful_localities"],
                        self.processing_stats["failed_localities"],
                        len(df),
                        len(df[df['price'].notna() & (df['price'] != '')]),
                        f"{(datetime.now() - self.processing_stats['start_time']).total_seconds() / 3600:.2f}",
                        f"{len(df) / max(1, self.processing_stats['successful_localities']):.1f}",
                        f"{(self.processing_stats['successful_localities'] / max(1, self.processing_stats['total_localities'])) * 100:.1f}"
                    ]
                }
                
                stats_df = pd.DataFrame(stats_data)
                stats_df.to_excel(writer, sheet_name="Automation_Stats", index=False)
            
            logging.info(f"Final results saved: {filepath}")
            return filepath
            
        except Exception as e:
            logging.error(f"Failed to save final results: {e}")
            return ""

    def _print_automation_summary(self):
        """Print comprehensive automation summary"""
        duration = datetime.now() - self.processing_stats["start_time"]
        
        print("\n" + "="*80)
        print("🎉 AUTOMATED GURGAON SCRAPING COMPLETED")
        print("="*80)
        
        print(f"📍 COVERAGE STATISTICS:")
        print(f"  • Total localities targeted: {self.processing_stats['total_localities']}")
        print(f"  • Successfully processed: {self.processing_stats['successful_localities']}")
        print(f"  • Failed localities: {self.processing_stats['failed_localities']}")
        
        if self.processing_stats['total_localities'] > 0:
            success_rate = (self.processing_stats['successful_localities'] / self.processing_stats['total_localities']) * 100
            print(f"  • Coverage success rate: {success_rate:.1f}%")
        
        print(f"\n📊 EXTRACTION RESULTS:")
        print(f"  • Total properties found: {self.processing_stats['total_listings']}")
        
        if self.all_extracted_data:
            df = pd.DataFrame(self.all_extracted_data)
            price_count = len(df[df['price'].notna() & (df['price'] != '')])
            print(f"  • Properties with price info: {price_count}/{len(df)} ({price_count/len(df)*100:.1f}%)")
            
            if 'locality_batch' in df.columns:
                unique_batches = df['locality_batch'].nunique()
                print(f"  • Locality batches with data: {unique_batches}")
                
                avg_per_locality = len(df) / max(1, self.processing_stats['successful_localities'])
                print(f"  • Average properties per successful locality: {avg_per_locality:.1f}")
        
        print(f"\n⏱️ TIMING:")
        print(f"  • Total processing time: {duration.total_seconds()/3600:.2f} hours")
        print(f"  • Started: {self.processing_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  • Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("="*80)


def create_automated_config():
    """Create configuration optimized for automated scraping"""
    config = configparser.ConfigParser()
    
    config["limits"] = {
        "max_listings_per_society": "50",  # Reduced for faster processing
        "max_scrolls": "30"
    }
    
    config["manual"] = {
        "selection_wait_time": "3"  # Reduced for automation
    }
    
    config["output"] = {
        "output_dir": "automated_gurgaon_output"
    }
    
    config["http"] = {
        "min_delay": "1.0",
        "max_delay": "2.0"  # Faster but still respectful
    }
    
    config["automation"] = {
        "batch_size": "3",  # NoBroker's locality limit
        "batch_delay_min": "5",
        "batch_delay_max": "10",
        "intermediate_save_frequency": "5"
    }
    
    return config


def main_automated():
    """Main function for automated Gurgaon-wide scraping"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("automated_gurgaon_scraper.log")
        ]
    )
    
    print("=" * 80)
    print("🏠 AUTOMATED GURGAON-WIDE 1BHK/1RK PROPERTY SCRAPER")
    print("=" * 80)
    print("🔄 AUTOMATION FEATURES:")
    print("  • Discovers all Gurgaon localities automatically")
    print("  • Processes localities in batches of 3 (NoBroker limit)")
    print("  • Sequential scraping across all areas")
    print("  • Comprehensive coverage of sectors and named areas")
    print("  • Automatic intermediate saves every 5 batches")
    print("  • Handles failures gracefully and continues")
    print("=" * 80)
    
    try:
        # Import the base scraper class from your existing script
        from your_existing_script import NoBrokerScraper  # Adjust import as needed
        
        # Create automated configuration
        config = create_automated_config()
        
        # Initialize automated scraper
        automated_scraper = AutomatedGurgaonScraper(config, NoBrokerScraper)
        
        print(f"\n🚀 Starting automated scraping...")
        print(f"📍 Target: All 1BHK/1RK flats for sale in Gurgaon")
        print(f"⏱️  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # Confirm before starting
        response = input("\nProceed with automated scraping? (y/N): ").strip().lower()
        if response not in ['y', 'yes']:
            print("Automation cancelled.")
            return
        
        # Run automated scraping
        output_path = automated_scraper.run_automated_scraping()
        
        if output_path:
            print(f"\n✅ AUTOMATION COMPLETED SUCCESSFULLY!")
            print(f"📁 Complete data saved to: {output_path}")
            print(f"📊 Check all sheets for comprehensive analysis")
        else:
            print(f"\n⚠️ AUTOMATION COMPLETED WITH ISSUES")
            print(f"❌ Limited or no data was extracted")
        
    except ImportError:
        print("\n❌ ERROR: Could not import base scraper class")
        print("💡 Make sure to adjust the import statement to match your existing script")
    
    except KeyboardInterrupt:
        print("\n⚠️ AUTOMATION INTERRUPTED BY USER")
        print("💾 Check output directory for any intermediate saves")
    
    except Exception as e:
        print(f"\n💥 AUTOMATION FAILED: {e}")
        logging.error(f"Automation failed: {e}")


if __name__ == "__main__":
    main_automated()