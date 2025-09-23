import os
import time
import logging
import random
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import re
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin, urlparse
import configparser
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementNotInteractableException
import cv2
import numpy as np
import requests
from PIL import Image
import pytesseract
import base64
from io import BytesIO


class ImageProcessor:
    """Advanced image processing for property listings"""
    
    def __init__(self, config):
        self.config = config
        self.enable_ocr = config.getboolean("image_processing", "enable_ocr", fallback=False)
        self.download_images = config.getboolean("image_processing", "download_images", fallback=False)
        self.image_analysis = config.getboolean("image_processing", "analyze_images", fallback=False)
        
    def analyze_property_image(self, image_url, card_data):
        """Analyze property images to extract additional information"""
        analysis_results = {
            'image_quality_score': 0,
            'detected_rooms': [],
            'image_text': '',
            'color_analysis': {},
            'image_type': 'unknown'
        }
        
        try:
            if not self.image_analysis:
                return analysis_results
                
            # Download image
            response = requests.get(image_url, timeout=10, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            
            if response.status_code != 200:
                return analysis_results
                
            # Convert to OpenCV format
            image = Image.open(BytesIO(response.content))
            cv_image = cv2.cvtColor(np.array(image), cv2.COLOR_RGB2BGR)
            
            # Image quality analysis
            analysis_results['image_quality_score'] = self._calculate_image_quality(cv_image)
            
            # Color analysis
            analysis_results['color_analysis'] = self._analyze_colors(cv_image)
            
            # OCR text extraction
            if self.enable_ocr:
                analysis_results['image_text'] = self._extract_text_from_image(image)
            
            # Room detection (basic)
            analysis_results['detected_rooms'] = self._detect_rooms(cv_image)
            
            # Image type classification
            analysis_results['image_type'] = self._classify_image_type(cv_image, card_data.get('full_card_text', ''))
            
            return analysis_results
            
        except Exception as e:
            logging.debug(f"Image analysis failed for {image_url}: {e}")
            return analysis_results
    
    def _calculate_image_quality(self, image):
        """Calculate image quality score based on sharpness and brightness"""
        try:
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Sharpness using Laplacian variance
            sharpness = cv2.Laplacian(gray, cv2.CV_64F).var()
            
            # Brightness
            brightness = np.mean(gray)
            
            # Normalize and combine scores
            sharpness_score = min(sharpness / 1000, 1) * 50  # Max 50 points
            brightness_score = (1 - abs(brightness - 128) / 128) * 50  # Max 50 points
            
            return round(sharpness_score + brightness_score, 1)
            
        except Exception:
            return 0
    
    def _analyze_colors(self, image):
        """Analyze dominant colors in the image"""
        try:
            # Resize for faster processing
            small_image = cv2.resize(image, (150, 150))
            
            # Convert to RGB
            rgb_image = cv2.cvtColor(small_image, cv2.COLOR_BGR2RGB)
            
            # Reshape for k-means
            data = rgb_image.reshape((-1, 3))
            data = np.float32(data)
            
            # Apply k-means clustering
            criteria = (cv2.TERM_CRITERIA_EPS + cv2.TERM_CRITERIA_MAX_ITER, 10, 1.0)
            k = 3
            _, labels, centers = cv2.kmeans(data, k, None, criteria, 10, cv2.KMEANS_RANDOM_CENTERS)
            
            # Calculate color percentages
            unique, counts = np.unique(labels, return_counts=True)
            color_percentages = {}
            
            for i, center in enumerate(centers):
                if i in unique:
                    percentage = (counts[np.where(unique == i)[0][0]] / len(data)) * 100
                    color_name = self._get_color_name(center)
                    color_percentages[color_name] = round(percentage, 1)
            
            return color_percentages
            
        except Exception:
            return {}
    
    def _get_color_name(self, rgb):
        """Convert RGB to color name"""
        r, g, b = rgb
        
        if r > 200 and g > 200 and b > 200:
            return 'white'
        elif r < 50 and g < 50 and b < 50:
            return 'black'
        elif r > g and r > b:
            return 'red'
        elif g > r and g > b:
            return 'green'
        elif b > r and b > g:
            return 'blue'
        elif r > 150 and g > 150:
            return 'yellow'
        elif r > 100 and g < 100 and b < 100:
            return 'brown'
        else:
            return 'mixed'
    
    def _extract_text_from_image(self, image):
        """Extract text from image using OCR"""
        try:
            # Configure tesseract for better accuracy
            config = '--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ₹.,/-'
            text = pytesseract.image_to_string(image, config=config)
            
            # Clean and filter text
            cleaned_text = ' '.join(text.split())
            
            # Extract meaningful information
            price_match = re.search(r'₹\s*[\d,\.]+\s*(cr|crore|lakh|l)', cleaned_text, re.IGNORECASE)
            area_match = re.search(r'\d+\s*(?:sq\.?ft|sqft)', cleaned_text, re.IGNORECASE)
            
            extracted_info = []
            if price_match:
                extracted_info.append(f"Price: {price_match.group()}")
            if area_match:
                extracted_info.append(f"Area: {area_match.group()}")
            
            return '; '.join(extracted_info) if extracted_info else cleaned_text[:200]
            
        except Exception as e:
            logging.debug(f"OCR extraction failed: {e}")
            return ''
    
    def _detect_rooms(self, image):
        """Basic room detection using simple image analysis"""
        try:
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Simple room indicators
            rooms = []
            
            # Look for rectangular shapes (windows, doors)
            edges = cv2.Canny(gray, 50, 150)
            contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
            
            rectangular_shapes = 0
            for contour in contours:
                approx = cv2.approxPolyDP(contour, 0.02 * cv2.arcLength(contour, True), True)
                if len(approx) == 4:
                    rectangular_shapes += 1
            
            if rectangular_shapes > 5:
                rooms.append('living_room')
            if rectangular_shapes > 10:
                rooms.append('bedroom')
            if rectangular_shapes > 15:
                rooms.append('kitchen')
                
            return rooms
            
        except Exception:
            return []
    
    def _classify_image_type(self, image, text_context):
        """Classify image type based on visual and contextual clues"""
        try:
            # Simple classification based on text context
            text_lower = text_context.lower()
            
            if any(word in text_lower for word in ['exterior', 'building', 'facade']):
                return 'exterior'
            elif any(word in text_lower for word in ['living', 'hall', 'room']):
                return 'interior'
            elif any(word in text_lower for word in ['kitchen', 'bathroom', 'bedroom']):
                return 'room_specific'
            elif any(word in text_lower for word in ['amenity', 'facility', 'gym', 'pool']):
                return 'amenity'
            else:
                return 'general'
                
        except Exception:
            return 'unknown'


class MagicBricksScraper:
    def __init__(self, config, run_id, start_ts):
        self.config = config
        self.run_id = run_id
        self.start_ts = start_ts

        # Configuration settings
        self.max_listings = int(config.get("limits", "max_listings_per_society", fallback="300"))
        self.max_scrolls = int(config.get("limits", "max_scrolls", fallback="35"))
        self.skip_no_images = config.getboolean("extraction_settings", "skip_cards_without_images", fallback=False)
        
        # Use the specific URL provided
        self.search_url = "https://www.magicbricks.com/property-for-sale/residential-real-estate?bedroom=1&cityName=Gurgaon"
        
        # Output settings
        self.output_dir = config.get("output", "output_dir", fallback="output")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Incremental save settings
        self.incremental_save = config.getboolean("output", "incremental_save", fallback=True)
        self.save_batch_size = int(config.get("output", "save_batch_size", fallback="10"))
        
        # Initialize output file paths
        timestamp = self.start_ts.strftime('%Y%m%d_%H%M%S')
        self.temp_output_path = os.path.join(self.output_dir, f"magicbricks_temp_{self.run_id}_{timestamp}.xlsx")
        self.final_output_path = os.path.join(self.output_dir, f"magicbricks_enhanced_{self.run_id}_{timestamp}.xlsx")
        self.backup_csv_path = os.path.join(self.output_dir, f"magicbricks_backup_{self.run_id}_{timestamp}.csv")
        
        # Data storage for incremental saves
        self.extracted_data = []
        
        # Delays
        self.min_delay = float(config.get("http", "min_delay", fallback="1.2"))
        self.max_delay = float(config.get("http", "max_delay", fallback="3.5"))

        # Enhanced interaction settings
        self.deep_extraction = config.getboolean("extraction_settings", "deep_extraction", fallback=True)
        self.click_dropdowns = config.getboolean("extraction_settings", "click_dropdowns", fallback=True)
        self.extract_hidden_info = config.getboolean("extraction_settings", "extract_hidden_info", fallback=True)

        # Initialize image processor
        self.image_processor = ImageProcessor(config)

        # Enhanced extraction patterns
        self.extraction_patterns = {
            'price': [
                r'₹\s*([0-9,\.]+)\s*(cr|crore|l|lakh|k|thousand)?',
                r'rs\.?\s*([0-9,\.]+)\s*(cr|crore|l|lakh|k|thousand)?',
                r'(\d+(?:,\d+)*(?:\.\d+)?)\s*(cr|crore|l|lakh|k|thousand)',
            ],
            'area': [
                r'(\d+(?:,\d+)*(?:\.\d+)?)\s*(?:sq\.?\s*ft|sqft|sq\s*feet)',
                r'(\d+(?:,\d+)*(?:\.\d+)?)\s*(?:sq\.?\s*m|sqm)',
                r'area[:\s]*(\d+(?:,\d+)*(?:\.\d+)?)',
            ],
            'bhk': [
                r'(\d+)\s*(?:bhk|bh k|bed)',
                r'(\d+)\s*(?:bedroom|bed\s*room)',
            ],
            'bathroom': [
                r'(\d+)\s*(?:bath|bathroom|toilet)',
                r'(\d+)\s*(?:washroom|wash\s*room)',
            ],
            'parking': [
                r'(\d+)\s*(?:parking|car\s*park)',
                r'parking[:\s]*(\d+)',
            ],
            'floor': [
                r'(\d+)(?:st|nd|rd|th)?\s*(?:floor|flr)',
                r'floor[:\s]*(\d+)',
                r'(\d+)\s*out\s*of\s*(\d+)',
            ]
        }

        self.stats = {
            "total_processed": 0,
            "successful_extractions": 0,
            "cards_with_images": 0,
            "cards_without_images": 0,
            "dropdowns_opened": 0,
            "enhanced_extractions": 0,
            "images_analyzed": 0,
            "hidden_info_extracted": 0
        }

    def _save_data_incremental(self, force_save=False):
        """Save data incrementally to prevent data loss"""
        if not self.incremental_save:
            return
        
        # Check if we should save (batch reached or forced)
        if not force_save and len(self.extracted_data) < self.save_batch_size:
            return
        
        if not self.extracted_data:
            return
        
        try:
            df = pd.DataFrame(self.extracted_data)
            
            # Save as CSV backup (faster and more reliable for incremental saves)
            df.to_csv(self.backup_csv_path, index=False)
            
            # Save as Excel with error handling
            try:
                with pd.ExcelWriter(self.temp_output_path, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Listings', index=False)
                    
                    # Add progress sheet
                    progress_data = {
                        'Metric': [
                            'Total Extracted', 'Cards with Images', 'Cards without Images', 
                            'Dropdowns Opened', 'Images Analyzed', 'Last Updated'
                        ],
                        'Value': [
                            len(self.extracted_data),
                            self.stats['cards_with_images'], 
                            self.stats['cards_without_images'],
                            self.stats['dropdowns_opened'],
                            self.stats.get('images_analyzed', 0),
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        ]
                    }
                    progress_df = pd.DataFrame(progress_data)
                    progress_df.to_excel(writer, sheet_name='Progress', index=False)
                
                logging.info("Incremental save completed: %d records saved to %s", 
                           len(self.extracted_data), self.temp_output_path)
                
            except Exception as excel_error:
                logging.warning("Excel save failed, CSV backup available: %s", excel_error)
                
        except Exception as e:
            logging.error("Incremental save failed: %s", e)

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        import signal
        import sys
        
        def signal_handler(sig, frame):
            logging.info("Interrupt received (Ctrl+C), saving data and exiting gracefully...")
            self._save_data_incremental(force_save=True)
            self._finalize_output()
            try:
                self.driver.quit()
            except:
                pass
            logging.info("Data saved successfully. Exiting.")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def _finalize_output(self):
        """Finalize output files with proper formatting"""
        if not self.extracted_data:
            logging.warning("No data to finalize")
            return None
        
        try:
            df = pd.DataFrame(self.extracted_data)
            
            # Define column order for better organization
            preferred_columns = [
                'card_number', 'extraction_timestamp', 'title', 'price', 'locality', 'society_name',
                'configuration', 'property_type', 'transaction_type', 'status', 
                'super_area', 'carpet_area', 'built_up_area', 'plot_area',
                'images_count', 'has_images', 'primary_image_url',
                'property_detail_url', 'society_url', 'total_links_count',
                'furnishing', 'bathrooms', 'parking', 'balconies', 'floor_info',
                'posted_date', 'owner_type', 'amenities', 'features',
                'extraction_completeness', 'full_card_text'
            ]
            
            # Reorder columns
            available_columns = [col for col in preferred_columns if col in df.columns]
            remaining_columns = [col for col in df.columns if col not in preferred_columns]
            final_columns = available_columns + remaining_columns
            df = df[final_columns]
            
            # Save final Excel file
            with pd.ExcelWriter(self.final_output_path, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Listings', index=False)
                
                # Add comprehensive summary sheet
                summary_data = {
                    'Metric': [
                        'Total Listings', 'Cards with Images', 'Cards without Images', 
                        'Dropdowns Opened', 'Images Analyzed', 'Average Completeness',
                        'Extraction Started', 'Extraction Completed', 'Duration (minutes)'
                    ],
                    'Value': [
                        len(self.extracted_data),
                        self.stats['cards_with_images'], 
                        self.stats['cards_without_images'],
                        self.stats['dropdowns_opened'],
                        self.stats.get('images_analyzed', 0),
                        f"{df['extraction_completeness'].mean():.1f}%" if 'extraction_completeness' in df.columns else 'N/A',
                        self.start_ts.strftime('%Y-%m-%d %H:%M:%S'),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        f"{(datetime.now() - self.start_ts).total_seconds() / 60:.1f}"
                    ]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                
                # Add extraction log sheet if available
                if hasattr(self, 'extraction_log'):
                    log_df = pd.DataFrame(self.extraction_log)
                    log_df.to_excel(writer, sheet_name='Extraction_Log', index=False)
            
            # Clean up temp file
            if os.path.exists(self.temp_output_path):
                try:
                    os.remove(self.temp_output_path)
                except:
                    pass
            
            logging.info("Final output saved: %s", self.final_output_path)
            return self.final_output_path
            
        except Exception as e:
            logging.error("Failed to finalize output: %s", e)
            # Return backup CSV if Excel fails
            if os.path.exists(self.backup_csv_path):
                logging.info("Returning CSV backup: %s", self.backup_csv_path)
                return self.backup_csv_path
            return None

    def _setup_webdriver(self):
        """Setup Chrome WebDriver with enhanced automation capabilities"""
        opts = webdriver.ChromeOptions()
        opts.add_argument("--start-maximized")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-web-security")
        opts.add_argument("--allow-running-insecure-content")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option('useAutomationExtension', False)
        
        # Enhanced user agent rotation
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ]
        opts.add_argument(f"--user-agent={random.choice(user_agents)}")
        
        self.driver = webdriver.Chrome(options=opts)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.wait = WebDriverWait(self.driver, 30)
        self.actions = ActionChains(self.driver)

    def _smart_sleep(self, min_time=None, max_time=None):
        """Intelligent sleep with jitter"""
        min_delay = min_time or self.min_delay
        max_delay = max_time or self.max_delay
        sleep_time = random.uniform(min_delay, max_delay)
        time.sleep(sleep_time)

    def _extract_with_patterns(self, text, field_type):
        """Extract data using regex patterns"""
        if not text or field_type not in self.extraction_patterns:
            return None
            
        text = str(text).lower().strip()
        
        for pattern in self.extraction_patterns[field_type]:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if field_type == 'floor' and len(match.groups()) > 1:
                    return f"{match.group(1)} of {match.group(2)}"
                return match.group(1).replace(',', '')
        
        return None

    def _click_and_expand_all_elements(self, card):
        """Advanced element expansion with image processing insights"""
        expandable_selectors = [
            # Transaction dropdown and details
            ".mb-srp__card__summary__transaction-type .dropdown",
            ".transaction .dropdown-toggle",
            "[data-testid*='transaction']",
            
            # Property details dropdowns  
            ".mb-srp__card__amenities__toggle",
            ".amenities-toggle", ".features-toggle", ".details-toggle",
            ".show-more", ".view-more", ".expand", ".dropdown-toggle",
            "[class*='expand']", "[class*='toggle']", "[class*='more']",
            
            # Image galleries and carousels
            ".image-gallery", ".photo-carousel", ".property-images",
            ".next-button", ".carousel-next",
            
            # Additional info sections
            ".property-highlights", ".key-features", ".project-details",
            ".builder-info", ".price-details"
        ]
        
        expanded_count = 0
        hidden_info = {}
        
        for selector in expandable_selectors:
            try:
                elements = card.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    try:
                        # Check if element is clickable and visible
                        if element.is_displayed() and element.is_enabled():
                            # Store pre-click state
                            pre_text = card.text
                            
                            # Scroll element into view
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", element)
                            self._smart_sleep(0.5, 1.0)
                            
                            # Try clicking
                            element.click()
                            expanded_count += 1
                            self._smart_sleep(1.0, 2.0)
                            
                            # Check for new content
                            post_text = card.text
                            if len(post_text) > len(pre_text):
                                new_content = post_text[len(pre_text):].strip()
                                if new_content:
                                    hidden_info[f'expanded_content_{expanded_count}'] = new_content
                            
                            logging.debug(f"Expanded element: {selector}")
                            
                    except (ElementNotInteractableException, Exception) as e:
                        logging.debug(f"Could not interact with {selector}: {e}")
                        continue
                        
            except Exception as e:
                logging.debug(f"Error finding expandable elements with {selector}: {e}")
                continue
        
        if expanded_count > 0:
            self.stats["dropdowns_opened"] += expanded_count
            self.stats["hidden_info_extracted"] += len(hidden_info)
            logging.info(f"  Expanded {expanded_count} elements, found {len(hidden_info)} hidden sections")
        
        return expanded_count > 0, hidden_info

    def _extract_comprehensive_data_with_images(self, card, card_index):
        """Main extraction with advanced image processing"""
        logging.info(f"Processing card {card_index}")
        
        data = {
            'card_number': card_index,
            'extraction_timestamp': datetime.now().isoformat()
        }
        
        try:
            # Step 1: Expand all interactive elements
            expanded, hidden_info = self._click_and_expand_all_elements(card)
            data.update(hidden_info)
            
            # Step 2: Get comprehensive text after expansion
            full_text = card.text.strip()
            data['full_card_text'] = full_text
            
            # Step 3: Extract structured data using patterns
            for field_type in self.extraction_patterns:
                extracted = self._extract_with_patterns(full_text, field_type)
                if extracted:
                    data[field_type] = extracted
            
            # Step 4: Extract specific fields with enhanced selectors
            field_extractions = {
                'title': ['.mb-srp__card__title', '.cardTitle', 'h2', 'h3', '.title', '[data-testid*="title"]'],
                'price': ['.mb-srp__card__price', '.price', '.priceValue', '[data-testid*="price"]'],
                'locality': ['.mb-srp__card__summary__locality', '.locality', '.location', '[data-testid*="locality"]'],
                'society_name': ['.societyName', '.projectName', '.buildingName', 'a[href*="project"]'],
                'configuration': ['.mb-srp__card__summary__config', '.configuration', '.bhk', '[data-testid*="config"]'],
                'property_type': ['.propertyType', '.apartmentType', '[data-testid*="type"]'],
                'transaction_type': ['.mb-srp__card__badge', '.transaction', '[data-testid*="transaction"]'],
                'status': ['.status', '.availability', '[data-testid*="status"]'],
                'furnishing': ['.furnishing', '.furnished', '[data-testid*="furnish"]'],
                'posted_date': ['.postedOn', '.postedDate', '[data-testid*="date"]'],
                'owner_type': ['.ownerType', '.postedBy', '[data-testid*="owner"]'],
                'contact_info': ['.contact', '.phone', '.mobile', '[data-testid*="contact"]'],
                'facing': ['.facing', '[data-testid*="facing"]'],
                'age_of_property': ['.age', '.construction', '[data-testid*="age"]'],
                'balconies': ['.balcony', '[data-testid*="balcon"]'],
                'floor_details': ['.floor', '.floorInfo', '[data-testid*="floor"]']
            }
            
            for field, selectors in field_extractions.items():
                if field not in data or not data.get(field):
                    for selector in selectors:
                        try:
                            element = card.find_element(By.CSS_SELECTOR, selector)
                            text = element.text.strip()
                            if text:
                                data[field] = text
                                break
                        except NoSuchElementException:
                            continue
            
            # Step 5: Enhanced image extraction with processing
            images = self._extract_and_analyze_images(card, data)
            data.update({
                'images_count': images['count'],
                'has_images': images['count'] > 0,
                'primary_image_url': images['primary_image'],
                'all_image_urls': '; '.join(images['urls']) if images['urls'] else None,
                'image_quality_scores': images.get('quality_scores', []),
                'extracted_image_text': images.get('ocr_text', ''),
                'dominant_colors': images.get('colors', {}),
                'image_types': images.get('types', [])
            })
            
            # Step 6: Extract amenities and features
            amenities = self._extract_amenities_features(card)
            data.update(amenities)
            
            # Step 7: Extract all links
            links = self._extract_all_links(card)
            data.update(links)
            
            # Step 8: Calculate completeness
            non_empty_fields = sum(1 for v in data.values() if v not in [None, '', 0, []])
            total_expected_fields = 40
            data['extraction_completeness'] = round((non_empty_fields / total_expected_fields) * 100, 1)
            
            self.stats['successful_extractions'] += 1
            
            logging.info(f"  Card {card_index}: {data['extraction_completeness']}% complete, "
                        f"{images['count']} images analyzed")
            
            return data
            
        except Exception as e:
            logging.error(f"  Failed to extract card {card_index}: {e}")
            data['extraction_error'] = str(e)
            return data

    def _extract_and_analyze_images(self, card, card_data):
        """Enhanced image extraction with analysis"""
        images = {
            'urls': [],
            'count': 0,
            'primary_image': None,
            'quality_scores': [],
            'ocr_text': '',
            'colors': {},
            'types': []
        }
        
        try:
            # Get all image URLs
            img_elements = card.find_elements(By.TAG_NAME, "img")
            
            for img in img_elements:
                for attr in ['src', 'data-src', 'data-original', 'data-lazy', 'data-srcset']:
                    url = img.get_attribute(attr)
                    if url and url not in images['urls'] and not url.startswith('data:'):
                        # Clean URL
                        if url.startswith('/'):
                            url = 'https://www.magicbricks.com' + url
                        images['urls'].append(url)
            
            # Background images
            try:
                bg_images = self.driver.execute_script("""
                    var card = arguments[0];
                    var images = [];
                    var elements = card.querySelectorAll('*');
                    
                    elements.forEach(function(el) {
                        var style = window.getComputedStyle(el);
                        var bgImage = style.backgroundImage;
                        if (bgImage && bgImage !== 'none' && bgImage.includes('url(')) {
                            var url = bgImage.match(/url\\(["']?([^"'\\)]+)["']?\\)/);
                            if (url && url[1] && !url[1].startsWith('data:')) {
                                images.push(url[1]);
                            }
                        }
                    });
                    
                    return [...new Set(images)];
                """, card)
                
                for url in bg_images:
                    if url not in images['urls']:
                        if url.startswith('/'):
                            url = 'https://www.magicbricks.com' + url
                        images['urls'].append(url)
                        
            except Exception:
                pass
            
            images['count'] = len(images['urls'])
            images['primary_image'] = images['urls'][0] if images['urls'] else None
            
            # Process images with AI
            if images['urls'] and self.image_processor.image_analysis:
                for i, url in enumerate(images['urls'][:3]):  # Analyze first 3 images
                    try:
                        analysis = self.image_processor.analyze_property_image(url, card_data)
                        
                        if analysis['image_quality_score'] > 0:
                            images['quality_scores'].append(analysis['image_quality_score'])
                        
                        if analysis['image_text']:
                            images['ocr_text'] += f"Image {i+1}: {analysis['image_text']}; "
                        
                        if analysis['color_analysis']:
                            images['colors'].update(analysis['color_analysis'])
                        
                        if analysis['image_type'] != 'unknown':
                            images['types'].append(analysis['image_type'])
                        
                        self.stats['images_analyzed'] += 1
                        
                    except Exception as e:
                        logging.debug(f"Failed to analyze image {url}: {e}")
            
            if images['count'] > 0:
                self.stats['cards_with_images'] += 1
            else:
                self.stats['cards_without_images'] += 1
            
            return images
            
        except Exception as e:
            logging.warning(f"Image extraction failed: {e}")
            return images

    def _extract_amenities_features(self, card):
        """Extract amenities and features"""
        amenities_data = {
            'amenities': [],
            'features': [],
            'highlights': [],
            'nearby_facilities': []
        }
        
        try:
            # Look for amenity sections
            amenity_selectors = [
                '.amenities', '.facilities', '.features', '.highlights',
                '[class*="amenity"]', '[class*="feature"]', '[class*="facility"]',
                '.nearby', '.location-advantages'
            ]
            
            for selector in amenity_selectors:
                try:
                    elements = card.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.text.strip()
                        if text and len(text) < 200:
                            # Categorize based on content
                            text_lower = text.lower()
                            if any(word in text_lower for word in ['gym', 'pool', 'park', 'security']):
                                amenities_data['amenities'].append(text)
                            elif any(word in text_lower for word in ['metro', 'school', 'hospital', 'mall']):
                                amenities_data['nearby_facilities'].append(text)
                            elif any(word in text_lower for word in ['premium', 'luxury', 'spacious']):
                                amenities_data['highlights'].append(text)
                            else:
                                amenities_data['features'].append(text)
                except Exception:
                    continue
            
            # Convert lists to strings
            for key in amenities_data:
                if amenities_data[key]:
                    amenities_data[key] = '; '.join(list(set(amenities_data[key])))
                else:
                    amenities_data[key] = None
            
            return amenities_data
            
        except Exception as e:
            logging.warning(f"Amenities extraction failed: {e}")
            return {k: None for k in amenities_data.keys()}

    def _extract_all_links(self, card):
        """Extract all links from card"""
        links_data = {
            'property_detail_url': None,
            'society_url': None,
            'builder_url': None,
            'all_links': [],
            'contact_links': [],
            'social_links': []
        }
        
        try:
            anchors = card.find_elements(By.TAG_NAME, "a")
            
            for anchor in anchors:
                href = anchor.get_attribute("href")
                if not href or href.startswith("javascript:"):
                    continue
                
                if href.startswith("/"):
                    href = "https://www.magicbricks.com" + href
                
                text_content = anchor.text.strip().lower()
                
                # Categorize links
                if "propertydetail" in href.lower() or "property-for-" in href.lower():
                    links_data['property_detail_url'] = href
                elif "project" in href.lower() or "society" in href.lower():
                    links_data['society_url'] = href
                elif "builder" in href.lower():
                    links_data['builder_url'] = href
                elif any(word in href.lower() for word in ["contact", "phone", "call"]):
                    links_data['contact_links'].append(href)
                elif any(word in href.lower() for word in ["facebook", "twitter", "instagram"]):
                    links_data['social_links'].append(href)
                
                links_data['all_links'].append(href)
            
            # Convert lists to strings
            for key in ['all_links', 'contact_links', 'social_links']:
                if links_data[key]:
                    links_data[key] = "; ".join(list(set(links_data[key])))
                else:
                    links_data[key] = None

            return links_data

        except Exception as e:
            logging.warning(f"Links extraction failed: {e}")
            return {k: None for k in links_data.keys()}

    def scrape_listings(self):
        """Scrape listings with incremental save and retries"""
        listings = []
        scrolls = 0
        last_count = 0

        try:
            self._setup_webdriver()
            self.driver.get(self.search_url)

            self.wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.mb-srp__card, div.listingCard, .srpCard"))
            )
            logging.info("Search page loaded, starting extraction...")

            while len(listings) < self.max_listings and scrolls < self.max_scrolls:
                card_els = self.driver.find_elements(By.CSS_SELECTOR, "div.mb-srp__card, div.listingCard, .srpCard")

                for idx, card in enumerate(card_els[last_count:], start=last_count + 1):
                    self._smart_sleep(0.5, 1.2)
                    try:
                        row = self._extract_comprehensive_data_with_images(card, idx)
                        listings.append(row)
                        self.extracted_data.append(row)

                        # Incremental save
                        self._save_data_incremental()

                    except Exception as e:
                        logging.warning(f"Card {idx} skipped: {e}")
                        continue

                    if len(listings) >= self.max_listings:
                        break

                last_count = len(card_els)
                scrolls += 1

                self.driver.execute_script("window.scrollBy(0, 1500);")
                self._smart_sleep(1.0, 2.0)

            logging.info(f"Scraping complete: {len(listings)} cards extracted")
            return listings

        except Exception as e:
            logging.error(f"Scrape failed: {e}")
            return listings

    def run(self):
        """Main run entrypoint"""
        try:
            self._setup_signal_handlers()
            listings = self.scrape_listings()
            output_file = self._finalize_output()
            self.stats["total_processed"] = len(listings)
            self.stats["end_time"] = datetime.now().isoformat()

            return {"stats": self.stats, "output_path": output_file}

        finally:
            try:
                self.driver.quit()
            except:
                pass


def create_default_config():
    config = configparser.ConfigParser()

    config["limits"] = {
        "max_listings_per_society": "200",
        "max_scrolls": "30",
    }

    config["extraction_settings"] = {
        "skip_cards_without_images": "False",
        "deep_extraction": "True",
        "click_dropdowns": "True",
        "extract_hidden_info": "True",
    }

    config["image_processing"] = {
        "enable_ocr": "False",
        "download_images": "False",
        "analyze_images": "True",
    }

    config["http"] = {
        "min_delay": "1.0",
        "max_delay": "3.0",
    }

    config["output"] = {
        "output_dir": "output",
        "incremental_save": "True",
        "save_batch_size": "10",
    }

    return config


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )

    config = create_default_config()
    scraper = MagicBricksScraper(
        config=config, run_id="001", start_ts=datetime.now()
    )
    result = scraper.run()

    print(json.dumps(result, indent=2))
