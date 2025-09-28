#!/usr/bin/env python3
"""
Enhanced NoBroker listing card extractor with improved data extraction
Fixed major issues with card detection and data parsing
"""

import os
import time
import logging
import random
import pandas as pd
from datetime import datetime
import cv2
import numpy as np
from PIL import Image
import io
import base64
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import re
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin, urlparse, quote_plus, parse_qs
import configparser
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementNotInteractableException,
    WebDriverException,
)
import undetected_chromedriver as uc
from typing import List, Dict, Optional, Tuple
import requests
from dataclasses import dataclass

# Configuration constants
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

@dataclass
class ListingData:
    """Structure for holding listing card data"""
    building_name: str = ""
    price: str = ""
    emi: str = ""
    buildup_area: str = ""
    facing: str = ""
    apartment_type: str = ""
    bathrooms: str = ""
    parking: str = ""
    floor: str = ""
    furnishing: str = ""
    age: str = ""
    nearby_places: List[str] = None
    image_count: int = 0
    image_urls: List[str] = None
    links: List[Dict[str, str]] = None
    broker_info: str = ""
    verification_status: str = ""
    possession_date: str = ""
    additional_amenities: List[str] = None
    
    def __post_init__(self):
        if self.nearby_places is None:
            self.nearby_places = []
        if self.image_urls is None:
            self.image_urls = []
        if self.links is None:
            self.links = []
        if self.additional_amenities is None:
            self.additional_amenities = []


class NoBrokerScraper:
    """Improved scraper with better element detection and data extraction"""

    def __init__(self, config, run_id: str, start_ts: datetime):
        self.config = config
        self.run_id = run_id
        self.start_ts = start_ts
        
        # Enhanced extraction settings
        self.max_listings = int(config.get("limits", "max_listings_per_society", fallback="50"))
        self.max_scrolls = int(config.get("limits", "max_scrolls", fallback="20"))
        self.manual_wait_time = int(config.get("manual", "selection_wait_time", fallback="5"))
        
        self.output_dir = config.get("output", "output_dir", fallback="output")
        os.makedirs(self.output_dir, exist_ok=True)
        
        timestamp = self.start_ts.strftime("%Y%m%d_%H%M%S")
        self.final_output_path = os.path.join(
            self.output_dir, f"nobroker_improved_{self.run_id}_{timestamp}.xlsx"
        )
        
        self.extracted_data = []
        self.min_delay = float(config.get("http", "min_delay", fallback="1.5"))
        self.max_delay = float(config.get("http", "max_delay", fallback="3.0"))
        
        # Enhanced tracking
        self.processed_cards = set()
        self.extraction_stats = {
            "total_cards_found": 0,
            "successful_extractions": 0,
            "failed_extractions": 0,
            "images_downloaded": 0,
            "links_extracted": 0
        }
        
        # Webdriver components
        self.driver = None
        self.wait = None
        self.actions = None

    def _setup_enhanced_webdriver(self):
        """Setup Chrome WebDriver with enhanced capabilities"""
        try:
            options = uc.ChromeOptions()
            options.add_argument("--start-maximized")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            
            # Enhanced preferences
            prefs = {
                "profile.managed_default_content_settings.images": 1,
                "profile.default_content_setting_values.notifications": 2,
                "profile.default_content_settings.popups": 0,
                "profile.managed_default_content_settings.media_stream": 2,
            }
            options.add_experimental_option("prefs", prefs)
            options.add_argument(f"--user-agent={DEFAULT_USER_AGENT}")
            
            self.driver = uc.Chrome(options=options)
            self.driver.set_window_size(1920, 1080)
            
            self.wait = WebDriverWait(self.driver, 10)
            self.actions = ActionChains(self.driver)
            
            logging.info("Enhanced WebDriver setup completed")
            
        except Exception as e:
            logging.error(f"Enhanced WebDriver setup failed: {e}")
            raise

    def find_property_cards_improved(self) -> List:
        """Improved method to find property cards using multiple strategies"""
        cards = []
        
        # Strategy 1: Look for common NoBroker card selectors
        card_selectors = [
            # NoBroker specific selectors
            '[class*="nb__1Z7Qc"]',  # Common NoBroker card class pattern
            '[class*="card"]',
            '[class*="listing"]',
            '[class*="property"]',
            '[data-testid*="card"]',
            '[data-testid*="listing"]',
            # Generic selectors
            'article',
            '.card-container',
            '.property-card',
            '.listing-card',
            # Backup broad selectors
            'div[role="button"]',
            'div[onclick*="property"]'
        ]
        
        for selector in card_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if self._is_valid_property_card(element):
                        # Create unique identifier
                        element_id = self._get_element_id(element)
                        if element_id not in self.processed_cards:
                            cards.append(element)
                            self.processed_cards.add(element_id)
                
                if cards:
                    logging.info(f"Found {len(cards)} cards using selector: {selector}")
                    break  # Use first successful selector
                    
            except Exception as e:
                logging.debug(f"Selector {selector} failed: {e}")
                continue
        
        # Strategy 2: If no cards found with selectors, try XPath patterns
        if not cards:
            cards = self._find_cards_by_xpath()
        
        # Strategy 3: Last resort - find by text patterns
        if not cards:
            cards = self._find_cards_by_text_patterns()
        
        logging.info(f"Total property cards found: {len(cards)}")
        return cards

    def _is_valid_property_card(self, element) -> bool:
        """Check if element is a valid property card"""
        try:
            # Must be visible
            if not element.is_displayed():
                return False
            
            # Check size - property cards should be reasonably sized
            size = element.size
            if size['height'] < 100 or size['width'] < 200:
                return False
            
            # Check if contains property-related content
            element_text = element.text.lower()
            
            # Must contain some property indicators
            property_indicators = [
                '₹', 'lacs', 'crore', 'bhk', 'sqft', 'bathroom', 'parking',
                'facing', 'furnished', 'floor', 'possession', 'emi'
            ]
            
            indicator_count = sum(1 for indicator in property_indicators if indicator in element_text)
            
            # Should have at least 2 property indicators
            if indicator_count < 2:
                return False
            
            # Check if it has clickable elements (links, buttons)
            clickable_elements = element.find_elements(By.CSS_SELECTOR, "a, button, [role='button'], [onclick]")
            
            return len(clickable_elements) > 0
            
        except Exception as e:
            logging.debug(f"Card validation failed: {e}")
            return False

    def _get_element_id(self, element) -> str:
        """Generate unique identifier for element"""
        try:
            # Try to get unique attributes
            element_id = element.get_attribute("id") or ""
            class_name = element.get_attribute("class") or ""
            data_testid = element.get_attribute("data-testid") or ""
            
            # If no unique attributes, use position and text
            if not any([element_id, data_testid]):
                location = element.location
                size = element.size
                text_hash = hash(element.text[:100])  # First 100 chars of text
                return f"{location['x']}_{location['y']}_{size['width']}_{size['height']}_{text_hash}"
            
            return f"{element_id}_{class_name}_{data_testid}"
            
        except Exception:
            return str(random.randint(1000, 9999))

    def _find_cards_by_xpath(self) -> List:
        """Find cards using XPath patterns"""
        cards = []
        
        xpath_patterns = [
            # Find divs that contain price and BHK info
            "//div[contains(text(), '₹') and contains(text(), 'BHK')]",
            "//div[contains(text(), 'Lacs') or contains(text(), 'Crore')]",
            # Find elements with property-related attributes
            "//div[contains(@class, 'card') or contains(@class, 'listing') or contains(@class, 'property')]",
            # Find clickable areas with property info
            "//div[@role='button' or @onclick][contains(text(), '₹') or contains(text(), 'BHK')]"
        ]
        
        for pattern in xpath_patterns:
            try:
                elements = self.driver.find_elements(By.XPATH, pattern)
                for element in elements:
                    # For XPath results, traverse up to find the card container
                    card_container = self._find_card_container(element)
                    if card_container and self._is_valid_property_card(card_container):
                        element_id = self._get_element_id(card_container)
                        if element_id not in self.processed_cards:
                            cards.append(card_container)
                            self.processed_cards.add(element_id)
                
                if cards:
                    logging.info(f"Found {len(cards)} cards using XPath: {pattern}")
                    break
                    
            except Exception as e:
                logging.debug(f"XPath {pattern} failed: {e}")
                continue
        
        return cards

    def _find_cards_by_text_patterns(self) -> List:
        """Last resort: find cards by looking for property text patterns"""
        cards = []
        
        try:
            # Find all elements containing price information
            price_elements = self.driver.find_elements(
                By.XPATH, 
                "//*[contains(text(), '₹') or contains(text(), 'Lacs') or contains(text(), 'Crore')]"
            )
            
            for element in price_elements:
                # Try to find the card container
                card_container = self._find_card_container(element, max_levels=8)
                
                if card_container and self._is_valid_property_card(card_container):
                    element_id = self._get_element_id(card_container)
                    if element_id not in self.processed_cards:
                        cards.append(card_container)
                        self.processed_cards.add(element_id)
            
            logging.info(f"Found {len(cards)} cards using text patterns")
            
        except Exception as e:
            logging.error(f"Text pattern search failed: {e}")
        
        return cards

    def _find_card_container(self, element, max_levels=5):
        """Find the card container by traversing up the DOM"""
        try:
            current = element
            
            for level in range(max_levels):
                if current:
                    # Check if current element looks like a card container
                    if self._looks_like_card_container(current):
                        return current
                    
                    # Move to parent
                    try:
                        parent = current.find_element(By.XPATH, "..")
                        if parent == current:  # Reached root
                            break
                        current = parent
                    except:
                        break
            
            return element  # Return original if no container found
            
        except Exception:
            return element

    def _looks_like_card_container(self, element) -> bool:
        """Check if element looks like a property card container"""
        try:
            class_name = (element.get_attribute("class") or "").lower()
            tag_name = element.tag_name.lower()
            
            # Check for card-like class names
            card_indicators = ['card', 'listing', 'property', 'item', 'result', 'tile']
            if any(indicator in class_name for indicator in card_indicators):
                return True
            
            # Check for semantic tags
            if tag_name in ['article', 'section']:
                return True
            
            # Check size - cards should be reasonably sized
            size = element.size
            if size['height'] >= 200 and size['width'] >= 300:
                # Check if it contains property-specific elements
                try:
                    has_price = element.find_elements(By.XPATH, ".//*[contains(text(), '₹')]")
                    has_bhk = element.find_elements(By.XPATH, ".//*[contains(text(), 'BHK')]")
                    has_links = element.find_elements(By.CSS_SELECTOR, "a, button")
                    
                    return len(has_price) > 0 and (len(has_bhk) > 0 or len(has_links) > 0)
                except:
                    pass
            
            return False
            
        except Exception:
            return False

    def extract_comprehensive_card_data(self, card_element) -> Optional[ListingData]:
        """Extract comprehensive data from a property card with improved selectors"""
        try:
            listing_data = ListingData()
            
            # Scroll to card and ensure it's visible
            self._scroll_to_element(card_element)
            time.sleep(0.5)
            
            # Get all text content for fallback extraction
            card_text = card_element.text
            card_html = card_element.get_attribute('outerHTML')
            
            # Extract basic information using improved methods
            self._extract_basic_info_improved(card_element, listing_data, card_text)
            
            # Extract images
            self._extract_image_data_improved(card_element, listing_data)
            
            # Extract links
            self._extract_links_improved(card_element, listing_data)
            
            # Extract nearby places with better parsing
            self._extract_nearby_places_improved(card_element, listing_data, card_text)
            
            # Extract additional details
            self._extract_additional_details_improved(card_element, listing_data, card_text)
            
            # Validate extracted data
            if self._validate_listing_data(listing_data):
                self.extraction_stats["successful_extractions"] += 1
                return listing_data
            else:
                self.extraction_stats["failed_extractions"] += 1
                return None
            
        except Exception as e:
            logging.debug(f"Comprehensive card extraction failed: {e}")
            self.extraction_stats["failed_extractions"] += 1
            return None

    def _scroll_to_element(self, element):
        """Scroll to ensure element is visible"""
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(0.3)
        except Exception:
            pass

    def _extract_basic_info_improved(self, card_element, listing_data: ListingData, card_text: str):
        """Extract basic property information with improved parsing"""
        try:
            # Building/Property name - try multiple strategies
            name_found = False
            
            # Strategy 1: Look for title elements
            title_selectors = [
                "h1", "h2", "h3", "h4", ".title", "[class*='title']", 
                "[class*='name']", "[class*='heading']", "[data-testid*='title']"
            ]
            
            for selector in title_selectors:
                try:
                    elements = card_element.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        text = elem.text.strip()
                        if text and len(text) > 3 and not text.isdigit():
                            # Avoid common non-title texts
                            avoid_texts = ['get owner details', 'contact', 'view', 'call', 'whatsapp']
                            if not any(avoid in text.lower() for avoid in avoid_texts):
                                listing_data.building_name = text
                                name_found = True
                                break
                    if name_found:
                        break
                except:
                    continue
            
            # Strategy 2: Extract from text patterns if no title found
            if not name_found:
                lines = card_text.split('\n')
                for line in lines[:5]:  # Check first few lines
                    line = line.strip()
                    if (line and len(line) > 5 and len(line) < 100 and 
                        not any(char.isdigit() for char in line[:10]) and  # Not starting with numbers
                        '₹' not in line and 'BHK' not in line):
                        listing_data.building_name = line
                        break
            
            # Price information - improved regex patterns
            price_patterns = [
                r'₹\s*([0-9,\.]+)\s*(Lacs?|Crores?|L|Cr)\b',  # ₹62 Lacs
                r'([0-9,\.]+)\s*(Lacs?|Crores?|L|Cr)\b',      # 62 Lacs
                r'₹\s*([0-9,\.]+)\b'                           # ₹6200000
            ]
            
            for pattern in price_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    listing_data.price = match.group(0)
                    break
            
            # Also try to find price in specific elements
            if not listing_data.price:
                price_selectors = [
                    "[class*='price']", "[class*='amount']", "[class*='cost']",
                    "[data-testid*='price']", "[title*='price']"
                ]
                
                for selector in price_selectors:
                    try:
                        elements = card_element.find_elements(By.CSS_SELECTOR, selector)
                        for elem in elements:
                            text = elem.text.strip()
                            if ('₹' in text or 'lacs' in text.lower() or 'crore' in text.lower()):
                                listing_data.price = text
                                break
                        if listing_data.price:
                            break
                    except:
                        continue
            
            # EMI information
            emi_patterns = [
                r'₹\s*([0-9,]+)\s*/?\s*month',
                r'EMI[:\s]*₹\s*([0-9,]+)',
                r'([0-9,]+)\s*/Month'
            ]
            
            for pattern in emi_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    listing_data.emi = match.group(0)
                    break
            
            # Apartment type - improved BHK detection
            bhk_patterns = [
                r'(\d+)\s*BHK',
                r'(\d+)\s*RK',
                r'(\d+)\s*Bedroom'
            ]
            
            for pattern in bhk_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    listing_data.apartment_type = f"{match.group(1)} BHK"
                    break
            
            # Buildup area - improved area detection
            area_patterns = [
                r'(\d{2,5})\s*sq\.?\s*ft\b',
                r'(\d{2,5})\s*sqft\b',
                r'(\d{2,5})\s*sq\s*metres',
                r'(\d{2,5})\s*sqm\b'
            ]
            
            for pattern in area_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    area_num = match.group(1)
                    listing_data.buildup_area = f"{area_num} sqft"
                    break
            
            # Facing direction
            facing_pattern = r'\b(North|South|East|West|NE|NW|SE|SW)[\s\-]?Facing\b'
            facing_match = re.search(facing_pattern, card_text, re.IGNORECASE)
            if facing_match:
                listing_data.facing = facing_match.group(1).upper()
            
            # Bathrooms
            bathroom_patterns = [
                r'(\d+)\s*Bath',
                r'(\d+)\s*Bathroom',
                r'(\d+)\s*Washroom'
            ]
            
            for pattern in bathroom_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    listing_data.bathrooms = match.group(1)
                    break
            
            # Parking
            parking_patterns = [
                r'(Bike\s*and\s*Car)\s*Parking',
                r'(Car)\s*Parking',
                r'(Bike)\s*Parking',
                r'(No\s*Parking)',
                r'(\d+)\s*Parking'
            ]
            
            for pattern in parking_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    listing_data.parking = match.group(1)
                    break
            
        except Exception as e:
            logging.debug(f"Basic info extraction failed: {e}")

    def _extract_nearby_places_improved(self, card_element, listing_data: ListingData, card_text: str):
        """Extract nearby places with improved parsing"""
        try:
            # Look for "Nearby" sections specifically
            nearby_elements = card_element.find_elements(By.XPATH, 
                ".//*[contains(text(), 'Nearby') or contains(text(), 'nearby')]")
            
            nearby_text = card_text
            for elem in nearby_elements:
                try:
                    # Get parent container that might have the nearby places
                    parent = elem.find_element(By.XPATH, "../..")
                    nearby_text += " " + parent.text
                except:
                    nearby_text += " " + elem.text
            
            # Enhanced place patterns with more specific matching
            place_patterns = [
                # Specific institution patterns
                r'([A-Za-z\s]+(?:Hospital|Medical|Clinic))\b',
                r'([A-Za-z\s]+(?:School|College|University|Institute))\b', 
                r'([A-Za-z\s]+(?:Mall|Market|Shopping|Store))\b',
                r'([A-Za-z\s]+(?:Station|Metro|Airport|Bus))\b',
                r'([A-Za-z\s]+(?:Park|Garden|Ground))\b',
                r'([A-Za-z\s]+(?:Temple|Church|Mosque|Gurudwara))\b',
                # Common place names
                r'\b(JSA HELIPAD|Union Bank|Uppal|Badshahpur)\b',
                r'\b([A-Za-z]+\s+(?:Club|Gym|Hospital|School|Mall|Park))\b'
            ]
            
            nearby_places = set()
            text_to_search = nearby_text.lower()
            
            for pattern in place_patterns:
                matches = re.findall(pattern, nearby_text, re.IGNORECASE)
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[0] if match[0] else match[1]
                    
                    cleaned = match.strip().title()
                    # Filter out very short or generic matches
                    if len(cleaned) > 3 and cleaned not in ['The', 'And', 'For', 'With']:
                        nearby_places.add(cleaned)
            
            # Also look for patterns like "5 min to XYZ"
            distance_patterns = [
                r'(\d+)\s*(?:min|km|m)\s*(?:to|from|away)\s+([A-Za-z\s]+)',
                r'([A-Za-z\s]+)\s*-\s*(\d+)\s*(?:min|km|m)'
            ]
            
            for pattern in distance_patterns:
                matches = re.findall(pattern, nearby_text, re.IGNORECASE)
                for match in matches:
                    place = match[1] if len(match) > 1 else match[0]
                    place = place.strip().title()
                    if len(place) > 3:
                        nearby_places.add(place)
            
            listing_data.nearby_places = list(nearby_places)[:15]  # Limit to 15 places
            
        except Exception as e:
            logging.debug(f"Nearby places extraction failed: {e}")

    def _extract_image_data_improved(self, card_element, listing_data: ListingData):
        """Extract image information with improved detection"""
        try:
            # Find all images in the card
            img_elements = card_element.find_elements(By.TAG_NAME, "img")
            
            valid_images = []
            for img in img_elements:
                try:
                    src = img.get_attribute("src") or img.get_attribute("data-src") or img.get_attribute("data-lazy")
                    if src and self._is_property_image(src):
                        valid_images.append(src)
                except:
                    continue
            
            # Also check for background images in divs
            bg_elements = card_element.find_elements(By.CSS_SELECTOR, "[style*='background-image']")
            for elem in bg_elements:
                try:
                    style = elem.get_attribute("style")
                    url_match = re.search(r'url\(["\']?([^"\']+)["\']?\)', style)
                    if url_match:
                        img_url = url_match.group(1)
                        if self._is_property_image(img_url):
                            valid_images.append(img_url)
                except:
                    continue
            
            listing_data.image_urls = list(set(valid_images))  # Remove duplicates
            listing_data.image_count = len(listing_data.image_urls)
            
            # Look for image count indicators in text
            card_text = card_element.text
            count_patterns = [
                r'(\d+)\s*photos?',
                r'(\d+)\s*images?',
                r'(\d+)/\d+',  # Like "5/24" indicating current/total
                r'View\s*(?:all\s*)?(\d+)\s*photos?'
            ]
            
            for pattern in count_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    try:
                        total_images = int(match.group(1))
                        if total_images > listing_data.image_count:
                            listing_data.image_count = total_images
                        break
                    except:
                        continue
            
            self.extraction_stats["images_downloaded"] += len(listing_data.image_urls)
                    
        except Exception as e:
            logging.debug(f"Image extraction failed: {e}")

    def _is_property_image(self, src: str) -> bool:
        """Check if the image source is a valid property image"""
        if not src or src.startswith("data:") or len(src) < 10:
            return False
        
        # Filter out common non-property images
        exclude_keywords = ['logo', 'icon', 'avatar', 'profile', 'banner', 'ad', 'advertisement']
        src_lower = src.lower()
        
        # Must not contain exclude keywords
        if any(keyword in src_lower for keyword in exclude_keywords):
            return False
        
        # Should contain property-related keywords or be from known property image domains
        property_keywords = ['property', 'house', 'apartment', 'flat', 'home', 'real', 'estate']
        property_domains = ['nobroker', 'cloudfront', 'amazonaws', 'images']
        
        return (any(keyword in src_lower for keyword in property_keywords) or
                any(domain in src_lower for domain in property_domains))

    def _extract_links_improved(self, card_element, listing_data: ListingData):
        """Extract all clickable links with improved detection"""
        try:
            # Find all links and buttons
            clickable_elements = card_element.find_elements(By.CSS_SELECTOR, 
                "a, button, [role='button'], [onclick], [data-href]")
            
            for element in clickable_elements:
                try:
                    href = (element.get_attribute("href") or 
                           element.get_attribute("data-href") or
                           element.get_attribute("onclick") or "")
                    
                    text = element.text.strip() or element.get_attribute("title") or element.get_attribute("aria-label") or "Link"
                    target = element.get_attribute("target")
                    
                    if href and href != "#" and len(text.strip()) > 0:
                        link_data = {
                            "url": href,
                            "text": text,
                            "opens_new_tab": target == "_blank"
                        }
                        listing_data.links.append(link_data)
                        
                except:
                    continue
            
            self.extraction_stats["links_extracted"] += len(listing_data.links)
            
        except Exception as e:
            logging.debug(f"Link extraction failed: {e}")

    def _extract_additional_details_improved(self, card_element, listing_data: ListingData, card_text: str):
        """Extract additional property details with improved parsing"""
        try:
            # Floor information - improved patterns
            floor_patterns = [
                r'(\d+)(?:st|nd|rd|th)?\s*Floor',
                r'Floor\s*[:-]?\s*(\d+)',
                r'(\d+)\s*/\s*\d+\s*Floor'  # Like "3/5 Floor"
            ]
            
            for pattern in floor_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    listing_data.floor = f"{match.group(1)} Floor"
                    break
            
            # Furnishing status - comprehensive patterns
            furnishing_patterns = [
                r'\b(Fully\s*Furnished)\b',
                r'\b(Semi\s*Furnished)\b', 
                r'\b(Unfurnished)\b',
                r'\b(Furnished)\b'
            ]
            
            for pattern in furnishing_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    listing_data.furnishing = match.group(1).title()
                    break
            
            # Property age
            age_patterns = [
                r'(\d+)\s*Year[s]?\s*Old',
                r'Age[:\s]*(\d+)\s*Year[s]?',
                r'(\d+)\s*Yr[s]?\s*Old'
            ]
            
            for pattern in age_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    listing_data.age = f"{match.group(1)} years"
                    break
            
            # Broker/Owner information
            broker_patterns = [
                r'(Owner)\b',
                r'(Broker)\b', 
                r'(Agent)\b',
                r'Posted\s*by[:\s]*([A-Za-z\s]+)'
            ]
            
            for pattern in broker_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    listing_data.broker_info = match.group(1).title()
                    break
            
            # Verification status
            if re.search(r'\bVerified\b', card_text, re.IGNORECASE):
                listing_data.verification_status = "Verified"
            elif re.search(r'\bUnverified\b', card_text, re.IGNORECASE):
                listing_data.verification_status = "Unverified"
            
            # Possession date
            possession_patterns = [
                r'Possession[:\s]*([A-Za-z]+\s*\d{4})',
                r'Ready\s*to\s*Move',
                r'Under\s*Construction'
            ]
            
            for pattern in possession_patterns:
                match = re.search(pattern, card_text, re.IGNORECASE)
                if match:
                    listing_data.possession_date = match.group(1) if match.groups() else match.group(0)
                    break
            
            # Amenities - comprehensive detection
            amenity_keywords = [
                'gym', 'swimming pool', 'pool', 'parking', '24x7 security', 'security',
                'lift', 'elevator', 'garden', 'playground', 'club house', 'clubhouse',
                'power backup', 'generator', 'water supply', 'bore well', 'rainwater harvesting',
                'children play area', 'jogging track', 'tennis court', 'badminton court',
                'basketball court', 'indoor games', 'library', 'multipurpose hall'
            ]
            
            found_amenities = []
            card_text_lower = card_text.lower()
            
            for amenity in amenity_keywords:
                if amenity in card_text_lower:
                    found_amenities.append(amenity.title())
            
            listing_data.additional_amenities = list(set(found_amenities))  # Remove duplicates
            
        except Exception as e:
            logging.debug(f"Additional details extraction failed: {e}")

    def _validate_listing_data(self, listing_data: ListingData) -> bool:
        """Validate extracted listing data with improved criteria"""
        try:
            # Must have either building name or price or apartment type
            has_basic_info = (listing_data.building_name or 
                             listing_data.price or 
                             listing_data.apartment_type)
            
            if not has_basic_info:
                return False
            
            # Must have at least one property-related field
            property_fields = [
                listing_data.apartment_type, listing_data.buildup_area, 
                listing_data.bathrooms, listing_data.facing, listing_data.parking
            ]
            
            has_property_info = any(field for field in property_fields)
            
            # Must have either property info or nearby places or images
            has_relevant_data = (has_property_info or 
                               len(listing_data.nearby_places) > 0 or 
                               listing_data.image_count > 0)
            
            return has_relevant_data
            
        except Exception:
            return False

    def _clean_building_name_and_location(self, raw_name: str):
        """
        Clean raw building name like:
        '1 RK Apartment In Aipl Joy Square For Sale In Sector 63'
        → returns ('Apartment in Aipl Joy Square', 'Sector 63')
        """
        if not raw_name:
            return "", ""
        
        # Extract location (Sector … or similar)
        loc_match = re.search(r'(Sector\s*\d+)', raw_name, re.IGNORECASE)
        location = loc_match.group(1).title() if loc_match else ""
        
        # Remove RK/BHK prefix
        cleaned = re.sub(r'^\d+\s*(RK|BHK)\s*', '', raw_name, flags=re.IGNORECASE)
        # Remove trailing "For Sale …"
        cleaned = re.sub(r'\s*For Sale.*', '', cleaned, flags=re.IGNORECASE)
        
        return cleaned.strip(), location

    def extract_all_listings(self) -> List[Dict]:
        """Main method to extract all property listings with improved logic"""
        try:
            all_extracted_data = []
            scroll_count = 0
            consecutive_no_cards = 0
            total_processed = 0
            
            logging.info(f"Starting extraction - Target: {self.max_listings} listings")
            
            while (len(all_extracted_data) < self.max_listings and 
                   scroll_count < self.max_scrolls and 
                   consecutive_no_cards < 3):
                
                logging.info(f"Processing page {scroll_count + 1}/{self.max_scrolls}...")
                
                property_cards = self.find_property_cards_improved()
                
                if not property_cards:
                    consecutive_no_cards += 1
                    logging.warning(f"No new cards found on page {scroll_count + 1}")
                    
                    if consecutive_no_cards >= 3:
                        logging.info("No new cards found in 3 consecutive attempts, stopping...")
                        break
                    
                    self._smart_scroll_and_wait()
                    scroll_count += 1
                    continue
                
                consecutive_no_cards = 0
                new_cards_count = len(property_cards)
                self.extraction_stats["total_cards_found"] += new_cards_count
                
                logging.info(f"Found {new_cards_count} property cards on page {scroll_count + 1}")
                
                successful_extractions = 0
                for i, card_element in enumerate(property_cards):
                    if len(all_extracted_data) >= self.max_listings:
                        break
                    
                    try:
                        listing_data = self.extract_comprehensive_card_data(card_element)
                        
                        if listing_data:
                            # ✅ Only keep 1 RK / 1 BHK
                            apt_type = (listing_data.apartment_type or "").strip().upper()
                            if apt_type not in ["1 BHK", "1 RK"]:
                                continue
                            
                            listing_dict = self._listing_data_to_dict(listing_data, len(all_extracted_data) + 1)
                            all_extracted_data.append(listing_dict)
                            successful_extractions += 1
                        else:
                            logging.debug(f"✗ Failed to extract data from card {i+1}")
                    
                    except Exception as e:
                        logging.debug(f"Error processing card {i+1}: {e}")
                        continue
                    
                    time.sleep(random.uniform(0.2, 0.5))
                
                logging.info(f"Page {scroll_count + 1} completed: {successful_extractions}/{new_cards_count} cards extracted")
                
                if len(all_extracted_data) < self.max_listings and scroll_count < self.max_scrolls - 1:
                    self._smart_scroll_and_wait()
                    
                scroll_count += 1
            
            logging.info(f"Extraction completed: {len(all_extracted_data)} listings extracted in {scroll_count} pages")
            return all_extracted_data
            
        except Exception as e:
            logging.error(f"Listing extraction failed: {e}")
            return []

    def _listing_data_to_dict(self, listing_data: ListingData, index: int) -> Dict:
        """Convert ListingData object to dictionary with proper column names"""
        data_dict = {
            # Basic information
            'listing_index': index,
            'building_name': listing_data.building_name or '',
            'price': listing_data.price or '',
            'emi': listing_data.emi or '',
            'buildup_area': listing_data.buildup_area or '',
            'facing': listing_data.facing or '',
            'apartment_type': listing_data.apartment_type or '',
            'bathrooms': listing_data.bathrooms or '',
            'parking': listing_data.parking or '',
            'floor': listing_data.floor or '',
            'furnishing': listing_data.furnishing or '',
            'property_age': listing_data.age or '',
            'broker_info': listing_data.broker_info or '',
            'verification_status': listing_data.verification_status or '',
            'possession_date': listing_data.possession_date or '',
            
            # Image information
            'image_count': listing_data.image_count,
            'image_urls': ', '.join(listing_data.image_urls) if listing_data.image_urls else '',
            
            # Nearby places
            'nearby_places_count': len(listing_data.nearby_places),
            'nearby_places': ', '.join(listing_data.nearby_places) if listing_data.nearby_places else '',
        }
        cleaned_name, location = self._clean_building_name_and_location(listing_data.building_name)
        if cleaned_name:
            data_dict['building_name'] = cleaned_name
        data_dict['location'] = location
        # Add separate columns for first 5 nearby places
        for i in range(5):
            place_key = f'nearby_place_{i+1}'
            if i < len(listing_data.nearby_places):
                data_dict[place_key] = listing_data.nearby_places[i]
            else:
                data_dict[place_key] = ''
        
        # Add link information
        data_dict['links_count'] = len(listing_data.links)
        data_dict['links_summary'] = ', '.join([link.get('text', '')[:20] for link in listing_data.links[:3]])
        
        # Add first 3 links detailed info
        for i in range(3):
            if i < len(listing_data.links):
                data_dict[f'link_{i+1}_text'] = listing_data.links[i].get('text', '')
                data_dict[f'link_{i+1}_url'] = listing_data.links[i].get('url', '')
            else:
                data_dict[f'link_{i+1}_text'] = ''
                data_dict[f'link_{i+1}_url'] = ''
        
        # Add amenities
        data_dict['amenities_count'] = len(listing_data.additional_amenities)
        data_dict['amenities'] = ', '.join(listing_data.additional_amenities) if listing_data.additional_amenities else ''
        
        # Metadata
        data_dict['extraction_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data_dict['run_id'] = self.run_id
        
        return data_dict

    def _smart_scroll_and_wait(self):
        """Intelligent scrolling with improved load detection"""
        try:
            # Store current page height
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            # Try clicking load more buttons first
            if self._try_load_more_buttons():
                logging.info("Clicked load more button, waiting for content...")
                time.sleep(3)
                return
            
            # Scroll down by viewport height
            self.driver.execute_script("window.scrollBy(0, window.innerHeight);")
            time.sleep(2)
            
            # Check if new content loaded
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            
            if new_height == last_height:
                # Try scrolling to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                # Final height check
                final_height = self.driver.execute_script("return document.body.scrollHeight")
                if final_height == last_height:
                    logging.info("No new content loaded after scrolling")
            
            # Dismiss any popups that might have appeared
            self._dismiss_popups_advanced()
            
        except Exception as e:
            logging.debug(f"Smart scroll failed: {e}")

    def _try_load_more_buttons(self) -> bool:
        """Try clicking load more buttons with comprehensive detection"""
        try:
            # Enhanced load more button selectors
            load_more_selectors = [
                "button[class*='load-more']",
                "button[class*='show-more']",
                "button[class*='view-more']",
                "a[class*='load-more']",
                "button[data-testid*='load']",
                "button[data-testid*='more']",
                "[role='button'][class*='more']",
                ".load-more-btn",
                ".show-more-btn",
                "button[aria-label*='more']"
            ]
            
            for selector in load_more_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        if (element.is_displayed() and element.is_enabled() and 
                            element.location['y'] > 0):  # Must be visible on screen
                            
                            # Scroll to button
                            self.driver.execute_script("arguments[0].scrollIntoView();", element)
                            time.sleep(0.5)
                            
                            # Try clicking
                            self.driver.execute_script("arguments[0].click();", element)
                            logging.info(f"Successfully clicked load more button: {selector}")
                            return True
                except Exception as elem_e:
                    logging.debug(f"Failed to click element with selector {selector}: {elem_e}")
                    continue
            
            # Try by text content with broader search
            load_texts = ["Load More", "Show More", "View More", "Load Additional", "See More", "More Results"]
            for text in load_texts:
                try:
                    # Try both button and link elements
                    xpath_patterns = [
                        f"//button[contains(normalize-space(text()), '{text}')]",
                        f"//a[contains(normalize-space(text()), '{text}')]",
                        f"//*[@role='button'][contains(normalize-space(text()), '{text}')]"
                    ]
                    
                    for xpath in xpath_patterns:
                        elements = self.driver.find_elements(By.XPATH, xpath)
                        for element in elements:
                            if (element.is_displayed() and element.is_enabled() and 
                                element.location['y'] > 0):
                                
                                # Scroll to button
                                self.driver.execute_script("arguments[0].scrollIntoView();", element)
                                time.sleep(0.5)
                                
                                # Try clicking
                                self.driver.execute_script("arguments[0].click();", element)
                                logging.info(f"Successfully clicked load more by text: {text}")
                                return True
                                
                except Exception as text_e:
                    logging.debug(f"Failed to click by text '{text}': {text_e}")
                    continue
            
            return False
            
        except Exception as e:
            logging.debug(f"Load more button detection failed: {e}")
            return False

    def _dismiss_popups_advanced(self):
        """Enhanced popup dismissal with comprehensive detection"""
        try:
            # Common popup close selectors
            close_selectors = [
                "button[aria-label*='close']",
                "button[aria-label*='Close']", 
                "button[title*='close']",
                "button[title*='Close']",
                ".modal-close",
                ".popup-close",
                ".close-btn",
                ".close-button",
                "[data-testid*='close']",
                "[data-dismiss*='modal']",
                ".overlay .close",
                "button.close",
                "[class*='close'][class*='button']"
            ]
            
            dismissed_count = 0
            for selector in close_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        if element.is_displayed():
                            self.driver.execute_script("arguments[0].click();", element)
                            dismissed_count += 1
                            time.sleep(0.3)
                except:
                    continue
            
            if dismissed_count > 0:
                logging.debug(f"Dismissed {dismissed_count} popups")
            
            # Try escape key as well
            try:
                self.actions.send_keys(Keys.ESCAPE).perform()
                time.sleep(0.2)
            except:
                pass
                
        except Exception as e:
            logging.debug(f"Advanced popup dismissal failed: {e}")

    def navigate_and_setup(self, url: str) -> bool:
        """Navigate to URL and perform initial setup"""
        try:
            logging.info(f"Navigating to: {url}")
            self.driver.get(url)
            time.sleep(5)
            
            # Initial popup dismissal
            self._dismiss_popups_advanced()
            
            # Wait for page to load
            try:
                self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                # Wait a bit more for dynamic content
                time.sleep(3)
            except TimeoutException:
                logging.warning("Page load timeout, continuing anyway...")
            
            # Check if we can find any property-related content
            property_indicators = self.driver.find_elements(By.XPATH, 
                "//*[contains(text(), '₹') or contains(text(), 'BHK') or contains(text(), 'Lacs')]")
            
            if not property_indicators:
                logging.warning("No property content found on page - may need manual setup")
            
            logging.info("="*70)
            logging.info("🔧 MANUAL SETUP PHASE")
            logging.info("="*70)
            logging.info("Please complete the following if needed:")
            logging.info("1. Handle any sign-in or captcha prompts")
            logging.info("2. Apply desired filters (location, price range, etc.)")
            logging.info("3. Ensure property listings are visible on page")
            logging.info("4. Close any modal dialogs or popups")
            logging.info("5. Scroll down to see if more listings load")
            logging.info(f"⏳ Auto-start in {self.manual_wait_time} seconds...")
            logging.info("="*70)
            
            # Countdown
            for i in range(self.manual_wait_time, 0, -1):
                print(f"\rStarting extraction in {i:2d} seconds... ", end="", flush=True)
                time.sleep(1)
            
            print("\n🚀 Starting improved property extraction...")
            
            # Final popup dismissal before starting
            self._dismiss_popups_advanced()
            
            return True
            
        except Exception as e:
            logging.error(f"Navigation and setup failed: {e}")
            return False

    def save_enhanced_data(self, extracted_data: List[Dict]) -> Optional[str]:
        """Save extracted data with enhanced formatting and analysis"""
        try:
            if not extracted_data:
                logging.warning("No data to save")
                return None
            
            df = pd.DataFrame(extracted_data)
            
            # Create output file with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(
                self.output_dir, 
                f"nobroker_improved_extraction_{timestamp}.xlsx"
            )
            
            with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
                # Main data sheet
                df.to_excel(writer, sheet_name="Property_Listings", index=False)
                
                # Summary sheet
                summary_data = {
                    "Metric": [
                        "Total Listings Extracted",
                        "Total Cards Detected",
                        "Successful Extractions", 
                        "Failed Extractions",
                        "Success Rate (%)",
                        "Images Found",
                        "Links Extracted",
                        "Listings with Price Data",
                        "Listings with Nearby Places",
                        "Average Nearby Places per Listing",
                        "Extraction Started",
                        "Extraction Completed",
                        "Processing Duration (minutes)"
                    ],
                    "Value": [
                        len(extracted_data),
                        self.extraction_stats["total_cards_found"],
                        self.extraction_stats["successful_extractions"],
                        self.extraction_stats["failed_extractions"],
                        f"{(self.extraction_stats['successful_extractions'] / max(1, self.extraction_stats['total_cards_found'])) * 100:.1f}%",
                        self.extraction_stats["images_downloaded"],
                        self.extraction_stats["links_extracted"],
                        len(df[df['price'].notna() & (df['price'] != '')]),
                        len(df[df['nearby_places_count'] > 0]),
                        f"{df['nearby_places_count'].mean():.1f}",
                        self.start_ts.strftime("%Y-%m-%d %H:%M:%S"),
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        f"{(datetime.now() - self.start_ts).total_seconds() / 60:.1f}"
                    ]
                }
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name="Extraction_Summary", index=False)
                
                # Data quality analysis
                quality_metrics = self._analyze_data_quality(df)
                quality_df = pd.DataFrame(list(quality_metrics.items()), columns=["Field", "Completeness"])
                quality_df.to_excel(writer, sheet_name="Data_Quality", index=False)
                
                # Nearby places analysis
                if 'nearby_places' in df.columns:
                    nearby_analysis = self._analyze_nearby_places(df)
                    if nearby_analysis:
                        nearby_df = pd.DataFrame(nearby_analysis)
                        nearby_df.to_excel(writer, sheet_name="Nearby_Places_Analysis", index=False)
            
            logging.info(f"Improved data saved to: {output_path}")
            return output_path
            
        except Exception as e:
            logging.error(f"Failed to save improved data: {e}")
            
            # Fallback to CSV
            try:
                csv_path = output_path.replace(".xlsx", ".csv") if 'output_path' in locals() else "emergency_data.csv"
                pd.DataFrame(extracted_data).to_csv(csv_path, index=False)
                logging.info(f"Fallback CSV saved: {csv_path}")
                return csv_path
            except Exception as csv_e:
                logging.error(f"Even CSV fallback failed: {csv_e}")
                return None

    def _analyze_data_quality(self, df: pd.DataFrame) -> Dict:
        """Analyze data quality with comprehensive metrics"""
        try:
            metrics = {}
            total_records = len(df)
            
            # Core field completeness analysis
            important_fields = [
                'building_name', 'price', 'apartment_type', 'buildup_area',
                'facing', 'bathrooms', 'parking', 'nearby_places_count'
            ]
            
            for field in important_fields:
                if field in df.columns:
                    if field == 'nearby_places_count':
                        complete_count = len(df[df[field] > 0])
                    else:
                        complete_count = len(df[df[field].notna() & (df[field] != '')])
                    
                    percentage = (complete_count / total_records * 100) if total_records > 0 else 0
                    metrics[field] = f"{complete_count}/{total_records} ({percentage:.1f}%)"
            
            return metrics
            
        except Exception as e:
            logging.debug(f"Data quality analysis failed: {e}")
            return {"Analysis Error": str(e)}

    def _analyze_nearby_places(self, df: pd.DataFrame) -> List[Dict]:
        """Analyze nearby places data"""
        try:
            if 'nearby_places' not in df.columns:
                return []
            
            # Count frequency of nearby places
            all_places = []
            for places_str in df['nearby_places'].dropna():
                if places_str:
                    places = [place.strip() for place in places_str.split(',')]
                    all_places.extend(places)
            
            if not all_places:
                return []
            
            # Count frequency
            from collections import Counter
            place_counts = Counter(all_places)
            
            # Create analysis data
            analysis_data = []
            for place, count in place_counts.most_common(20):  # Top 20
                analysis_data.append({
                    'Place': place,
                    'Frequency': count,
                    'Percentage': f"{(count / len(all_places) * 100):.1f}%"
                })
            
            return analysis_data
            
        except Exception as e:
            logging.debug(f"Nearby places analysis failed: {e}")
            return []

    def run(self, target_url: str = None) -> Optional[str]:
        """Main execution method - maintains compatibility with original interface"""
        if target_url is None:
            target_url = "https://www.nobroker.in/1bhk-flats-for-sale-in-gurgaon-gurgaon"
        
        return self.run_improved_extraction(target_url)

    def run_improved_extraction(self, target_url: str) -> Optional[str]:
        """Main execution method for improved extraction"""
        self._setup_enhanced_webdriver()
        
        try:
            if not self.navigate_and_setup(target_url):
                return None
            
            # Perform extraction
            extracted_data = self.extract_all_listings()
            
            if not extracted_data:
                logging.warning("No data was extracted")
                return None
            
            # Save results
            output_path = self.save_enhanced_data(extracted_data)
            
            # Print final summary
            self._print_final_summary(extracted_data)
            
            return output_path
            
        except Exception as e:
            logging.error(f"Improved extraction failed: {e}")
            return None
        finally:
            try:
                if self.driver:
                    self.driver.quit()
            except:
                pass

    def _print_final_summary(self, extracted_data: List[Dict]):
        """Print comprehensive extraction summary"""
        logging.info("="*80)
        logging.info("🎉 IMPROVED EXTRACTION COMPLETED")
        logging.info("="*80)
        
        stats = self.extraction_stats
        duration = (datetime.now() - self.start_ts).total_seconds() / 60
        
        logging.info(f"📊 EXTRACTION STATISTICS:")
        logging.info(f"  • Total listings extracted: {len(extracted_data)}")
        logging.info(f"  • Cards detected: {stats['total_cards_found']}")
        logging.info(f"  • Successful extractions: {stats['successful_extractions']}")
        logging.info(f"  • Failed extractions: {stats['failed_extractions']}")
        
        if stats['total_cards_found'] > 0:
            success_rate = (stats['successful_extractions'] / stats['total_cards_found']) * 100
            logging.info(f"  • Success rate: {success_rate:.1f}%")
        
        logging.info(f"  • Images found: {stats['images_downloaded']}")
        logging.info(f"  • Links extracted: {stats['links_extracted']}")
        logging.info(f"  • Processing time: {duration:.1f} minutes")
        
        if extracted_data:
            # Analyze extraction quality
            df = pd.DataFrame(extracted_data)
            
            price_count = len(df[df['price'].notna() & (df['price'] != '')])
            nearby_count = len(df[df['nearby_places_count'] > 0])
            avg_nearby = df['nearby_places_count'].mean()
            
            logging.info(f"\n📋 DATA QUALITY SUMMARY:")
            logging.info(f"  • Listings with price: {price_count}/{len(extracted_data)} ({price_count/len(extracted_data)*100:.1f}%)")
            logging.info(f"  • Listings with nearby places: {nearby_count}/{len(extracted_data)} ({nearby_count/len(extracted_data)*100:.1f}%)")
            logging.info(f"  • Average nearby places per listing: {avg_nearby:.1f}")
            
            # Show sample data
            sample = extracted_data[0]
            logging.info(f"\n📋 SAMPLE EXTRACTED DATA:")
            sample_fields = ['building_name', 'price', 'apartment_type', 'buildup_area', 'nearby_places_count']
            for field in sample_fields:
                if field in sample and sample[field]:
                    value = str(sample[field])[:50] + "..." if len(str(sample[field])) > 50 else sample[field]
                    logging.info(f"  • {field}: {value}")
        
        logging.info("="*80)


# Create improved configuration
def create_improved_config():
    """Create improved configuration file"""
    config = configparser.ConfigParser()
    
    config["limits"] = {
        "max_listings_per_society": "100",
        "max_scrolls": "50"
    }
    
    config["manual"] = {
        "selection_wait_time": "10"
    }
    
    config["output"] = {
        "output_dir": "improved_output"
    }
    
    config["http"] = {
        "min_delay": "1.0",
        "max_delay": "2.5"
    }
    
    config["extraction"] = {
        "detailed_mode": "true",
        "extract_images": "true", 
        "extract_links": "true",
        "extract_nearby_places": "true"
    }
    
    return config


def main():
    """Improved main function with better error handling"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("improved_nobroker_scraper.log")
        ]
    )
    
    print("=" * 80)
    print("🏠 IMPROVED NoBroker Property Data Extractor")
    print("=" * 80)
    print("🔧 Key Improvements:")
    print("  • Better property card detection using multiple strategies")
    print("  • Enhanced price and nearby places extraction")
    print("  • Improved data validation and quality analysis")
    print("  • More robust element finding and error handling")
    print("  • Comprehensive data extraction from card content")
    print("=" * 80)
    
    config_path = "improved_config.ini"
    
    # Create or load configuration
    if not os.path.exists(config_path):
        config = create_improved_config()
        with open(config_path, "w") as f:
            config.write(f)
        logging.info(f"Created improved configuration: {config_path}")
    else:
        config = configparser.ConfigParser()
        config.read(config_path)
    
    # URL options with more variety
    default_url = "hhttps://www.nobroker.in/1bhk-1rhk-flats-for-sale-in-gurgaon-gurgaon"
    url_options = [
        "https://www.nobroker.in/property/sale/gurgaon",
        # "https://www.nobroker.in/property/sale/pune", 
        # "https://www.nobroker.in/property/sale/bangalore",
        # "https://www.nobroker.in/property/sale/delhi",
        # "https://www.nobroker.in/property/sale/noida",
        # "https://www.nobroker.in/property/sale/gurgaon",
        # "https://www.nobroker.in/property/rent/mumbai",
        # "https://www.nobroker.in/property/rent/bangalore"
    ]
    
    print(f"\n🎯 Default URL: {default_url}")
    print(f"\n📋 Other popular options:")
    for i, url in enumerate(url_options, 1):
        city = url.split('/')[-1].title()
        prop_type = "Sale" if "/sale/" in url else "Rent"
        print(f"  {i}. {city} - {prop_type}")
    
    # URL selection
    target_url = default_url
    try:
        print(f"\n" + "="*60)
        choice = input(f"Select URL (1-{len(url_options)}) or press Enter for default: ").strip()
        
        if choice.isdigit():
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(url_options):
                target_url = url_options[choice_idx]
                print(f"✓ Selected: {target_url}")
        elif choice.startswith("http"):
            target_url = choice
            print(f"✓ Custom URL: {target_url}")
        else:
            print(f"✓ Using default: {target_url}")
            
    except KeyboardInterrupt:
        print("\n👋 Exiting...")
        return
    except Exception as e:
        print(f"⚠️ Input error, using default URL: {e}")
    
    # Initialize and run scraper
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    start_ts = datetime.now()
    scraper = NoBrokerScraper(config, run_id, start_ts)
    
    try:
        print(f"\n🚀 Starting improved extraction...")
        print(f"📍 Target URL: {target_url}")
        print(f"⏱️  Started at: {start_ts.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 Target listings: {scraper.max_listings}")
        print("=" * 80)
        
        output_path = scraper.run_improved_extraction(target_url)
        
        if output_path:
            print("\n" + "="*80)
            print("✅ EXTRACTION COMPLETED SUCCESSFULLY!")
            print("="*80)
            print(f"📁 Data saved to: {output_path}")
            print(f"📊 Check the Excel file for:")
            print("   • Property_Listings: Main extracted data")
            print("   • Extraction_Summary: Statistics and metrics")
            print("   • Data_Quality: Field completeness analysis")
            print("   • Nearby_Places_Analysis: Most common nearby places")
            print("="*80)
        else:
            print("\n" + "="*80)
            print("⚠️ EXTRACTION COMPLETED WITH ISSUES")
            print("="*80)
            print("❌ No data was successfully extracted")
            print("💡 Possible reasons:")
            print("   • Website structure changed")
            print("   • Access blocked or captcha required")
            print("   • No property listings found on the page")
            print("   • Network connectivity issues")
            print("\n💭 Try:")
            print("   • Using a different URL")
            print("   • Checking the manual setup phase more carefully")
            print("   • Running again with different filters")
            print("="*80)
            
    except KeyboardInterrupt:
        print("\n" + "="*80)
        print("⚠️ EXTRACTION INTERRUPTED BY USER")
        print("="*80)
        
        # Try to save partial data
        try:
            if hasattr(scraper, 'extracted_data') and scraper.extracted_data:
                print("💾 Attempting to save partial data...")
                emergency_save = scraper.save_enhanced_data(scraper.extracted_data)
                if emergency_save:
                    print(f"✅ Partial data saved to: {emergency_save}")
                else:
                    print("❌ Failed to save partial data")
            else:
                print("❌ No data available to save")
        except Exception as save_e:
            print(f"❌ Failed to save partial data: {save_e}")
        
        print("="*80)
        
    except Exception as e:
        print("\n" + "="*80)
        print("💥 FATAL ERROR OCCURRED")
        print("="*80)
        logging.error(f"Fatal error: {e}")
        print(f"❌ Error: {e}")
        print("\n💡 This might be due to:")
        print("   • Website blocking automated access")
        print("   • Changes in website structure")
        print("   • WebDriver issues")
        print("   • Network problems")
        print("\n🔧 Try:")
        print("   • Running the script again")
        print("   • Updating Chrome/ChromeDriver")
        print("   • Using a VPN if access is blocked")
        print("   • Checking the log file for more details")
        print("="*80)


if __name__ == "__main__":
    main()