#!/usr/bin/env python3
"""
Trade Show Exhibitor Directory Scraper

A reliable, extensible scraper for extracting exhibitor data from trade show directories.
Supports static scraping, API detection, and dynamic DOM scraping with pagination.
"""

import argparse
import json
import logging
import os
import re
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, Request

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_MAX_EXHIBITORS = 200
MIN_RESULTS_FOR_STATIC = 10
MAX_PAGINATION_PAGES = 10
COMPANY_NAME_MIN_LEN = 2
COMPANY_NAME_MAX_LEN = 80
DEFAULT_OUTPUT_DIR = './outputs'

# Metadata keywords that indicate non-company-name content
METADATA_KEYWORDS = ['booth', 'stand', 'hall', 'location', 'category']


def extract_event_name(url: str, html: Optional[str] = None) -> str:
    """
    Extract event name from URL or HTML content.
    
    Args:
        url: The exhibitor directory URL
        html: Optional HTML content to parse
        
    Returns:
        Event name as string
    """
    # Try to extract from URL first
    parsed = urlparse(url)
    hostname = parsed.netloc.lower()
    
    # Extract from subdomain or path
    if 'mapyourshow.com' in hostname:
        # Format: eventname.mapyourshow.com
        subdomain = hostname.split('.')[0]
        if subdomain and subdomain != 'www':
            return subdomain.upper()
    
    # Try to extract from path
    path_parts = [p for p in parsed.path.split('/') if p]
    if path_parts:
        # Remove common suffixes
        event_part = path_parts[0].replace('-', ' ').replace('_', ' ').title()
        if event_part and len(event_part) > 2:
            return event_part
    
    # Try to extract from HTML if provided
    if html:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Look for common event name patterns
            title_tag = soup.find('title')
            if title_tag:
                title_text = title_tag.get_text(strip=True)
                # Extract meaningful part
                if 'exhibitor' in title_text.lower():
                    parts = title_text.split('-')
                    if len(parts) > 1:
                        return parts[0].strip()
                return title_text.split('|')[0].strip()
            
            # Look for h1 or event name in meta tags
            h1 = soup.find('h1')
            if h1:
                return h1.get_text(strip=True)[:100]
        except Exception as e:
            logger.debug(f"Error extracting event name from HTML: {e}")
    
    # Fallback: use hostname or path
    if hostname and hostname != 'www':
        return hostname.split('.')[0].upper()
    
    return "Trade Show Event"


def normalize_company_name(raw: str) -> Optional[str]:
    """
    Normalize and validate company name.
    
    Args:
        raw: Raw company name string
        
    Returns:
        Normalized company name or None if invalid
    """
    if not raw:
        return None
    
    # Strip whitespace
    cleaned = raw.strip()
    
    # Remove extra whitespace
    cleaned = ' '.join(cleaned.split())
    
    # Check length
    if len(cleaned) < COMPANY_NAME_MIN_LEN or len(cleaned) > COMPANY_NAME_MAX_LEN:
        return None
    
    # Check for metadata keywords (case-insensitive)
    cleaned_lower = cleaned.lower()
    for keyword in METADATA_KEYWORDS:
        if keyword in cleaned_lower:
            return None
    
    # Check if it looks like a sentence (ends with punctuation and is long)
    if len(cleaned) > 30 and cleaned.rstrip().endswith(('.', '!', '?')):
        return None
    
    # Check if it's all caps and too long (might be metadata)
    if cleaned.isupper() and len(cleaned) > 50:
        return None
    
    return cleaned


def split_name_and_blurb(raw_text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Split combined text into company name and blurb.
    
    Args:
        raw_text: Combined text that may contain name and description
        
    Returns:
        Tuple of (company_name, company_blurb)
    """
    if not raw_text:
        return None, None
    
    cleaned = ' '.join(raw_text.strip().split())
    
    # Try splitting by common delimiters
    # First line or first sentence often contains the name
    lines = cleaned.split('\n')
    if len(lines) > 1:
        # First line is likely the name
        name = normalize_company_name(lines[0])
        if name:
            blurb = ' '.join(lines[1:]).strip()
            blurb = blurb if blurb and len(blurb) > 10 else None
            return name, blurb
    
    # Try splitting by sentence boundaries
    sentences = re.split(r'[.!?]\s+', cleaned)
    if len(sentences) > 1:
        # First sentence might be the name
        name = normalize_company_name(sentences[0])
        if name and len(name) < 60:  # Names are usually shorter
            blurb = '. '.join(sentences[1:]).strip()
            blurb = blurb if blurb and len(blurb) > 10 else None
            return name, blurb
    
    # Try splitting by length - if text is very long, first part is likely name
    if len(cleaned) > 80:
        # Take first 60 chars as potential name
        potential_name = cleaned[:60].strip()
        # Try to break at word boundary
        last_space = potential_name.rfind(' ')
        if last_space > 20:
            potential_name = potential_name[:last_space]
        
        name = normalize_company_name(potential_name)
        if name:
            blurb = cleaned[len(potential_name):].strip()
            blurb = blurb if blurb and len(blurb) > 10 else None
            return name, blurb
    
    # If we can't split confidently, treat entire text as potential name
    name = normalize_company_name(cleaned)
    return name, None


def static_scrape(url: str, max_results: int = DEFAULT_MAX_EXHIBITORS) -> List[Dict[str, Any]]:
    """
    Attempt static scraping using requests + BeautifulSoup.
    
    Args:
        url: Exhibitor directory URL
        max_results: Maximum number of exhibitors to return
        
    Returns:
        List of exhibitor dictionaries
    """
    logger.info(f"Attempting static scrape of {url}")
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        event_name = extract_event_name(url, response.text)
        
        exhibitors = []
        seen_names = set()
        
        # Common patterns for exhibitor listings
        # Look for links, divs, or list items that might contain exhibitor info
        selectors = [
            'a[href*="exhibitor"]',
            'a[href*="company"]',
            '.exhibitor',
            '.exhibitor-item',
            '.company',
            '.company-name',
            '[data-exhibitor]',
            '[data-company]',
        ]
        
        elements = []
        for selector in selectors:
            found = soup.select(selector)
            if found:
                elements = found
                logger.info(f"Found {len(elements)} elements with selector: {selector}")
                break
        
        # If no specific selector worked, try broader patterns
        if not elements:
            # Look for any links that might be exhibitor links
            all_links = soup.find_all('a', href=True)
            # Filter to likely exhibitor links
            elements = [e for e in all_links if any(
                keyword in e.get('href', '').lower() 
                for keyword in ['exhibitor', 'company', 'vendor', 'booth']
            )]
        
        for element in elements:
            if len(exhibitors) >= max_results:
                break
                
            # Try multiple extraction strategies
            company_name = None
            company_blurb = None
            
            # Strategy 1: Get text from anchor or element
            text = element.get_text(strip=True)
            if text:
                company_name, company_blurb = split_name_and_blurb(text)
            
            # Strategy 2: Check for dedicated name attributes/elements
            if not company_name:
                # Check aria-label, title, data attributes
                for attr in ['aria-label', 'title', 'data-name', 'data-company']:
                    attr_value = element.get(attr)
                    if attr_value:
                        company_name = normalize_company_name(attr_value)
                        if company_name:
                            break
            
            # Strategy 3: Look for child elements with name
            if not company_name:
                name_elem = element.find(['h1', 'h2', 'h3', 'h4', '.name', '.company-name', '[class*="name"]'])
                if name_elem:
                    name_text = name_elem.get_text(strip=True)
                    company_name = normalize_company_name(name_text)
                    
                    # Look for description in sibling or parent
                    desc_elem = element.find(['p', '.description', '.blurb', '[class*="desc"]'])
                    if desc_elem:
                        company_blurb = desc_elem.get_text(strip=True)
                        if len(company_blurb) < 10:
                            company_blurb = None
            
            # Validate and add
            if company_name:
                name_lower = company_name.lower()
                if name_lower not in seen_names:
                    seen_names.add(name_lower)
                    exhibitors.append({
                        'event_name': event_name,
                        'company_name': company_name,
                        'company_blurb': company_blurb if company_blurb and len(company_blurb) > 10 else None
                    })
        
        logger.info(f"Static scrape found {len(exhibitors)} exhibitors")
        return exhibitors
        
    except Exception as e:
        logger.warning(f"Static scrape failed: {e}")
        return []


def playwright_detect_api(url: str) -> Optional[Dict[str, Any]]:
    """
    Use Playwright to detect API calls that return exhibitor data.
    
    Args:
        url: Exhibitor directory URL
        
    Returns:
        API request info dict or None if no API detected
    """
    logger.info(f"Attempting to detect API for {url}")
    
    api_request_info = None
    
    def handle_request(request: Request):
        nonlocal api_request_info
        if api_request_info:
            return
        
        url_pattern = request.url.lower()
        # Look for API-like URLs
        if any(keyword in url_pattern for keyword in ['api', 'exhibitor', 'company', 'gallery', 'list', 'search']):
            # Check if it's JSON
            if 'json' in url_pattern or request.resource_type in ['fetch', 'xhr']:
                method = request.method
                headers = request.headers
                post_data = request.post_data
                
                # Extract query params
                parsed = urlparse(request.url)
                params = parse_qs(parsed.query)
                
                api_request_info = {
                    'url': request.url,
                    'method': method,
                    'headers': dict(headers),
                    'params': params,
                    'post_data': post_data
                }
                logger.info(f"Detected potential API: {request.url}")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            
            page.on('request', handle_request)
            
            # Navigate and wait a bit for API calls
            page.goto(url, wait_until='networkidle', timeout=30000)
            page.wait_for_timeout(2000)  # Wait for additional API calls
            
            browser.close()
            
            return api_request_info
            
    except Exception as e:
        logger.warning(f"API detection failed: {e}")
        return None


def api_fetch(api_request_info: Dict[str, Any], max_results: int = DEFAULT_MAX_EXHIBITORS) -> List[Dict[str, Any]]:
    """
    Fetch exhibitor data from detected API with pagination support.
    
    Args:
        api_request_info: API request information from playwright_detect_api
        max_results: Maximum number of exhibitors to return
        
    Returns:
        List of exhibitor dictionaries
    """
    logger.info(f"Fetching from API: {api_request_info['url']}")
    
    exhibitors = []
    seen_names = set()
    event_name = extract_event_name(api_request_info['url'])
    
    # Extract base URL and params
    parsed = urlparse(api_request_info['url'])
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    params = api_request_info.get('params', {})
    
    # Try pagination
    page_num = 1
    max_pages = MAX_PAGINATION_PAGES
    
    while page_num <= max_pages and len(exhibitors) < max_results:
        # Update pagination params
        pagination_params = {}
        # Copy existing params (they come as lists from parse_qs)
        for k, v in params.items():
            pagination_params[k] = v if isinstance(v, list) else [v]
        
        # Common pagination parameter names - find existing or add new
        pagination_set = False
        for param_name in ['page', 'pagenum', 'pageNumber', 'offset', 'start']:
            # Check if this param exists (case-insensitive)
            existing_key = None
            for k in pagination_params.keys():
                if k.lower() == param_name.lower():
                    existing_key = k
                    break
            
            if existing_key:
                pagination_params[existing_key] = [str(page_num)]
                pagination_set = True
                break
        
        if not pagination_set:
            # Try adding common pagination params
            pagination_params['page'] = [str(page_num)]
        
        # Flatten params for requests
        flat_params = {}
        for k, v in pagination_params.items():
            flat_params[k] = v[0] if isinstance(v, list) and v else v
        
        try:
            headers = api_request_info.get('headers', {})
            # Remove browser-specific headers that might cause issues
            headers.pop('content-length', None)
            headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            
            if api_request_info['method'].upper() == 'POST':
                response = requests.post(
                    base_url,
                    headers=headers,
                    params=flat_params,
                    json=json.loads(api_request_info['post_data']) if api_request_info.get('post_data') else None,
                    timeout=30
                )
            else:
                response = requests.get(
                    base_url,
                    headers=headers,
                    params=flat_params,
                    timeout=30
                )
            
            response.raise_for_status()
            
            # Try to parse as JSON
            try:
                data = response.json()
            except ValueError:
                logger.warning(f"API response is not JSON on page {page_num}")
                break
            
            # Extract exhibitors from JSON response
            # Try common response structures
            items = None
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # Common keys
                for key in ['data', 'results', 'exhibitors', 'companies', 'items', 'list']:
                    if key in data:
                        items = data[key]
                        break
                if not items:
                    items = [data]  # Single item response
            
            if not items:
                logger.warning(f"No items found in API response on page {page_num}")
                break
            
            page_count = 0
            for item in items:
                if len(exhibitors) >= max_results:
                    break
                
                # Extract company name from various possible fields
                company_name = None
                company_blurb = None
                
                if isinstance(item, dict):
                    # Try common field names
                    for name_field in ['name', 'companyName', 'company_name', 'exhibitorName', 'title', 'company']:
                        if name_field in item and item[name_field]:
                            raw_name = str(item[name_field])
                            company_name = normalize_company_name(raw_name)
                            if company_name:
                                break
                    
                    # Try description fields
                    for desc_field in ['description', 'blurb', 'summary', 'tagline', 'about', 'bio']:
                        if desc_field in item and item[desc_field]:
                            company_blurb = str(item[desc_field]).strip()
                            if len(company_blurb) < 10:
                                company_blurb = None
                            break
                
                if company_name:
                    name_lower = company_name.lower()
                    if name_lower not in seen_names:
                        seen_names.add(name_lower)
                        exhibitors.append({
                            'event_name': event_name,
                            'company_name': company_name,
                            'company_blurb': company_blurb
                        })
                        page_count += 1
            
            logger.info(f"Page {page_num}: Found {page_count} new exhibitors (total: {len(exhibitors)})")
            
            # Stop if no new items or reached max
            if page_count == 0 or len(exhibitors) >= max_results:
                break
            
            page_num += 1
            
        except Exception as e:
            logger.warning(f"Error fetching page {page_num}: {e}")
            break
    
    logger.info(f"API fetch completed: {len(exhibitors)} exhibitors")
    return exhibitors


def playwright_dom_scrape(url: str, max_results: int = DEFAULT_MAX_EXHIBITORS) -> List[Dict[str, Any]]:
    """
    Use Playwright to scrape rendered DOM with pagination support.
    
    Args:
        url: Exhibitor directory URL
        max_results: Maximum number of exhibitors to return
        
    Returns:
        List of exhibitor dictionaries
    """
    logger.info(f"Attempting Playwright DOM scrape of {url}")
    
    exhibitors = []
    seen_names = set()
    event_name = None
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            
            page.goto(url, wait_until='networkidle', timeout=30000)
            page.wait_for_timeout(2000)  # Wait for dynamic content
            
            # Extract event name
            event_name = extract_event_name(url, page.content())
            
            page_num = 1
            max_pages = MAX_PAGINATION_PAGES
            last_count = 0
            
            while page_num <= max_pages and len(exhibitors) < max_results:
                # Extract exhibitors from current page
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Try various selectors
                elements = []
                selectors = [
                    'a[href*="exhibitor"]',
                    '.exhibitor',
                    '.exhibitor-item',
                    '.company',
                    '[data-exhibitor]',
                    'article',
                    '.card',
                    '.listing-item'
                ]
                
                for selector in selectors:
                    found = soup.select(selector)
                    if found:
                        elements = found
                        logger.info(f"Found {len(elements)} elements with selector: {selector}")
                        break
                
                # If no specific selector, try broader approach
                if not elements:
                    # Look for links with exhibitor-related text/attributes
                    all_links = soup.find_all('a', href=True)
                    elements = [e for e in all_links if any(
                        keyword in (e.get('href', '') + ' ' + e.get_text()).lower()
                        for keyword in ['exhibitor', 'company', 'vendor']
                    )]
                
                current_page_count = 0
                for element in elements:
                    if len(exhibitors) >= max_results:
                        break
                    
                    company_name = None
                    company_blurb = None
                    
                    # Extract name
                    text = element.get_text(strip=True)
                    if text:
                        company_name, company_blurb = split_name_and_blurb(text)
                    
                    # Try attributes
                    if not company_name:
                        for attr in ['aria-label', 'title', 'data-name']:
                            attr_value = element.get(attr)
                            if attr_value:
                                company_name = normalize_company_name(attr_value)
                                if company_name:
                                    break
                    
                    # Try child elements
                    if not company_name:
                        name_elem = element.find(['h1', 'h2', 'h3', 'h4', '.name', '[class*="name"]'])
                        if name_elem:
                            name_text = name_elem.get_text(strip=True)
                            company_name = normalize_company_name(name_text)
                            
                            desc_elem = element.find(['p', '.description', '[class*="desc"]'])
                            if desc_elem:
                                company_blurb = desc_elem.get_text(strip=True)
                                if len(company_blurb) < 10:
                                    company_blurb = None
                    
                    if company_name:
                        name_lower = company_name.lower()
                        if name_lower not in seen_names:
                            seen_names.add(name_lower)
                            exhibitors.append({
                                'event_name': event_name,
                                'company_name': company_name,
                                'company_blurb': company_blurb if company_blurb and len(company_blurb) > 10 else None
                            })
                            current_page_count += 1
                
                logger.info(f"Page {page_num}: Found {current_page_count} new exhibitors (total: {len(exhibitors)})")
                
                # Try to find and click pagination
                if len(exhibitors) >= max_results:
                    break
                
                # Look for next page button
                next_button = None
                try:
                    # Common next button selectors
                    next_selectors = [
                        'button:has-text("Next")',
                        'a:has-text("Next")',
                        '[aria-label*="next" i]',
                        '.next',
                        '.pagination-next',
                        'button[class*="next"]',
                        'a[class*="next"]'
                    ]
                    
                    for selector in next_selectors:
                        try:
                            next_button = page.query_selector(selector)
                            if next_button:
                                break
                        except:
                            continue
                    
                    if next_button:
                        # Check if button is disabled
                        is_disabled = next_button.get_attribute('disabled') or \
                                     'disabled' in (next_button.get_attribute('class') or '')
                        
                        if not is_disabled:
                            next_button.click()
                            page.wait_for_timeout(2000)
                            page_num += 1
                            
                            # Check if we got new results
                            if len(exhibitors) == last_count:
                                logger.info("No new results, stopping pagination")
                                break
                            last_count = len(exhibitors)
                        else:
                            logger.info("Next button disabled, stopping pagination")
                            break
                    else:
                        # Try "Load more" button
                        load_more = page.query_selector('button:has-text("Load more"), a:has-text("Load more")')
                        if load_more:
                            load_more.click()
                            page.wait_for_timeout(2000)
                            page_num += 1
                        else:
                            logger.info("No pagination found, stopping")
                            break
                except Exception as e:
                    logger.debug(f"Pagination attempt failed: {e}")
                    break
            
            browser.close()
            
    except Exception as e:
        logger.warning(f"Playwright DOM scrape failed: {e}")
    
    logger.info(f"Playwright DOM scrape completed: {len(exhibitors)} exhibitors")
    return exhibitors


def discover_directory_links(url: str) -> List[str]:
    """
    Discover exhibitor directory links from a discovery/source page (e.g., ASI guide).
    
    Args:
        url: Source page URL
        
    Returns:
        List of discovered directory URLs
    """
    logger.info(f"Discovering directory links from {url}")
    
    directory_links = []
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for links that might be exhibitor directories
        # Common patterns: URLs containing "exhibitor", "directory", "gallery", "who"
        all_links = soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True).lower()
            
            # Make absolute URL
            absolute_url = urljoin(url, href)
            
            # Check if it looks like an exhibitor directory
            href_lower = href.lower()
            text_lower = text.lower()
            
            # URL patterns
            url_patterns = [
                'exhibitor',
                'directory',
                'gallery',
                'who.*exhibit',
                'exhibiting',
                'vendor',
                'booth.*list'
            ]
            
            # Text patterns
            text_patterns = [
                'exhibitor',
                'directory',
                'who.*exhibit',
                'exhibiting',
                'vendor',
                'booth'
            ]
            
            matches_url = any(re.search(pattern, href_lower) for pattern in url_patterns)
            matches_text = any(re.search(pattern, text_lower) for pattern in text_patterns)
            
            if matches_url or matches_text:
                # Additional validation: check surrounding context
                parent = link.parent
                if parent:
                    parent_text = parent.get_text(strip=True).lower()
                    if any(keyword in parent_text for keyword in ['exhibitor', 'directory', 'vendor', 'booth']):
                        if absolute_url not in directory_links:
                            directory_links.append(absolute_url)
                            logger.info(f"Discovered directory link: {absolute_url}")
        
        logger.info(f"Discovered {len(directory_links)} directory links")
        
    except Exception as e:
        logger.warning(f"Error discovering directory links: {e}")
    
    return directory_links


def scrape_exhibitors(url: str, max_results: int = DEFAULT_MAX_EXHIBITORS) -> List[Dict[str, Any]]:
    """
    Main orchestration function to scrape exhibitors from a URL.
    
    Args:
        url: Exhibitor directory URL or discovery page URL
        max_results: Maximum number of exhibitors to return
        
    Returns:
        List of exhibitor dictionaries
    """
    logger.info(f"Starting scrape for: {url}")
    
    # Check if this is a discovery page (ASI guide)
    if 'asicentral.com' in url.lower() or re.search(r'trade.*show.*planning', url.lower()):
        directory_links = discover_directory_links(url)
        if directory_links:
            logger.info(f"Processing {len(directory_links)} discovered directory links")
            all_exhibitors = []
            seen_names = set()
            
            for dir_url in directory_links:
                if len(all_exhibitors) >= max_results:
                    break
                
                dir_exhibitors = scrape_single_directory(dir_url, max_results - len(all_exhibitors))
                for exhibitor in dir_exhibitors:
                    name_lower = exhibitor['company_name'].lower()
                    if name_lower not in seen_names:
                        seen_names.add(name_lower)
                        all_exhibitors.append(exhibitor)
                        if len(all_exhibitors) >= max_results:
                            break
            
            logger.info(f"Total exhibitors from discovery: {len(all_exhibitors)}")
            return all_exhibitors[:max_results]
    
    # Regular directory scraping
    return scrape_single_directory(url, max_results)


def scrape_single_directory(url: str, max_results: int = DEFAULT_MAX_EXHIBITORS) -> List[Dict[str, Any]]:
    """
    Scrape a single exhibitor directory URL.
    
    Args:
        url: Exhibitor directory URL
        max_results: Maximum number of exhibitors to return
        
    Returns:
        List of exhibitor dictionaries
    """
    strategy_used = None
    exhibitors = []
    
    # Strategy 1: Try static scraping
    exhibitors = static_scrape(url, max_results)
    if len(exhibitors) >= MIN_RESULTS_FOR_STATIC:
        strategy_used = "static"
        logger.info(f"Static scraping successful: {len(exhibitors)} exhibitors")
    else:
        logger.info(f"Static scraping found only {len(exhibitors)} exhibitors, trying Playwright")
        
        # Strategy 2: Try API detection
        api_info = playwright_detect_api(url)
        if api_info:
            strategy_used = "api"
            api_exhibitors = api_fetch(api_info, max_results)
            if len(api_exhibitors) > len(exhibitors):
                exhibitors = api_exhibitors
                logger.info(f"API fetch successful: {len(exhibitors)} exhibitors")
        
        # Strategy 3: Fallback to DOM scraping
        if not exhibitors or len(exhibitors) < MIN_RESULTS_FOR_STATIC:
            strategy_used = "dom"
            dom_exhibitors = playwright_dom_scrape(url, max_results)
            if len(dom_exhibitors) > len(exhibitors):
                exhibitors = dom_exhibitors
                logger.info(f"DOM scraping successful: {len(exhibitors)} exhibitors")
    
    # Deduplicate and limit
    seen_names = set()
    deduplicated = []
    for exhibitor in exhibitors:
        name_lower = exhibitor['company_name'].lower()
        if name_lower not in seen_names:
            seen_names.add(name_lower)
            deduplicated.append(exhibitor)
            if len(deduplicated) >= max_results:
                break
    
    # Logging summary
    logger.info(f"Strategy used: {strategy_used or 'static'}")
    logger.info(f"Total valid exhibitors: {len(deduplicated)}")
    blurb_count = sum(1 for e in deduplicated if e.get('company_blurb'))
    logger.info(f"Exhibitors with blurb: {blurb_count}")
    
    return deduplicated[:max_results]


def extract_domain_from_url(url_str: str) -> Optional[str]:
    """
    Extract domain from a URL string.
    
    Args:
        url_str: URL string
        
    Returns:
        Domain string or None
    """
    if not url_str:
        return None
    try:
        parsed = urlparse(url_str)
        domain = parsed.netloc or parsed.path.split('/')[0] if parsed.path else None
        if domain:
            domain = domain.replace('www.', '').split(':')[0]
            return domain.lower()
    except Exception:
        pass
    return None


def run(source_url: str, limit: int = DEFAULT_MAX_EXHIBITORS) -> List[Dict[str, str]]:
    """
    Run the scraper and return results in pipeline format.
    
    Args:
        source_url: Source URL to scrape
        limit: Maximum number of companies to scrape
        
    Returns:
        List of dicts with keys: company_name, domain, company_blurb, source_url
    """
    logger.info(f"Scraping exhibitors from: {source_url} (limit: {limit})")
    
    # Scrape exhibitors (call the main function)
    exhibitors = scrape_single_directory(source_url, max_results=limit)
    
    # Convert to pipeline format
    results = []
    for exhibitor in exhibitors:
        company_name = exhibitor.get('company_name', '').strip()
        if not company_name:
            continue
        
        # Skip obviously non-company entries
        if any(skip in company_name.lower() for skip in ['exhibitor search', 'all exhibitors', 'search', 'filter']):
            continue
        
        # Skip entries that look like descriptions (too long, contain "is", "are", "delivers", etc.)
        if len(company_name) > 60 or any(word in company_name.lower() for word in ['delivers', 'is a', 'are a', 'provides', 'specializes']):
            continue
        
        # Extract company_blurb (from HTML scraping only, no API)
        company_blurb = exhibitor.get('company_blurb', '').strip() if exhibitor.get('company_blurb') else ''
        # Limit to 240 chars
        if company_blurb and len(company_blurb) > 240:
            company_blurb = company_blurb[:237] + '...'
        
        # Domain extraction (empty for now, can be enriched later)
        domain = ''
        
        results.append({
            'company_name': company_name,
            'domain': domain,
            'company_blurb': company_blurb,
            'source_url': source_url
        })
    
    logger.info(f"Scraped {len(results)} companies")
    return results


def generate_filename(url: str, event_name: Optional[str] = None) -> str:
    """
    Generate a filename from URL or event name.
    
    Args:
        url: The URL being scraped
        event_name: Optional event name
        
    Returns:
        Filename string (without extension)
    """
    if event_name:
        # Clean event name for filename
        slug = re.sub(r'[^a-zA-Z0-9]+', '-', event_name.lower())
        slug = slug.strip('-')
        return slug
    
    # Fallback to URL-based slug
    parsed = urlparse(url)
    hostname = parsed.netloc.replace('www.', '').split('.')[0]
    path_parts = [p for p in parsed.path.split('/') if p]
    
    if path_parts:
        slug = '-'.join([hostname] + path_parts[:2])
    else:
        slug = hostname
    
    # Clean slug
    slug = re.sub(r'[^a-zA-Z0-9-]+', '-', slug.lower())
    slug = re.sub(r'-+', '-', slug).strip('-')
    
    return slug[:50]  # Limit length


def save_results(exhibitors: List[Dict[str, Any]], out_dir: str, out_file: Optional[str] = None, 
                 url: Optional[str] = None, event_name: Optional[str] = None) -> str:
    """
    Save exhibitor results to a JSON file.
    
    Args:
        exhibitors: List of exhibitor dictionaries
        out_dir: Output directory path
        out_file: Optional specific filename (without path)
        url: Optional URL for filename generation
        event_name: Optional event name for filename generation
        
    Returns:
        Full path to saved file
    """
    # Create output directory if it doesn't exist
    os.makedirs(out_dir, exist_ok=True)
    
    # Generate filename if not provided
    if not out_file:
        base_name = generate_filename(url or '', event_name)
        timestamp = datetime.now().strftime('%Y-%m-%d')
        out_file = f"{base_name}_{timestamp}.json"
    
    # Ensure .json extension
    if not out_file.endswith('.json'):
        out_file += '.json'
    
    # Full path
    file_path = os.path.join(out_dir, out_file)
    
    # Save JSON
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(exhibitors, f, indent=2, ensure_ascii=False)
    
    return file_path


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description='Scrape exhibitor directory from trade show websites',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scrape_exhibitors.py "https://example.com/exhibitor-directory"
  python scrape_exhibitors.py "https://example.com/exhibitor-directory" --out-dir ./results
  python scrape_exhibitors.py "https://example.com/exhibitor-directory" --out-file custom-name.json
  python scrape_exhibitors.py "https://example.com/exhibitor-directory" --max-results 100
        """
    )
    parser.add_argument('url', help='Exhibitor directory URL')
    parser.add_argument('--out-dir', default=DEFAULT_OUTPUT_DIR, 
                       help=f'Output directory (default: {DEFAULT_OUTPUT_DIR})')
    parser.add_argument('--out-file', default=None,
                       help='Output filename (default: auto-generated from event name/URL + date)')
    parser.add_argument('--max-results', type=int, default=DEFAULT_MAX_EXHIBITORS,
                       help=f'Maximum number of exhibitors to return (default: {DEFAULT_MAX_EXHIBITORS})')
    
    args = parser.parse_args()
    
    try:
        # Scrape exhibitors
        exhibitors = scrape_exhibitors(args.url, max_results=args.max_results)
        
        # Extract event name for filename generation
        event_name = None
        if exhibitors:
            event_name = exhibitors[0].get('event_name')
        
        # Save to file
        file_path = save_results(
            exhibitors, 
            args.out_dir, 
            args.out_file, 
            args.url, 
            event_name
        )
        
        # Also print to stdout
        output_json = json.dumps(exhibitors, indent=2, ensure_ascii=False)
        print(output_json)
        
        # Print success message
        rel_path = os.path.relpath(file_path)
        print(f"\nSaved results to {rel_path} ({len(exhibitors)} exhibitors)", file=sys.stderr)
        
    except Exception as e:
        logger.error(f"Scraping failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
