#!/usr/bin/env python3
"""
Exhibitor Data Enrichment Pipeline

This script enriches scraped exhibitor data with company information including:
- Industry classification
- Employee count/range
- Revenue/revenue range
- HQ location
- Company website/domain

USAGE:
    python enrich_exhibitors.py <input_json_file> [--api-key <key>] [--output <output.csv>]
    
EXAMPLES:
    # With Clearbit API key (recommended for best results)
    python enrich_exhibitors.py outputs/isasignexpo2026_2026-01-15.json --api-key YOUR_CLEARBIT_API_KEY
    
    # Without API key (uses LinkedIn fallback - limited data)
    python enrich_exhibitors.py outputs/isasignexpo2026_2026-01-15.json
    
    # Custom output file
    python enrich_exhibitors.py outputs/exhibitors.json --api-key YOUR_KEY --output enriched.csv

API KEY SETUP:
    - Get a Serper API key from: https://serper.dev (Google Search API)
    - The script will search for company websites using Serper and extract domains
    - Serper is used for domain discovery; for full enrichment (industry, employees, revenue),
      you may want to combine with Clearbit or other company data APIs
    - If API key is invalid/missing, the script will still attempt domain extraction but with lower success rate
    
OUTPUT:
    - CSV file with all required fields (see schema below)
    - Domains are extracted even without API key
    - Enrichment data (industry, employees, revenue) requires valid API key
    
OUTPUT SCHEMA:
    company_name, matched_domain, website, industry, employee_count, employee_range,
    revenue, revenue_range, hq_city, hq_state, hq_country, source_url,
    source_industry, source_employee, source_revenue, confidence
"""

import argparse
import csv
import json
import logging
import re
import sys
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Any
from urllib.parse import urlparse, urljoin

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
SERPER_API_BASE = 'https://google.serper.dev/search'
CLEARBIT_API_BASE = 'https://company.clearbit.com/v2/companies/find'
LINKEDIN_SEARCH_BASE = 'https://www.linkedin.com/search/results/companies/'
RATE_LIMIT_DELAY = 0.5  # Seconds between API calls
MAX_RETRIES = 3


class EnrichmentProvider:
    """Pluggable interface for company data enrichment providers."""
    
    def enrich_by_domain(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Enrich company data using domain.
        
        Args:
            domain: Company domain (e.g., 'example.com')
            
        Returns:
            Dictionary with enrichment data or None if not found
        """
        raise NotImplementedError


class SerperProvider(EnrichmentProvider):
    """Serper API provider for company domain discovery and basic enrichment."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            'X-API-KEY': api_key,
            'Content-Type': 'application/json'
        })
    
    def search_company_domain(self, company_name: str) -> Optional[str]:
        """
        Search for company website using Serper API.
        
        Args:
            company_name: Company name
            
        Returns:
            Domain string or None
        """
        try:
            query = f"{company_name} official website"
            payload = {
                'q': query,
                'num': 5
            }
            
            response = self.session.post(SERPER_API_BASE, json=payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                organic_results = data.get('organic', [])
                
                # Look for the most likely company website
                for result in organic_results:
                    link = result.get('link', '')
                    title = result.get('title', '').lower()
                    snippet = result.get('snippet', '').lower()
                    
                    # Skip social media and directory sites
                    if any(skip in link.lower() for skip in ['facebook.com', 'linkedin.com', 'twitter.com', 
                                                             'instagram.com', 'crunchbase', 'zoominfo', 
                                                             'bloomberg', 'wikipedia.org']):
                        continue
                    
                    # Extract domain
                    domain = extract_domain_from_url(link)
                    if domain:
                        # Prefer .com domains
                        if '.com' in domain:
                            return domain
                        # Otherwise return first valid domain
                        return domain
                
                # If no .com found, return first valid domain
                for result in organic_results:
                    link = result.get('link', '')
                    domain = extract_domain_from_url(link)
                    if domain and not any(skip in domain for skip in ['facebook', 'linkedin', 'twitter', 'instagram']):
                        return domain
            
            return None
            
        except Exception as e:
            logger.debug(f"Serper search failed for {company_name}: {e}")
            return None
    
    def enrich_by_domain(self, domain: str, company_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Enrich company data using Serper search results.
        Searches for company information and extracts data from search snippets.
        
        Args:
            domain: Company domain
            company_name: Optional company name for better search queries
            
        Returns:
            Dictionary with enrichment data extracted from search results
        """
        enrichment = {
            'industry': None,
            'employee_count': None,
            'employee_range': None,
            'revenue': None,
            'revenue_range': None,
            'hq_city': None,
            'hq_state': None,
            'hq_country': None,
            'website': f"https://{domain}",
            'source_industry': None,
            'source_employee': None,
            'source_revenue': None,
            'confidence': 0.6
        }
        
        try:
            # Strategy 1: Search for company info on LinkedIn/Crunchbase
            search_queries = [
                f"{company_name or domain} LinkedIn company employees industry" if company_name else f"{domain} LinkedIn company",
                f"{company_name or domain} Crunchbase revenue employees" if company_name else f"{domain} Crunchbase",
                f"{company_name or domain} company information employees revenue industry" if company_name else f"{domain} company info"
            ]
            
            all_text = ""
            
            for query in search_queries[:2]:  # Limit to 2 queries to save API calls
                try:
                    payload = {
                        'q': query,
                        'num': 5
                    }
                    
                    response = self.session.post(SERPER_API_BASE, json=payload, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        organic_results = data.get('organic', [])
                        
                        # Collect all snippets and titles
                        for result in organic_results:
                            snippet = result.get('snippet', '')
                            title = result.get('title', '')
                            all_text += f" {title} {snippet}"
                        
                        # Also check knowledge graph if available
                        kg = data.get('knowledgeGraph', {})
                        if kg:
                            all_text += f" {kg.get('description', '')} {kg.get('type', '')}"
                    
                    time.sleep(0.3)  # Rate limiting between queries
                    
                except Exception as e:
                    logger.debug(f"Serper query failed: {e}")
                    continue
            
            # Extract information from collected text
            if all_text:
                # Extract industry
                industry = self._extract_industry(all_text, company_name)
                if industry:
                    enrichment['industry'] = industry
                    enrichment['source_industry'] = 'serper_search'
                    enrichment['confidence'] = max(enrichment['confidence'], 0.65)
                
                # Extract employee range
                employee_range = self._extract_employee_range(all_text)
                if employee_range:
                    enrichment['employee_range'] = employee_range
                    enrichment['source_employee'] = 'serper_search'
                    enrichment['confidence'] = max(enrichment['confidence'], 0.65)
                
                # Extract revenue range
                revenue_range = self._extract_revenue_range(all_text)
                if revenue_range:
                    enrichment['revenue_range'] = revenue_range
                    enrichment['source_revenue'] = 'serper_search'
                    enrichment['confidence'] = max(enrichment['confidence'], 0.65)
                
                # Extract location
                location = self._extract_location(all_text)
                if location:
                    enrichment['hq_city'] = location.get('city')
                    enrichment['hq_state'] = location.get('state')
                    enrichment['hq_country'] = location.get('country')
            
            return enrichment if any([enrichment.get('industry'), enrichment.get('employee_range'), 
                                     enrichment.get('revenue_range'), enrichment.get('hq_city')]) else None
            
        except Exception as e:
            logger.debug(f"Serper enrichment failed for {domain}: {e}")
            return None
    
    def _extract_industry(self, text: str, company_name: Optional[str] = None) -> Optional[str]:
        """Extract industry from text."""
        text_lower = text.lower()
        
        # Common industry keywords
        industry_patterns = {
            'Manufacturing': ['manufacturing', 'manufacturer', 'producer', 'production'],
            'Technology': ['technology', 'tech', 'software', 'saas', 'it services'],
            'Signage': ['signage', 'sign', 'graphics', 'display', 'visual'],
            'Printing': ['printing', 'print', 'graphic arts', 'digital print'],
            'Advertising': ['advertising', 'marketing', 'ad agency', 'marketing agency'],
            'Retail': ['retail', 'retailer', 'store', 'shopping'],
            'Construction': ['construction', 'building', 'contractor'],
            'Electronics': ['electronics', 'electronic', 'components'],
            'Plastics': ['plastics', 'plastic', 'polymer'],
            'Lighting': ['lighting', 'led', 'illumination'],
            'Equipment': ['equipment', 'machinery', 'machines'],
            'Materials': ['materials', 'material', 'supplies']
        }
        
        for industry, keywords in industry_patterns.items():
            if any(keyword in text_lower for keyword in keywords):
                return industry
        
        return None
    
    def _extract_employee_range(self, text: str) -> Optional[str]:
        """Extract employee range from text."""
        # Look for employee count patterns
        patterns = [
            r'(\d+(?:,\d+)*)\s*(?:employees?|people|staff)',
            r'(?:employees?|people|staff)[:\s]+(\d+(?:,\d+)*)',
            r'(\d+)\s*-\s*(\d+)\s*(?:employees?|people)',
            r'(?:between|from)\s+(\d+)\s*(?:and|to)\s+(\d+)\s*(?:employees?|people)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                if isinstance(matches[0], tuple):
                    # Range pattern
                    try:
                        low = int(matches[0][0].replace(',', ''))
                        high = int(matches[0][1].replace(',', ''))
                        return f"{low}-{high}"
                    except:
                        continue
                else:
                    # Single number
                    try:
                        count = int(matches[0].replace(',', ''))
                        # Convert to range
                        if count < 10:
                            return "1-10"
                        elif count < 50:
                            return "11-50"
                        elif count < 200:
                            return "51-200"
                        elif count < 500:
                            return "201-500"
                        elif count < 1000:
                            return "501-1000"
                        elif count < 5000:
                            return "1001-5000"
                        elif count < 10000:
                            return "5001-10000"
                        else:
                            return "10000+"
                    except:
                        continue
        
        # Look for range keywords
        range_keywords = {
            '1-10': ['startup', 'small team', 'few employees'],
            '11-50': ['small company', 'small business'],
            '51-200': ['mid-size', 'medium company'],
            '201-500': ['growing company'],
            '501-1000': ['large company'],
            '1000+': ['enterprise', 'large enterprise', 'thousands of employees']
        }
        
        text_lower = text.lower()
        for range_val, keywords in range_keywords.items():
            if any(kw in text_lower for kw in keywords):
                return range_val
        
        return None
    
    def _extract_revenue_range(self, text: str) -> Optional[str]:
        """Extract revenue range from text."""
        # Look for revenue patterns
        patterns = [
            r'\$(\d+(?:\.\d+)?)\s*(?:million|M|billion|B)',
            r'(?:revenue|annual revenue|sales)[:\s]+\$?(\d+(?:\.\d+)?)\s*(?:million|M|billion|B)',
            r'\$(\d+(?:,\d+)*)\s*(?:in revenue|revenue)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                try:
                    value = float(matches[0].replace(',', ''))
                    unit = 'million' if 'million' in text.lower() or 'M' in matches[0] else 'billion' if 'billion' in text.lower() or 'B' in matches[0] else 'million'
                    
                    if unit == 'billion':
                        value = value * 1000  # Convert to millions
                    
                    # Convert to range
                    if value < 1:
                        return "<$1M"
                    elif value < 5:
                        return "$1M-$5M"
                    elif value < 10:
                        return "$5M-$10M"
                    elif value < 50:
                        return "$10M-$50M"
                    elif value < 100:
                        return "$50M-$100M"
                    elif value < 500:
                        return "$100M-$500M"
                    else:
                        return "$500M+"
                except:
                    continue
        
        return None
    
    def _extract_location(self, text: str) -> Optional[Dict[str, str]]:
        """Extract location information from text."""
        location = {}
        
        # Look for city, state patterns (US)
        us_pattern = r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),\s*([A-Z]{2})'
        matches = re.findall(us_pattern, text)
        if matches:
            location['city'] = matches[0][0]
            location['state'] = matches[0][1]
            location['country'] = 'United States'
            return location
        
        # Look for "based in" or "headquartered in"
        based_pattern = r'(?:based in|headquartered in|located in|HQ in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
        matches = re.findall(based_pattern, text, re.IGNORECASE)
        if matches:
            location['city'] = matches[0]
            return location
        
        return None


class ClearbitProvider(EnrichmentProvider):
    """Clearbit API provider for company enrichment."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.auth = (api_key, '')  # Clearbit uses API key as username
    
    def enrich_by_name(self, company_name: str) -> Optional[Dict[str, Any]]:
        """
        Search Clearbit by company name (fallback when domain is unknown).
        
        Args:
            company_name: Company name
            
        Returns:
            Dictionary with enrichment data including domain
        """
        try:
            # Clearbit name search endpoint
            url = f"https://company.clearbit.com/v1/domains/find?name={requests.utils.quote(company_name)}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 404:
                return None
            elif response.status_code == 429:
                logger.warning(f"Clearbit: Rate limited for name search {company_name}")
                time.sleep(2)
                return None
            elif response.status_code != 200:
                return None
            
            # Name search returns just the domain, so we need to enrich by that domain
            domain_data = response.json()
            domain = domain_data.get('domain')
            if domain:
                return self.enrich_by_domain(domain)
            
            return None
        except Exception as e:
            logger.debug(f"Clearbit name search failed for {company_name}: {e}")
            return None
        
    def enrich_by_domain(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Enrich company data using Clearbit API.
        
        Args:
            domain: Company domain
            
        Returns:
            Dictionary with enrichment data
        """
        try:
            url = f"{CLEARBIT_API_BASE}?domain={domain}"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 404:
                logger.debug(f"Clearbit: No data for domain {domain}")
                return None
            elif response.status_code == 401:
                logger.error(f"Clearbit: Authentication failed. Please check your API key.")
                # Don't spam errors - only log once
                if not hasattr(self, '_auth_error_logged'):
                    logger.error("API key appears to be invalid or expired. Continuing without enrichment...")
                    self._auth_error_logged = True
                return None
            elif response.status_code == 429:
                logger.warning(f"Clearbit: Rate limited for {domain}, waiting...")
                time.sleep(2)
                return None
            elif response.status_code != 200:
                logger.debug(f"Clearbit: Error {response.status_code} for {domain}")
                return None
            
            data = response.json()
            
            # Extract and normalize data
            enrichment = {
                'industry': data.get('category', {}).get('industry') or data.get('category', {}).get('sector'),
                'employee_count': data.get('metrics', {}).get('employees'),
                'employee_range': self._normalize_employee_range(data.get('metrics', {}).get('employees')),
                'revenue': None,  # Clearbit doesn't provide exact revenue
                'revenue_range': self._normalize_revenue_range(data.get('metrics', {}).get('annualRevenue')),
                'hq_city': data.get('geo', {}).get('city'),
                'hq_state': data.get('geo', {}).get('state'),
                'hq_country': data.get('geo', {}).get('country'),
                'website': data.get('domain') and f"https://{data.get('domain')}" or None,
                'source_industry': 'clearbit',
                'source_employee': 'clearbit',
                'source_revenue': 'clearbit',
                'confidence': 0.9  # High confidence for API data
            }
            
            return enrichment
            
        except Exception as e:
            logger.warning(f"Clearbit enrichment failed for {domain}: {e}")
            return None
    
    def _normalize_employee_range(self, count: Optional[int]) -> Optional[str]:
        """Convert employee count to range."""
        if not count:
            return None
        
        if count < 10:
            return "1-10"
        elif count < 50:
            return "11-50"
        elif count < 200:
            return "51-200"
        elif count < 500:
            return "201-500"
        elif count < 1000:
            return "501-1000"
        elif count < 5000:
            return "1001-5000"
        elif count < 10000:
            return "5001-10000"
        else:
            return "10000+"
    
    def _normalize_revenue_range(self, revenue: Optional[float]) -> Optional[str]:
        """Convert revenue to range string."""
        if not revenue:
            return None
        
        # Revenue is typically in USD
        if revenue < 1_000_000:
            return "<$1M"
        elif revenue < 5_000_000:
            return "$1M-$5M"
        elif revenue < 10_000_000:
            return "$5M-$10M"
        elif revenue < 50_000_000:
            return "$10M-$50M"
        elif revenue < 100_000_000:
            return "$50M-$100M"
        elif revenue < 500_000_000:
            return "$100M-$500M"
        else:
            return "$500M+"


class LinkedInProvider(EnrichmentProvider):
    """LinkedIn scraping provider for employee range (fallback when API key not available)."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def enrich_by_domain(self, domain: str) -> Optional[Dict[str, Any]]:
        """
        Attempt to extract employee range from LinkedIn company page.
        Note: This is a fallback method with lower confidence.
        
        Args:
            domain: Company domain
            
        Returns:
            Dictionary with enrichment data (limited fields)
        """
        try:
            # Try to find LinkedIn company page
            company_name = domain.split('.')[0]  # Use domain name as company name hint
            
            # Search LinkedIn (this is a simplified approach)
            # In production, you'd want to use LinkedIn's official API or more sophisticated scraping
            search_url = f"{LINKEDIN_SEARCH_BASE}?keywords={company_name}"
            
            # Use Playwright for dynamic content
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context()
                page = context.new_page()
                
                try:
                    page.goto(search_url, wait_until='networkidle', timeout=15000)
                    page.wait_for_timeout(2000)
                    
                    html = page.content()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Look for employee count indicators (LinkedIn structure varies)
                    # This is a simplified extraction - LinkedIn's structure changes frequently
                    employee_text = None
                    for text in soup.stripped_strings:
                        if 'employees' in text.lower() or 'employee' in text.lower():
                            # Try to extract number
                            match = re.search(r'(\d+(?:,\d+)*)\s*(?:employees?|people)', text, re.IGNORECASE)
                            if match:
                                employee_text = match.group(1)
                                break
                    
                    browser.close()
                    
                    if employee_text:
                        # Convert to range
                        count = int(employee_text.replace(',', ''))
                        employee_range = self._count_to_range(count)
                        
                        return {
                            'industry': None,
                            'employee_count': None,
                            'employee_range': employee_range,
                            'revenue': None,
                            'revenue_range': None,
                            'hq_city': None,
                            'hq_state': None,
                            'hq_country': None,
                            'website': None,
                            'source_industry': None,
                            'source_employee': 'linkedin_scrape',
                            'source_revenue': None,
                            'confidence': 0.5  # Lower confidence for scraped data
                        }
                    
                except Exception as e:
                    browser.close()
                    logger.debug(f"LinkedIn scraping failed for {domain}: {e}")
                    return None
                    
        except Exception as e:
            logger.debug(f"LinkedIn enrichment failed for {domain}: {e}")
            return None
    
    def _count_to_range(self, count: int) -> str:
        """Convert employee count to range."""
        if count < 10:
            return "1-10"
        elif count < 50:
            return "11-50"
        elif count < 200:
            return "51-200"
        elif count < 500:
            return "201-500"
        elif count < 1000:
            return "501-1000"
        elif count < 5000:
            return "1001-5000"
        elif count < 10000:
            return "5001-10000"
        else:
            return "10000+"


def extract_domain_from_url(url: str) -> Optional[str]:
    """
    Extract domain from a URL.
    
    Args:
        url: Source URL
        
    Returns:
        Domain string or None
    """
    if not url:
        return None
    
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split('/')[0] if parsed.path else None
        
        if domain:
            # Remove www. prefix
            domain = domain.replace('www.', '')
            # Remove port if present
            domain = domain.split(':')[0]
            return domain.lower()
    except Exception:
        pass
    
    return None


def extract_domain_from_exhibitor_page(source_url: str, company_name: str) -> Tuple[Optional[str], float]:
    """
    Extract company domain from exhibitor detail page.
    
    Args:
        source_url: URL to exhibitor detail page
        company_name: Company name for fallback search
        
    Returns:
        Tuple of (domain, confidence_score)
    """
    if not source_url:
        return None, 0.0
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(source_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Strategy 1: Look for official website links
        website_links = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            text = link.get_text(strip=True).lower()
            
            # Check if it looks like a website link
            if any(keyword in text for keyword in ['website', 'visit', 'www', 'http']):
                absolute_url = urljoin(source_url, href)
                domain = extract_domain_from_url(absolute_url)
                if domain and domain not in ['facebook.com', 'twitter.com', 'linkedin.com', 'instagram.com']:
                    website_links.append((domain, 0.8))
            
            # Check href for website patterns
            if href.startswith('http') and not any(social in href.lower() for social in ['facebook', 'twitter', 'linkedin', 'instagram']):
                domain = extract_domain_from_url(href)
                if domain:
                    website_links.append((domain, 0.7))
        
        # Strategy 2: Look in meta tags
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            if meta.get('property') == 'og:url' or meta.get('name') == 'og:url':
                content = meta.get('content', '')
                domain = extract_domain_from_url(content)
                if domain:
                    return domain, 0.9
        
        # Strategy 3: Extract from page URL if it's a company subdomain
        page_domain = extract_domain_from_url(source_url)
        if page_domain and not any(x in page_domain for x in ['mapyourshow.com', 'exhibitor', 'booth']):
            return page_domain, 0.6
        
        # Return best match from links
        if website_links:
            # Sort by confidence and return best
            website_links.sort(key=lambda x: x[1], reverse=True)
            return website_links[0]
        
        return None, 0.0
        
    except Exception as e:
        logger.debug(f"Failed to extract domain from {source_url}: {e}")
        return None, 0.0


def search_company_website(company_name: str) -> Tuple[Optional[str], float]:
    """
    Fallback: Attempt to construct or search for company website using company name.
    
    Args:
        company_name: Company name
        
    Returns:
        Tuple of (domain, confidence_score)
    """
    if not company_name:
        return None, 0.0
    
    # Strategy 1: Try common domain patterns from company name
    # Clean company name for domain construction
    clean_name = re.sub(r'[^a-zA-Z0-9\s]', '', company_name.lower())
    clean_name = re.sub(r'\s+', '', clean_name)  # Remove spaces
    
    # Try common TLDs
    common_tlds = ['com', 'net', 'org', 'co', 'io']
    for tld in common_tlds:
        potential_domain = f"{clean_name}.{tld}"
        # Quick check if domain might exist (very basic heuristic)
        # In production, you'd do a DNS lookup or use a search API
        if len(clean_name) > 3:  # Reasonable length
            return potential_domain, 0.3
    
    # Strategy 2: Try to extract from company name if it contains URL-like text
    url_pattern = r'(?:https?://)?(?:www\.)?([a-zA-Z0-9-]+\.[a-zA-Z]{2,})'
    match = re.search(url_pattern, company_name)
    if match:
        domain = match.group(1)
        return domain, 0.4
    
    return None, 0.0


def merge_enrichment_data(existing: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge enrichment data, keeping best values by confidence.
    Never overwrite existing data with empty values.
    
    Args:
        existing: Existing enrichment data
        new: New enrichment data to merge
        
    Returns:
        Merged enrichment data
    """
    merged = existing.copy()
    new_confidence = new.get('confidence', 0.0)
    existing_confidence = existing.get('confidence', 0.0)
    
    # Only update if new data has higher confidence or existing is empty
    for key in ['industry', 'employee_count', 'employee_range', 'revenue', 'revenue_range',
                'hq_city', 'hq_state', 'hq_country', 'website']:
        new_value = new.get(key)
        existing_value = existing.get(key)
        
        # Never overwrite with empty/None
        if new_value and (not existing_value or new_confidence > existing_confidence):
            merged[key] = new_value
    
    # Update source fields
    for key in ['source_industry', 'source_employee', 'source_revenue']:
        if new.get(key) and (not existing.get(key) or new_confidence > existing_confidence):
            merged[key] = new.get(key)
    
    # Update confidence to max
    merged['confidence'] = max(new_confidence, existing_confidence)
    
    return merged


def enrich_exhibitors(input_file: str, api_key: Optional[str] = None, output_file: Optional[str] = None) -> str:
    """
    Main enrichment function.
    
    Args:
        input_file: Path to input JSON file
        api_key: Optional API key for enrichment provider
        output_file: Optional output CSV file path
        
    Returns:
        Path to output file
    """
    logger.info(f"Loading exhibitors from {input_file}")
    
    # Load input data
    with open(input_file, 'r', encoding='utf-8') as f:
        exhibitors = json.load(f)
    
    logger.info(f"Loaded {len(exhibitors)} exhibitors")
    
    # Initialize enrichment provider
    # Default to Serper (Google Search API) - can be configured for Clearbit if needed
    if api_key:
        # Try Serper first (most common for this use case)
        provider = SerperProvider(api_key)
        logger.info("Using Serper API for domain discovery and enrichment")
    else:
        provider = LinkedInProvider()
        logger.info("Using LinkedIn scraping (no API key provided)")
    
    # Process exhibitors
    enriched_data = []
    domain_to_companies = defaultdict(list)
    match_failures = []
    ambiguous_domains = []
    
    for idx, exhibitor in enumerate(exhibitors):
        company_name = exhibitor.get('company_name', '')
        source_url = exhibitor.get('source_url')  # May not exist in current data
        
        # Skip obviously non-company entries
        if any(skip in company_name.lower() for skip in ['exhibitor search', 'all exhibitors', 'search', 'filter']):
            logger.debug(f"Skipping non-company entry: {company_name}")
            continue
        
        # Skip entries that look like descriptions (too long, contain "is", "are", "delivers", etc.)
        if len(company_name) > 60 or any(word in company_name.lower() for word in ['delivers', 'is a', 'are a', 'provides', 'specializes']):
            logger.debug(f"Skipping description-like entry: {company_name[:50]}...")
            continue
        
        logger.info(f"Processing {idx + 1}/{len(exhibitors)}: {company_name}")
        
        # Initialize enrichment record
        enrichment = {
            'company_name': company_name,
            'matched_domain': None,
            'website': None,
            'industry': None,
            'employee_count': None,
            'employee_range': None,
            'revenue': None,
            'revenue_range': None,
            'hq_city': None,
            'hq_state': None,
            'hq_country': None,
            'source_url': source_url or '',
            'source_industry': None,
            'source_employee': None,
            'source_revenue': None,
            'confidence': 0.0
        }
        
        # Step 1: Extract domain
        domain = None
        domain_confidence = 0.0
        
        if source_url:
            domain, domain_confidence = extract_domain_from_exhibitor_page(source_url, company_name)
        
        if not domain and api_key and isinstance(provider, SerperProvider):
            # Use Serper to search for company website
            time.sleep(RATE_LIMIT_DELAY)
            searched_domain = provider.search_company_domain(company_name)
            if searched_domain:
                domain = searched_domain
                domain_confidence = 0.7  # Higher confidence for search-based discovery
                logger.info(f"  Found domain via Serper: {domain}")
        
        if not domain:
            # Fallback: try to construct from company name (very low confidence)
            domain, domain_confidence = search_company_website(company_name)
        
        if domain:
            enrichment['matched_domain'] = domain
            enrichment['website'] = f"https://{domain}"
            enrichment['confidence'] = domain_confidence
            
            # Track for deduplication
            domain_to_companies[domain].append({
                'company_name': company_name,
                'enrichment': enrichment
            })
        else:
            match_failures.append(company_name)
            logger.warning(f"Could not extract domain for {company_name}")
        
        # Step 2: Enrich using domain
        if domain:
            time.sleep(RATE_LIMIT_DELAY)  # Rate limiting
            if isinstance(provider, SerperProvider):
                provider_data = provider.enrich_by_domain(domain, company_name)
            else:
                provider_data = provider.enrich_by_domain(domain)
            
            if provider_data:
                enrichment = merge_enrichment_data(enrichment, provider_data)
                if enrichment.get('industry') or enrichment.get('employee_range'):
                    logger.info(f"  Enriched: {enrichment.get('industry')} | {enrichment.get('employee_range')}")
                else:
                    logger.debug(f"  Domain found but limited enrichment data")
        elif api_key and isinstance(provider, SerperProvider):
            # Serper already tried to find domain above, so this is a fallback
            # Try to get basic info from search results
            time.sleep(RATE_LIMIT_DELAY)
            provider_data = provider.enrich_by_domain(company_name)  # Search by name
            if provider_data and provider_data.get('website'):
                domain_from_provider = extract_domain_from_url(provider_data['website'])
                if domain_from_provider:
                    enrichment['matched_domain'] = domain_from_provider
                    enrichment['website'] = provider_data['website']
                    enrichment['confidence'] = max(enrichment['confidence'], 0.6)
                    enrichment = merge_enrichment_data(enrichment, provider_data)
                    logger.info(f"  Found domain via Serper search: {domain_from_provider}")
        
        enriched_data.append(enrichment)
    
    # Step 3: Handle duplicates by domain
    logger.info("Deduplicating by domain...")
    seen_domains = set()
    deduplicated = []
    
    for enrichment in enriched_data:
        domain = enrichment.get('matched_domain')
        if domain:
            if domain in seen_domains:
                # Check if we should keep this one (higher confidence)
                existing_idx = next(
                    (i for i, e in enumerate(deduplicated) if e.get('matched_domain') == domain),
                    None
                )
                if existing_idx is not None:
                    existing = deduplicated[existing_idx]
                    if enrichment.get('confidence', 0) > existing.get('confidence', 0):
                        deduplicated[existing_idx] = enrichment
                        ambiguous_domains.append(domain)
                    continue
            seen_domains.add(domain)
        
        deduplicated.append(enrichment)
    
    # Step 4: Write output
    if not output_file:
        # Generate output filename
        import os
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        output_file = f"{base_name}_enriched.csv"
    
    logger.info(f"Writing enriched data to {output_file}")
    
    fieldnames = [
        'company_name', 'matched_domain', 'website', 'industry',
        'employee_count', 'employee_range', 'revenue', 'revenue_range',
        'hq_city', 'hq_state', 'hq_country', 'source_url',
        'source_industry', 'source_employee', 'source_revenue', 'confidence'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(deduplicated)
    
    # Log summary
    logger.info(f"\n=== Enrichment Summary ===")
    logger.info(f"Total exhibitors processed: {len(exhibitors)}")
    logger.info(f"Domains matched: {len([e for e in deduplicated if e.get('matched_domain')])}")
    logger.info(f"Match failures: {len(match_failures)}")
    logger.info(f"Ambiguous domains: {len(ambiguous_domains)}")
    logger.info(f"Companies with industry: {len([e for e in deduplicated if e.get('industry')])}")
    logger.info(f"Companies with employee data: {len([e for e in deduplicated if e.get('employee_range') or e.get('employee_count')])}")
    logger.info(f"Companies with revenue data: {len([e for e in deduplicated if e.get('revenue_range') or e.get('revenue')])}")
    
    if match_failures:
        logger.warning(f"Match failures: {match_failures[:10]}...")  # Show first 10
    
    return output_file


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description='Enrich exhibitor data with company information',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python enrich_exhibitors.py outputs/exhibitors.json --api-key YOUR_API_KEY
  python enrich_exhibitors.py outputs/exhibitors.json --api-key YOUR_API_KEY --output enriched.csv
  python enrich_exhibitors.py outputs/exhibitors.json  # Uses LinkedIn fallback
        """
    )
    parser.add_argument('input_file', help='Input JSON file with exhibitor data')
    parser.add_argument('--api-key', default=None, help='API key for enrichment provider (Clearbit)')
    parser.add_argument('--output', default=None, help='Output CSV file (default: <input>_enriched.csv)')
    
    args = parser.parse_args()
    
    try:
        output_file = enrich_exhibitors(args.input_file, args.api_key, args.output)
        print(f"\n✓ Enrichment complete! Results saved to: {output_file}", file=sys.stderr)
        
    except Exception as e:
        logger.error(f"Enrichment failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
