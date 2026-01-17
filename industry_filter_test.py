#!/usr/bin/env python3
"""
Industry Filter Test Script

Determines if companies are in target application industries based on
keyword matching of website content.
"""

import csv
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from typing import Optional, List, Tuple, Dict
import sys


# Configuration
TIMEOUT = 15
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Target keywords (case-insensitive substring match)
TARGET_KEYWORDS = [
    "signage", "sign shop", "wayfinding", "sign systems",
    "large format", "wide format", "grand format",
    "printing", "digital printing", "inkjet printing", "print shop",
    "vehicle wrap", "car wrap", "fleet graphics", "wrap film",
    "architectural graphics", "window film", "glass film", "wall graphics", "interior graphics",
    "decals", "labels", "nameplates", "industrial graphics",
    "protective film", "laminate", "overlaminate",
    "display graphics", "exhibition graphics", "trade show graphics"
]

# Non-target keywords
NON_TARGET_KEYWORDS = [
    "software", "saas", "platform", "cloud",
    "marketing agency", "branding agency", "design studio", "creative agency",
    "association", "nonprofit", "foundation",
    "university", "college", "school",
    "dental", "medical", "clinic", "hospital",
    "restaurant", "food service",
    "logistics", "supply chain", "trucking", "freight"
]

# Additional pages to try (after homepage)
ADDITIONAL_PAGES = ["/industries", "/solutions", "/applications", "/markets", "/products"]


def fetch_url(url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """
    Fetch a URL with retries and backoff.
    
    Args:
        url: URL to fetch
        retries: Number of retry attempts
        
    Returns:
        Response object if successful, None otherwise
    """
    headers = {"User-Agent": USER_AGENT}
    
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=TIMEOUT, allow_redirects=True)
            if response.status_code == 200:
                return response
            elif response.status_code in [301, 302, 303, 307, 308]:
                # Follow redirects are handled by allow_redirects=True
                # But if we get a redirect status, we might want to check the final URL
                continue
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                pass  # Final attempt failed
    
    return None


def extract_text(html_content: str) -> str:
    """
    Extract visible text from HTML using BeautifulSoup.
    Removes script, style, nav elements, and normalizes whitespace.
    
    Args:
        html_content: HTML content as string
        
    Returns:
        Normalized lowercase text
    """
    soup = BeautifulSoup(html_content, 'lxml')
    
    # Remove script, style, nav, and other non-content elements
    for element in soup(['script', 'style', 'nav', 'header', 'footer', 'meta', 'link']):
        element.decompose()
    
    # Get text and normalize
    text = soup.get_text(separator=' ', strip=True)
    
    # Normalize whitespace and convert to lowercase
    text = ' '.join(text.split())
    text = text.lower()
    
    return text


def find_keywords(text: str, keywords: List[str]) -> List[str]:
    """
    Find which keywords appear in the text (case-insensitive substring match).
    
    Args:
        text: Text to search in
        keywords: List of keywords to search for
        
    Returns:
        List of matched keywords
    """
    matched = []
    text_lower = text.lower()
    
    for keyword in keywords:
        if keyword.lower() in text_lower:
            matched.append(keyword)
    
    return matched


def determine_base_url(website: Optional[str], matched_domain: Optional[str]) -> Optional[str]:
    """
    Determine the base URL to fetch.
    Prefer website if it starts with http, else use matched_domain.
    
    Args:
        website: Website URL (may be None or empty)
        matched_domain: Matched domain (may be None or empty)
        
    Returns:
        Base URL string or None
    """
    if website and website.strip().startswith(('http://', 'https://')):
        return website.strip()
    elif matched_domain and matched_domain.strip():
        domain = matched_domain.strip()
        # Remove any leading http:// or https:// if present
        domain = domain.lstrip('http://').lstrip('https://').lstrip('/')
        return f"https://{domain}/"
    
    return None


def fetch_company_pages(base_url: str) -> List[Tuple[str, str]]:
    """
    Fetch up to 3 pages per company:
    1. Homepage (base_url)
    2-3. First 2 successful pages from additional pages list
    
    Args:
        base_url: Base URL of the company
        
    Returns:
        List of tuples (url, text_content) for successfully fetched pages
    """
    pages_content = []
    
    # Fetch homepage
    response = fetch_url(base_url)
    if response:
        text = extract_text(response.text)
        pages_content.append((base_url, text))
    
    # Try additional pages (stop after first 2 successful 200 responses)
    successful_count = 0
    for page_path in ADDITIONAL_PAGES:
        if successful_count >= 2:
            break
        
        full_url = urljoin(base_url.rstrip('/') + '/', page_path.lstrip('/'))
        response = fetch_url(full_url)
        if response:
            text = extract_text(response.text)
            pages_content.append((full_url, text))
            successful_count += 1
    
    return pages_content


def classify_company(pages_content: List[Tuple[str, str]]) -> Tuple[str, List[str], str, str]:
    """
    Classify company as YES/NO based on keyword matching.
    
    Args:
        pages_content: List of (url, text) tuples from fetched pages
        
    Returns:
        Tuple of (is_target_industry, matched_keywords, evidence_url, notes)
    """
    if not pages_content:
        return ("NO", [], "", "no_pages_fetched")
    
    # Combine all text from all pages
    all_text = " ".join([text for _, text in pages_content])
    
    # Find target keywords
    target_matches = find_keywords(all_text, TARGET_KEYWORDS)
    
    # Find non-target keywords
    non_target_matches = find_keywords(all_text, NON_TARGET_KEYWORDS)
    
    # Decision logic:
    # - If ANY target keyword appears → YES
    # - Else if any non-target keyword appears and no target keyword → NO
    # - Else (unclear/no signal) → NO (conservative)
    
    if target_matches:
        # Find the first URL where a target keyword was found
        evidence_url = ""
        for url, text in pages_content:
            for keyword in target_matches:
                if keyword.lower() in text.lower():
                    evidence_url = url
                    break
            if evidence_url:
                break
        
        return ("YES", target_matches, evidence_url, "")
    elif non_target_matches:
        return ("NO", [], "", "non_target_keywords_found")
    else:
        return ("NO", [], "", "no_keywords_found")


def process_row(row: Dict[str, str]) -> Dict[str, str]:
    """
    Process a single row from the CSV.
    
    Args:
        row: Dictionary representing a CSV row
        
    Returns:
        Dictionary with results
    """
    company_name = row.get('company_name', '')
    matched_domain = row.get('matched_domain', '')
    website = row.get('website', '')
    
    # Determine base URL
    base_url = determine_base_url(website, matched_domain)
    
    if not base_url:
        return {
            'company_name': company_name,
            'matched_domain': matched_domain,
            'website': website,
            'is_target_industry': 'NO',
            'matched_keywords': '',
            'evidence_url': '',
            'notes': 'no_valid_url'
        }
    
    # Fetch pages
    try:
        pages_content = fetch_company_pages(base_url)
    except Exception as e:
        return {
            'company_name': company_name,
            'matched_domain': matched_domain,
            'website': website,
            'is_target_industry': 'NO',
            'matched_keywords': '',
            'evidence_url': '',
            'notes': f'error: {str(e)[:50]}'
        }
    
    # Classify
    is_target, matched_keywords, evidence_url, notes = classify_company(pages_content)
    
    return {
        'company_name': company_name,
        'matched_domain': matched_domain,
        'website': website,
        'is_target_industry': is_target,
        'matched_keywords': ', '.join(matched_keywords),
        'evidence_url': evidence_url,
        'notes': notes
    }


def main():
    """Main execution function."""
    input_file = 'test_enriched.csv'
    output_file = 'industry_filter_test_output.csv'
    
    print(f"Loading {input_file}...")
    
    # Read input CSV
    rows_to_process = []
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i < 10:  # Only first 10 rows
                    rows_to_process.append(row)
                else:
                    break
    except FileNotFoundError:
        print(f"Error: {input_file} not found!")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading {input_file}: {e}")
        sys.exit(1)
    
    print(f"Processing {len(rows_to_process)} rows...")
    print()
    
    # Process each row
    results = []
    for i, row in enumerate(rows_to_process, 1):
        company_name = row.get('company_name', 'Unknown')
        print(f"[{i}/{len(rows_to_process)}] Processing: {company_name}")
        
        result = process_row(row)
        results.append(result)
        
        # Small delay to be respectful
        time.sleep(0.5)
    
    # Write output CSV
    print()
    print(f"Writing results to {output_file}...")
    
    output_columns = [
        'company_name', 'matched_domain', 'website', 'is_target_industry',
        'matched_keywords', 'evidence_url', 'notes'
    ]
    
    try:
        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=output_columns)
            writer.writeheader()
            writer.writerows(results)
    except Exception as e:
        print(f"Error writing {output_file}: {e}")
        sys.exit(1)
    
    # Print summary
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Processed rows: {len(results)}")
    
    yes_count = sum(1 for r in results if r['is_target_industry'] == 'YES')
    no_count = sum(1 for r in results if r['is_target_industry'] == 'NO')
    
    print(f"YES count: {yes_count}")
    print(f"NO count: {no_count}")
    print()
    
    if yes_count > 0:
        print("YES companies:")
        for r in results:
            if r['is_target_industry'] == 'YES':
                print(f"  - {r['company_name']}")
    
    print()
    print(f"Results saved to {output_file}")


if __name__ == "__main__":
    main()
