#!/usr/bin/env python3
"""
Industry Filter Script

Determines if companies are in target application industries based on
keyword matching of website content. Processes all rows in test_enriched.csv.
"""

import csv
import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import Optional, List, Tuple, Dict
import sys


# Configuration
TIMEOUT = 15
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
REQUEST_DELAY = 0.5  # Delay between companies to be respectful

# Classification thresholds
THRESHOLD_YES = 3  # Score >= 3 → YES
THRESHOLD_MAYBE_MIN = 1  # Score in [1,2] → MAYBE
THRESHOLD_NO_MAX = 0  # Score <= 0 → NO

# Keyword scoring groups (case-insensitive substring match)
STRONG_KEYWORDS = {  # +3 points each
    "wide format", "wide-format", "large format", "large-format",
    "signage", "sign shop",
    "vehicle wrap", "car wrap", "fleet graphics",
    "architectural graphics", "wall wrap", "window film",
    "vinyl wrap", "commercial graphics"
}
MEDIUM_KEYWORDS = {  # +2 points each
    "graphics", "graphic", "printing", "print", "digital print",
    "banner", "display", "exhibit", "trade show display",
    "lamination", "film", "adhesive", "wrap",
    "plotter", "cutting", "cnc", "fabrication"
}
WEAK_KEYWORDS = {  # +1 point each
    "installation", "wayfinding", "retail graphics",
    "branding", "promotional"
}
HARD_NEGATIVE_KEYWORDS = {  # -3 points, only if total positive score == 0
    "restaurant", "bank", "insurance", "investment",
    "dental", "hospital", "clinic",
    "school", "university",
    "real estate", "law firm", "accounting"
}
SOFT_NEGATIVE_KEYWORDS = {  # -1 point, but never block if any positive keyword exists
    "software", "consulting", "media", "association"
}

# Legacy keywords (kept for backward compatibility with old code paths)
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


def score_company_text(text: str) -> Tuple[int, List[str]]:
    """
    Score company text based on keyword matching.
    Uses local fields only (no API calls).
    
    Args:
        text: Text to score (lowercase)
        
    Returns:
        Tuple of (score, matched_keywords_list)
    """
    if not text:
        return (0, [])
    
    text_lower = text.lower()
    positive_score = 0
    matched_keywords = []
    
    # Check strong keywords (+3)
    for keyword in STRONG_KEYWORDS:
        if keyword.lower() in text_lower:
            positive_score += 3
            matched_keywords.append(keyword)
    
    # Check medium keywords (+2)
    for keyword in MEDIUM_KEYWORDS:
        if keyword.lower() in text_lower:
            positive_score += 2
            matched_keywords.append(keyword)
    
    # Check weak keywords (+1)
    for keyword in WEAK_KEYWORDS:
        if keyword.lower() in text_lower:
            positive_score += 1
            matched_keywords.append(keyword)
    
    # Apply negative keywords
    negative_penalty = 0
    has_positive = positive_score > 0
    
    # Hard negatives (-3) only if no positive score
    if positive_score == 0:
        for keyword in HARD_NEGATIVE_KEYWORDS:
            if keyword.lower() in text_lower:
                negative_penalty = -3
                break  # Only apply once
    
    # Soft negatives (-1) never block if positive exists
    if not has_positive:
        for keyword in SOFT_NEGATIVE_KEYWORDS:
            if keyword.lower() in text_lower:
                negative_penalty += -1
                break  # Only apply once
    
    final_score = max(0, positive_score + negative_penalty)
    
    return (final_score, matched_keywords)


def classify_company(company_name: str = "", domain: str = "", company_blurb: str = "") -> Tuple[str, int, List[str], str]:
    """
    Classify company using scoring system.
    Uses ONLY local fields (company_name + domain + company_blurb).
    NO API calls.
    
    Args:
        company_name: Company name
        domain: Domain name
        company_blurb: Company description from HTML scraping
        
    Returns:
        Tuple of (fit_bucket, score, matched_keywords, evidence_snippet)
        fit_bucket: "YES", "MAYBE", or "NO"
    """
    # Build text from company_name + domain + company_blurb (normalized)
    text_parts = []
    if company_name:
        text_parts.append(company_name.strip())
    if domain:
        text_parts.append(domain.strip())
    if company_blurb:
        text_parts.append(company_blurb.strip())
    
    if not text_parts:
        return ("NO", 0, [], "no_data")
    
    combined_text = " ".join(text_parts).lower()
    
    # Score the text
    score, matched_keywords = score_company_text(combined_text)
    
    # Determine bucket based on thresholds
    if score >= THRESHOLD_YES:
        fit_bucket = "YES"
    elif score >= THRESHOLD_MAYBE_MIN:
        fit_bucket = "MAYBE"
    else:
        fit_bucket = "NO"
    
    # Create evidence snippet (max 120 chars)
    if matched_keywords:
        evidence = ", ".join(matched_keywords[:5])  # Limit to first 5 keywords
        if len(evidence) > 120:
            evidence = evidence[:117] + "..."
    else:
        evidence = "no_keywords" if score == 0 else "low_score"
    
    return (fit_bucket, score, matched_keywords, evidence)


def process_row(row: Dict[str, str]) -> Dict[str, str]:
    """
    Process a single row from the CSV.
    Uses ONLY local fields (no API calls, no web fetching).
    
    Args:
        row: Dictionary representing a CSV row with company_name, domain, company_blurb
        
    Returns:
        Dictionary with results including fit_bucket, score, etc.
    """
    company_name = row.get('company_name', '')
    domain = row.get('matched_domain', '') or row.get('domain', '')
    company_blurb = row.get('company_blurb', '')
    
    # Classify using local fields only (no API, no web fetching)
    fit_bucket, score, matched_keywords, evidence_snippet = classify_company(
        company_name=company_name, domain=domain, company_blurb=company_blurb
    )
    
    # Backward compatibility: fit_yes_no
    fit_yes_no = "YES" if fit_bucket == "YES" else "NO"
    
    return {
        'company_name': company_name,
        'domain': domain,
        'company_blurb': company_blurb,
        'fit_bucket': fit_bucket,
        'score': str(score),
        'fit_yes_no': fit_yes_no,  # Backward compatibility
        'matched_keywords': ', '.join(matched_keywords),
        'evidence_snippet': evidence_snippet[:120]  # Ensure max 120 chars
    }


def classify(input_csv: str, output_csv: str, progress_callback=None, debug: bool = False) -> Dict[str, int]:
    """
    Classify companies from input CSV and write results to output CSV.
    
    Args:
        input_csv: Path to input CSV with columns: company_name, domain, source_url
        output_csv: Path to output CSV
        progress_callback: Optional callback function(count, total) for progress updates
        debug: If True, print debug information about MAYBE and borderline NO companies
        
    Returns:
        Dictionary with counts: {'total': int, 'yes': int, 'maybe': int, 'no': int}
    """
    print(f"Loading {input_csv}...")
    
    # Read input CSV
    rows_to_process = []
    try:
        with open(input_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows_to_process.append(row)
    except FileNotFoundError:
        raise FileNotFoundError(f"Input file not found: {input_csv}")
    except Exception as e:
        raise Exception(f"Error reading {input_csv}: {e}")
    
    total_rows = len(rows_to_process)
    print(f"Found {total_rows} rows to process.")
    
    # Process each row
    results = []
    start_time = time.time()
    
    for i, row in enumerate(rows_to_process, 1):
        company_name = row.get('company_name', 'Unknown')
        domain = row.get('domain', '')
        source_url = row.get('source_url', '')
        
        # Progress indicator
        if progress_callback:
            progress_callback(i, total_rows)
        else:
            elapsed = time.time() - start_time
            if i > 1:
                avg_time_per_row = elapsed / (i - 1)
                estimated_remaining = avg_time_per_row * (total_rows - i)
                print(f"[{i}/{total_rows} ({i*100//total_rows}%)] Processing: {company_name} | "
                      f"ETA: {estimated_remaining/60:.1f} min")
            else:
                print(f"[{i}/{total_rows} ({i*100//total_rows}%)] Processing: {company_name}")
        
        # Get company_blurb from input
        company_blurb = row.get('company_blurb', '')
        
        # Adapt row format for process_row
        adapted_row = {
            'company_name': company_name,
            'domain': domain,
            'company_blurb': company_blurb
        }
        
        result = process_row(adapted_row)
        
        # Extract results
        fit_bucket = result.get('fit_bucket', 'NO')
        score = result.get('score', '0')
        fit_yes_no = result.get('fit_yes_no', 'NO')
        matched_keywords = result.get('matched_keywords', '')
        evidence_snippet = result.get('evidence_snippet', '')
        
        # Determine industry guess from keywords
        industry_guess = ''
        if matched_keywords:
            keyword_lower = matched_keywords.lower()
            if any(k in keyword_lower for k in ['large format', 'wide format', 'grand format', 'printing', 'print']):
                industry_guess = 'Large-format printing'
            elif any(k in keyword_lower for k in ['architectural graphics', 'window film', 'glass film', 'wall graphics']):
                industry_guess = 'Architectural graphics'
            elif any(k in keyword_lower for k in ['vehicle wrap', 'car wrap', 'fleet graphics', 'wraps']):
                industry_guess = 'Vehicle wraps'
            elif any(k in keyword_lower for k in ['signage', 'sign shop', 'wayfinding', 'sign systems']):
                industry_guess = 'Commercial signage'
            elif any(k in keyword_lower for k in ['industrial graphics', 'decals', 'labels', 'nameplates']):
                industry_guess = 'Industrial graphics'
            elif any(k in keyword_lower for k in ['graphics', 'printing', 'display']):
                industry_guess = 'Signage/Graphics'
        
        pipeline_result = {
            'company_name': company_name,
            'domain': domain,
            'company_blurb': company_blurb,
            'source_url': source_url,
            'industry_guess': industry_guess,
            'fit_bucket': fit_bucket,
            'score': score,
            'fit_yes_no': fit_yes_no,  # Backward compatibility
            'evidence_snippet': evidence_snippet[:120]  # Ensure max 120 chars
        }
        
        results.append(pipeline_result)
        
        # Small delay to be respectful
        time.sleep(REQUEST_DELAY)
    
    # Write output CSV
    print(f"Writing results to {output_csv}...")
    
    output_columns = [
        'company_name', 'domain', 'company_blurb', 'source_url', 'industry_guess',
        'fit_bucket', 'score', 'evidence_snippet', 'fit_yes_no'
    ]
    
    try:
        os.makedirs(os.path.dirname(output_csv) if os.path.dirname(output_csv) else '.', exist_ok=True)
        with open(output_csv, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=output_columns)
            writer.writeheader()
            writer.writerows(results)
    except Exception as e:
        raise Exception(f"Error writing {output_csv}: {e}")
    
    # Calculate summary
    yes_count = sum(1 for r in results if r['fit_bucket'] == 'YES')
    maybe_count = sum(1 for r in results if r['fit_bucket'] == 'MAYBE')
    no_count = sum(1 for r in results if r['fit_bucket'] == 'NO')
    
    print(f"Classification complete: {yes_count} YES, {maybe_count} MAYBE, {no_count} NO")
    
    # Debug output
    if debug:
        print("\n" + "=" * 60)
        print("DEBUG: Top 20 MAYBE companies (by score):")
        print("=" * 60)
        maybe_results = sorted([r for r in results if r['fit_bucket'] == 'MAYBE'], 
                              key=lambda x: x.get('score', 0), reverse=True)[:20]
        for r in maybe_results:
            print(f"  [{r.get('score', 0)}] {r.get('company_name', 'Unknown')}: {r.get('evidence_snippet', '')}")
        
        print("\n" + "=" * 60)
        print("DEBUG: Top 20 NO companies (highest scores - borderline):")
        print("=" * 60)
        no_results = sorted([r for r in results if r['fit_bucket'] == 'NO'], 
                           key=lambda x: x.get('score', 0), reverse=True)[:20]
        for r in no_results:
            print(f"  [{r.get('score', 0)}] {r.get('company_name', 'Unknown')}: {r.get('evidence_snippet', '')}")
    
    return {
        'total': len(results),
        'yes': yes_count,
        'maybe': maybe_count,
        'no': no_count
    }


def main():
    """Main execution function."""
    input_file = 'test_enriched.csv'
    output_file = 'industry_filter_output.csv'
    
    try:
        classify(input_file, output_file)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
