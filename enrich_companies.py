"""
Company Enrichment Script using Serper API

This script enriches company data from a CSV file with:
- Employee range (from LinkedIn company pages)
- Revenue range (from public sources)
- Key decision maker information (from LinkedIn profiles)

Usage:
    1. Set SERPER_API_KEY environment variable:
       export SERPER_API_KEY="your-api-key-here"
       (Windows: set SERPER_API_KEY=your-api-key-here)
    
    2. Run the script:
       python enrich_companies.py

Requirements:
    - requests
    - pandas
    - SERPER_API_KEY environment variable
"""

import os
import csv
import json
import re
import time
import logging
from typing import Optional, Dict, List, Tuple
import requests
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Serper API endpoint
SERPER_API_BASE = 'https://google.serper.dev/search'

# Employee range patterns
EMPLOYEE_RANGES = [
    '5000+',
    '1001–5000',
    '501–1000',
    '201–500',
    '51–200',
    '11–50',
    '1–10'
]

# Revenue range patterns
REVENUE_RANGES = [
    '$100M+',
    '$30–100M',
    '$10–30M',
    '<$10M'
]

# Decision maker title keywords (in priority order)
DECISION_MAKER_TITLES = [
    'Director of Product',
    'Head of Product',
    'R&D Director',
    'Director of R&D',
    'Materials Manager',
    'Engineering Director',
    'Innovation Director',
    'Procurement Director',
    'Strategic Sourcing Director'
]


class CompanyEnricher:
    """Enriches company data using Serper API."""
    
    def __init__(self, api_key: Optional[str] = None, cache_file: Optional[str] = None):
        self.api_key = api_key
        self.cache_file = cache_file or 'outputs/cache_serper.json'
        self.cache = self._load_cache()
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({
                'X-API-KEY': api_key,
                'Content-Type': 'application/json'
            })
        self.stats = {
            'companies_processed': 0,
            'employee_ranges_found': 0,
            'revenue_ranges_found': 0,
            'decision_makers_found': 0,
            'serper_calls': 0,
            'cache_hits': 0
        }
        self.rate_limit_delay = 0.5  # Seconds between API calls
        self.max_retries = 3
    
    def _load_cache(self) -> Dict:
        """Load cache from file."""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}
    
    def _save_cache(self):
        """Save cache to file."""
        os.makedirs(os.path.dirname(self.cache_file) if os.path.dirname(self.cache_file) else '.', exist_ok=True)
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")
    
    def _get_cache_key(self, query: str) -> str:
        """Generate cache key from query."""
        return query.lower().strip()
    
    def _get_from_cache(self, query: str) -> Optional[Dict]:
        """Get result from cache."""
        key = self._get_cache_key(query)
        if key in self.cache:
            self.stats['cache_hits'] += 1
            return self.cache[key]
        return None
    
    def _save_to_cache(self, query: str, result: Dict):
        """Save result to cache."""
        key = self._get_cache_key(query)
        self.cache[key] = result
    
    def search_serper(self, query: str, num_results: int = 10) -> Optional[Dict]:
        """Search using Serper API with caching and retries."""
        # Check cache first
        cached = self._get_from_cache(query)
        if cached:
            return cached
        
        if not self.api_key:
            return None
        
        # Rate limiting
        time.sleep(self.rate_limit_delay)
        
        # Retry logic
        for attempt in range(self.max_retries):
            try:
                payload = {
                    'q': query,
                    'num': num_results
                }
                response = self.session.post(SERPER_API_BASE, json=payload, timeout=15)
                response.raise_for_status()
                result = response.json()
                
                # Save to cache
                self._save_to_cache(query, result)
                self.stats['serper_calls'] += 1
                return result
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = (attempt + 1) * 2  # Exponential backoff
                    logger.debug(f"Serper search failed (attempt {attempt + 1}/{self.max_retries}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.debug(f"Serper search failed for query '{query}': {e}")
                    return None
            except Exception as e:
                logger.debug(f"Unexpected error in Serper search for query '{query}': {e}")
                return None
        
        return None
    
    def normalize_employee_range(self, text: str) -> Optional[str]:
        """
        Normalize employee count text into standard ranges.
        
        Returns one of: 1–10, 11–50, 51–200, 201–500, 501–1000, 1001–5000, 5000+
        """
        if not text:
            return None
        
        text_lower = text.lower()
        
        # Extract numbers from text
        numbers = re.findall(r'\d+', text.replace(',', ''))
        if not numbers:
            return None
        
        # Try to find range patterns
        # Pattern: "X-Y employees" or "X to Y employees"
        range_match = re.search(r'(\d+)[\s\-–—to]+(\d+)\s*(?:employees?|staff|people|workers)', text_lower)
        if range_match:
            min_val = int(range_match.group(1))
            max_val = int(range_match.group(2))
            # Determine range
            if max_val <= 10:
                return '1–10'
            elif max_val <= 50:
                return '11–50'
            elif max_val <= 200:
                return '51–200'
            elif max_val <= 500:
                return '201–500'
            elif max_val <= 1000:
                return '501–1000'
            elif max_val <= 5000:
                return '1001–5000'
            else:
                return '5000+'
        
        # Pattern: "X+ employees" or "over X employees"
        plus_match = re.search(r'(?:over|more than|above|at least)\s*(\d+)\s*(?:employees?|staff|people)', text_lower)
        if plus_match:
            val = int(plus_match.group(1))
            if val >= 5000:
                return '5000+'
            elif val >= 1000:
                return '1001–5000'
            elif val >= 500:
                return '501–1000'
            elif val >= 200:
                return '201–500'
            elif val >= 50:
                return '51–200'
            elif val >= 10:
                return '11–50'
            else:
                return '1–10'
        
        # Pattern: "X employees" (single number)
        single_match = re.search(r'(\d+)\s*(?:employees?|staff|people|workers)', text_lower)
        if single_match:
            val = int(single_match.group(1))
            if val >= 5000:
                return '5000+'
            elif val >= 1000:
                return '1001–5000'
            elif val >= 500:
                return '501–1000'
            elif val >= 200:
                return '201–500'
            elif val >= 50:
                return '51–200'
            elif val >= 10:
                return '11–50'
            else:
                return '1–10'
        
        return None
    
    def get_employee_range_with_confidence(self, company_name: str, company_domain: Optional[str] = None) -> Tuple[Optional[str], Optional[str], float]:
        """
        Get employee range using multi-pass query strategy.
        Returns: (employee_range, source_url, confidence)
        """
        if not self.api_key:
            return (None, None, 0.0)
        
        domain_part = f" {company_domain}" if company_domain else ""
        
        # Multi-pass queries (stop when confident)
        queries = [
            f'site:linkedin.com/company "{company_name}"{domain_part}',
            f'"{company_name}"{domain_part} about team employees',
            f'"{company_name}"{domain_part} revenue employees',
            f'"{company_name}"{domain_part} company size'
        ]
        
        for query in queries:
            results = self.search_serper(query, num_results=5)
            
            if not results or 'organic' not in results:
                continue
            
            for item in results.get('organic', []):
                snippet = item.get('snippet', '')
                title = item.get('title', '')
                link = item.get('link', '')
                combined_text = f"{title} {snippet}".lower()
                
                # Prefer results containing domain
                confidence = 0.6
                if company_domain and company_domain.lower() in combined_text:
                    confidence = 0.9
                elif 'linkedin.com/company' in link.lower():
                    confidence = 0.8
                
                # Look for employee count information
                if 'employee' in combined_text or 'staff' in combined_text or 'team' in combined_text:
                    employee_range = self.normalize_employee_range(f"{title} {snippet}")
                    if employee_range and confidence >= 0.6:
                        return (employee_range, link, confidence)
            
            # Small delay between queries
            time.sleep(self.rate_limit_delay)
        
        return (None, None, 0.0)
    
    def normalize_revenue_range(self, text: str) -> Optional[str]:
        """
        Normalize revenue text into standard ranges.
        
        Returns one of: <$10M, $10–30M, $30–100M, $100M+
        """
        if not text:
            return None
        
        text_lower = text.lower()
        
        # Extract revenue numbers (in millions)
        # Pattern: "$X million" or "$XM" or "$X M"
        revenue_match = re.search(r'\$?\s*(\d+(?:\.\d+)?)\s*(?:million|m\b|m\s)', text_lower)
        if revenue_match:
            val = float(revenue_match.group(1))
            
            if val >= 100:
                return '$100M+'
            elif val >= 30:
                return '$30–100M'
            elif val >= 10:
                return '$10–30M'
            else:
                return '<$10M'
        
        # Pattern: "under $X million" or "less than $X million"
        under_match = re.search(r'(?:under|less than|below)\s*\$?\s*(\d+(?:\.\d+)?)\s*(?:million|m\b)', text_lower)
        if under_match:
            val = float(under_match.group(1))
            if val <= 10:
                return '<$10M'
        
        return None
    
    def get_revenue_range_with_confidence(self, company_name: str, company_domain: Optional[str] = None) -> Tuple[Optional[str], Optional[str], float]:
        """
        Get revenue range using multi-pass query strategy.
        Returns: (revenue_range, source_url, confidence)
        """
        if not self.api_key:
            return (None, None, 0.0)
        
        domain_part = f" {company_domain}" if company_domain else ""
        
        # Multi-pass queries (stop when confident)
        queries = [
            f'"{company_name}"{domain_part} annual revenue',
            f'"{company_name}"{domain_part} revenue',
            f'"{company_name}"{domain_part} company revenue',
            f'"{company_name}"{domain_part} revenue employees'
        ]
        
        if company_domain:
            queries.insert(0, f'site:{company_domain} revenue')
        
        for query in queries:
            results = self.search_serper(query, num_results=5)
            
            if not results or 'organic' not in results:
                continue
            
            for item in results.get('organic', []):
                snippet = item.get('snippet', '')
                title = item.get('title', '')
                link = item.get('link', '')
                combined_text = f"{title} {snippet}".lower()
                
                # Prefer results containing domain
                confidence = 0.6
                if company_domain and company_domain.lower() in combined_text:
                    confidence = 0.9
                
                # Look for revenue information
                if 'revenue' in combined_text or 'million' in combined_text:
                    revenue_range = self.normalize_revenue_range(f"{title} {snippet}")
                    if revenue_range and confidence >= 0.6:
                        return (revenue_range, link, confidence)
            
            # Small delay between queries
            time.sleep(self.rate_limit_delay)
        
        return (None, None, 0.0)
    
    def extract_linkedin_profile(self, item: Dict) -> Optional[Tuple[str, str, str]]:
        """
        Extract LinkedIn profile information from search result.
        
        Returns: (name, title, linkedin_url) or None
        """
        title = item.get('title', '')
        snippet = item.get('snippet', '')
        link = item.get('link', '')
        
        # Must be a LinkedIn profile URL
        if 'linkedin.com/in/' not in link.lower():
            return None
        
        # Extract name from title (usually "Name | Title | LinkedIn")
        name_match = re.search(r'^([^|]+)', title)
        if not name_match:
            # Try to extract from snippet
            name_match = re.search(r'([A-Z][a-z]+ [A-Z][a-z]+)', snippet)
        
        if not name_match:
            return None
        
        name = name_match.group(1).strip()
        
        # Extract title from title or snippet
        job_title = None
        title_text = f"{title} {snippet}".lower()
        
        for keyword in DECISION_MAKER_TITLES:
            if keyword.lower() in title_text:
                # Try to get the full title from the text
                title_pattern = re.compile(re.escape(keyword), re.IGNORECASE)
                if title_pattern.search(title):
                    # Extract title from title field
                    title_match = re.search(r'\|([^|]+)\|', title)
                    if title_match:
                        job_title = title_match.group(1).strip()
                    else:
                        job_title = keyword
                elif title_pattern.search(snippet):
                    # Extract from snippet
                    context_match = re.search(rf'([^.]*{re.escape(keyword)}[^.]*)', snippet, re.IGNORECASE)
                    if context_match:
                        job_title = context_match.group(1).strip()
                    else:
                        job_title = keyword
                break
        
        if not job_title:
            return None
        
        return (name, job_title, link)
    
    def get_decision_makers(self, company_name: str) -> List[Dict]:
        """
        Get key decision makers from LinkedIn profiles.
        
        Returns: List of dicts with keys: name, title, linkedin_url, email_if_public, source
        """
        decision_makers = []
        
        if not self.api_key:
            return decision_makers
        
        # Search for decision makers with priority titles
        for title_keyword in DECISION_MAKER_TITLES[:3]:  # Limit to top 3 titles to save API calls
            query = f'"{company_name}" "{title_keyword}" LinkedIn'
            results = self.search_serper(query, num_results=5)
            
            if not results or 'organic' not in results:
                continue
            
            # Check each result
            for item in results.get('organic', []):
                profile = self.extract_linkedin_profile(item)
                if profile:
                    name, job_title, linkedin_url = profile
                    # Verify it's actually for this company
                    snippet = item.get('snippet', '').lower()
                    title = item.get('title', '').lower()
                    company_lower = company_name.lower()
                    
                    if company_lower in snippet or company_lower in title:
                        # Check if we already have this person
                        if not any(dm['linkedin_url'] == linkedin_url for dm in decision_makers):
                            decision_makers.append({
                                'name': name,
                                'title': job_title,
                                'linkedin_url': linkedin_url,
                                'email_if_public': '',  # Serper doesn't provide emails
                                'source': 'serper_linkedin_search'
                            })
                            logger.debug(f"Found decision maker for {company_name}: {name} - {job_title}")
                            if len(decision_makers) >= 3:  # Limit to 3 decision makers
                                return decision_makers
            
            # Small delay between queries
            time.sleep(self.rate_limit_delay)
        
        return decision_makers
    
    def get_decision_makers_with_confidence(self, company_name: str, company_domain: Optional[str] = None) -> Tuple[List[Dict], Optional[str], float]:
        """
        Get decision makers using multi-pass query strategy.
        Returns: (decision_makers_list, source_url, confidence)
        """
        decision_makers = []
        best_confidence = 0.0
        best_source = None
        
        if not self.api_key:
            return ([], None, 0.0)
        
        domain_part = f" {company_domain}" if company_domain else ""
        
        # Search for decision makers with priority titles (up to 3)
        for title_keyword in DECISION_MAKER_TITLES[:3]:
            queries = [
                f'"{company_name}" "{title_keyword}" LinkedIn{domain_part}',
                f'"{company_name}" {title_keyword}{domain_part}',
            ]
            
            for query in queries:
                results = self.search_serper(query, num_results=5)
                
                if not results or 'organic' not in results:
                    continue
                
                for item in results.get('organic', []):
                    profile = self.extract_linkedin_profile(item)
                    if profile:
                        name, job_title, linkedin_url = profile
                        
                        # Verify it's actually for this company
                        snippet = item.get('snippet', '').lower()
                        title = item.get('title', '').lower()
                        link = item.get('link', '')
                        company_lower = company_name.lower()
                        
                        confidence = 0.6
                        if company_lower in snippet or company_lower in title:
                            confidence = 0.8
                        if company_domain and company_domain.lower() in (snippet + title):
                            confidence = 0.9
                        
                        # Check if we already have this person
                        if not any(dm['linkedin_url'] == linkedin_url for dm in decision_makers):
                            if confidence >= 0.6:
                                decision_makers.append({
                                    'name': name,
                                    'title': job_title,
                                    'linkedin_url': linkedin_url,
                                    'email_if_public': '',  # Serper doesn't provide emails
                                    'source': 'serper_linkedin_search'
                                })
                                best_confidence = max(best_confidence, confidence)
                                best_source = link
                                
                                if len(decision_makers) >= 3:  # Limit to 3
                                    return (decision_makers, best_source, best_confidence)
                
                time.sleep(self.rate_limit_delay)
        
        return (decision_makers, best_source, best_confidence) if decision_makers else ([], None, 0.0)
    
    def enrich_company(self, row: Dict) -> Dict:
        """Enrich a single company row."""
        company_name = str(row.get('company_name', '')).strip()
        
        # Handle domain - can be from 'domain', 'matched_domain', or 'company_domain'
        domain_val = row.get('domain', '') or row.get('matched_domain', '') or row.get('company_domain', '')
        if pd.notna(domain_val) and domain_val:
            company_domain = str(domain_val).strip()
            # Clean up domain
            company_domain = company_domain.replace('http://', '').replace('https://', '').replace('www.', '').split('/')[0]
        else:
            company_domain = ''
        
        if not company_name:
            return row
        
        logger.info(f"Processing: {company_name}")
        
        error_note = ''
        if not self.api_key:
            error_note = 'API key missing'
        
        # Get employee range with confidence
        employee_range, employee_source, employee_confidence = (None, None, 0.0)
        if self.api_key:
            employee_range, employee_source, employee_confidence = self.get_employee_range_with_confidence(
                company_name, company_domain
            )
            if employee_range:
                self.stats['employee_ranges_found'] += 1
            time.sleep(self.rate_limit_delay)
        
        # Get revenue range with confidence
        revenue_range, revenue_source, revenue_confidence = (None, None, 0.0)
        if self.api_key:
            revenue_range, revenue_source, revenue_confidence = self.get_revenue_range_with_confidence(
                company_name, company_domain
            )
            if revenue_range:
                self.stats['revenue_ranges_found'] += 1
            time.sleep(self.rate_limit_delay)
        
        # Get decision makers with confidence
        decision_makers, decision_makers_source, decision_makers_confidence = ([], None, 0.0)
        if self.api_key:
            decision_makers, decision_makers_source, decision_makers_confidence = self.get_decision_makers_with_confidence(
                company_name, company_domain
            )
            if decision_makers:
                self.stats['decision_makers_found'] += len(decision_makers)
        
        # Small delay between companies
        time.sleep(self.rate_limit_delay)
        
        # Build enriched row in pipeline format with detailed schema
        enriched_row = {
            'company_name': company_name,
            'domain': company_domain,
            'company_blurb': row.get('company_blurb', ''),
            'source_url': row.get('source_url', ''),
            'fit_bucket': row.get('fit_bucket', 'YES'),
            'industry_guess': row.get('industry_guess', ''),
            'score': row.get('score', '0'),
            'evidence_snippet': row.get('evidence_snippet', ''),
            'employee_range': employee_range or '',
            'employee_source': employee_source or '',
            'employee_confidence': f"{employee_confidence:.2f}" if employee_confidence > 0 else '',
            'revenue_range': revenue_range or '',
            'revenue_source': revenue_source or '',
            'revenue_confidence': f"{revenue_confidence:.2f}" if revenue_confidence > 0 else '',
            'decision_makers': json.dumps(decision_makers) if decision_makers else '[]',
            'decision_makers_source': decision_makers_source or '',
            'decision_makers_confidence': f"{decision_makers_confidence:.2f}" if decision_makers_confidence > 0 else '',
            'error_note': error_note
        }
        
        self.stats['companies_processed'] += 1
        return enriched_row
    
    def enrich_csv(self, input_file: str, output_file: str, progress_callback=None, include_maybe: bool = True):
        """
        Enrich all companies in the input CSV file.
        ALWAYS enriches YES + MAYBE companies (NO rows are dropped).
        """
        logger.info(f"Reading input file: {input_file}")
        
        # Read input CSV
        try:
            df = pd.read_csv(input_file)
        except Exception as e:
            raise Exception(f"Failed to read input file: {e}")
        
        total_rows_read = len(df)
        logger.info(f"DEBUG: Total rows read from CSV: {total_rows_read}")
        logger.info(f"DEBUG: CSV columns found: {list(df.columns)}")
        
        # ALWAYS filter to YES + MAYBE (drop NO rows)
        rows_yes = 0
        rows_maybe = 0
        rows_selected_for_enrich = 0
        
        if 'fit_bucket' in df.columns:
            rows_yes = len(df[df['fit_bucket'] == 'YES'])
            rows_maybe = len(df[df['fit_bucket'] == 'MAYBE'])
            df = df[df['fit_bucket'].isin(['YES', 'MAYBE'])].copy()
            rows_selected_for_enrich = len(df)
            logger.info(f"DEBUG: Rows with fit_bucket=YES: {rows_yes}")
            logger.info(f"DEBUG: Rows with fit_bucket=MAYBE: {rows_maybe}")
            logger.info(f"DEBUG: Selected for enrichment: {rows_selected_for_enrich}")
            
            # Debug: print first 3 selected rows
            if rows_selected_for_enrich > 0:
                logger.info("DEBUG: First 3 selected rows:")
                for idx, row in df.head(3).iterrows():
                    logger.info(f"  - {row.get('company_name', 'N/A')} | domain: {row.get('domain', 'N/A')} | fit_bucket: {row.get('fit_bucket', 'N/A')}")
        elif 'fit_yes_no' in df.columns:
            df = df[df['fit_yes_no'] == 'YES'].copy()
            rows_selected_for_enrich = len(df)
            rows_yes = rows_selected_for_enrich
            logger.info(f"DEBUG: Using fit_yes_no column (fallback)")
            logger.info(f"DEBUG: Rows with fit_yes_no=YES: {rows_yes}")
            logger.info(f"DEBUG: Selected for enrichment: {rows_selected_for_enrich}")
            
            # Debug: print first 3 selected rows
            if rows_selected_for_enrich > 0:
                logger.info("DEBUG: First 3 selected rows:")
                for idx, row in df.head(3).iterrows():
                    logger.info(f"  - {row.get('company_name', 'N/A')} | domain: {row.get('domain', 'N/A')} | fit_yes_no: {row.get('fit_yes_no', 'N/A')}")
        else:
            logger.warning("No fit_bucket or fit_yes_no column found - processing all companies")
            rows_selected_for_enrich = len(df)
        
        logger.info(f"Found {rows_selected_for_enrich} companies to enrich: {rows_yes} YES, {rows_maybe} MAYBE")
        
        if not self.api_key:
            logger.warning("SERPER_API_KEY not set - enrichment will be skipped")
            logger.warning("Writing output with empty enrichment fields and error_note='API key missing'")
            # Still write output with empty enrichment fields
            enriched_rows = []
            for idx, row in df.iterrows():
                company_name = str(row.get('company_name', ''))
                domain_val = row.get('domain', '') or row.get('matched_domain', '') or row.get('company_domain', '')
                company_domain = str(domain_val).strip() if pd.notna(domain_val) and domain_val else ''
                
                # Preserve fit_bucket if available, otherwise use fit_yes_no
                fit_bucket = row.get('fit_bucket', '')
                if not fit_bucket and 'fit_yes_no' in row:
                    fit_bucket = row.get('fit_yes_no', 'YES')
                
                enriched_rows.append({
                    'company_name': company_name,
                    'domain': company_domain,
                    'company_blurb': row.get('company_blurb', ''),
                    'source_url': row.get('source_url', ''),
                    'fit_bucket': fit_bucket,
                    'industry_guess': row.get('industry_guess', ''),
                    'score': row.get('score', '0'),
                    'evidence_snippet': row.get('evidence_snippet', ''),
                    'employee_range': '',
                    'employee_source': '',
                    'employee_confidence': '',
                    'revenue_range': '',
                    'revenue_source': '',
                    'revenue_confidence': '',
                    'decision_makers': '[]',
                    'decision_makers_source': '',
                    'decision_makers_confidence': '',
                    'error_note': 'API key missing'
                })
                # IMPORTANT: Count this as processed
                self.stats['companies_processed'] += 1
        else:
            # Process each company
            enriched_rows = []
            total = len(df)
            for idx, row in df.iterrows():
                company_name = str(row.get('company_name', 'Unknown'))
                
                if progress_callback:
                    progress_callback(idx + 1, total)
                else:
                    logger.info(f"[{idx + 1}/{total}] Processing: {company_name}")
                
                try:
                    enriched_row = self.enrich_company(row.to_dict())
                    enriched_rows.append(enriched_row)
                except Exception as e:
                    logger.error(f"Error processing {company_name}: {e}")
                    # Add row with empty enrichment fields (fail-soft)
                    domain_val = row.get('domain', '') or row.get('matched_domain', '') or row.get('company_domain', '')
                    company_domain = str(domain_val).strip() if pd.notna(domain_val) and domain_val else ''
                    enriched_rows.append({
                        'company_name': company_name,
                        'domain': company_domain,
                        'company_blurb': row.get('company_blurb', ''),
                        'source_url': row.get('source_url', ''),
                        'fit_bucket': row.get('fit_bucket', 'YES'),
                        'industry_guess': row.get('industry_guess', ''),
                        'score': row.get('score', '0'),
                        'evidence_snippet': row.get('evidence_snippet', ''),
                        'employee_range': '',
                        'employee_source': '',
                        'employee_confidence': '',
                        'revenue_range': '',
                        'revenue_source': '',
                        'revenue_confidence': '',
                        'decision_makers': '[]',
                        'decision_makers_source': '',
                        'decision_makers_confidence': '',
                        'error_note': 'API key missing' if not self.api_key else ''
                    })
        
        # Save cache
        self._save_cache()
        
        # Write output CSV
        logger.info(f"Writing output file: {output_file}")
        os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else '.', exist_ok=True)
        output_df = pd.DataFrame(enriched_rows)
        output_df.to_csv(output_file, index=False)
        
        # Ensure stats are complete
        final_stats = {
            'companies_processed': self.stats['companies_processed'],
            'serper_calls': self.stats.get('serper_calls', 0),
            'cache_hits': self.stats.get('cache_hits', 0),
            'employee_ranges_found': self.stats.get('employee_ranges_found', 0),
            'revenue_ranges_found': self.stats.get('revenue_ranges_found', 0),
            'decision_makers_found': self.stats.get('decision_makers_found', 0)
        }
        
        # Print statistics
        logger.info("\n" + "="*50)
        logger.info("ENRICHMENT STATISTICS")
        logger.info("="*50)
        logger.info(f"Companies processed: {final_stats['companies_processed']}")
        logger.info(f"Employee ranges found: {final_stats['employee_ranges_found']}")
        logger.info(f"Revenue ranges found: {final_stats['revenue_ranges_found']}")
        logger.info(f"Decision makers identified: {final_stats['decision_makers_found']}")
        if self.api_key:
            logger.info(f"Serper API calls: {final_stats['serper_calls']}")
            logger.info(f"Cache hits: {final_stats['cache_hits']}")
        logger.info("="*50)
        
        return final_stats


def enrich(input_csv: str, output_csv: str, api_key: Optional[str] = None, 
           cache_file: Optional[str] = None, progress_callback=None, include_maybe: bool = True) -> Dict:
    """
    Enrich companies from input CSV and write to output CSV.
    
    Args:
        input_csv: Path to input CSV (should have fit_bucket or fit_yes_no column)
        output_csv: Path to output CSV
        api_key: Optional Serper API key (if None, reads from SERPER_API_KEY env var)
        cache_file: Optional path to cache file (default: outputs/cache_serper.json)
        progress_callback: Optional callback function(count, total) for progress updates
        include_maybe: If True, enrich YES + MAYBE companies; otherwise YES only
        
    Returns:
        Dictionary with enrichment statistics
    """
    if api_key is None:
        api_key = os.getenv('SERPER_API_KEY')
    
    enricher = CompanyEnricher(api_key=api_key, cache_file=cache_file)
    stats = enricher.enrich_csv(input_csv, output_csv, progress_callback=progress_callback, include_maybe=include_maybe)
    return stats


def main():
    """Main function."""
    # Get API key from environment
    api_key = os.getenv('SERPER_API_KEY')
    
    # File paths
    input_file = 'industry_filter_test_output.csv'
    output_file = 'enriched_output.csv'
    
    # Check if input file exists
    if not os.path.exists(input_file):
        logger.error(f"Input file not found: {input_file}")
        return
    
    # Create enricher and process
    enrich(input_file, output_file, api_key=api_key)
    
    logger.info(f"\nEnrichment complete! Output saved to: {output_file}")


if __name__ == '__main__':
    main()
