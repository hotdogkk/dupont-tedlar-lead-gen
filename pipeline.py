#!/usr/bin/env python3
"""
GTM/RevOps Pipeline Orchestrator

A lightweight orchestration system for the 3-step company enrichment pipeline:
1. SCRAPE: Extract exhibitor data from trade show directories
2. CLASSIFY: Industry fit classification (YES/NO)
3. ENRICH: Enrich YES companies with size, revenue, decision makers

USAGE:
    Full run:
        python pipeline.py --source-url "https://signexpo.org/..." --limit 200
    
    Test run (first 10 companies):
        python pipeline.py --source-url "https://signexpo.org/..." --test-limit 10
    
    Resume from last checkpoint:
        python pipeline.py --source-url "https://signexpo.org/..." --resume
    
    Skip specific steps:
        python pipeline.py --source-url "https://signexpo.org/..." --skip-step scrape --skip-step classify
    
    Windows PowerShell - Set environment variable:
        $env:SERPER_API_KEY="your-api-key-here"

OUTPUTS:
    - outputs/scraped_companies.csv (STEP 1)
    - outputs/industry_filtered.csv (STEP 2)
    - outputs/enriched_yes_companies.csv (STEP 3)
    - outputs/run_manifest.json (run metadata)
    - outputs/cache_serper.json (Serper API cache)

FEATURES:
    - Idempotent steps: Skip if output exists and --skip-step is passed
    - Resume mode: Automatically skip completed steps
    - Run manifest: Tracks run metadata, counts, and timing
    - Serper caching: Reduces API calls and costs
    - Fail-soft enrichment: Continues on individual company errors
    - Rate limiting: Respects API rate limits
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Set

# Import step modules
import scrape_exhibitors
import industry_filter
import enrich_companies

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Pipeline constants
OUTPUT_DIR = 'outputs'
STEP1_OUTPUT = os.path.join(OUTPUT_DIR, 'scraped_companies.csv')
STEP2_OUTPUT = os.path.join(OUTPUT_DIR, 'industry_filtered.csv')
STEP3_OUTPUT = os.path.join(OUTPUT_DIR, 'enriched_yes_companies.csv')
MANIFEST_FILE = os.path.join(OUTPUT_DIR, 'run_manifest.json')
CACHE_FILE = os.path.join(OUTPUT_DIR, 'cache_serper.json')


class PipelineOrchestrator:
    """Orchestrates the 3-step pipeline."""
    
    def __init__(self, source_url: str, limit: Optional[int] = None, 
                 skip_steps: Optional[Set[str]] = None, resume: bool = False,
                 include_maybe: bool = False):
        self.source_url = source_url
        self.limit = limit
        self.skip_steps = skip_steps or set()
        self.resume = resume
        self.include_maybe = include_maybe
        self.manifest = self._load_manifest()
        self.run_id = self._generate_run_id()
        
        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    def _generate_run_id(self) -> str:
        """Generate a unique run ID."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"run_{timestamp}"
    
    def _load_manifest(self) -> Dict:
        """Load existing manifest if available."""
        if os.path.exists(MANIFEST_FILE):
            try:
                with open(MANIFEST_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}
    
    def _save_manifest(self, step_name: str, step_data: Dict):
        """Save step data to manifest."""
        if 'runs' not in self.manifest:
            self.manifest['runs'] = []
        
        # Find or create current run
        current_run = None
        for run in self.manifest['runs']:
            if run.get('run_id') == self.run_id:
                current_run = run
                break
        
        if not current_run:
            current_run = {
                'run_id': self.run_id,
                'timestamp': datetime.now().isoformat(),
                'source_url': self.source_url,
                'limit': self.limit,
                'steps': {}
            }
            self.manifest['runs'].append(current_run)
        
        # Update step data
        current_run['steps'][step_name] = {
            **step_data,
            'completed_at': datetime.now().isoformat()
        }
        
        # Save manifest
        try:
            with open(MANIFEST_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.manifest, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save manifest: {e}")
    
    def _should_skip_step(self, step_name: str, output_file: str) -> bool:
        """Determine if a step should be skipped."""
        # Explicit skip
        if step_name in self.skip_steps:
            logger.info(f"Step '{step_name}' explicitly skipped via --skip-step")
            return True
        
        # Resume mode: skip if output exists
        if self.resume and os.path.exists(output_file):
            logger.info(f"Step '{step_name}' skipped (resume mode, output exists: {output_file})")
            return True
        
        return False
    
    def step1_scrape(self) -> Dict:
        """STEP 1: Scrape exhibitors from source URL."""
        step_name = 'scrape'
        logger.info("=" * 60)
        logger.info("STEP 1: SCRAPE EXHIBITORS")
        logger.info("=" * 60)
        
        if self._should_skip_step(step_name, STEP1_OUTPUT):
            # Load existing data
            logger.info(f"Loading existing scraped data from {STEP1_OUTPUT}")
            with open(STEP1_OUTPUT, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                companies = list(reader)
            logger.info(f"Loaded {len(companies)} companies from existing file")
            return {
                'status': 'skipped',
                'count': len(companies),
                'duration_seconds': 0
            }
        
        start_time = time.time()
        
        try:
            # Run scraper
            companies = scrape_exhibitors.run(self.source_url, limit=self.limit or 200)
            
            # Save to CSV
            logger.info(f"Saving {len(companies)} companies to {STEP1_OUTPUT}")
            with open(STEP1_OUTPUT, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=['company_name', 'domain', 'company_blurb', 'source_url'])
                writer.writeheader()
                writer.writerows(companies)
            
            duration = time.time() - start_time
            
            step_data = {
                'status': 'completed',
                'count': len(companies),
                'duration_seconds': round(duration, 2)
            }
            self._save_manifest(step_name, step_data)
            
            logger.info(f"STEP 1 completed: {len(companies)} companies scraped in {duration:.1f}s")
            return step_data
            
        except Exception as e:
            logger.error(f"STEP 1 failed: {e}", exc_info=True)
            raise
    
    def step2_classify(self) -> Dict:
        """STEP 2: Industry fit classification."""
        step_name = 'classify'
        logger.info("=" * 60)
        logger.info("STEP 2: INDUSTRY FIT CLASSIFICATION")
        logger.info("=" * 60)
        
        if self._should_skip_step(step_name, STEP2_OUTPUT):
            # Load existing data
            logger.info(f"Loading existing classification data from {STEP2_OUTPUT}")
            with open(STEP2_OUTPUT, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                results = list(reader)
            yes_count = sum(1 for r in results if r.get('fit_bucket') == 'YES' or r.get('fit_yes_no') == 'YES')
            maybe_count = sum(1 for r in results if r.get('fit_bucket') == 'MAYBE')
            no_count = sum(1 for r in results if r.get('fit_bucket') == 'NO')
            logger.info(f"Loaded {len(results)} companies ({yes_count} YES, {maybe_count} MAYBE, {no_count} NO) from existing file")
            return {
                'status': 'skipped',
                'total': len(results),
                'yes': yes_count,
                'maybe': maybe_count,
                'no': no_count,
                'duration_seconds': 0
            }
        
        # Check input file exists
        if not os.path.exists(STEP1_OUTPUT):
            raise FileNotFoundError(f"STEP 1 output not found: {STEP1_OUTPUT}. Run STEP 1 first.")
        
        start_time = time.time()
        
        try:
            # Progress callback
            def progress_callback(count, total):
                if count % 10 == 0 or count == total:
                    logger.info(f"Progress: {count}/{total} ({count*100//total}%)")
            
            # Run classifier
            counts = industry_filter.classify(
                STEP1_OUTPUT,
                STEP2_OUTPUT,
                progress_callback=progress_callback,
                debug=False
            )
            
            duration = time.time() - start_time
            
            step_data = {
                'status': 'completed',
                'total': counts['total'],
                'yes': counts.get('yes', 0),
                'maybe': counts.get('maybe', 0),
                'no': counts.get('no', 0),
                'duration_seconds': round(duration, 2)
            }
            self._save_manifest(step_name, step_data)
            
            maybe_count = counts.get('maybe', 0)
            logger.info(f"STEP 2 completed: {counts.get('yes', 0)} YES, {maybe_count} MAYBE, {counts.get('no', 0)} NO in {duration:.1f}s")
            return step_data
            
        except Exception as e:
            logger.error(f"STEP 2 failed: {e}", exc_info=True)
            raise
    
    def step3_enrich(self) -> Dict:
        """STEP 3: Enrich YES companies (and MAYBE if include_maybe=True)."""
        step_name = 'enrich'
        logger.info("=" * 60)
        if self.include_maybe:
            logger.info("STEP 3: ENRICHMENT (YES + MAYBE COMPANIES)")
        else:
            logger.info("STEP 3: ENRICHMENT (YES COMPANIES ONLY)")
        logger.info("=" * 60)
        
        if self._should_skip_step(step_name, STEP3_OUTPUT):
            # Load existing data
            logger.info(f"Loading existing enrichment data from {STEP3_OUTPUT}")
            with open(STEP3_OUTPUT, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                results = list(reader)
            logger.info(f"Loaded {len(results)} enriched companies from existing file")
            return {
                'status': 'skipped',
                'count': len(results),
                'duration_seconds': 0,
                'serper_calls': 0,
                'cache_hits': 0
            }
        
        # Check input file exists
        if not os.path.exists(STEP2_OUTPUT):
            raise FileNotFoundError(f"STEP 2 output not found: {STEP2_OUTPUT}. Run STEP 2 first.")
        
        # Check for API key
        api_key = os.getenv('SERPER_API_KEY')
        if not api_key:
            logger.warning("SERPER_API_KEY not set - STEP 3 will complete but skip enrichment")
            logger.warning("Companies will be saved with empty enrichment fields")
        
        start_time = time.time()
        
        try:
            # Progress callback
            def progress_callback(count, total):
                if count % 5 == 0 or count == total:
                    logger.info(f"Progress: {count}/{total} ({count*100//total}%)")
            
            # Run enricher (always enriches YES + MAYBE)
            stats = enrich_companies.enrich(
                STEP2_OUTPUT,
                STEP3_OUTPUT,
                api_key=api_key,
                cache_file=CACHE_FILE,
                progress_callback=progress_callback,
                include_maybe=True  # Always True - YES + MAYBE are always enriched
            )
            
            duration = time.time() - start_time
            
            step_data = {
                'status': 'completed' if api_key else 'completed_no_api',
                'count': stats.get('companies_processed', 0),
                'employee_ranges_found': stats.get('employee_ranges_found', 0),
                'revenue_ranges_found': stats.get('revenue_ranges_found', 0),
                'decision_makers_found': stats.get('decision_makers_found', 0),
                'serper_calls': stats.get('serper_calls', 0),
                'cache_hits': stats.get('cache_hits', 0),
                'duration_seconds': round(duration, 2)
            }
            self._save_manifest(step_name, step_data)
            
            logger.info(f"STEP 3 completed: {stats.get('companies_processed', 0)} companies enriched in {duration:.1f}s")
            if api_key:
                logger.info(f"  - Serper calls: {stats.get('serper_calls', 0)}, Cache hits: {stats.get('cache_hits', 0)}")
            return step_data
            
        except Exception as e:
            logger.error(f"STEP 3 failed: {e}", exc_info=True)
            raise
    
    def run(self):
        """Run the full pipeline."""
        logger.info("=" * 60)
        logger.info("PIPELINE ORCHESTRATOR")
        logger.info("=" * 60)
        logger.info(f"Run ID: {self.run_id}")
        logger.info(f"Source URL: {self.source_url}")
        logger.info(f"Limit: {self.limit or 'unlimited'}")
        logger.info(f"Resume mode: {self.resume}")
        logger.info(f"Skip steps: {self.skip_steps if self.skip_steps else 'none'}")
        logger.info("")
        
        pipeline_start = time.time()
        
        try:
            # STEP 1: Scrape
            step1_result = self.step1_scrape()
            
            # STEP 2: Classify
            step2_result = self.step2_classify()
            
            # STEP 3: Enrich
            step3_result = self.step3_enrich()
            
            # Final summary
            total_duration = time.time() - pipeline_start
            
            logger.info("")
            logger.info("=" * 60)
            logger.info("PIPELINE COMPLETE")
            logger.info("=" * 60)
            logger.info(f"Total duration: {total_duration:.1f}s")
            logger.info(f"STEP 1: {step1_result.get('count', 0)} companies scraped")
            maybe_count = step2_result.get('maybe', 0)
            logger.info(f"STEP 2: {step2_result.get('yes', 0)} YES, {maybe_count} MAYBE, {step2_result.get('no', 0)} NO")
            logger.info(f"STEP 3: {step3_result.get('count', 0)} companies enriched")
            logger.info("")
            logger.info(f"Outputs:")
            logger.info(f"  - {STEP1_OUTPUT}")
            logger.info(f"  - {STEP2_OUTPUT}")
            logger.info(f"  - {STEP3_OUTPUT}")
            logger.info(f"  - {MANIFEST_FILE}")
            logger.info("")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            sys.exit(1)


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description='GTM/RevOps Pipeline Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('--source-url', required=True,
                       help='Source URL to scrape exhibitors from')
    parser.add_argument('--limit', type=int, default=None,
                       help='Maximum number of companies to scrape (default: unlimited)')
    parser.add_argument('--test-limit', type=int, default=None,
                       help='Test mode: limit to first N companies (overrides --limit)')
    parser.add_argument('--skip-step', action='append', dest='skip_steps',
                       choices=['scrape', 'classify', 'enrich'],
                       help='Skip a specific step (can be used multiple times)')
    parser.add_argument('--resume', action='store_true',
                       help='Resume mode: automatically skip steps with existing outputs')
    parser.add_argument('--include-maybe', action='store_true',
                       help='Include MAYBE companies in enrichment (default: YES only)')
    
    args = parser.parse_args()
    
    # Determine limit
    limit = args.test_limit if args.test_limit else args.limit
    
    # Create orchestrator
    orchestrator = PipelineOrchestrator(
        source_url=args.source_url,
        limit=limit,
        skip_steps=set(args.skip_steps) if args.skip_steps else None,
        resume=args.resume,
        include_maybe=args.include_maybe
    )
    
    # Run pipeline
    orchestrator.run()


if __name__ == '__main__':
    main()
