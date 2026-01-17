# GTM/RevOps Pipeline Orchestrator

Business Context -- This system is designed for DuPont Tedlar’s Graphics & Signage GTM team
to systematically identify and qualify high-fit prospects from industry events. By automating exhibitor scraping, ICP-based industry filtering, and company enrichment,
the pipeline reduces manual research time and produces outreach-ready lead lists that can be routed into CRM, LinkedIn automation, or outbound sales workflows.

A lightweight orchestration system for the 3-step company enrichment pipeline:
1. **SCRAPE**: Extract exhibitor data from trade show directories
2. **CLASSIFY**: Industry fit classification (YES/MAYBE/NO) using keyword scoring
3. **ENRICH**: Enrich YES + MAYBE companies with size, revenue, decision makers

## Quick Start

### Prerequisites

- Python 3.8+
- Dependencies: `pip install -r requirements.txt`
- Serper API key (for enrichment step)

### Basic Usage

```bash
# Set API key (Windows PowerShell)
$env:SERPER_API_KEY="your-api-key-here"

# Full pipeline run
python pipeline.py --source-url "https://signexpo.org/..." --limit 200

# Test run (10 companies)
python pipeline.py --source-url "https://signexpo.org/..." --test-limit 10
```

## Pipeline Steps

### STEP 1 — SCRAPE
Extracts exhibitor data from trade show directories using HTML scraping (no API calls).

**Output:** `outputs/scraped_companies.csv`
- Columns: `company_name`, `domain`, `company_blurb`, `source_url`

### STEP 2 — CLASSIFY
Classifies companies using keyword scoring on local fields only (no API calls).

**Output:** `outputs/industry_filtered.csv`
- Columns: `company_name`, `domain`, `company_blurb`, `source_url`, `industry_guess`, `fit_bucket`, `score`, `evidence_snippet`, `fit_yes_no`
- Classification: YES (score ≥3), MAYBE (score 1-2), NO (score ≤0)

### STEP 3 — ENRICH
Enriches YES + MAYBE companies using Serper API.

**Output:** `outputs/enriched_yes_companies.csv`
- Includes: employee ranges, revenue ranges, decision makers with confidence scores

## Pipeline Features

- **Idempotent steps**: Skip if output exists + `--skip-step` flag
- **Resume mode**: Auto-skip completed steps when `--resume` is used
- **Run manifest**: Tracks all run metadata, counts, and timing (`outputs/run_manifest.json`)
- **Serper caching**: Reduces API calls and costs (`outputs/cache_serper.json`)
- **Fail-soft enrichment**: Continues on individual company errors
- **Rate limiting**: Built-in delays and retries for Serper API
- **Graceful API key handling**: Completes steps 1-2 even without API key

## Command-Line Options

```bash
python pipeline.py --source-url <URL> [OPTIONS]

Required:
  --source-url          Source URL to scrape exhibitors from

Optional:
  --limit N            Maximum number of companies to scrape
  --test-limit N       Test mode: limit to first N companies
  --skip-step STEP     Skip a specific step (scrape/classify/enrich)
  --resume             Resume mode: auto-skip steps with existing outputs
  --include-maybe      Include MAYBE companies in enrichment (default: always YES+MAYBE)
```

## Clean Outputs

The pipeline generates CSV and JSON files in the `outputs/` directory. To clean generated artifacts:

```bash
# Preview what would be deleted (safe)
python tools/clean_outputs.py --dry-run

# Clean but keep API cache
python tools/clean_outputs.py --keep-cache

# Clean everything
python tools/clean_outputs.py
```

**Important Notes:**
- After cleaning, the first run should **NOT** use `--resume` flag
- Use `--resume` only when output CSVs exist from a previous run
- The cleanup script only deletes generated artifacts, never source code
- Cache file (`cache_serper.json`) can be preserved with `--keep-cache` to avoid re-fetching API data

## Output Files

The pipeline generates these canonical files in `outputs/`:

- `scraped_companies.csv` - STEP 1 output
- `industry_filtered.csv` - STEP 2 output  
- `enriched_yes_companies.csv` - STEP 3 output (includes YES + MAYBE)
- `run_manifest.json` - Run metadata and statistics
- `cache_serper.json` - Serper API cache (optional, can be cleaned)

## Environment Variables

```bash
# Windows PowerShell
$env:SERPER_API_KEY="your-api-key-here"

# Linux/Mac
export SERPER_API_KEY="your-api-key-here"
```

## Project Structure

```
.
├── pipeline.py              # Main orchestrator
├── scrape_exhibitors.py     # STEP 1: Scraping
├── industry_filter.py        # STEP 2: Classification
├── enrich_companies.py       # STEP 3: Enrichment
├── requirements.txt         # Dependencies
├── tools/
│   └── clean_outputs.py    # Cleanup utility
├── outputs/                 # Generated artifacts (gitignored)
└── README.md               # This file
```

## License

Internal use only.
