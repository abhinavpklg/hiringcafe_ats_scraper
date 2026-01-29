# ATS Scraper

This is a two-phase scraping system for discovering and extracting job listings from Avature-hosted career sites.

## Overview

This project scrapes job data from Avature career pages using a seed file of known URLs. It discovers new job listings, extracts full details, and outputs structured data in CSV and JSON formats.

### Features

- Discovers job board endpoints from seed URLs
- Validates live endpoints with active job listings  
- Handles pagination automatically (`jobOffset` parameter)
- Extracts full job details including descriptions
- Deduplicates and cleans data
- Resume capability for long-running extractions
- Outputs both CSV and JSON formats

### Files
**1. Code (2 Python scripts)**
```
phase1_discovery.py — Discovers live Avature career sites and harvests job listings
phase2_extraction.py — Extracts full job details (description, location, date, etc.)
```

**2. Discovered Avature URLs**
```
live_endpoints.csv — 433 validated Avature career site endpoints
discovered_jobs.csv — 13,580 unique job URLs discovered
```
**3. Scraped Job Data**
```
jobs_full_details.csv — Complete dataset with all extracted fields
jobs_full_details.json — Same data in JSON format (includes HTML descriptions)
```

## Requirements

- Python 3.7+
- `requests` library

```bash
pip install requests
```

## Quick Start and Worflow

### 1. Prepare Seed File

Paste the seed file named `Urls.txt` containing Avature URLs

### 2. Run Phase 1: Discovery

```bash
python phase1_discovery.py
```

**What it does:**
- Parses seed URLs to extract domains and site paths
- Validates which endpoints have active job listings
- Harvests job URLs via pagination
- Cleans and deduplicates results
- Identifies NEW jobs not in the original seed file

**Runtime:** ~3-5 hours for 15,000+ site paths

**Output:**
| File | Description |
|------|-------------|
| `discovered_jobs.csv` | New job URLs ready for extraction |
| `live_endpoints.csv` | Active career site endpoints |


### 3. Run Phase 2: Extraction

```bash
python phase2_extraction.py
```

**What it does:**
- Visits each job URL from Phase 1
- Extracts full job details (description, location, date, etc.)
- Saves progress every 100 jobs (resumable)

**Runtime:** ~6 hours for 13,000+ jobs

**Output:**
| File | Description |
|------|-------------|
| `jobs_full_details.csv` | Complete dataset (text descriptions) |
| `jobs_full_details.json` | Complete dataset (includes HTML) |
| `extraction_progress.json` | Checkpoint file (for resume capability) |

## Output Schema

### jobs_full_details.csv

| Field | Description |
|-------|-------------|
| `title` | Job title |
| `url` | Job detail page URL |
| `apply_url` | Application URL |
| `location` | Job location (if available) |
| `date_posted` | Posting date (if available) |
| `department` | Department (if available) |
| `employment_type` | Full-time, Part-time, etc. |
| `description_text` | Clean text description |
| `source_domain` | Source Avature subdomain |
| `extraction_status` | success/error status |
| `extracted_at` | Extraction timestamp |

### jobs_full_details.json

Same fields as CSV, plus:
- `description_html` - Raw HTML description

## Configuration

Edit the configuration section at the top of each script:

### Phase 1 (phase1_discovery.py)
```python
REQUEST_TIMEOUT = 20      # Seconds per request
PAGE_SIZE = 20            # Avature pagination size
MAX_PAGES = 50            # Max pages per endpoint (50 × 20 = 1000 jobs)
DELAY_BETWEEN_REQUESTS = 0.3
```

### Phase 2 (phase2_extraction.py)
```python
REQUEST_TIMEOUT = 20
DELAY_BETWEEN_REQUESTS = 0.3
MAX_RETRIES = 2
SAVE_INTERVAL = 100       # Save progress every N jobs
```

## Workflow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         PHASE 1: DISCOVERY                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   Urls.txt ──► Parse URLs ──► Extract Site Paths                │
│                                      │                          │
│                                      ▼                          │
│                              Validate Endpoints                  │
│                                      │                          │
│                                      ▼                          │
│                              Harvest Jobs (/SearchJobs/)         │
│                                      │                          │
│                                      ▼                          │
│                              Clean & Deduplicate                 │
│                                      │                          │
│                                      ▼                          │
│                              discovered_jobs.csv                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────┐
│                        PHASE 2: EXTRACTION                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   discovered_jobs.csv ──► Visit Each Job URL                    │
│                                      │                          │
│                                      ▼                          │
│                              Extract Details                     │
│                              • Description                       │
│                              • Location                          │
│                              • Date Posted                       │
│                              • Department                        │
│                              • Apply URL                         │
│                                      │                          │
│                                      ▼                          │
│                        jobs_full_details.csv/json               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```
## Resume Capability

Phase 2 saves progress to `extraction_progress.json` every 100 jobs. If interrupted, simply re-run the script to resume from where it stopped.

To start fresh, delete `extraction_progress.json` before running.
