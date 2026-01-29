#!/usr/bin/env python3
"""
PHASE 2: EXTRACTION

Extracts full job details from discovered job URLs.

Input:  discovered_jobs.csv (from Phase 1)
Output: jobs_full_details.csv, jobs_full_details.json

Extracts:
- Job Title
- Job Description (text and HTML)
- Application URL
- Location
- Date Posted
- Department
- Employment Type
"""

import csv
import json
import re
import time
import requests
from datetime import datetime
from html import unescape
from urllib.parse import urlparse

# CONFIGURATION #
INPUT_FILE = "discovered_jobs.csv"
OUTPUT_CSV = "jobs_full_details.csv"
OUTPUT_JSON = "jobs_full_details.json"
PROGRESS_FILE = "extraction_progress.json"
STATS_FILE = "extraction_stats.txt"

REQUEST_TIMEOUT = 20
DELAY_BETWEEN_REQUESTS = 0.3
MAX_RETRIES = 2
SAVE_INTERVAL = 100  # Save progress every 100th jobs

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def log(msg):
    """Print with timestamp."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def load_jobs(filepath):
    """Load jobs to process."""
    jobs = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                jobs.append(row)
        log(f"Loaded {len(jobs):,} jobs from '{filepath}'")
        return jobs
    except FileNotFoundError:
        log(f"ERROR: File not found: {filepath}")
        return []


def load_progress():
    """Load progress from previous run."""
    try:
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    except:
        return {'completed_urls': [], 'results': []}


def save_progress(progress):
    """Save progress for resume capability."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)


def clean_html(html):
    """Remove HTML tags and clean text."""
    if not html:
        return ""
    html = re.sub(r'<script[^>]*>[\s\S]*?</script>', '', html, flags=re.IGNORECASE)
    html = re.sub(r'<style[^>]*>[\s\S]*?</style>', '', html, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', html)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_job_details(url):
    """Fetch a job detail page and extract all available information."""
    result = {
        'url': url,
        'status': 'unknown',
        'description_html': '',
        'description_text': '',
        'location': '',
        'date_posted': '',
        'department': '',
        'employment_type': '',
        'apply_url': url,
        'error': None
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            
            if response.status_code != 200:
                result['status'] = f'http_{response.status_code}'
                result['error'] = f"HTTP {response.status_code}"
                return result
            
            html = response.text
            result['status'] = 'success'
            
            # === EXTRACT JOB DESCRIPTION ===
            desc_patterns = [
                r'<div[^>]*class=["\'][^"\']*job-description[^"\']*["\'][^>]*>([\s\S]*?)</div>',
                r'<div[^>]*class=["\'][^"\']*jobDescription[^"\']*["\'][^>]*>([\s\S]*?)</div>',
                r'<div[^>]*class=["\'][^"\']*description[^"\']*["\'][^>]*>([\s\S]*?)</div>',
                r'<div[^>]*class=["\'][^"\']*job-details[^"\']*["\'][^>]*>([\s\S]*?)</div>',
                r'<div[^>]*class=["\'][^"\']*jobDetails[^"\']*["\'][^>]*>([\s\S]*?)</div>',
                r'<div[^>]*class=["\'][^"\']*posting-description[^"\']*["\'][^>]*>([\s\S]*?)</div>',
                r'<div[^>]*id=["\']job-description["\'][^>]*>([\s\S]*?)</div>',
                r'<div[^>]*id=["\']jobDescription["\'][^>]*>([\s\S]*?)</div>',
                r'<section[^>]*class=["\'][^"\']*description[^"\']*["\'][^>]*>([\s\S]*?)</section>',
                r'<article[^>]*class=["\'][^"\']*job[^"\']*["\'][^>]*>([\s\S]*?)</article>',
            ]
            
            for pattern in desc_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    result['description_html'] = match.group(1).strip()
                    result['description_text'] = clean_html(result['description_html'])
                    if len(result['description_text']) > 50:
                        break
            
            # Fallback: paragraph content
            if len(result['description_text']) < 50:
                paragraphs = re.findall(r'<p[^>]*>([\s\S]*?)</p>', html, re.IGNORECASE)
                long_paragraphs = [p for p in paragraphs if len(clean_html(p)) > 100]
                if long_paragraphs:
                    result['description_html'] = '<p>' + '</p><p>'.join(long_paragraphs[:5]) + '</p>'
                    result['description_text'] = clean_html(result['description_html'])
            
            # === EXTRACT LOCATION ===
            location_patterns = [
                r'<[^>]*class=["\'][^"\']*location[^"\']*["\'][^>]*>([^<]+)',
                r'<[^>]*itemprop=["\']jobLocation["\'][^>]*>([^<]+)',
                r'(?:Location|Office|City)[\s:]+</?\w+[^>]*>?\s*([A-Z][^<\n]{3,50})',
                r'"addressLocality"\s*:\s*"([^"]+)"',
                r'"jobLocation"[^}]*"name"\s*:\s*"([^"]+)"',
            ]
            
            for pattern in location_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    loc = clean_html(match.group(1)).strip()
                    if loc and 2 < len(loc) < 100:
                        result['location'] = loc
                        break
            
            # === EXTRACT DATE POSTED ===
            date_patterns = [
                r'<[^>]*itemprop=["\']datePosted["\'][^>]*content=["\']([^"\']+)["\']',
                r'"datePosted"\s*:\s*"([^"]+)"',
                r'(?:Posted|Date|Published)[\s:]+([A-Z][a-z]+ \d{1,2},? \d{4})',
                r'(?:Posted|Date|Published)[\s:]+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
                r'(\d{4}-\d{2}-\d{2})T\d{2}:\d{2}',
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    result['date_posted'] = match.group(1).strip()
                    break
            
            # === EXTRACT DEPARTMENT ===
            dept_patterns = [
                r'<[^>]*class=["\'][^"\']*department[^"\']*["\'][^>]*>([^<]+)',
                r'(?:Department|Team|Division)[\s:]+</?\w+[^>]*>?\s*([^<\n]{3,50})',
                r'"department"\s*:\s*"([^"]+)"',
            ]
            
            for pattern in dept_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    dept = clean_html(match.group(1)).strip()
                    if dept and len(dept) > 2:
                        result['department'] = dept
                        break
            
            # === EXTRACT EMPLOYMENT TYPE ===
            type_patterns = [
                r'<[^>]*itemprop=["\']employmentType["\'][^>]*>([^<]+)',
                r'"employmentType"\s*:\s*"([^"]+)"',
                r'(?:Job Type|Employment|Contract)[\s:]+([^<\n]{3,30})',
            ]
            
            for pattern in type_patterns:
                match = re.search(pattern, html, re.IGNORECASE)
                if match:
                    result['employment_type'] = clean_html(match.group(1)).strip()
                    break
            
            # === EXTRACT APPLY URL ===
            apply_patterns = [
                r'<a[^>]*class=["\'][^"\']*apply[^"\']*["\'][^>]*href=["\']([^"\']+)["\']',
                r'<a[^>]*href=["\']([^"\']+)["\'][^>]*class=["\'][^"\']*apply[^"\']*["\']',
                r'<a[^>]*href=["\']([^"\']*[Aa]pply[^"\']*)["\']',
            ]
            
            for pattern in apply_patterns:
                match = re.search(pattern, html)
                if match:
                    apply_url = match.group(1)
                    if apply_url.startswith('/'):
                        parsed = urlparse(url)
                        apply_url = f"{parsed.scheme}://{parsed.netloc}{apply_url}"
                    result['apply_url'] = apply_url
                    break
            
            return result
            
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                result['status'] = 'error'
                result['error'] = str(type(e).__name__)
            time.sleep(1)
    
    return result


def main():
    print("=" * 70)
    print("PHASE 2: EXTRACTION")
    print("=" * 70)
    start_time = time.time()
    
    # Load jobs
    jobs = load_jobs(INPUT_FILE)
    if not jobs:
        return
    
    # Load progress
    progress = load_progress()
    completed_urls = set(progress.get('completed_urls', []))
    results = progress.get('results', [])
    
    if completed_urls:
        log(f"Resuming from {len(completed_urls):,} previously processed jobs")
    
    # Filter remaining
    remaining = [j for j in jobs if j.get('url') not in completed_urls]
    log(f"Processing {len(remaining):,} remaining jobs...")
    
    success_count = 0
    error_count = 0
    
    for i, job in enumerate(remaining):
        url = job.get('url', '')
        title = job.get('title', '')
        
        if not url:
            continue
        
        # Extract details
        details = extract_job_details(url)
        
        # Combine data
        full_job = {
            'title': title,
            'url': url,
            'description_text': details['description_text'],
            'description_html': details['description_html'],
            'location': details['location'] or job.get('location', ''),
            'date_posted': details['date_posted'],
            'department': details['department'],
            'employment_type': details['employment_type'],
            'apply_url': details['apply_url'],
            'source_domain': job.get('source_domain', ''),
            'source_path': job.get('source_path', ''),
            'extraction_status': details['status'],
            'extracted_at': datetime.now().isoformat()
        }
        
        results.append(full_job)
        completed_urls.add(url)
        
        if details['status'] == 'success':
            success_count += 1
            desc_len = len(details['description_text'])
            print(f"[{i+1}/{len(remaining)}] ✓ {title[:45]}... ({desc_len} chars)")
        else:
            error_count += 1
            print(f"[{i+1}/{len(remaining)}] ✗ {title[:45]}... ({details['error']})")
        
        # Save progress periodically
        if (i + 1) % SAVE_INTERVAL == 0:
            save_progress({'completed_urls': list(completed_urls), 'results': results})
            log(f"Progress saved: {len(results):,} jobs processed")
        
        time.sleep(DELAY_BETWEEN_REQUESTS)
    
    elapsed = time.time() - start_time
    
    # Save final results
    log("Saving final results...")
    
    # CSV
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['title', 'url', 'apply_url', 'location', 'date_posted', 
                     'department', 'employment_type', 'description_text',
                     'source_domain', 'extraction_status', 'extracted_at']
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for job in results:
            writer.writerow(job)
    
    # JSON
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    # Statistics
    jobs_with_desc = len([j for j in results if len(j.get('description_text', '')) > 50])
    jobs_with_loc = len([j for j in results if j.get('location')])
    jobs_with_date = len([j for j in results if j.get('date_posted')])
    
    stats = f"""
{'='*70}
PHASE 2: EXTRACTION COMPLETE
{'='*70}

Time elapsed:           {elapsed/60:.1f} minutes
Total jobs processed:   {len(results):,}
Successful extractions: {success_count:,}
Errors:                 {error_count:,}

Field Coverage:
  Job descriptions:     {jobs_with_desc:,} ({jobs_with_desc/len(results)*100:.1f}%)
  Locations:            {jobs_with_loc:,} ({jobs_with_loc/len(results)*100:.1f}%)
  Dates posted:         {jobs_with_date:,} ({jobs_with_date/len(results)*100:.1f}%)

Output Files:
  {OUTPUT_CSV}
  {OUTPUT_JSON}
{'='*70}
"""
    print(stats)
    
    with open(STATS_FILE, 'w') as f:
        f.write(stats)
    
    log(f"Saved {len(results):,} jobs to '{OUTPUT_CSV}' and '{OUTPUT_JSON}'")


if __name__ == "__main__":
    main()
