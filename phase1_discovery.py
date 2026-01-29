#!/usr/bin/env python3
"""
PHASE 1: DISCOVERY
 

Discovers + harvests job listings from Avature-hosted career sites.

Input:  Urls.txt (seed file with known Avature URLs)
Output: discovered_jobs.csv (unique job listings ready for extraction)

Process:
1. Parse seed file to extract domains and site paths
2. Validate endpoints to find live job boards
3. Harvest job listings via pagination
4. Clean and deduplicate results
5. Identify new discoveries not in seed file
"""

import csv
import re
import time
import requests
from urllib.parse import urlparse, parse_qs, urljoin
from collections import defaultdict
from datetime import datetime

#  CONFIGURATION #
INPUT_FILE = "Urls.txt"
OUTPUT_FILE = "discovered_jobs.csv"
LIVE_ENDPOINTS_FILE = "live_endpoints.csv"
STATS_FILE = "discovery_stats.txt"

REQUEST_TIMEOUT = 20
PAGE_SIZE = 20
MAX_PAGES = 50
DELAY_BETWEEN_REQUESTS = 0.3

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# Paths to skip (non-job pages)
SKIP_PATHS = {
    'error', 'resetpassword', 'login', 'register', 'applicationmethods',
    'recommendationmethods', 'profile', 'account', 'settings', 'infographics',
    'images', 'assets', 'static', 'css', 'js', 'fonts', 'api', 'webhook'
}

# False positive title filters
BLACKLIST_TITLES = {
    'email', 'linkedin', 'facebook', 'twitter', 'instagram', 'youtube',
    'share', 'tweet', 'post', 'follow', 'subscribe', 'x', 'tiktok',
    'home', 'back', 'next', 'previous', 'menu', 'search', 'filter',
    'apply', 'apply now', 'learn more', 'read more', 'view all',
    'see all', 'show more', 'load more', 'click here', 'view details',
    'jobs', 'careers', 'opportunities', 'positions', 'openings',
    'login', 'sign in', 'register', 'sign up', 'my account', 'profile',
    'reset password', 'forgot password', 'logout', 'sign out',
    'contact', 'contact us', 'about', 'about us', 'privacy', 'terms',
    'cookie', 'cookies', 'legal', 'disclaimer', 'help', 'faq', 'support',
    'close', 'cancel', 'submit', 'save', 'delete', 'edit', 'update',
}

BLACKLIST_PATTERNS = [
    r'^[\W\d]+$', r'^.{1,2}$', r'^\d+$', r'^#\d+', r'@', r'^https?://', r'\.com|\.net|\.org',
]

BLACKLIST_URL_PATTERNS = [
    r'/Error', r'/ResetPassword', r'/Login', r'/Register', r'/Profile',
    r'/Account', r'/Privacy', r'/Terms', r'/Contact', r'/About', r'share[=/]', r'social[=/]',
]


def log(msg):
    """Print with timestamp."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# STEP 1: PARSE SEED FILE #


def load_seed_urls(filepath):
    """Load all URLs from seed file."""
    urls = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                tokens = line.strip().split()
                for token in tokens:
                    if token.startswith(('http://', 'https://')):
                        urls.append(token)
        log(f"Loaded {len(urls):,} URLs from '{filepath}'")
        return urls
    except FileNotFoundError:
        log(f"ERROR: File not found: {filepath}")
        return []


def extract_site_paths(urls):
    """Extract unique domain + site_path combinations from URLs."""
    site_paths = defaultdict(set)
    
    for url in urls:
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname
            if not hostname or 'avature.net' not in hostname:
                continue
            
            hostname = hostname.lower()
            path = parsed.path.strip('/')
            if not path:
                continue
            
            segments = path.split('/')
            action_keywords = {'jobdetail', 'searchjobs', 'applicationmethods', 
                             'apply', 'job', 'position', 'requisition'}
            
            site_path_segments = []
            for seg in segments:
                seg_lower = seg.lower()
                if seg_lower in action_keywords or seg.isdigit():
                    break
                if '=' in seg or seg_lower in SKIP_PATHS:
                    break
                site_path_segments.append(seg)
            
            if site_path_segments:
                site_path = '/'.join(site_path_segments)
                site_paths[hostname].add(site_path)
        except:
            continue
    
    # Flatten to list
    all_paths = []
    for domain, paths in site_paths.items():
        for path in paths:
            all_paths.append({
                'domain': domain,
                'site_path': path,
                'base_url': f"https://{domain}/{path}"
            })
    
    log(f"Extracted {len(all_paths):,} unique domain/path combinations")
    return all_paths


# STEP 2: VALIDATE ENDPOINTS #

def test_endpoint(base_url):
    """Quick test if an endpoint has job listings."""
    search_url = f"{base_url}/SearchJobs/"
    try:
        response = requests.get(search_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if response.status_code != 200:
            return False, 0
        
        html = response.text
        job_links = re.findall(r'href=["\']([^"\']*(?:JobDetail|jobId)[^"\']*)["\']', html, re.IGNORECASE)
        
        if job_links:
            return True, len(set(job_links))
        if 'error' in response.url.lower():
            return False, 0
        return False, 0
    except:
        return False, 0


def validate_endpoints(site_paths):
    """Find endpoints with active job listings."""
    log(f"Validating {len(site_paths):,} endpoints...")
    
    live_endpoints = []
    for i, endpoint in enumerate(site_paths):
        has_jobs, count = test_endpoint(endpoint['base_url'])
        
        if has_jobs:
            endpoint['estimated_jobs'] = count
            live_endpoints.append(endpoint)
            print(f"    [✓] {endpoint['domain']}/{endpoint['site_path']} (~{count} jobs)")
        
        if (i + 1) % 200 == 0:
            log(f"Progress: {i+1}/{len(site_paths)}, {len(live_endpoints)} live")
        
        time.sleep(0.1)
    
    log(f"Found {len(live_endpoints)} live endpoints with job listings")
    return live_endpoints


# STEP 3: HARVEST JOBS# 

def extract_jobs_from_html(html, base_url):
    jobs = []
    pattern1 = r'<a[^>]*href=["\']([^"\']*JobDetail[^"\']*)["\'][^>]*>([^<]+)</a>'
    pattern2 = r'<a[^>]*href=["\']([^"\']*[?&]jobId=\d+[^"\']*)["\'][^>]*>([^<]*)</a>'
    
    all_matches = re.findall(pattern1, html, re.IGNORECASE) + re.findall(pattern2, html, re.IGNORECASE)
    
    seen_urls = set()
    for href, title in all_matches:
        if href.startswith('/'):
            parsed_base = urlparse(base_url)
            full_url = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
        elif href.startswith('http'):
            full_url = href
        else:
            full_url = urljoin(base_url, href)
        
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)
        
        title = re.sub(r'\s+', ' ', title.strip())
        if len(title) < 3:
            url_parts = urlparse(full_url).path.split('/')
            for part in url_parts:
                if len(part) > 10 and '-' in part:
                    title = part.replace('-', ' ').title()
                    break
        
        if title and len(title) >= 3:
            jobs.append({'title': title, 'url': full_url, 'location': ''})
    
    return jobs


def scrape_endpoint(endpoint_info):
    """Scrape all jobs from an endpoint with pagination."""
    base_url = endpoint_info['base_url']
    all_jobs = []
    seen_urls = set()
    offset = 0
    consecutive_empty = 0
    
    while offset < MAX_PAGES * PAGE_SIZE:
        search_url = f"{base_url}/SearchJobs/?jobOffset={offset}"
        
        try:
            response = requests.get(search_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if response.status_code != 200 or 'error' in response.url.lower():
                break
            
            page_jobs = extract_jobs_from_html(response.text, base_url)
            new_jobs = [j for j in page_jobs if j['url'] not in seen_urls]
            
            for job in new_jobs:
                seen_urls.add(job['url'])
                job['source_domain'] = endpoint_info['domain']
                job['source_path'] = endpoint_info['site_path']
            
            if not new_jobs:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    break
            else:
                consecutive_empty = 0
                all_jobs.extend(new_jobs)
            
            offset += PAGE_SIZE
            time.sleep(DELAY_BETWEEN_REQUESTS)
        except:
            break
    
    return all_jobs


def harvest_all_endpoints(live_endpoints):
    """Harvest jobs from all live endpoints."""
    log(f"Harvesting jobs from {len(live_endpoints)} endpoints...")
    
    all_jobs = []
    for i, endpoint in enumerate(live_endpoints):
        jobs = scrape_endpoint(endpoint)
        if jobs:
            all_jobs.extend(jobs)
            print(f"[{i+1}/{len(live_endpoints)}] {endpoint['domain']}/{endpoint['site_path']}: {len(jobs)} jobs")
        else:
            print(f"[{i+1}/{len(live_endpoints)}] {endpoint['domain']}/{endpoint['site_path']}: no jobs")
    
    log(f"Harvested {len(all_jobs):,} total job listings")
    return all_jobs


# STEP 4: CLEAN AND DEDUPLICATE #

def normalize_title(title):
    """Normalize title for comparison."""
    return re.sub(r'\s+', ' ', title.lower().strip())


def extract_job_id(url):
    """Extract job ID for deduplication."""
    try:
        parsed = urlparse(url)
        domain = parsed.hostname or ""
        query = parse_qs(parsed.query)
        
        for param in ['jobId', 'id', 'jobid']:
            if param in query:
                return (query[param][0], domain)
        
        match = re.search(r'/JobDetail/(?:[^/]+/)?(\d+)', parsed.path, re.IGNORECASE)
        if match:
            return (match.group(1), domain)
        
        return (None, domain)
    except:
        return (None, "")


def is_false_positive(job):
    """Check if job is a false positive."""
    title = job.get('title', '').strip()
    url = job.get('url', '')
    
    if not title or len(title) < 3 or len(title) > 200:
        return True
    
    if normalize_title(title) in BLACKLIST_TITLES:
        return True
    
    for pattern in BLACKLIST_PATTERNS:
        if re.search(pattern, title, re.IGNORECASE):
            return True
    
    for pattern in BLACKLIST_URL_PATTERNS:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    
    return False


def clean_and_deduplicate(jobs):
    """Remove false positives and duplicates."""
    # Filter false positives
    filtered = [j for j in jobs if not is_false_positive(j)]
    log(f"After filtering false positives: {len(filtered):,} jobs")
    
    # Deduplicate by job ID
    by_id = defaultdict(list)
    no_id = []
    
    for job in filtered:
        job_id, domain = extract_job_id(job['url'])
        if job_id:
            by_id[(job_id, domain)].append(job)
        else:
            no_id.append(job)
    
    deduped = []
    for key, dupes in by_id.items():
        dupes.sort(key=lambda j: (j['url'].startswith('https'), -len(j['url'])), reverse=True)
        deduped.append(dupes[0])
    
    seen_titles = set()
    for job in no_id:
        _, domain = extract_job_id(job['url'])
        key = (normalize_title(job['title']), domain)
        if key not in seen_titles:
            seen_titles.add(key)
            deduped.append(job)
    
    log(f"After deduplication: {len(deduped):,} unique jobs")
    return deduped


# STEP 5: IDENTIFY NEW DISCOVERIES #

def identify_new_jobs(jobs, seed_urls):
    """Identify jobs not in the original seed file."""
    seed_normalized = set()
    seed_job_ids = set()
    
    for url in seed_urls:
        seed_normalized.add(re.sub(r'^https?://', '', url.lower().strip()).rstrip('/'))
        job_id, domain = extract_job_id(url)
        if job_id:
            seed_job_ids.add((job_id, domain))
    
    new_jobs = []
    existing_jobs = []
    
    for job in jobs:
        url = job.get('url', '')
        normalized = re.sub(r'^https?://', '', url.lower().strip()).rstrip('/')
        job_id, domain = extract_job_id(url)
        
        is_existing = normalized in seed_normalized or (job_id and (job_id, domain) in seed_job_ids)
        
        if is_existing:
            existing_jobs.append(job)
        else:
            new_jobs.append(job)
    
    log(f"Already in seed: {len(existing_jobs):,}, NEW discoveries: {len(new_jobs):,}")
    return new_jobs, existing_jobs

# MAIN #

def main():
    print("=" * 70)
    print("PHASE 1: DISCOVERY")
    print("=" * 70)
    start_time = time.time()
    
    # Step 1: Load seed file
    log("STEP 1: Loading seed file...")
    seed_urls = load_seed_urls(INPUT_FILE)
    if not seed_urls:
        return
    
    # Step 2: Extract site paths
    log("STEP 2: Extracting site paths...")
    site_paths = extract_site_paths(seed_urls)
    
    # Step 3: Validate endpoints
    log("STEP 3: Validating endpoints...")
    live_endpoints = validate_endpoints(site_paths)
    
    if not live_endpoints:
        log("No live endpoints found. Exiting.")
        return
    
    # Save live endpoints
    with open(LIVE_ENDPOINTS_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['domain', 'site_path', 'base_url', 'estimated_jobs'])
        for ep in live_endpoints:
            writer.writerow([ep['domain'], ep['site_path'], ep['base_url'], ep.get('estimated_jobs', 0)])
    log(f"Saved live endpoints to '{LIVE_ENDPOINTS_FILE}'")
    
    # Step 4: Harvest jobs
    log("STEP 4: Harvesting job listings...")
    raw_jobs = harvest_all_endpoints(live_endpoints)
    
    # Step 5: Clean and deduplicate
    log("STEP 5: Cleaning and deduplicating...")
    clean_jobs = clean_and_deduplicate(raw_jobs)
    
    # Step 6: Identify new discoveries
    log("STEP 6: Identifying new discoveries...")
    new_jobs, existing_jobs = identify_new_jobs(clean_jobs, seed_urls)
    
    # Save results
    log("Saving results...")
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['title', 'url', 'location', 'source_domain', 'source_path'])
        for job in new_jobs:
            writer.writerow([job['title'], job['url'], job.get('location', ''), 
                           job.get('source_domain', ''), job.get('source_path', '')])
    
    elapsed = time.time() - start_time
    
    # Statistics
    stats = f"""
{'='*70}
PHASE 1: DISCOVERY COMPLETE
{'='*70}

Time elapsed:           {elapsed/60:.1f} minutes
Seed URLs processed:    {len(seed_urls):,}
Site paths found:       {len(site_paths):,}
Live endpoints:         {len(live_endpoints):,}
Raw jobs harvested:     {len(raw_jobs):,}
Clean unique jobs:      {len(clean_jobs):,}
Already in seed:        {len(existing_jobs):,}
NEW DISCOVERIES:        {len(new_jobs):,}

Output: {OUTPUT_FILE}
{'='*70}
"""
    print(stats)
    
    with open(STATS_FILE, 'w') as f:
        f.write(stats)
    
    log(f"Saved {len(new_jobs):,} discovered jobs to '{OUTPUT_FILE}'")


if __name__ == "__main__":
    main()
