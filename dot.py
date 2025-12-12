import asyncio
import os
import random
import re
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
from playwright.async_api import async_playwright

# =============================================================================
# CONFIGURATION
# =============================================================================
NUM_WORKERS = 4  # Reduced - DDG rate limits aggressively
VISIBLE_WORKER = 0  # Which worker shows visible browser (for auditing)
MAX_URLS_TO_CHECK = 10
PAGE_LOAD_TIMEOUT = 12000
VERBOSE = True  # Set to False for less output
ALL_VISIBLE = True  # Run all browsers visible (DDG blocks headless)

DELAY_BETWEEN_COMPANIES = (3, 6)
DELAY_AFTER_SEARCH = (2, 4)
DDG_ERROR_RETRY_DELAY = 10  # Seconds to wait after DDG error before retry

OUTPUT_FILE = "towing_companies_verified.csv"
CHECKPOINT_FILE = "processed_dots.csv"

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

STATE_NAMES = {
    "MA": ["massachusetts"], "CT": ["connecticut"], "RI": ["rhode island"],
    "NH": ["new hampshire"], "VT": ["vermont"], "ME": ["maine"], "NY": ["new york"],
}

SKIP_DOMAINS = [
    'duck.ai', 'duckduckgo.com', 'bing.com', 'google.com', 'apple.com',
    'apps.apple.com', 'maps.apple.com', 'webcache.googleusercontent.com',
]

PRIORITY_DOMAINS = [
    'facebook.com', 'yellowpages.com', 'mapquest.com',
    'bbb.org', 'chamberofcommerce.com', 'manta.com'
]

PRIMARY_TOW_KEYWORDS = [
    'towing service', 'tow truck', 'tow service', 'we tow', 'our towing',
    'towing company', 'wrecker service', 'flatbed tow', '24 hour tow',
    'emergency towing', '24/7 towing', 'local towing', 'towing rates',
    'tow your', 'towing needs', 'professional towing', 'towing available',
    'heavy duty towing', 'light duty towing', 'medium duty towing',
    'roadside assistance', 'vehicle recovery', 'accident recovery',
    'jump start', 'lockout service', 'winch out', 'winching',
]

SECONDARY_TOW_KEYWORDS = ['towing', 'tow', 'wrecker', 'wrecking']

FALSE_POSITIVE_PATTERNS = [
    r'towing in \w+', r'towing near \w+', r'towing services in \w+',
    r'\w+ towing companies', r'more towing', r'related.*towing',
    r'see more.*towing', r'other towing', r'find towing',
    r'search.*towing', r'browse.*towing', r'people also viewed.*towing',
    r'you might also like.*towing', r'similar.*towing',
]

TOW_NAME_PATTERN = re.compile(r'\b(tow|towing|tows|towed|wrecker|wrecking)\b', re.IGNORECASE)
SUFFIX_PATTERN = re.compile(r'\b(INC|LLC|CORP|CO|LTD|INCORPORATED|CORPORATION)\.?\b', re.IGNORECASE)

# =============================================================================
# GLOBALS
# =============================================================================
results_lock = asyncio.Lock()
stats = {'start_time': None, 'searched': 0, 'found_towing': 0}


# =============================================================================
# UTILITIES
# =============================================================================
def log(msg, worker_id=None):
    ts = datetime.now().strftime("%H:%M:%S")
    prefix = f"[W{worker_id}]" if worker_id is not None else ""
    print(f"[{ts}]{prefix} {msg}")


def vlog(msg, worker_id=None):
    """Verbose log - only prints if VERBOSE is True"""
    if VERBOSE:
        log(msg, worker_id)


def clean_company_name(name):
    if not name:
        return None
    name = SUFFIX_PATTERN.sub('', name)
    name = name.replace('&', 'and')
    name = re.sub(r'[^\w\s\']', ' ', name)
    return ' '.join(name.split()).strip() or None


def normalize_text(text):
    if not text:
        return ""
    return re.sub(r'[^\w\s]', '', str(text).lower()).strip()


def has_tow_in_name(legal_name, dba_name):
    text = f"{legal_name or ''} {dba_name or ''}"
    return bool(TOW_NAME_PATTERN.search(text))


def is_valid_url(url):
    if not url or not url.startswith('http'):
        return False
    url_lower = url.lower()
    return not any(skip in url_lower for skip in SKIP_DOMAINS)


def get_domain_priority(url):
    url_lower = url.lower()
    for i, domain in enumerate(PRIORITY_DOMAINS):
        if domain in url_lower:
            return i
    return 100


def sort_urls_by_priority(urls):
    return sorted(set(urls), key=get_domain_priority)


# =============================================================================
# MATCHING LOGIC
# =============================================================================
def check_address_match(page_text, row):
    page_lower = normalize_text(page_text)

    city = normalize_text(row.get('PHY_CITY', ''))
    city_match = city and len(city) > 2 and city in page_lower

    state = row.get('PHY_STATE', '')
    state_match = state in STATE_NAMES and any(v in page_lower for v in STATE_NAMES[state])

    zip_code = str(row.get('PHY_ZIP', ''))[:5]
    zip_match = len(zip_code) == 5 and zip_code.isdigit() and zip_code in page_text

    street = str(row.get('PHY_STREET', ''))
    street_num = re.match(r'^(\d{2,})', street)
    street_match = street_num and street_num.group(1) in page_text

    return (city_match and (zip_match or state_match)) or (zip_match and street_match)


def check_towing_mention(page_text):
    page_lower = page_text.lower()

    if any(kw in page_lower for kw in PRIMARY_TOW_KEYWORDS):
        return True

    if not any(kw in page_lower for kw in SECONDARY_TOW_KEYWORDS):
        return False

    for pattern in FALSE_POSITIVE_PATTERNS:
        if re.search(pattern, page_lower):
            first_portion = page_lower[:int(len(page_lower) * 0.6)]
            if not any(kw in first_portion for kw in SECONDARY_TOW_KEYWORDS):
                return False

    return True


# =============================================================================
# PAGE EXTRACTION
# =============================================================================
MAIN_CONTENT_JS = '''() => {
    const selectors = [
        'main', '[role="main"]', '#main-content', '#content', '.main-content',
        'article', '.business-info', '.listing-content', '.profile-content'
    ];
    for (const sel of selectors) {
        const el = document.querySelector(sel);
        if (el?.innerText?.length > 200) return el.innerText;
    }
    const body = document.body.cloneNode(true);
    ['footer', 'nav', '[role="navigation"]', '.footer', '.nav', '.sidebar', '#footer', '#nav', '.related-searches']
        .forEach(sel => body.querySelectorAll(sel).forEach(el => el.remove()));
    return body.innerText || "";
}'''


async def get_page_text(page, url):
    try:
        response = await page.goto(url, timeout=PAGE_LOAD_TIMEOUT, wait_until='domcontentloaded')
        if response and response.status >= 400:
            return None
        await asyncio.sleep(0.3)
        text = await page.evaluate(MAIN_CONTENT_JS)
        return text or await page.evaluate('() => document.body.innerText || ""')
    except:
        return None


async def extract_urls_from_ddg(page):
    urls = []
    try:
        articles = await page.query_selector_all('article[data-testid="result"]')
        for article in articles[:12]:
            links = await article.query_selector_all('a[href^="http"]')
            for link in links:
                href = await link.get_attribute('href')
                if href and is_valid_url(href):
                    urls.append(href)
                    break
    except:
        pass

    if len(urls) < 5:
        try:
            all_links = await page.query_selector_all('a[href^="http"]')
            for link in all_links[:30]:
                href = await link.get_attribute('href')
                if is_valid_url(href) and href not in urls:
                    urls.append(href)
                    if len(urls) >= 12:
                        break
        except:
            pass

    return urls


async def search_duckduckgo(page, query, worker_id=None):
    """Returns (urls, status) where status is:
       - True: success
       - False: blocked/failed (don't retry)
       - "RESTART": DDG error page, browser needs restart
    """
    try:
        url = f"https://duckduckgo.com/?q={quote_plus(query)}"
        await page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
        await asyncio.sleep(random.uniform(*DELAY_AFTER_SEARCH))

        # Check for DDG error page (418.html redirect)
        if "static-pages/418" in page.url or "static-pages" in page.url:
            vlog(f"    ⚠ DDG error page detected: {page.url}", worker_id)
            return [], "RESTART"

        # Check for "Unexpected error" in page content
        try:
            body_text = await page.evaluate('() => document.body?.innerText || ""')
            if "Unexpected error" in body_text:
                vlog(f"    ⚠ DDG 'Unexpected error' detected", worker_id)
                return [], "RESTART"
        except:
            pass

        if page.url == "about:blank" or "duckduckgo.com" not in page.url:
            return [], False

        try:
            await page.wait_for_selector('article[data-testid="result"]', timeout=4000)
        except:
            content = await page.content()
            if len(content) < 1000:
                return [], False

        urls = await extract_urls_from_ddg(page)
        return urls, True
    except Exception as e:
        vlog(f"    ✗ DDG exception: {e}", worker_id)
        return [], False


# =============================================================================
# COMPANY PROCESSING
# =============================================================================
async def process_company(page, row, search_name, worker_id):
    state = row.get('PHY_STATE', '')
    city = row.get('PHY_CITY', '')

    all_urls = []

    # Exact query
    exact_query = f'"{search_name}" {city} {state}'
    vlog(f"    Query: {exact_query}", worker_id)
    urls, status = await search_duckduckgo(page, exact_query, worker_id)
    if status == "RESTART":
        return None, "DDG_RESTART"
    if not status:
        vlog(f"    ✗ DDG blocked on exact query", worker_id)
        return None, "DDG_BLOCKED"
    vlog(f"    Found {len(urls)} URLs from exact query", worker_id)
    all_urls.extend(urls)

    # Broad query if needed
    if len(all_urls) < 5:
        await asyncio.sleep(random.uniform(*DELAY_AFTER_SEARCH))
        broad_query = f'{search_name} {city} {state}'
        vlog(f"    Query (broad): {broad_query}", worker_id)
        urls, status = await search_duckduckgo(page, broad_query, worker_id)
        if status == "RESTART":
            return None, "DDG_RESTART"
        if not status:
            vlog(f"    ✗ DDG blocked on broad query", worker_id)
            return None, "DDG_BLOCKED"
        new_urls = [u for u in urls if u not in all_urls]
        vlog(f"    Found {len(new_urls)} additional URLs from broad query", worker_id)
        all_urls.extend(new_urls)

    if not all_urls:
        vlog(f"    ✗ No URLs found from any search", worker_id)
        return None, "NO_URLS"

    sorted_urls = sort_urls_by_priority(all_urls)[:MAX_URLS_TO_CHECK]
    vlog(f"    Checking {len(sorted_urls)} URLs:", worker_id)

    for i, url in enumerate(sorted_urls, 1):
        await asyncio.sleep(0.5)

        # Extract domain for cleaner logging
        domain = url.split('/')[2] if len(url.split('/')) > 2 else url

        page_text = await get_page_text(page, url)
        if not page_text:
            vlog(f"      [{i}] {domain}: ✗ Failed to load", worker_id)
            continue

        addr_match = check_address_match(page_text, row)
        if not addr_match:
            vlog(f"      [{i}] {domain}: ✗ Address mismatch", worker_id)
            continue

        tow_match = check_towing_mention(page_text)
        if not tow_match:
            vlog(f"      [{i}] {domain}: ✗ Address OK, no towing keywords", worker_id)
            continue

        vlog(f"      [{i}] {domain}: ✓ MATCH (address + towing)", worker_id)
        return url, "TOWING_FOUND"

    vlog(f"    ✗ No towing found in any of {len(sorted_urls)} URLs", worker_id)
    return None, "NO_TOWING"


# =============================================================================
# WORKER
# =============================================================================
async def create_context(browser, worker_id):
    """Create a new browser context with anti-detection settings"""
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
        viewport={'width': 1920, 'height': 1080},
    )
    await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
    page = await context.new_page()
    return context, page


async def worker(worker_id, queue, results_dict, processed_dots, browser, browser_visible):
    use_browser = browser_visible if worker_id == VISIBLE_WORKER else browser

    context, page = await create_context(use_browser, worker_id)
    processed_count = 0
    restart_count = 0
    max_restarts = 3  # Max restarts per company

    while True:
        try:
            idx, row = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        try:
            dot_number = int(row['DOT_NUMBER'])
            legal_name = row.get('LEGAL_NAME', '')
            log(f"[{idx}] {legal_name} (DOT: {dot_number})", worker_id)

            found_towing = False
            website_found = None
            current_restart_count = 0

            # Try legal name
            clean_legal = clean_company_name(legal_name)
            if clean_legal:
                await asyncio.sleep(random.uniform(*DELAY_BETWEEN_COMPANIES))
                website_found, status = await process_company(page, row, clean_legal, worker_id)

                # Handle DDG restart needed
                while status == "DDG_RESTART" and current_restart_count < max_restarts:
                    current_restart_count += 1
                    restart_count += 1
                    log(f"  ⚠ DDG error page, restarting browser (attempt {current_restart_count}/{max_restarts})",
                        worker_id)
                    await context.close()
                    await asyncio.sleep(DDG_ERROR_RETRY_DELAY)  # Longer wait for DDG to cool down
                    context, page = await create_context(use_browser, worker_id)
                    await asyncio.sleep(random.uniform(*DELAY_BETWEEN_COMPANIES))
                    website_found, status = await process_company(page, row, clean_legal, worker_id)

                if status == "DDG_RESTART":
                    log(f"  ✗ DDG keeps failing after {max_restarts} restarts, skipping", worker_id)
                    status = "SKIPPED"

                if status == "DDG_BLOCKED":
                    log("  ⚠ DDG blocked, re-queuing", worker_id)
                    await queue.put((idx, row))
                    await asyncio.sleep(60)
                    continue

                if status == "TOWING_FOUND":
                    found_towing = True
                    log(f"  ✓ TOWING FOUND: {legal_name}", worker_id)

            # Try DBA name if not found
            if not found_towing:
                dba_name = row.get('DBA_NAME')
                if dba_name and pd.notna(dba_name):
                    clean_dba = clean_company_name(dba_name)
                    if clean_dba and clean_dba != clean_legal:
                        vlog(f"  Trying DBA: {clean_dba}", worker_id)
                        await asyncio.sleep(random.uniform(*DELAY_BETWEEN_COMPANIES))
                        website_found, status = await process_company(page, row, clean_dba, worker_id)

                        # Handle DDG restart needed
                        while status == "DDG_RESTART" and current_restart_count < max_restarts:
                            current_restart_count += 1
                            restart_count += 1
                            log(f"  ⚠ DDG error page, restarting browser (attempt {current_restart_count}/{max_restarts})",
                                worker_id)
                            await context.close()
                            await asyncio.sleep(DDG_ERROR_RETRY_DELAY)
                            context, page = await create_context(use_browser, worker_id)
                            await asyncio.sleep(random.uniform(*DELAY_BETWEEN_COMPANIES))
                            website_found, status = await process_company(page, row, clean_dba, worker_id)

                        if status == "DDG_RESTART":
                            log(f"  ✗ DDG keeps failing after {max_restarts} restarts, skipping", worker_id)
                            status = "SKIPPED"

                        if status == "DDG_BLOCKED":
                            log("  ⚠ DDG blocked, re-queuing", worker_id)
                            await queue.put((idx, row))
                            await asyncio.sleep(60)
                            continue

                        if status == "TOWING_FOUND":
                            found_towing = True
                            log(f"  ✓ TOWING FOUND (DBA): {dba_name}", worker_id)

            # Save result
            async with results_lock:
                stats['searched'] += 1
                if found_towing:
                    stats['found_towing'] += 1
                    company_data = row.to_dict()
                    company_data['DOT_NUMBER'] = dot_number
                    company_data['WEBSITE_URL'] = website_found
                    company_data['VERIFIED_AT'] = datetime.now().isoformat()
                    results_dict[dot_number] = company_data
                processed_dots.add(dot_number)

            # Refresh context periodically
            processed_count += 1
            if processed_count >= 30:
                await context.close()
                context, page = await create_context(use_browser, worker_id)
                processed_count = 0

        except Exception as e:
            log(f"Error: {e}", worker_id)

    await context.close()
    log(f"Worker finished (restarted {restart_count} times)", worker_id)


# =============================================================================
# PERIODIC SAVE
# =============================================================================
async def periodic_save(results_dict, processed_dots, total_to_search):
    while True:
        await asyncio.sleep(60)
        async with results_lock:
            if not results_dict:
                continue

            pd.DataFrame(list(results_dict.values())).to_csv(OUTPUT_FILE, index=False)
            pd.DataFrame({'DOT_NUMBER': sorted(processed_dots)}).to_csv(CHECKPOINT_FILE, index=False)

            elapsed = (datetime.now() - stats['start_time']).total_seconds() / 60
            searched = stats['searched']
            found = stats['found_towing']
            rate = found / searched * 100 if searched else 0
            speed = searched / elapsed if elapsed else 0
            remaining = total_to_search - searched
            eta = remaining / speed if speed else 0

            log(f"--- STATS: {len(processed_dots)} processed, {len(results_dict)} towing total ---")
            log(f"    Searched: {searched} | Found: {found} ({rate:.1f}%) | Speed: {speed:.1f}/min | ETA: {eta:.0f} min ---")


# =============================================================================
# MAIN
# =============================================================================
async def main():
    log("=" * 60)
    log(f"DOT TOWING SCRAPER - {NUM_WORKERS} PARALLEL WORKERS")
    log("=" * 60)

    # Load data
    log("Loading data...")
    df = pd.read_parquet("Company_Census_File.parquet")
    log(f"Total rows: {len(df)}")

    columns = [
        "DOT_NUMBER", "LEGAL_NAME", "DBA_NAME",
        "COMPANY_OFFICER_1", "COMPANY_OFFICER_2",
        "TRUCK_UNITS", "POWER_UNITS", "TOTAL_CDL", "TOTAL_DRIVERS",
        "PHY_STREET", "PHY_CITY", "PHY_STATE", "PHY_ZIP", "PHY_CNTY",
        "EMAIL_ADDRESS", "CRGO_DRIVETOW"
    ]
    df = df[columns].copy()
    df['DOT_NUMBER'] = df['DOT_NUMBER'].astype(int)

    # Split auto-approve vs search
    df['HAS_TOW_NAME'] = df.apply(lambda r: has_tow_in_name(r.get('LEGAL_NAME'), r.get('DBA_NAME')), axis=1)
    df_auto = df[df['HAS_TOW_NAME']]
    df_search = df[~df['HAS_TOW_NAME']]
    log(f"Auto-approve: {len(df_auto)}, Need search: {len(df_search)}")

    # Load checkpoint
    processed_dots = set()
    if os.path.exists(CHECKPOINT_FILE):
        processed_dots = set(pd.read_csv(CHECKPOINT_FILE)['DOT_NUMBER'].astype(int))
        log(f"Already processed: {len(processed_dots)}")

    # Load existing results
    results_dict = {}
    if os.path.exists(OUTPUT_FILE):
        for _, row in pd.read_csv(OUTPUT_FILE).iterrows():
            dot = int(row['DOT_NUMBER'])
            results_dict[dot] = row.to_dict()
            results_dict[dot]['DOT_NUMBER'] = dot
        log(f"Existing results: {len(results_dict)} unique companies")

    # Auto-approve
    auto_count = 0
    for _, row in df_auto.iterrows():
        dot = int(row['DOT_NUMBER'])
        if dot not in results_dict:
            data = row.to_dict()
            data['DOT_NUMBER'] = dot
            data['WEBSITE_URL'] = "AUTO_APPROVED"
            data['VERIFIED_AT'] = datetime.now().isoformat()
            results_dict[dot] = data
            processed_dots.add(dot)
            auto_count += 1
    log(f"Auto-approved: {auto_count} new")

    # Save auto-approved
    if results_dict:
        pd.DataFrame(list(results_dict.values())).to_csv(OUTPUT_FILE, index=False)
        pd.DataFrame({'DOT_NUMBER': sorted(processed_dots)}).to_csv(CHECKPOINT_FILE, index=False)

    # Filter remaining
    already_done = processed_dots | set(results_dict.keys())
    df_remaining = df_search[~df_search['DOT_NUMBER'].isin(already_done)]
    log(f"Remaining to search: {len(df_remaining)}")

    if len(df_remaining) == 0:
        log(f"Done! Total: {len(results_dict)} towing companies")
        return

    # Create queue
    queue = asyncio.Queue()
    for idx, (_, row) in enumerate(df_remaining.iterrows(), 1):
        await queue.put((idx, row))

    total_to_search = len(df_remaining)
    stats['start_time'] = datetime.now()

    log(f"Starting {NUM_WORKERS} workers (W{VISIBLE_WORKER} visible for auditing)...")

    async with async_playwright() as p:
        # DDG blocks headless browsers, so use visible mode for all if configured
        browser_args = ['--disable-blink-features=AutomationControlled', '--disable-dev-shm-usage', '--disable-gpu']

        if ALL_VISIBLE:
            log("All browsers running in visible mode (DDG blocks headless)")
            browser = await p.chromium.launch(headless=False, args=browser_args)
            browser_visible = browser  # Same browser for all workers
        else:
            browser = await p.chromium.launch(
                headless=True,
                args=browser_args + ['--no-sandbox', '--disable-extensions', '--disable-plugins', '--disable-images']
            )
            browser_visible = await p.chromium.launch(headless=False, args=browser_args)

        workers = [asyncio.create_task(worker(i, queue, results_dict, processed_dots, browser, browser_visible))
                   for i in range(NUM_WORKERS)]
        save_task = asyncio.create_task(periodic_save(results_dict, processed_dots, total_to_search))

        await asyncio.gather(*workers)
        save_task.cancel()

        await browser.close()
        if not ALL_VISIBLE:
            await browser_visible.close()

    # Final save
    pd.DataFrame(list(results_dict.values())).to_csv(OUTPUT_FILE, index=False)
    pd.DataFrame({'DOT_NUMBER': sorted(processed_dots)}).to_csv(CHECKPOINT_FILE, index=False)

    log("=" * 60)
    log(f"COMPLETE: {len(results_dict)} towing companies")
    log("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
