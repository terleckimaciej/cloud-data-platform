from requests_html import AsyncHTMLSession
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
from datetime import date
import asyncio
import random
import nest_asyncio
import gcsfs
from fake_useragent import UserAgent
import pyppeteer


# === FIXING Chromium ERROR in encountered in Cloud Run ===
# Removing sandbox restrictions
import pyppeteer

for flag in ["--no-sandbox", "--disable-dev-shm-usage"]:
    if flag in pyppeteer.launcher.DEFAULT_ARGS:
        pyppeteer.launcher.DEFAULT_ARGS.remove(flag)



# --- CONFIG ---
BASE_URL = "https://it.pracuj.pl"
URL_TPL = "https://it.pracuj.pl/praca?pn={page}"
MAX_PAGES = 1000

# 🔹 Output path in Data Lake
bucket_path = f"gs://pracuj-pl-data-lake/raw/job_listings_{date.today()}.parquet"

# HTML selectors
CARD_SEL       = "div[data-test='default-offer'], div[data-test='recommended-offer']"
TITLE_LINK_SEL = "a[data-test='link-offer-title'], a[data-test='link-offer']"
COMPANY_SEL    = "a[data-test='link-company-profile'], [data-test='text-company-name']"
DATE_SEL       = "p[data-test='text-added']"


async def fetch_page(asession, page: int):
    url = URL_TPL.format(page=page)
    print(f"➡️  Loading page {page}: {url}")
    headers = {"User-Agent": UserAgent().random}
    r = await asession.get(url, headers=headers)
    await r.html.arender(timeout=40, sleep=random.uniform(2, 5))
    return r.html.html


def parse_listing_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(CARD_SEL)
    rows = []

    for card in cards:
        a = card.select_one(TITLE_LINK_SEL)
        if not a:
            continue

        title = a.get_text(" ", strip=False) or a.get("title") or ""
        href = a.get("href")
        url = urljoin(BASE_URL, href) if href else None

        comp_el = card.select_one(COMPANY_SEL)
        company = comp_el.get_text(" ", strip=False) if comp_el else None

        date_el = card.select_one(DATE_SEL)
        date_added = date_el.get_text(" ", strip=False) if date_el else None

        if title and url:
            rows.append({
                "title_raw": title,
                "company_raw": company,
                "date_added_raw": date_added,
                "url": url
            })
    return rows


async def main():
    asession = AsyncHTMLSession()
    all_rows = []

    for page in range(1, MAX_PAGES + 1):
        try:
            html = await fetch_page(asession, page)
            page_rows = parse_listing_html(html)
            print(f"✅  Page {page}: {len(page_rows)} offers")
            if not page_rows:
                break
            all_rows.extend(page_rows)
        except Exception as e:
            print(f"⚠️  Error at {page}: {e}")
            continue

    await asession.close()
    df = pd.DataFrame(all_rows).drop_duplicates(subset=["url"]).reset_index(drop=True)
    print(f"\n📊 {len(df)} unique offers scraped")
    return df


def save_to_gcs(df):
    fs = gcsfs.GCSFileSystem(token="cloud")
    with fs.open(bucket_path, "wb") as f:
        df.to_parquet(f, index=False)
    print(f"✅ Data saved at: {bucket_path}")


if __name__ == "__main__":
    nest_asyncio.apply()
    df = asyncio.get_event_loop().run_until_complete(main())
    print(df.head())
    save_to_gcs(df)

