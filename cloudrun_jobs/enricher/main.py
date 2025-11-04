import argparse
import os
import pandas as pd
import gcsfs
import pyppeteer
from google.cloud import bigquery
import nest_asyncio
nest_asyncio.apply()

from requests_html import AsyncHTMLSession
from bs4 import BeautifulSoup
import asyncio
import random
from datetime import date, timedelta
import re

# --- Fix Chromium args for Cloud Run ---
for flag in ["--no-sandbox", "--disable-dev-shm-usage"]:
    if flag in pyppeteer.launcher.DEFAULT_ARGS:
        pyppeteer.launcher.DEFAULT_ARGS.remove(flag)


# --- KONFIG ---
INPUT_PATH  = f"gs://pracuj-pl-data-lake/raw/job_listings_{date.today()}.parquet"
OUTPUT_PATH = f"gs://pracuj-pl-data-lake/enriched/job_details_enriched_{date.today()}.parquet"

PROJECT_ID = "pracuj-pl-pipeline"
DATASET_ID = "jobs_dw"
FINAL_TABLE = f"{PROJECT_ID}.{DATASET_ID}.curated_jobs"

MAX_OFFERS = 20000
CONCURRENCY = 2
LOOKBACK_DAYS = 45  # tylko ostatnie 45 dni ogłoszeń z BQ


# === FUNKCJE POMOCNICZE ===

def first_text(soup: BeautifulSoup, selectors: list[str]) -> str | None:
    for css in selectors:
        el = soup.select_one(css)
        if el:
            txt = el.get_text(" ", strip=True)
            if txt:
                return txt
    return None


def extract_offer_id(url: str) -> str | None:
    if not isinstance(url, str):
        return None
    m = re.search(r",oferta,(\d+)", url)
    return m.group(1) if m else None


def extract_valid_until(soup: BeautifulSoup) -> str | None:
    try:
        box = soup.find("div", {"data-test": "section-duration-info"})
        if not box:
            return None
        ps = [" ".join(p.stripped_strings) for p in box.find_all("p")]
        ps = [t for t in ps if t]
        if not ps:
            return None
        if len(ps) == 1:
            return ps[0].strip()
        p0, p1 = ps[0].strip(), ps[1].strip()
        if not (p1.startswith("(") and p1.endswith(")")):
            p1 = f"({p1})"
        return f"{p0} {p1}"
    except Exception:
        return None


def extract_bullet_points(soup: BeautifulSoup, section_test_name: str) -> list[str] | None:
    try:
        section = soup.find("section", {"data-test": section_test_name})
        if not section:
            return None
        items = [li.get_text(" ", strip=True) for li in section.find_all("li")]
        return [x for x in items if x] or None
    except Exception:
        return None


def extract_requirements(soup: BeautifulSoup) -> tuple[list[str] | None, list[str] | None]:
    try:
        req_section = soup.find("section", {"data-test": "section-requirements"})
        req_list = []
        if req_section:
            req_list = [
                li.get_text(" ", strip=True)
                for li in req_section.select("ul[data-test='aggregate-bullet-model'] li")
            ]
            req_list = [x for x in req_list if x]

        opt_section = soup.find(
            "div",
            {"data-test": "offer-sub-section",
             "data-scroll-id": lambda x: x and x.startswith("requirements-optional")}
        )
        opt_list = []
        if opt_section:
            opt_list = [
                li.get_text(" ", strip=True)
                for li in opt_section.select("ul[data-test='aggregate-bullet-model'] li")
            ]
            opt_list = [x for x in opt_list if x]

        return (req_list or None, opt_list or None)
    except Exception:
        return (None, None)


def extract_specialization(soup: BeautifulSoup) -> list[str] | None:
    try:
        li = soup.find("li", {"data-test": "it-specializations"})
        if not li:
            return None
        divs = li.find_all("div", recursive=False)
        text = divs[1].get_text(" ", strip=True) if len(divs) >= 2 else li.get_text(" ", strip=True)
        if not text:
            return None
        text = text.replace("Specjalizacje:", "").replace("Specializations:", "").strip()
        parts = [x.strip() for x in text.split(",") if x.strip()]
        return parts or None
    except Exception:
        return None


def extract_technologies(soup: BeautifulSoup):
    req = [el.get_text(strip=True) for el in soup.select("span[data-test='item-technologies-expected']")]
    opt = [el.get_text(strip=True) for el in soup.select("span[data-test='item-technologies-optional']")]
    return (req or None, opt or None)


# === GŁÓWNY PARSER ===

def parse_detail_html(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    title   = first_text(soup, ["h1[data-scroll-id='job-title']", "h1[data-test='text-positionName']"])
    company = first_text(soup, ["h2[data-scroll-id='employer-name']", "h2[data-test='text-employerName']"])
    salary  = first_text(soup, ["div[data-test='text-earningAmount']"])
    salary_unit = first_text(soup, [
        "div[data-scroll-id='contract-types-salary'] > span",
        "div[data-scroll-id='contract-types-salary'] span"
    ])
    valid_until_raw = extract_valid_until(soup)
    location = first_text(soup, ["li[data-test='sections-benefit-workplaces']"])
    contract = first_text(soup, ["li[data-test='sections-benefit-contracts']"])
    schedule = first_text(soup, ["li[data-test='sections-benefit-work-schedule']"])
    level    = first_text(soup, ["li[data-test='sections-benefit-employment-type-name']"])
    mode     = first_text(soup, [
        "li[data-test='sections-benefit-work-modes-hybrid']",
        "li[data-test='sections-benefit-work-modes-many']",
        "li[data-test='sections-benefit-work-modes-home-office']"
    ])
    specialization = extract_specialization(soup)
    responsibilities = extract_bullet_points(soup, "section-responsibilities")
    req_mandatory, req_optional = extract_requirements(soup)
    tech_required, tech_optional = extract_technologies(soup)

    return {
        "job_title": title,
        "company_name": company,
        "salary_raw": salary,
        "salary_unit_raw": salary_unit,
        "valid_until_raw": valid_until_raw,
        "location": location,
        "contract_type": contract,
        "work_schedule": schedule,
        "position_level": level,
        "work_mode": mode,
        "specialization": specialization,
        "responsibilities_list": responsibilities,
        "requirements_mandatory": req_mandatory,
        "requirements_optional": req_optional,
        "technologies_required": tech_required,
        "technologies_optional": tech_optional
    }


# === ASYNC SCRAPER ===

async def fetch_one(asession, url, date_added, idx: int, total: int):
    """Pobiera pojedyncze ogłoszenie i loguje postęp."""
    try:
        r = await asession.get(url)

        # retry 3x w przypadku błędu renderowania
        for attempt in range(3):
            try:
                await r.html.arender(timeout=40, sleep=random.uniform(2, 4))
                break
            except Exception as e:
                print(f"⚠️ Render error ({attempt+1}/3) for {url}: {e}")
                await asyncio.sleep(3)

        data = parse_detail_html(r.html.html)
        data["url"] = url
        data["date_added_raw"] = date_added
        data["offer_id"] = extract_offer_id(url)

        # 💬 Nowe logowanie postępu
        print(f"[{idx}/{total}] ✅ OK: {url}")

        return data

    except Exception as e:
        print(f"[{idx}/{total}] ⚠️ Błąd dla {url}: {e}")
        return None


async def main():
    fs = gcsfs.GCSFileSystem(token="cloud")
    print(f"📥 Wczytuję dane wejściowe z {INPUT_PATH} ...")
    with fs.open(INPUT_PATH, "rb") as f:
        df_in = pd.read_parquet(f)

    print(f"🔗 Przetwarzam {len(df_in)} ofert (surowych).")

    df_in["offer_id"] = df_in["url"].apply(extract_offer_id)

    bq_client = bigquery.Client(project=PROJECT_ID)
    cutoff_date = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()

    query = f"""
    SELECT DISTINCT offer_id
    FROM `{FINAL_TABLE}`
    WHERE scrap_date >= DATE('{cutoff_date}')
    """
    df_existing = bq_client.query(query).to_dataframe()
    existing_ids = set(df_existing["offer_id"].dropna().astype(str).tolist())
    print(f"📊 Znaleziono {len(existing_ids)} istniejących ofert w BQ (ostatnie {LOOKBACK_DAYS} dni).")

    df_new = (
        df_in[["url", "offer_id", "date_added_raw"]]
        .dropna(subset=["offer_id"])
        .drop_duplicates(subset=["offer_id"])
    )
    df_new = df_new[~df_new["offer_id"].isin(existing_ids)]

    print(f"🆕 Nowych ofert do scrapowania: {len(df_new)}")
    if df_new.empty:
        print("✅ Brak nowych ofert — kończę proces.")
        return pd.DataFrame()

    df_iter = df_new.head(MAX_OFFERS)
    total = len(df_iter)
    asession = AsyncHTMLSession()
    sem = asyncio.Semaphore(CONCURRENCY)
    results = []

    async def limited_fetch(idx, row):
        async with sem:
            res = await fetch_one(asession, row["url"], row["date_added_raw"], idx + 1, total)
            if res:
                results.append(res)

    # 💬 enumerate dodaje indeksy (0,1,2,...)
    await asyncio.gather(*[limited_fetch(idx, row) for idx, row in df_iter.iterrows()])
    await asession.close()

    df_out = pd.DataFrame(results)
    print(f"📊 Zebrano {len(df_out)} ofert.")

    with fs.open(OUTPUT_PATH, "wb") as f:
        df_out.to_parquet(f, index=False)
    print(f"✅ Zapisano wzbogacony plik do: {OUTPUT_PATH}")

    return df_out


if __name__ == "__main__":
    df_details = asyncio.get_event_loop().run_until_complete(main())
    if not df_details.empty:
        print("\n📋 Podgląd danych:")
        print(df_details.head())

