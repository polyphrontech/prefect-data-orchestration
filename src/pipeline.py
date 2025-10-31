from prefect import flow, task, get_run_logger
import pandas as pd
import requests
import json
from pathlib import Path
from typing import List, Dict, Any


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
INPUT_FILE = DATA_DIR / "local_sales.csv"
OUTPUT_FILE = DATA_DIR / "cleaned_sales.csv"
DB_FILE = BASE_DIR / "etl.db"

COUNTRY_API = "https://restcountries.com/v3.1/all?fields=name,region,currencies"
EXCHANGE_API = "https://api.exchangerate.host/live"
BACKUP_EXCHANGE_API = "https://open.er-api.com/v6/latest/USD"  

EXCHANGE_ACCESS_KEY = "6015f36bd07cedb55182c6f05b375a3a" 

COUNTRY_CACHE = DATA_DIR / "restcountries_cache.json"


def session_with_retries(retries: int = 3):
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# Load local CSV

@task(name="load_sales_data", retries=3, retry_delay_seconds=5)
def load_sales_data(csv_path: Path = INPUT_FILE) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Loading sales data from {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows")
    return df


# Fetch country data 
@task(name="fetch_country_data", retries=3, retry_delay_seconds=[10, 20, 30])
def fetch_country_data(url: str = COUNTRY_API) -> List[Dict]:
    logger = get_run_logger()

    if COUNTRY_CACHE.exists():
        logger.info("Loading country data from cache")
        with open(COUNTRY_CACHE, "r", encoding="utf-8") as f:
            return json.load(f)

    logger.info(f"Fetching country data from {url}")
    session = session_with_retries(retries=3)
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    COUNTRY_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with open(COUNTRY_CACHE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Fetched & cached {len(data)} countries")
    return data


# Fetch exchange rates 
@task(name="fetch_exchange_rates", retries=3, retry_delay_seconds=[5, 10, 15])
def fetch_exchange_rates(access_key: str = EXCHANGE_ACCESS_KEY) -> Dict[str, float]:
    logger = get_run_logger()
    session = session_with_retries(retries=3)

    # Try original API first
    url = f"{EXCHANGE_API}?access_key={access_key}&source=USD"
    logger.info(f"Trying original API: {url}")

    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get("success"):
            rates = data.get("rates", {})
            if len(rates) > 1:  # More than just USD
                rates["USD"] = 1.0
                logger.info(f"Original API success: {len(rates)} rates")
                return rates
            else:
                logger.warning("Original API returned only USD → using backup")
    except Exception as e:
        logger.warning(f"Original API failed ({e}) → using backup")

    # Fallback to open.er-api.com
    logger.info(f"Using backup API: {BACKUP_EXCHANGE_API}")
    resp = session.get(BACKUP_EXCHANGE_API, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if data.get("result") != "success":
        raise ValueError(f"Backup API error: {data.get('error-type')}")

    rates = data.get("rates", {})
    logger.info(f"Backup API success: {len(rates)} rates (base: USD)")

    # Confirm EA currencies
    ea_currencies = {"KES", "UGX", "TZS", "RWF"}
    missing = ea_currencies - rates.keys()
    if missing:
        raise ValueError(f"Missing currencies in backup: {missing}")

    return rates


# TRANSFORM:
@task(name="transform_data")
def transform_data(sales_df: pd.DataFrame, country_data: List[Dict], rates: Dict[str, float]) -> pd.DataFrame:
    logger = get_run_logger()

    # Parse country data
    countries = []
    for c in country_data:
        name = c.get("name", {}).get("common", "")
        region = c.get("region", "UNKNOWN")
        currencies = c.get("currencies", {})
        currency = list(currencies.keys())[0] if currencies else "USD"
        if name:
            countries.append({"country": name, "region": region, "currency": currency})
    country_df = pd.DataFrame(countries)

    # Clean sales
    sales = sales_df.copy()
    sales.columns = sales.columns.str.lower().str.replace(" ", "_")
    sales["amount"] = pd.to_numeric(sales["amount"], errors="coerce")
    sales = sales.dropna(subset=["amount", "country"])

    # Merge
    merged = sales.merge(country_df, on="country", how="left")
    merged["region"] = merged["region"].fillna("UNKNOWN")
    merged["currency"] = merged["currency"].fillna("USD")

    # Convert to USD (real rates only)
    merged["usd_amount"] = merged.apply(
        lambda row: round(row["amount"] / rates.get(row["currency"], 1.0), 2), axis=1
    )

    logger.info(f"Transformed {len(merged)} rows with real USD conversion")
    logger.info(f"Sample: {merged[['country', 'currency', 'amount', 'usd_amount']].head(2).to_dict()}")
    return merged


# LOAD:
@task(name="load_data")
def load_data(df: pd.DataFrame, csv_path: Path = OUTPUT_FILE):
    logger = get_run_logger()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    logger.info(f"Cleaned data saved to {csv_path.name}")


# Load to SQLite
@task(name="load_to_sqlite")
def load_to_sqlite(df: pd.DataFrame, db_path: Path = DB_FILE, table: str = "sales_enriched"):
    import sqlite3
    logger = get_run_logger()
    conn = sqlite3.connect(db_path)
    df.to_sql(table, conn, if_exists="replace", index=False)
    conn.close()
    logger.info(f"Data loaded to SQLite: {db_path.name}[{table}]")


@flow(name="Sales Enrichment Pipeline", log_prints=True)
def main_pipeline(save_sqlite: bool = False):
    sales_df = load_sales_data()
    country_data = fetch_country_data()
    rates = fetch_exchange_rates()
    enriched_df = transform_data(sales_df, country_data, rates)
    load_data(enriched_df)
    if save_sqlite:
        load_to_sqlite(enriched_df)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Prefect sales enrichment pipeline")
    parser.add_argument("--save-sqlite", action="store_true")
    args = parser.parse_args()

    main_pipeline(save_sqlite=args.save_sqlite)
