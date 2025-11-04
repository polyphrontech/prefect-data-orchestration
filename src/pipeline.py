from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path
from typing import List, Dict

import pandas as pd
import requests
from prefect import flow, task, get_run_logger
from prefect.cache_policies import CachePolicy

# Configuration & Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
INPUT_FILE = DATA_DIR / "local_sales.csv"
OUTPUT_FILE = DATA_DIR / "cleaned_sales.csv"
DB_FILE = BASE_DIR / "etl.db"

# APIs
COUNTRY_API = "https://restcountries.com/v3.1/all?fields=name,region,currencies"
EXCHANGE_API = "https://api.exchangerate.host/live"

# fetching our exchange access key from .env
from dotenv import load_dotenv
import os
load_dotenv()
EXCHANGE_ACCESS_KEY = os.getenv("EXCHANGE_API_KEY")

if not EXCHANGE_ACCESS_KEY:
    raise ValueError("Missing EXCHANGE_API_KEY in .env file")

# Task: Load local sales CSV
@task(name="load_sales_data", retries=3, retry_delay_seconds=5)
def load_sales_data(csv_path: Path = INPUT_FILE) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Loading sales data from {csv_path}")

    if not csv_path.exists():
        raise FileNotFoundError(f"Input file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows from CSV")
    return df

def country_data_cache_key(task, inputs, **kwargs) -> str:
    """Static cache key — data reused for 7 days"""
    return "restcountries-v1"

# Task: Fetch country data (cached for 7 days)
@task(
    name="fetch_country_data",
    retries=3,
    retry_delay_seconds=[10, 20, 30],
    cache_key_fn=country_data_cache_key,
    cache_expiration=timedelta(days=7),
)
def fetch_country_data(url: str = COUNTRY_API) -> List[Dict]:
    logger = get_run_logger()
    logger.info(f"Fetching country data from {url}")

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    logger.info(f"Fetched {len(data)} countries")
    return data

# Fetch exchange rates 
@task(name="fetch_exchange_rates", retries=3, retry_delay_seconds=[5, 10, 15])
def fetch_exchange_rates(access_key: str = EXCHANGE_ACCESS_KEY) -> Dict[str, float]:
    """
    Fetch live exchange rates from exchangerate.host.
    Returns a dict like: {'AED': 3.672501, 'EUR': 0.86833, ...}
    """
    logger = get_run_logger()
    url = f"{EXCHANGE_API}?access_key={access_key}&source=USD"
    logger.info(f"Fetching exchange rates from: {url}")

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if not data.get("success"):
        error_msg = data.get("error", {}).get("info", "Unknown error")
        raise ValueError(f"Exchange rate API failed: {error_msg}")

    quotes = data.get("quotes", {})
    if not quotes:
        raise ValueError("No exchange rates found in API response")

    # Convert "USDAED" → "AED", "USDEUR" → "EUR", etc.
    rates = {code[3:]: rate for code, rate in quotes.items() if code.startswith("USD")}
    
    # Ensure USD is 1.0
    rates["USD"] = 1.0

    logger.info(f"Successfully fetched {len(rates)} exchange rates (base: USD)")
    return rates

# Transform & enrich data
@task(name="transform_data")
def transform_data(
    sales_df: pd.DataFrame,
    country_data: List[Dict],
    rates: Dict[str, float],
) -> pd.DataFrame:
    logger = get_run_logger()

    # Build country lookup table
    countries = []
    for c in country_data:
        name = c.get("name", {}).get("common", "")
        region = c.get("region", "UNKNOWN")
        currencies = c.get("currencies", {})
        currency_code = list(currencies.keys())[0] if currencies else "USD"
        if name:
            countries.append({"country": name, "region": region, "currency": currency_code})

    country_df = pd.DataFrame(countries)
    logger.info(f"Parsed {len(country_df)} countries from API")

    # Clean sales data
    sales = sales_df.copy()
    sales.columns = sales.columns.str.lower().str.replace(r"\s+", "_", regex=True)
    sales["amount"] = pd.to_numeric(sales["amount"], errors="coerce")
    sales = sales.dropna(subset=["amount", "country"])

    logger.info(f"After cleaning: {len(sales)} valid rows")

    # Merge with country data
    merged = sales.merge(country_df, on="country", how="left")
    merged["region"] = merged["region"].fillna("UNKNOWN")
    merged["currency"] = merged["currency"].fillna("USD")

    # Convert to USD
    def convert_to_usd(row):
        rate = rates.get(row["currency"], 1.0)
        return round(row["amount"] / rate, 2) if rate > 0 else 0.0

    merged["usd_amount"] = merged.apply(convert_to_usd, axis=1)

    logger.info(f"Transformed {len(merged)} rows with USD conversion")
    logger.debug(f"Sample:\n{merged[['country', 'currency', 'amount', 'usd_amount']].head(2)}")

    return merged

# Save cleaned CSV
@task(name="save_cleaned_csv")
def save_cleaned_csv(df: pd.DataFrame, csv_path: Path = OUTPUT_FILE) -> None:
    logger = get_run_logger()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    logger.info(f"Cleaned data saved to {csv_path}")


# Load to SQLite
@task(name="load_to_sqlite")
def load_to_sqlite(
    df: pd.DataFrame,
    db_path: Path = DB_FILE,
    table_name: str = "sales_enriched",
) -> None:
    import sqlite3

    logger = get_run_logger()
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    logger.info(f"Data loaded to SQLite: {db_path.name}[{table_name}]")


# Main Flow
@flow(name="Sales Enrichment Pipeline", log_prints=True)
def sales_enrichment_pipeline(save_sqlite: bool = False) -> None:
    # Extract
    sales_df = load_sales_data()
    country_data = fetch_country_data()
    exchange_rates = fetch_exchange_rates()

    # Transform
    enriched_df = transform_data(sales_df, country_data, exchange_rates)

    # Load
    save_cleaned_csv(enriched_df)
    if save_sqlite:
        load_to_sqlite(enriched_df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run the sales data enrichment pipeline with Prefect"
    )
    parser.add_argument(
        "--save-sqlite",
        action="store_true",
        help="Also load enriched data into SQLite",
    )
    args = parser.parse_args()

    sales_enrichment_pipeline(save_sqlite=args.save_sqlite)