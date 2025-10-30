"""
FINAL WORKING VERSION
"""

from prefect import flow, task, get_run_logger
import pandas as pd
import requests
from pathlib import Path
from typing import Dict


# CONFIG
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
INPUT_FILE = DATA_DIR / "local_sales.csv"
OUTPUT_FILE = DATA_DIR / "cleaned_sales.csv"
DB_FILE = BASE_DIR / "etl.db"

EXCHANGE_API = "https://api.frankfurter.app/latest"


# BUILT-IN COUNTRY DATA
COUNTRY_DATA = {
    "Kenya": {"region": "Africa", "currency": "KES"},
    "Uganda": {"region": "Africa", "currency": "UGX"},
    "Tanzania": {"region": "Africa", "currency": "TZS"},
    "Rwanda": {"region": "Africa", "currency": "RWF"},
}


def session_with_retries(retries: int = 3):
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    session = requests.Session()
    retry = Retry(total=retries, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


@task(name="load_sales_data", retries=3, retry_delay_seconds=5)
def load_sales_data(csv_path: Path = INPUT_FILE) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Loading sales data from {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows")
    return df


@task(name="fetch_api_data", retries=3, retry_delay_seconds=[5, 10, 15])
def fetch_api_data(url: str = EXCHANGE_API) -> Dict[str, float]:
    logger = get_run_logger()
    logger.info(f"Fetching exchange rates from {url}")
    session = session_with_retries()
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    rates_eur = data.get("rates", {})
    usd_rate = rates_eur.get("USD", 1.0)
    usd_rates = {cur: rate / usd_rate for cur, rate in rates_eur.items()}
    usd_rates["EUR"] = 1.0 / usd_rate
    usd_rates["USD"] = 1.0

    # Fallbacks
    fallbacks = {"KES": 0.0077, "UGX": 0.00027, "TZS": 0.00038, "RWF": 0.00077}
    usd_rates.update(fallbacks)

    logger.info(f"Fetched {len(usd_rates)} rates")
    return usd_rates


@task(name="transform_data")
def transform_data(sales_df: pd.DataFrame, rates: Dict[str, float]) -> pd.DataFrame:
    logger = get_run_logger()

    # Build country DF
    country_list = [
        {"country": name, "region": info["region"], "currency": info["currency"]}
        for name, info in COUNTRY_DATA.items()
    ]
    country_df = pd.DataFrame(country_list)

    # Clean sales
    sales = sales_df.copy()
    sales.columns = sales.columns.str.lower().str.replace(" ", "_")
    sales["amount"] = pd.to_numeric(sales["amount"], errors="coerce")
    sales = sales.dropna(subset=["amount", "country"])

    # Merge
    merged = sales.merge(country_df, on="country", how="left")
    merged["region"] = merged["region"].fillna("UNKNOWN")
    merged["currency"] = merged["currency"].fillna("USD")

    # CONVERT TO USD
    merged["usd_amount"] = merged.apply(
        lambda row: round(row["amount"] * rates.get(row["currency"], 1.0), 2), axis=1
    )

    logger.info(f"Transformed {len(merged)} rows")
    logger.info(f"Sample USD: {merged[['country', 'amount', 'usd_amount']].head().to_dict()}")
    return merged


@task(name="load_data")
def load_data(df: pd.DataFrame, csv_path: Path = OUTPUT_FILE):
    logger = get_run_logger()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    logger.info(f"Saved to {csv_path.name}")


@task(name="load_to_sqlite")
def load_to_sqlite(df: pd.DataFrame, db_path: Path = DB_FILE, table: str = "sales_enriched"):
    import sqlite3
    logger = get_run_logger()
    conn = sqlite3.connect(db_path)
    df.to_sql(table, conn, if_exists="replace", index=False)
    conn.close()
    logger.info(f"Loaded to SQLite: {db_path.name}")


@flow(name="Sales Enrichment Pipeline", log_prints=True)
def main_pipeline(save_sqlite: bool = False):
    sales_df = load_sales_data()
    rates = fetch_api_data()  # ← Now returns RATES
    enriched_df = transform_data(sales_df, rates)  # ← Pass RATES, not country data
    load_data(enriched_df)
    if save_sqlite:
        load_to_sqlite(enriched_df)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--save-sqlite", action="store_true")
    args = parser.parse_args()
    main_pipeline(save_sqlite=args.save_sqlite)
    