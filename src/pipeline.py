from prefect import flow, task
import pandas as pd
import requests
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
INPUT_FILE = DATA_DIR / "local_sales.csv"
OUTPUT_FILE = DATA_DIR / "cleaned_sales.csv"
API_URL = "https://restcountries.com/v3.1/all"


@task(retries=3, retry_delay_seconds=5)
def load_sales_data():
    """Load local CSV file into a DataFrame."""
    df = pd.read_csv(INPUT_FILE)
    print(f"✅ Loaded {len(df)} sales records.")
    return df


@task(retries=3, retry_delay_seconds=10)
def fetch_api_data(url=API_URL):
    """Fetch data from a public REST API."""
    print(f"📡 Fetching data from {url}")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    print(f"✅ Retrieved {len(data)} API records.")
    return data


@task
def transform_data(sales_df, api_data):
    """Example transformation: add region info from country API."""
    api_df = pd.json_normalize(api_data)
    country_lookup = api_df[["name.common", "region"]].rename(
        columns={"name.common": "country"}
    )
    merged = sales_df.merge(country_lookup, on="country", how="left")
    print(f"🔄 Merged dataset shape: {merged.shape}")
    return merged


@task
def load_data(df):
    """Save transformed data to CSV."""
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Saved cleaned data to {OUTPUT_FILE.name}")


@flow(name="Sales Enrichment Pipeline")
def main_pipeline(api_url: str = API_URL):
    """Main Prefect flow orchestrating the pipeline."""
    sales_df = load_sales_data()
    api_data = fetch_api_data(api_url)
    cleaned_df = transform_data(sales_df, api_data)
    load_data(cleaned_df)


if __name__ == "__main__":
    main_pipeline()
