# 🧭 Prefect Assignment: CSV + REST API Data Pipeline

## Overview
This exercise demonstrates how to orchestrate a small ETL (Extract, Transform, Load) pipeline using [Prefect](https://docs.prefect.io/).

You'll combine data from:
- A local CSV file (`data/local_sales.csv`)
- A public REST API (default: [restcountries.com](https://restcountries.com/v3.1/all))

The goal is to enrich the sales data with external attributes (e.g., region info) and save the result as `data/cleaned_sales.csv`.

---

## 🚀 Setup

### 1. Create a virtual environment
```bash
python -m venv .venv
source .venv/bin/activate   # on macOS/Linux
.venv\Scripts\activate      # on Windows
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the pipeline
```bash
python src/pipeline.py
```

You should see Prefect logs showing each task running successfully.

---

## 🧱 Tasks Breakdown
| Step | Task | Description |
|------|------|--------------|
| Extract | `load_sales_data()` | Load local CSV file into DataFrame |
| Extract | `fetch_api_data()` | Fetch country data via REST API |
| Transform | `transform_data()` | Merge sales data with API data |
| Load | `load_data()` | Save merged dataset to CSV |

---

## 🧪 Deliverables
- Working `pipeline.py` with Prefect flow
- Updated `cleaned_sales.csv`
- Optional: SQLite DB version of cleaned data
- Short explanation of what was learned

---

## 🌱 Stretch Goals
- Add retry + logging improvements  
- Parameterize API URL or date  
- Schedule flow in Prefect Cloud  
- Send Slack/email alert on success/failure
