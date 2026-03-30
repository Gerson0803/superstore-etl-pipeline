import os
from pathlib import Path

# Base directories
ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
ORIGINAL_DIR = DATA_DIR / "original"
STAGING_DIR = DATA_DIR / "staging"
PROCESSED_DIR = DATA_DIR / "processed"
WAREHOUSE_DIR = DATA_DIR / "warehouse"

# File names
RAW_CSV = ORIGINAL_DIR / "Sample - Superstore.csv"
STAGING_FILE = STAGING_DIR / "superstore_staging.parquet"
PROCESSED_FILE = PROCESSED_DIR / "superstore_processed.parquet"
WAREHOUSE_DB = WAREHOUSE_DIR / "superstore.db"

# Expected columns from Superstore
EXPECTED_COLUMNS = [
    "Row ID",
    "Order ID",
    "Order Date",
    "Ship Date",
    "Ship Mode",
    "Customer ID",
    "Customer Name",
    "Segment",
    "Country",
    "City",
    "State",
    "Postal Code",
    "Region",
    "Product ID",
    "Category",
    "Sub-Category",
    "Product Name",
    "Sales",
    "Quantity",
    "Discount",
    "Profit",
]

def ensure_directories():
    for d in [ORIGINAL_DIR, STAGING_DIR, PROCESSED_DIR, WAREHOUSE_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def get_schema():
    return EXPECTED_COLUMNS
