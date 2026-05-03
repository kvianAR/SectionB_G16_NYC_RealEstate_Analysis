"""
etl_pipeline.py
================
NYC Property Sales — End-to-End ETL Pipeline
Capstone 2 | DVA | Newton School of Technology | Group 16

Usage:
    python etl_pipeline.py

Input  : data/raw/nyc-rolling-sales.csv
Output : data/processed/nyc_sales_cleaned.csv
         data/processed/kpi_borough.csv
         data/processed/kpi_neighborhood.csv
         data/processed/kpi_monthly.csv
         data/processed/final_tableau_data.csv

Pipeline Steps:
    1.  Load raw dataset
    2.  Drop irrelevant columns
    3.  Standardise column names
    4.  Fix data types (numeric + date)
    5.  Fix hidden missing values (blank strings)
    6.  Fix year_built zeros (borough-level median imputation)
    7.  Remove non-market sales (< $10,000)
    8.  Drop null gross_square_feet rows
    9.  Remove zero gross_square_feet rows
    10. Percentile clipping on gross_square_feet (1st-99th)
    11. Remove duplicate rows
    12. Feature engineering
    13. Export cleaned dataset
    14. Compute and export KPI tables
"""

import os
import sys
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")


# ── Configuration ──────────────────────────────────────────────────

RAW_PATH       = "data/raw/nyc-rolling-sales.csv"
PROCESSED_DIR  = "data/processed"

OUTPUT_CLEANED    = os.path.join(PROCESSED_DIR, "nyc_sales_cleaned.csv")
OUTPUT_TABLEAU    = os.path.join(PROCESSED_DIR, "final_tableau_data.csv")
OUTPUT_BOROUGH    = os.path.join(PROCESSED_DIR, "kpi_borough.csv")
OUTPUT_NEIGHBOUR  = os.path.join(PROCESSED_DIR, "kpi_neighborhood.csv")
OUTPUT_MONTHLY    = os.path.join(PROCESSED_DIR, "kpi_monthly.csv")

MARKET_PRICE_THRESHOLD = 10_000   # Below this = non-market transfer
MIN_NEIGHBOURHOOD_TXN  = 20       # Minimum transactions for KPI reliability

BOROUGH_MAP = {
    1: "Manhattan",
    2: "Bronx",
    3: "Brooklyn",
    4: "Queens",
    5: "Staten Island"
}


# ── Logging helper ─────────────────────────────────────────────────

def log(step, msg, rows=None):
    row_info = f" | Rows: {rows:,}" if rows is not None else ""
    print(f"[Step {step:02d}]{row_info} — {msg}")


# ── Step functions ─────────────────────────────────────────────────

def load_raw(path):
    """Step 1 — Load raw dataset. Never modify this file."""
    if not os.path.exists(path):
        print(f"ERROR: Raw file not found at '{path}'")
        print("Please place the raw dataset at data/raw/nyc-rolling-sales.csv")
        sys.exit(1)
    df = pd.read_csv(path)
    log(1, f"Raw dataset loaded from '{path}'", rows=len(df))
    return df


def drop_irrelevant_columns(df):
    """
    Step 2 — Drop columns with no analytical value.
    - APARTMENT NUMBER : 82% blank — not relevant to borough/neighbourhood analysis
    - EASE-MENT        : 100% blank — zero information
    - Unnamed: 0       : Leftover index from previous export
    """
    drop_cols = ["APARTMENT NUMBER", "EASE-MENT", "Unnamed: 0"]
    existing  = [c for c in drop_cols if c in df.columns]
    df = df.drop(columns=existing, errors="ignore")
    log(2, f"Dropped {len(existing)} irrelevant column(s): {existing}", rows=len(df))
    return df


def standardise_column_names(df):
    """
    Step 3 — Standardise column names.
    Lowercase, strip whitespace, replace spaces with underscores.
    """
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    log(3, f"Column names standardised. Columns: {df.columns.tolist()}", rows=len(df))
    return df


def fix_data_types(df):
    """
    Step 4 — Fix data types.
    Numeric columns are stored as strings with commas or dash placeholders.
    sale_date needs datetime parsing.
    """
    # Numeric columns — strip commas, replace dash with NaN, convert
    for col in ["sale_price", "gross_square_feet", "land_square_feet"]:
        df[col] = (
            df[col].astype(str)
                   .str.replace(",", "")
                   .str.strip()
                   .replace("-", np.nan)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Date
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")

    # Year built
    df["year_built"] = pd.to_numeric(df["year_built"], errors="coerce")

    log(4, "Data types fixed: sale_price, gross_square_feet, land_square_feet → float | sale_date → datetime", rows=len(df))
    return df


def fix_hidden_missing_values(df):
    """
    Step 5 — Fix hidden missing values stored as blank strings.
    tax_class_at_present has 249 rows with ' ' (single space) instead of NaN.
    isnull() returns 0 for these — they must be stripped and replaced explicitly.
    """
    before = len(df)

    # Surface hidden blanks across all string columns
    hidden = {}
    for col in df.select_dtypes(include="object").columns:
        count = (df[col].astype(str).str.strip() == "").sum()
        if count > 0:
            hidden[col] = count

    if hidden:
        print(f"         Hidden blank entries found: {hidden}")

    # Fix tax_class_at_present specifically
    df["tax_class_at_present"] = df["tax_class_at_present"].astype(str).str.strip()
    df["tax_class_at_present"] = df["tax_class_at_present"].replace(["", "nan"], np.nan)
    df = df.dropna(subset=["tax_class_at_present"])
    df["tax_class_at_present"] = df["tax_class_at_present"].astype("category")

    log(5, f"Hidden missing values fixed. Dropped {before - len(df)} blank tax_class rows.", rows=len(df))
    return df


def fix_year_built(df):
    """
    Step 6 — Fix year_built zeros.
    Year 0 is not a valid construction year — treated as missing.
    Imputed with borough-level median (not global) to preserve geographic variation.
    Manhattan median ~1917 vs Staten Island median ~1965 — global median would distort both.
    """
    zero_count = (df["year_built"] == 0).sum()
    df["year_built"] = df["year_built"].replace(0, np.nan)
    df["year_built"] = (
        df.groupby("borough")["year_built"]
          .transform(lambda x: x.fillna(x.median()))
    )
    log(6, f"year_built: {zero_count} zero values replaced with borough-level median.", rows=len(df))
    return df


def remove_non_market_sales(df):
    """
    Step 7 — Remove non-market sales (sale_price < $10,000).
    Sales below $10,000 are intra-family transfers, estate distributions,
    or administrative entries — not arm's-length market transactions.
    A $1 or $5,000 sale in NYC does not represent what a buyer would pay.
    Industry-standard threshold: $10,000.
    """
    before = len(df)
    zero_prices   = (df["sale_price"] == 0).sum()
    null_prices   = df["sale_price"].isnull().sum()
    low_prices    = (df["sale_price"] < MARKET_PRICE_THRESHOLD).sum()

    df = df[df["sale_price"] >= MARKET_PRICE_THRESHOLD]

    log(7, f"Non-market sales removed: {before - len(df):,} rows "
        f"(zero: {zero_prices}, null: {null_prices}, sub-$10K: {low_prices})", rows=len(df))
    return df


def handle_missing_sqft(df):
    """
    Step 8 — Drop rows where gross_square_feet is null.
    gross_square_feet is the denominator of price_per_sqft — our primary KPI.
    Imputing a fabricated sqft value would produce fabricated KPIs.
    Dropping is the only analytically honest choice.
    """
    before = len(df)
    null_gross = df["gross_square_feet"].isnull().sum()
    null_land  = df["land_square_feet"].isnull().sum()
    df = df.dropna(subset=["gross_square_feet", "land_square_feet"])
    log(8, f"Dropped rows with null sqft: gross={null_gross}, land={null_land}. "
        f"Total removed: {before - len(df):,}", rows=len(df))
    return df


def remove_zero_sqft(df):
    """
    Step 9 — Remove zero gross_square_feet rows.
    Division by zero produces inf in price_per_sqft.
    """
    before = len(df)
    df = df[df["gross_square_feet"] > 0]
    log(9, f"Zero gross_square_feet rows removed: {before - len(df):,}", rows=len(df))
    return df


def remove_sqft_outliers(df):
    """
    Step 10 — Remove gross_square_feet outliers using percentile clipping.
    IQR method NOT used here — IQR produces a NEGATIVE lower fence for this column
    because the data is right-skewed (Q1=1,470, IQR=1,230 → lower = -375 sqft).
    A negative square footage is physically impossible.
    1st-99th percentile clipping is used instead.
    """
    lower = df["gross_square_feet"].quantile(0.01)
    upper = df["gross_square_feet"].quantile(0.99)
    before = len(df)

    df = df[
        (df["gross_square_feet"] >= lower) &
        (df["gross_square_feet"] <= upper)
    ]

    log(10, f"gross_square_feet outliers removed (1st-99th pct: {lower:.0f}–{upper:.0f} sqft). "
        f"Removed: {before - len(df):,}", rows=len(df))
    return df


def remove_duplicates(df):
    """Step 11 — Remove exact duplicate rows."""
    before = len(df)
    df = df.drop_duplicates()
    log(11, f"Duplicate rows removed: {before - len(df):,}", rows=len(df))
    return df


def feature_engineering(df):
    """
    Step 12 — Create derived analytical columns.
    - borough_name    : Human-readable borough name from integer code
    - price_per_sqft  : Primary value KPI — normalises price for size comparison
    - sale_year       : Year extracted from sale_date
    - sale_month      : Month extracted from sale_date
    - building_age    : Years since construction at time of sale
    - sale_season     : Categorical season label
    - price_category  : Budget-based price bin for Tableau filtering
    """
    # Borough name mapping
    df["borough_name"] = df["borough"].map(BOROUGH_MAP)

    # Date features
    df["sale_year"]  = df["sale_date"].dt.year
    df["sale_month"] = df["sale_date"].dt.month

    # Price per sqft — core KPI
    df["price_per_sqft"] = df["sale_price"] / df["gross_square_feet"]
    df["price_per_sqft"] = df["price_per_sqft"].replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["price_per_sqft"])

    # Building age
    df["building_age"] = df["sale_year"] - df["year_built"]
    df["building_age"] = df["building_age"].clip(lower=0)  # no negative ages

    # Season
    season_map = {12: "Winter", 1: "Winter", 2: "Winter",
                  3: "Spring",  4: "Spring",  5: "Spring",
                  6: "Summer",  7: "Summer",  8: "Summer",
                  9: "Fall",   10: "Fall",   11: "Fall"}
    df["sale_season"] = df["sale_month"].map(season_map)

    # Price category bins
    df["price_category"] = pd.cut(
        df["sale_price"],
        bins=[0, 300_000, 600_000, 1_000_000, float("inf")],
        labels=["Low (<$300K)", "Mid-Low ($300K-$600K)",
                "Mid-High ($600K-$1M)", "High (>$1M)"]
    )

    log(12, "Feature engineering complete: borough_name, price_per_sqft, sale_year, "
        "sale_month, building_age, sale_season, price_category", rows=len(df))
    return df


def export_cleaned(df, path):
    """Step 13 — Export the cleaned dataset to data/processed/."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    log(13, f"Cleaned dataset exported to '{path}'", rows=len(df))


# ── KPI Computation ────────────────────────────────────────────────

def compute_borough_kpis(df):
    """Borough-level KPI aggregation."""
    citywide_median = df["sale_price"].median()

    kpis = df.groupby("borough_name").agg(
        median_sale_price     = ("sale_price",       "median"),
        mean_sale_price       = ("sale_price",       "mean"),
        median_price_per_sqft = ("price_per_sqft",   "median"),
        transaction_volume    = ("sale_price",        "count"),
        avg_gross_sqft        = ("gross_square_feet", "median"),
        avg_building_age      = ("building_age",      "mean"),
        total_sales_value     = ("sale_price",        "sum"),
    ).round(2)

    kpis["affordability_index"] = (
        kpis["median_sale_price"] / citywide_median
    ).round(3)

    kpis = kpis.sort_values("median_sale_price", ascending=False)
    return kpis


def compute_neighbourhood_kpis(df):
    """Neighbourhood-level KPI aggregation."""
    citywide_median = df["sale_price"].median()

    kpis = df.groupby(["borough_name", "neighborhood"]).agg(
        median_sale_price     = ("sale_price",       "median"),
        median_price_per_sqft = ("price_per_sqft",   "median"),
        transaction_volume    = ("sale_price",        "count"),
        avg_gross_sqft        = ("gross_square_feet", "median"),
        avg_building_age      = ("building_age",      "mean"),
    ).round(2).reset_index()

    # Filter: minimum transactions for statistical reliability
    kpis = kpis[kpis["transaction_volume"] >= MIN_NEIGHBOURHOOD_TXN]
    kpis["affordability_index"] = (
        kpis["median_sale_price"] / citywide_median
    ).round(3)

    kpis = kpis.sort_values("median_price_per_sqft", ascending=False)
    return kpis


def compute_monthly_kpis(df):
    """Monthly time-series KPI aggregation."""
    kpis = df.groupby(["sale_year", "sale_month"]).agg(
        median_sale_price  = ("sale_price", "median"),
        transaction_volume = ("sale_price",  "count"),
        total_sales_value  = ("sale_price",  "sum"),
    ).round(2).reset_index()

    kpis["period"] = pd.to_datetime(
        kpis[["sale_year", "sale_month"]]
            .rename(columns={"sale_year": "year", "sale_month": "month"})
            .assign(day=1)
    )
    kpis = kpis.sort_values("period")
    return kpis


def export_kpis(df):
    """Step 14 — Compute and export all KPI tables."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Borough KPIs
    borough_kpis = compute_borough_kpis(df)
    borough_kpis.to_csv(OUTPUT_BOROUGH)
    print(f"         Borough KPIs saved: {OUTPUT_BOROUGH}")

    # Neighbourhood KPIs
    nbr_kpis = compute_neighbourhood_kpis(df)
    nbr_kpis.to_csv(OUTPUT_NEIGHBOUR, index=False)
    print(f"         Neighbourhood KPIs saved: {OUTPUT_NEIGHBOUR} ({len(nbr_kpis)} neighbourhoods)")

    # Monthly KPIs
    monthly_kpis = compute_monthly_kpis(df)
    monthly_kpis.to_csv(OUTPUT_MONTHLY, index=False)
    print(f"         Monthly KPIs saved: {OUTPUT_MONTHLY}")

    # Final Tableau-ready dataset
    tableau_cols = [
        "borough", "borough_name", "neighborhood",
        "building_class_category", "tax_class_at_present",
        "address", "zip_code",
        "residential_units", "commercial_units", "total_units",
        "gross_square_feet", "land_square_feet",
        "year_built", "building_age",
        "sale_price", "price_per_sqft", "price_category",
        "sale_date", "sale_year", "sale_month", "sale_season",
    ]
    tableau_cols = [c for c in tableau_cols if c in df.columns]
    df[tableau_cols].to_csv(OUTPUT_TABLEAU, index=False)
    print(f"         Tableau-ready dataset saved: {OUTPUT_TABLEAU}")

    log(14, "All KPI tables exported.", rows=len(df))


# ── Summary ────────────────────────────────────────────────────────

def print_summary(df):
    """Print headline KPIs for audit trail."""
    print()
    print("=" * 55)
    print("  PIPELINE COMPLETE — FINAL DATASET SUMMARY")
    print("=" * 55)
    print(f"  Total transactions    : {len(df):,}")
    print(f"  Boroughs              : {df['borough_name'].nunique()}")
    print(f"  Neighbourhoods        : {df['neighborhood'].nunique()}")
    print(f"  Date range            : {df['sale_date'].min().date()} to {df['sale_date'].max().date()}")
    print(f"  Citywide median price : ${df['sale_price'].median():,.0f}")
    print(f"  Citywide median $/sqft: ${df['price_per_sqft'].median():.2f}")
    print(f"  Columns               : {df.shape[1]}")
    print()
    print("  Borough Breakdown:")
    borough_summary = df.groupby("borough_name").agg(
        count=("sale_price", "count"),
        median_price=("sale_price", "median"),
        median_ppsf=("price_per_sqft", "median")
    )
    for borough, row in borough_summary.iterrows():
        print(f"    {borough:<15} {int(row['count']):>6,} txns | "
              f"Median: ${row['median_price']:>10,.0f} | "
              f"$/sqft: ${row['median_ppsf']:>6.0f}")
    print("=" * 55)


# ── Main ───────────────────────────────────────────────────────────

def run_pipeline():
    print()
    print("=" * 55)
    print("  NYC PROPERTY SALES — ETL PIPELINE")
    print("  DVA Capstone 2 | Group 16")
    print("=" * 55)
    print()

    # Run all steps sequentially
    df = load_raw(RAW_PATH)
    df = drop_irrelevant_columns(df)
    df = standardise_column_names(df)
    df = fix_data_types(df)
    df = fix_hidden_missing_values(df)
    df = fix_year_built(df)
    df = remove_non_market_sales(df)
    df = handle_missing_sqft(df)
    df = remove_zero_sqft(df)
    df = remove_sqft_outliers(df)
    df = remove_duplicates(df)
    df = feature_engineering(df)

    # Export
    export_cleaned(df, OUTPUT_CLEANED)
    export_kpis(df)

    # Summary
    print_summary(df)


if __name__ == "__main__":
    run_pipeline()
