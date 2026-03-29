"""
GACC (China Customs) Data Extraction Guide
===========================================

The GACC Interactive Tables at stats.customs.gov.cn have HS-code level
trade data for China, including 2025 and 2026 data. However, the site
is WAF-protected and can't be scraped programmatically.

This script helps you:
  1. Understand what queries to run in the browser
  2. Process the pasted/downloaded data into CSV
  3. Ingest CSVs into the pipeline (converts to parquet, deduplicates)

WORKFLOW
--------
  1. Open browser to: http://stats.customs.gov.cn/indexEn
  2. Click into the iframe query form
  3. Set parameters (see QUERIES below)
  4. Click "Enquiry"
  5. Copy the results table → paste into a CSV file in data/gold_trade/gacc/
  6. Run this script to ingest

QUERIES TO RUN
--------------
  Gold Imports:   year=2025, startMonth=1, endMonth=12, commodity=7108, flow=Import(1), currency=USD, monthly=checked
  Gold Exports:   year=2025, startMonth=1, endMonth=12, commodity=7108, flow=Export(0), currency=USD, monthly=checked
  Silver Imports: year=2025, startMonth=1, endMonth=12, commodity=7106, flow=Import(1), currency=USD, monthly=checked
  Silver Exports: year=2025, startMonth=1, endMonth=12, commodity=7106, flow=Export(0), currency=USD, monthly=checked

  Repeat for 2026 (months 1-2 available as of Mar 2026).

CSV FORMAT
----------
Save each query result as a CSV with these columns:
  partner,period,value_usd,net_weight_kg

  partner    = country name (e.g. "Switzerland", "Australia")
  period     = YYYYMM format (e.g. 202501)
  value_usd  = trade value in USD
  net_weight_kg = weight in kg (optional)

Naming convention:
  gacc_7108_M_2025.csv    (Gold imports 2025)
  gacc_7108_X_2025.csv    (Gold exports 2025)
  gacc_7106_M_2025.csv    (Silver imports 2025)
  gacc_7106_X_2025.csv    (Silver exports 2025)

Place files in: data/gold_trade/gacc/
"""

from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.comtrade_bulk_client import ComtradeBulkClient


def ingest_all_gacc_csvs():
    """Find and ingest all GACC CSV files in data/gold_trade/gacc/."""
    gacc_dir = PROJECT_ROOT / "data" / "gold_trade" / "gacc"
    if not gacc_dir.exists():
        print(f"GACC directory not found: {gacc_dir}")
        print("Create it and place your CSV files there.")
        return

    csv_files = sorted(gacc_dir.glob("gacc_*.csv"))
    if not csv_files:
        print(f"No gacc_*.csv files found in {gacc_dir}")
        print("See the QUERIES section above for what to extract.")
        return

    client = ComtradeBulkClient()

    for csv_path in csv_files:
        # Parse hs_code and flow from filename: gacc_{hs}_{flow}_{year}.csv
        parts = csv_path.stem.split("_")
        if len(parts) < 3:
            print(f"  Skipping {csv_path.name} — unexpected name format")
            continue

        hs_code = parts[1]  # e.g. "7108"
        flow_code = parts[2]  # e.g. "M" or "X"

        print(f"  Ingesting {csv_path.name} (HS {hs_code}, flow {flow_code})...")
        try:
            df = client.ingest_gacc_csv(csv_path, hs_code=hs_code, flow_code=flow_code)
            print(f"    → {len(df)} records, date range: {df['date'].min()} → {df['date'].max()}")
        except Exception as e:
            print(f"    ERROR: {e}")


def show_gacc_coverage():
    """Show what GACC data is already cached."""
    client = ComtradeBulkClient()
    info = client.get_available_cache_info()
    gacc_files = [f for f in info if f.get("source") == "gacc"]

    if not gacc_files:
        print("No GACC data cached yet.")
        print("Run the browser queries and save CSVs to data/gold_trade/gacc/")
        return

    print(f"\nGACC cached files ({len(gacc_files)}):")
    for f in gacc_files:
        print(f"  {f['file']:40s}  {f['commodity']:8s}  {f['size_kb']:.1f} KB")


def compare_sources():
    """Compare data coverage across Comtrade vs GACC vs Mirror for China."""
    client = ComtradeBulkClient()
    df = client.load_all_cached(countries=["156"])

    if df.empty:
        print("No China data loaded.")
        return

    if "source" not in df.columns:
        df["source"] = "comtrade"

    print(f"\nChina data coverage ({len(df):,} total records):")
    print(f"{'Source':<12} {'Records':>8} {'Date Range':>25} {'Commodities'}")
    print("-" * 70)
    for src, group in df.groupby("source"):
        commodities = ", ".join(sorted(group["commodity"].unique()))
        date_range = f"{group['date'].min().strftime('%Y-%m')} → {group['date'].max().strftime('%Y-%m')}"
        print(f"{src:<12} {len(group):>8,} {date_range:>25} {commodities}")

    # Show overlap
    if "source" in df.columns and df["source"].nunique() > 1:
        dedup_cols = ["period", "partner_code", "flow_code", "hs_code"]
        existing = [c for c in dedup_cols if c in df.columns]
        if existing:
            total_before = len(df)
            total_after = len(df.drop_duplicates(subset=existing))
            overlap = total_before - total_after
            print(f"\nOverlap (duplicate records across sources): {overlap:,}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="GACC data management")
    parser.add_argument("action", choices=["ingest", "coverage", "compare"],
                       help="ingest: process CSVs → parquet; coverage: show cached GACC; compare: cross-source comparison")
    args = parser.parse_args()

    if args.action == "ingest":
        ingest_all_gacc_csvs()
    elif args.action == "coverage":
        show_gacc_coverage()
    elif args.action == "compare":
        compare_sources()
