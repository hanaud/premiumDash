"""
Load UAE gold trade and premium data from Excel files into parquet cache.

Reads cowork/UAE_Gold_Trade_Historical_Data.xlsx and caches monthly market data
(Dubai premium, SGE premium, gold price) for integration into the SpreadEngine.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
COWORK_DIR = PROJECT_ROOT / "cowork"
CACHE_DIR = PROJECT_ROOT / "data" / "gold_trade"


def load_dubai_premium_data(
    force_refresh: bool = False,
    source_file: Optional[Path] = None,
) -> pd.DataFrame:
    """
    Load Dubai and SGE premium data from Excel and cache to parquet.

    Args:
        force_refresh: If True, re-read Excel file; otherwise use cached parquet if available
        source_file: Override default Excel file location

    Returns:
        DataFrame with columns: Date, Dubai_Premium_USD_oz, SGE_Premium_USD_oz,
        COMEX_Gold_Close_USD, and other market data from Monthly_Data sheet
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / "monthly_premiums.parquet"

    # Try cache first
    if cache_file.exists() and not force_refresh:
        try:
            df = pd.read_parquet(cache_file)
            logger.info(f"Loaded Dubai premium data from cache: {cache_file} ({len(df)} rows)")
            return df
        except Exception as e:
            logger.warning(f"Failed to load cache {cache_file}: {e}; re-reading Excel")

    # Read from Excel
    xlsx_path = source_file or COWORK_DIR / "UAE_Gold_Trade_Historical_Data.xlsx"
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Excel file not found: {xlsx_path}")

    logger.info(f"Reading gold trade data from: {xlsx_path}")

    # Read Monthly_Data sheet
    monthly = pd.read_excel(xlsx_path, sheet_name="Monthly_Data", parse_dates=["Date"])

    # Select columns we need
    cols_needed = [
        "Date",
        "Dubai_Premium_USD_oz",
        "SGE_Premium_USD_oz",
        "COMEX_Gold_Close_USD",
        "Silver_Close_USD",
        "USD_INR",
        "USD_CNY",
        "USD_TRY",
        "DXY_Index",
        "US_10Y_Yield",
        "VIX",
        "WTI_Crude_USD",
        "Gold_Silver_Ratio",
        "India_Gold_Total_Duty_Pct",
        "India_Gold_Imports_USD_Bn",
    ]

    # Only keep columns that exist
    available_cols = [c for c in cols_needed if c in monthly.columns]
    df = monthly[available_cols].copy()

    # Set Date as index and sort
    df = df.set_index("Date").sort_index()

    # Forward-fill NaN premiums (data often sparse) for smoother interpolation
    # Only for premium columns, not for price data
    premium_cols = ["Dubai_Premium_USD_oz", "SGE_Premium_USD_oz"]
    for col in premium_cols:
        if col in df.columns:
            df[col] = df[col].ffill().bfill()

    # Cache to parquet
    try:
        df.to_parquet(cache_file)
        logger.info(f"Cached {len(df)} rows to: {cache_file}")
    except Exception as e:
        logger.warning(f"Failed to cache to parquet: {e}")

    return df


def load_swiss_gold_exports(
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    Load Swiss gold exports to UAE (already cached from earlier work).

    Returns:
        DataFrame with columns: Date, net_weight_tonnes, value_usd
    """
    cache_file = CACHE_DIR / "swiss_impex_gold.parquet"

    if cache_file.exists() and not force_refresh:
        try:
            df = pd.read_parquet(cache_file)
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date").sort_index()
            logger.info(f"Loaded Swiss gold exports from cache: {len(df)} rows")
            return df
        except Exception as e:
            logger.warning(f"Failed to load Swiss cache: {e}")

    logger.info("Swiss gold export cache not found")
    return pd.DataFrame()


def load_annual_trade_flows() -> pd.DataFrame:
    """
    Load UAE annual aggregate trade flows from Excel.

    Returns:
        DataFrame with Year and annual import/export values
    """
    xlsx_path = COWORK_DIR / "UAE_Gold_Trade_Historical_Data.xlsx"
    if not xlsx_path.exists():
        logger.warning(f"Excel file not found: {xlsx_path}")
        return pd.DataFrame()

    annual = pd.read_excel(xlsx_path, sheet_name="UAE_Annual_Aggregate")
    return annual


def load_trade_partner_flows() -> pd.DataFrame:
    """
    Load UAE bilateral trade flows by partner country from Excel.

    Returns:
        DataFrame with Year and bilateral import/export columns
    """
    xlsx_path = COWORK_DIR / "UAE_Gold_Trade_Historical_Data.xlsx"
    if not xlsx_path.exists():
        logger.warning(f"Excel file not found: {xlsx_path}")
        return pd.DataFrame()

    # Try the annual version first
    try:
        partners = pd.read_excel(xlsx_path, sheet_name="Annual_Trade_By_Partner")
        return partners
    except Exception:
        # Fall back to monthly
        try:
            partners = pd.read_excel(xlsx_path, sheet_name="Monthly_Trade_Partners")
            return partners
        except Exception:
            logger.warning("Could not find trade partner data sheet")
            return pd.DataFrame()


def load_india_duty_timeline() -> pd.DataFrame:
    """
    Load India gold import duty timeline from Excel.

    Returns:
        DataFrame with Date and duty percentage columns
    """
    xlsx_path = COWORK_DIR / "UAE_Gold_Trade_Historical_Data.xlsx"
    if not xlsx_path.exists():
        logger.warning(f"Excel file not found: {xlsx_path}")
        return pd.DataFrame()

    duty = pd.read_excel(xlsx_path, sheet_name="India_Duty_Timeline", parse_dates=["Date"])
    return duty


def compute_premium_zscore(df: pd.DataFrame, window: int = 252) -> pd.Series:
    """
    Compute rolling z-score of Dubai Premium.

    Args:
        df: DataFrame with Dubai_Premium_USD_oz column
        window: Rolling window size (default 252 = 1 year of trading days)

    Returns:
        Series of z-scores
    """
    if "Dubai_Premium_USD_oz" not in df.columns:
        return pd.Series(np.nan, index=df.index)

    prem = df["Dubai_Premium_USD_oz"].dropna()
    rolling_mean = prem.rolling(window=window, min_periods=30).mean()
    rolling_std = prem.rolling(window=window, min_periods=30).std()

    zscore = (prem - rolling_mean) / rolling_std
    return zscore.reindex(df.index)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    # Test: Load and cache
    df = load_dubai_premium_data(force_refresh=True)
    print(f"\nLoaded {len(df)} rows of premium data")
    print(f"Date range: {df.index.min()} to {df.index.max()}")
    print(f"\nColumns: {df.columns.tolist()}")
    print(f"\nLast 5 rows:")
    print(df.tail())
    print(f"\nDubai Premium stats (USD/oz):")
    print(df["Dubai_Premium_USD_oz"].describe())
