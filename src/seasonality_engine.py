"""
Gold trade seasonality engine.

Computes monthly seasonality heatmaps for any reporter country:
  - Average monthly import/export by partner country
  - Recent breakdown detection (deviation from seasonal norms)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .gold_trade_client import GoldTradeDataClient, GOLD_TRADING_COUNTRIES

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = PROJECT_ROOT / "data" / "gold_trade"

MONTH_NAMES = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


class SeasonalityEngine:
    """Compute gold trade seasonality maps and detect pattern breakdowns."""

    def __init__(
        self,
        proxy_url: Optional[str] = None,
        comtrade_api_key: Optional[str] = None,
    ):
        self._client = GoldTradeDataClient(
            cache_dir=CACHE_DIR,
            comtrade_api_key=comtrade_api_key,
            proxy_url=proxy_url,
        )
        # In-memory cache: {reporter_code: DataFrame}
        self._data_cache: dict[str, pd.DataFrame] = {}

    @staticmethod
    def available_countries() -> dict[str, str]:
        """Return dict of M49 code → country name for supported countries."""
        return dict(GOLD_TRADING_COUNTRIES)

    def load_country_data(
        self,
        reporter_code: str,
        start_year: int = 2018,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Load monthly partner-level trade data for a country.

        Returns DataFrame with columns:
            date, period, reporter, partner, partner_code,
            flow_code, flow, value_usd, net_weight_kg
        """
        if not force_refresh and reporter_code in self._data_cache:
            return self._data_cache[reporter_code]

        df = self._client.fetch_country_trade(
            reporter_code=reporter_code,
            flow="MX",
            start_year=start_year,
            force_refresh=force_refresh,
        )

        if not df.empty:
            self._data_cache[reporter_code] = df

        return df

    def compute_seasonality(
        self,
        reporter_code: str,
        flow_code: str = "M",
        metric: str = "value_usd",
        top_n: int = 15,
        start_year: int = 2018,
    ) -> pd.DataFrame:
        """
        Compute average monthly trade by partner for a focus country.

        Parameters
        ----------
        reporter_code : str
            M49 code of the focus country.
        flow_code : str
            'M' for imports, 'X' for exports.
        metric : str
            'value_usd' or 'net_weight_kg'.
        top_n : int
            Number of top partner countries to include.
        start_year : int
            First year to include in the historical average.

        Returns
        -------
        DataFrame with index=partner countries, columns=month names (Jan..Dec),
        values=historical average of the metric.
        """
        df = self.load_country_data(reporter_code, start_year=start_year)
        if df.empty:
            return pd.DataFrame()

        # Filter by flow
        mask = df["flow_code"] == flow_code
        subset = df.loc[mask].copy()

        if subset.empty or metric not in subset.columns:
            return pd.DataFrame()

        # Exclude "World" aggregate row
        if "partner" in subset.columns:
            subset = subset[~subset["partner"].str.contains("World", case=False, na=False)]

        # Extract month number
        subset["month"] = subset["date"].dt.month

        # Find top N partners by total trade value
        partner_totals = (
            subset.groupby("partner")[metric]
            .sum()
            .nlargest(top_n)
        )
        top_partners = partner_totals.index.tolist()

        # Filter to top partners and compute monthly averages
        subset = subset[subset["partner"].isin(top_partners)]
        pivot = (
            subset.groupby(["partner", "month"])[metric]
            .mean()
            .unstack(fill_value=0)
        )

        # Rename columns to month names
        pivot.columns = [MONTH_NAMES[m - 1] for m in pivot.columns]

        # Ensure all 12 months are present
        for m in MONTH_NAMES:
            if m not in pivot.columns:
                pivot[m] = 0.0
        pivot = pivot[MONTH_NAMES]

        # Sort rows by total descending
        pivot["_total"] = pivot.sum(axis=1)
        pivot = pivot.sort_values("_total", ascending=False).drop(columns="_total")

        return pivot

    def compute_breakdown(
        self,
        reporter_code: str,
        flow_code: str = "M",
        metric: str = "value_usd",
        top_n: int = 15,
        recent_months: int = 6,
        start_year: int = 2018,
        zscore_mode: str = "row",
    ) -> pd.DataFrame:
        """
        Detect seasonality breakdown in recent months.

        Compares recent months' values against the historical seasonal average.
        Returns a z-score-like deviation matrix.

        Parameters
        ----------
        zscore_mode : str
            'row'   — z-score computed per partner (each row normalised
                       independently). Highlights which months are unusual
                       *for that specific partner*.
            'table' — z-score computed across the entire table using a
                       single global mean/std. Highlights which (partner,
                       month) cells are the biggest absolute outliers in
                       the whole dataset.

        Returns
        -------
        DataFrame with index=partner countries, columns=month names,
        values=z-score of recent deviation from seasonal norm.
        NaN for months not yet in the recent window.
        """
        df = self.load_country_data(reporter_code, start_year=start_year)
        if df.empty:
            return pd.DataFrame()

        mask = df["flow_code"] == flow_code
        subset = df.loc[mask].copy()

        if subset.empty or metric not in subset.columns:
            return pd.DataFrame()

        if "partner" in subset.columns:
            subset = subset[~subset["partner"].str.contains("World", case=False, na=False)]

        subset["month"] = subset["date"].dt.month
        subset["year"] = subset["date"].dt.year

        # Top N partners by total
        partner_totals = subset.groupby("partner")[metric].sum().nlargest(top_n)
        top_partners = partner_totals.index.tolist()
        subset = subset[subset["partner"].isin(top_partners)]

        # Compute historical stats per (partner, month)
        hist_stats = (
            subset.groupby(["partner", "month"])[metric]
            .agg(["mean", "std", "count"])
        )

        # Get the most recent date in data
        max_date = subset["date"].max()
        cutoff = max_date - pd.DateOffset(months=recent_months)
        recent = subset[subset["date"] > cutoff]

        if recent.empty:
            return pd.DataFrame()

        # Compute recent averages per (partner, month)
        recent_avg = recent.groupby(["partner", "month"])[metric].mean()

        # Compute per-cell z-scores: (recent - historical_mean) / historical_std
        zscore_records = []
        for (partner, month), recent_val in recent_avg.items():
            if (partner, month) in hist_stats.index:
                row = hist_stats.loc[(partner, month)]
                hist_mean = row["mean"]
                hist_std = row["std"]
                if hist_std > 0 and row["count"] >= 3:
                    z = (recent_val - hist_mean) / hist_std
                else:
                    z = 0.0
                zscore_records.append({
                    "partner": partner,
                    "month": month,
                    "zscore": z,
                    "recent_val": recent_val,
                    "hist_mean": hist_mean,
                })

        if not zscore_records:
            return pd.DataFrame()

        zdf = pd.DataFrame(zscore_records)
        pivot = zdf.pivot(index="partner", columns="month", values="zscore")

        # Rename columns to month names
        pivot.columns = [MONTH_NAMES[m - 1] for m in pivot.columns]
        for m in MONTH_NAMES:
            if m not in pivot.columns:
                pivot[m] = np.nan
        pivot = pivot[MONTH_NAMES]

        # Apply global z-score normalisation if requested
        if zscore_mode == "table":
            # Re-standardise the entire matrix with a single mean/std
            vals = pivot.values.flatten()
            valid = vals[~np.isnan(vals)]
            if len(valid) > 1:
                g_mean = np.nanmean(valid)
                g_std = np.nanstd(valid, ddof=1)
                if g_std > 0:
                    pivot = (pivot - g_mean) / g_std

        # Sort by absolute deviation (most anomalous first)
        pivot["_abs_total"] = pivot.abs().sum(axis=1)
        pivot = pivot.sort_values("_abs_total", ascending=False).drop(columns="_abs_total")

        # Store latest data month as attribute so the UI can flag it
        latest_month_name = MONTH_NAMES[max_date.month - 1]
        pivot.attrs["latest_month"] = latest_month_name
        pivot.attrs["latest_date"] = max_date.strftime("%b %Y")

        return pivot

    def get_recent_vs_seasonal(
        self,
        reporter_code: str,
        flow_code: str = "M",
        metric: str = "value_usd",
        top_n: int = 15,
        recent_months: int = 6,
        start_year: int = 2018,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Return both the recent actual values and historical averages
        for the recent months window, for overlay comparison.

        Returns
        -------
        (recent_pivot, historical_pivot) — both DataFrames with
        index=partner, columns=month names within the recent window.
        """
        df = self.load_country_data(reporter_code, start_year=start_year)
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        mask = df["flow_code"] == flow_code
        subset = df.loc[mask].copy()

        if subset.empty or metric not in subset.columns:
            return pd.DataFrame(), pd.DataFrame()

        if "partner" in subset.columns:
            subset = subset[~subset["partner"].str.contains("World", case=False, na=False)]

        subset["month"] = subset["date"].dt.month

        partner_totals = subset.groupby("partner")[metric].sum().nlargest(top_n)
        top_partners = partner_totals.index.tolist()
        subset = subset[subset["partner"].isin(top_partners)]

        # Historical averages
        hist_avg = (
            subset.groupby(["partner", "month"])[metric]
            .mean()
            .unstack(fill_value=0)
        )
        hist_avg.columns = [MONTH_NAMES[m - 1] for m in hist_avg.columns]

        # Recent values
        max_date = subset["date"].max()
        cutoff = max_date - pd.DateOffset(months=recent_months)
        recent = subset[subset["date"] > cutoff]

        recent_avg = (
            recent.groupby(["partner", "month"])[metric]
            .mean()
            .unstack(fill_value=0)
        )
        if not recent_avg.empty:
            recent_avg.columns = [MONTH_NAMES[m - 1] for m in recent_avg.columns]

        return recent_avg, hist_avg
