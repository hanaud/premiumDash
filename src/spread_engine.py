"""
Spread calculation engine.

Reads the YAML config, fetches data via DataManager, and computes
each spread with:
  - Unit / FX conversion
  - Bloomberg snapshot field alignment (both legs use the same field)
  - Roll-adjusted returns (avoids mixing expiries in return calcs)
  - Synthetic calculations (lease rates, ratios)
  - Multi-window statistics (1W, 1M, 1Y z-scores & percentiles)
  - Percentile bands for chart visualisation
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field as dc_field
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
import yaml

from .data_manager import DataManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
#  Config model
# ---------------------------------------------------------------------------
@dataclass
class SpreadDef:
    id: str
    category: str
    name: str
    description: str
    leg1: Optional[str]
    leg2: Optional[str]
    unit: str = ""
    multiplier: float = 1.0
    invert: bool = False
    enabled: bool = True
    computation: str = "difference"  # "difference" or "ratio"
    fx_divisor: Optional[str] = None
    bbg_field: str = "PX_LAST"      # Bloomberg field for both legs
    synthetic: Optional[str] = None
    leg1_contracts: list[str] = dc_field(default_factory=list)
    extra: dict = dc_field(default_factory=dict)


@dataclass
class ExpiryResult:
    """Lightweight container for one back-month expiry spread."""
    label: str              # e.g. "HG2", "GC3"
    ticker: str             # e.g. "HG2 Comdty"
    series: pd.Series       # spread time series for this expiry
    current_value: float
    change_1d: float
    leg1_series: Optional[pd.Series] = None


@dataclass
class SpreadResult:
    definition: SpreadDef
    series: pd.Series           # time series of spread values (front month)
    current_value: float
    change_1d: float
    change_1w: float
    change_1m: float
    # --- Multi-window statistics ---
    # 1-week (5 trading days)
    percentile_1w: float
    z_score_1w: float
    mean_1w: float
    std_1w: float
    # 1-month (21 trading days)
    percentile_1m: float
    z_score_1m: float
    mean_1m: float
    std_1m: float
    # 1-year (lookback)
    percentile_1y: float
    z_score_1y: float
    mean_1y: float
    std_1y: float
    min_1y: float
    max_1y: float
    # --- Percentile bands (1Y trailing) ---
    pct_10: float = np.nan
    pct_25: float = np.nan
    pct_50: float = np.nan
    pct_75: float = np.nan
    pct_90: float = np.nan
    # --- Leg data ---
    leg1_series: Optional[pd.Series] = None
    leg2_series: Optional[pd.Series] = None
    expiry_results: list[ExpiryResult] = dc_field(default_factory=list)


class SpreadEngine:
    """Load config, fetch data, compute spreads."""

    def __init__(self, config_path: str | Path, data_manager: DataManager):
        self.config_path = Path(config_path)
        self.dm = data_manager
        self.config: dict[str, Any] = {}
        self.spread_defs: list[SpreadDef] = []
        self._load_config()

    # ------------------------------------------------------------------
    #  Config loading
    # ------------------------------------------------------------------
    def _load_config(self) -> None:
        with open(self.config_path) as f:
            self.config = yaml.safe_load(f)

        self.spread_defs = []
        for s in self.config.get("spreads", []):
            sd = SpreadDef(
                id=s["id"],
                category=s.get("category", ""),
                name=s.get("name", s["id"]),
                description=s.get("description", ""),
                leg1=s.get("leg1"),
                leg2=s.get("leg2"),
                unit=s.get("unit", ""),
                multiplier=float(s.get("multiplier", 1.0)),
                invert=bool(s.get("invert", False)),
                enabled=bool(s.get("enabled", True)),
                computation=s.get("computation", "difference"),
                fx_divisor=s.get("fx_divisor"),
                bbg_field=s.get("field", "PX_LAST"),
                synthetic=s.get("synthetic"),
                leg1_contracts=s.get("leg1_contracts", []),
            )
            self.spread_defs.append(sd)

    def reload_config(self) -> None:
        self._load_config()

    # ------------------------------------------------------------------
    #  Ticker ↔ field mapping
    # ------------------------------------------------------------------
    def ticker_field_map(self) -> dict[str, set[str]]:
        """Map each ticker to the set of Bloomberg fields it needs."""
        result: dict[str, set[str]] = {}
        for sd in self.spread_defs:
            if not sd.enabled:
                continue
            f = sd.bbg_field
            for ticker in filter(None, [sd.leg1, sd.leg2, sd.fx_divisor]):
                result.setdefault(ticker, set()).add(f)
            for contract in sd.leg1_contracts:
                result.setdefault(contract, set()).add(f)
        # Synthetic tickers (hardcoded in lease rate formulas)
        for ticker, fields_needed in self._synthetic_tickers().items():
            result.setdefault(ticker, set()).update(fields_needed)
        return result

    def _synthetic_tickers(self) -> dict[str, set[str]]:
        """Return ticker → fields needed by synthetic calculations."""
        result: dict[str, set[str]] = {}
        for sd in self.spread_defs:
            if not sd.enabled or not sd.synthetic:
                continue
            if sd.synthetic == "gold_lease_1m":
                for t in ["GC1 Comdty", "GC2 Comdty", "XAU Curncy", "SOFRRATE Index"]:
                    result.setdefault(t, set()).add("PX_LAST")
            elif sd.synthetic == "silver_lease_1m":
                for t in ["SI1 Comdty", "SI2 Comdty", "XAG Curncy", "SOFRRATE Index"]:
                    result.setdefault(t, set()).add("PX_LAST")
        return result

    def all_tickers(self) -> list[str]:
        return sorted(self.ticker_field_map().keys())

    # ------------------------------------------------------------------
    #  Main computation
    # ------------------------------------------------------------------
    def compute_all(
        self,
        start: dt.date | None = None,
        end: dt.date | None = None,
        force_refresh: bool = False,
    ) -> list[SpreadResult]:
        settings = self.config.get("settings", {})
        lookback = settings.get("dashboard", {}).get("default_lookback_days", 365)
        if end is None:
            end = dt.date.today()
        if start is None:
            start = end - dt.timedelta(days=lookback + 90)  # extra buffer for stats

        # 1. Build field requirements & fetch
        tfm = self.ticker_field_map()
        if not tfm:
            return []

        # Group tickers by field to minimise API calls
        field_to_tickers: dict[str, list[str]] = {}
        for ticker, fields_needed in tfm.items():
            for f in fields_needed:
                field_to_tickers.setdefault(f, []).append(ticker)

        # Fetch per field group
        all_raw: dict[str, pd.DataFrame] = {}
        for bbg_field, field_tickers in field_to_tickers.items():
            raw = self.dm.get_history(
                field_tickers, start, end,
                fields=[bbg_field],
                force_refresh=force_refresh,
            )
            if not raw.empty:
                all_raw[bbg_field] = raw

        if not all_raw:
            logger.warning("No data returned from DataManager")
            return []

        # 2. Build prices dict: (ticker, field) → Series
        prices: dict[tuple[str, str], pd.Series] = {}
        for bbg_field, raw in all_raw.items():
            for ticker in raw.columns.get_level_values(0).unique():
                col = raw[ticker]
                if bbg_field in col.columns:
                    prices[(ticker, bbg_field)] = col[bbg_field].dropna()

        # 3. Compute each spread
        results: list[SpreadResult] = []
        for sd in self.spread_defs:
            if not sd.enabled:
                continue
            try:
                result = self._compute_one(sd, prices, lookback)
                if result is not None:
                    results.append(result)
            except Exception:
                logger.exception("Failed to compute spread %s", sd.id)

        return results

    # ------------------------------------------------------------------
    #  Multi-window trailing statistics helper
    # ------------------------------------------------------------------
    @staticmethod
    def _trailing_stats(
        spread: pd.Series, current: float, windows: list[tuple[int, str]]
    ) -> dict[str, float]:
        """
        Compute mean, std, z-score, percentile for multiple trailing windows.
        Returns dict with keys like 'mean_1w', 'z_score_1y', etc.
        """
        from scipy import stats as sp_stats
        result: dict[str, float] = {}
        for window_days, suffix in windows:
            # Use 1.5× days to account for weekends/holidays
            cutoff = spread.index.max() - pd.Timedelta(days=int(window_days * 1.5))
            trailing = spread.loc[spread.index >= cutoff]
            mean = trailing.mean()
            std = trailing.std()
            result[f"mean_{suffix}"] = mean
            result[f"std_{suffix}"] = std
            result[f"z_score_{suffix}"] = (current - mean) / std if std > 0 else 0.0
            result[f"percentile_{suffix}"] = (
                sp_stats.percentileofscore(trailing.dropna(), current)
                if len(trailing.dropna()) > 1 else 50.0
            )
        return result

    # ------------------------------------------------------------------
    #  Single spread computation
    # ------------------------------------------------------------------
    def _compute_one(
        self,
        sd: SpreadDef,
        prices: dict[tuple[str, str], pd.Series],
        lookback: int,
    ) -> SpreadResult | None:
        # Handle synthetic spreads
        if sd.synthetic:
            return self._compute_synthetic(sd, prices, lookback)

        f = sd.bbg_field

        # Standalone series (no leg2)
        if sd.leg2 is None:
            key1 = (sd.leg1, f)
            if key1 not in prices:
                return None
            spread = prices[key1].copy()
            leg1_s = spread.copy()
            leg2_s = None
        else:
            key1 = (sd.leg1, f)
            key2 = (sd.leg2, f)
            if key1 not in prices or key2 not in prices:
                return None

            leg1_raw = prices[key1]
            leg2_raw = prices[key2]

            # ---- Same-day alignment ----
            common_idx = leg1_raw.index.intersection(leg2_raw.index)
            if common_idx.empty:
                logger.warning("No overlapping dates for %s", sd.id)
                return None

            leg1 = leg1_raw.loc[common_idx].copy()
            leg2 = leg2_raw.loc[common_idx].copy()

            # FX conversion
            if sd.fx_divisor:
                fx_key = (sd.fx_divisor, f)
                if fx_key in prices:
                    fx = prices[fx_key].reindex(common_idx).ffill()
                    leg1 = leg1 / fx

            # Apply multiplier to leg1 (unit conversion, e.g. ¢/lb → $/mt)
            leg1_converted = leg1 * sd.multiplier

            # Compute spread
            if sd.computation == "ratio":
                spread = leg1_converted / leg2
            elif sd.invert:
                spread = leg2 - leg1_converted
            else:
                spread = leg1_converted - leg2

            leg1_s = leg1_converted
            leg2_s = leg2

        # ---- Roll-aware return series ----
        spread = spread.dropna()
        if spread.empty:
            return None

        returns = spread.pct_change() if sd.computation == "ratio" else spread.diff()
        if len(returns.dropna()) > 20:
            roll_threshold = returns.std() * 4
            roll_dates = returns.abs() > roll_threshold
            returns_clean = returns.copy()
            returns_clean[roll_dates] = np.nan
        else:
            returns_clean = returns

        # ---- Current value & changes ----
        current = spread.iloc[-1]
        prev_1d = spread.iloc[-2] if len(spread) >= 2 else np.nan
        prev_1w = spread.shift(5).iloc[-1] if len(spread) >= 6 else np.nan
        prev_1m = spread.shift(21).iloc[-1] if len(spread) >= 22 else np.nan

        if sd.computation == "ratio":
            chg_1d = current / prev_1d - 1 if not np.isnan(prev_1d) and prev_1d != 0 else np.nan
            chg_1w = current / prev_1w - 1 if not np.isnan(prev_1w) and prev_1w != 0 else np.nan
            chg_1m = current / prev_1m - 1 if not np.isnan(prev_1m) and prev_1m != 0 else np.nan
        else:
            chg_1d = current - prev_1d if not np.isnan(prev_1d) else np.nan
            chg_1w = current - prev_1w if not np.isnan(prev_1w) else np.nan
            chg_1m = current - prev_1m if not np.isnan(prev_1m) else np.nan

        # ---- Multi-window trailing stats ----
        stats = self._trailing_stats(spread, current, [
            (5, "1w"), (21, "1m"), (lookback, "1y"),
        ])

        # ---- Percentile bands (1Y trailing) ----
        cutoff_1y = spread.index.max() - pd.Timedelta(days=int(lookback * 1.5))
        trailing_1y = spread.loc[spread.index >= cutoff_1y].dropna()
        if len(trailing_1y) > 1:
            pct_10 = np.nanpercentile(trailing_1y, 10)
            pct_25 = np.nanpercentile(trailing_1y, 25)
            pct_50 = np.nanpercentile(trailing_1y, 50)
            pct_75 = np.nanpercentile(trailing_1y, 75)
            pct_90 = np.nanpercentile(trailing_1y, 90)
        else:
            pct_10 = pct_25 = pct_50 = pct_75 = pct_90 = np.nan

        result = SpreadResult(
            definition=sd,
            series=spread,
            current_value=current,
            change_1d=chg_1d,
            change_1w=chg_1w,
            change_1m=chg_1m,
            # 1W
            percentile_1w=stats["percentile_1w"],
            z_score_1w=stats["z_score_1w"],
            mean_1w=stats["mean_1w"],
            std_1w=stats["std_1w"],
            # 1M
            percentile_1m=stats["percentile_1m"],
            z_score_1m=stats["z_score_1m"],
            mean_1m=stats["mean_1m"],
            std_1m=stats["std_1m"],
            # 1Y
            percentile_1y=stats["percentile_1y"],
            z_score_1y=stats["z_score_1y"],
            mean_1y=stats["mean_1y"],
            std_1y=stats["std_1y"],
            min_1y=trailing_1y.min() if len(trailing_1y) > 0 else np.nan,
            max_1y=trailing_1y.max() if len(trailing_1y) > 0 else np.nan,
            # Percentile bands
            pct_10=pct_10,
            pct_25=pct_25,
            pct_50=pct_50,
            pct_75=pct_75,
            pct_90=pct_90,
            # Legs
            leg1_series=leg1_s,
            leg2_series=leg2_s,
        )

        # ---- Multi-expiry: compute additional contracts ----
        for contract_ticker in sd.leg1_contracts:
            try:
                exp = self._compute_expiry(sd, contract_ticker, prices)
                if exp is not None:
                    result.expiry_results.append(exp)
            except Exception:
                logger.exception("Failed expiry %s for %s", contract_ticker, sd.id)

        return result

    def _compute_expiry(
        self,
        sd: SpreadDef,
        leg1_ticker: str,
        prices: dict[tuple[str, str], pd.Series],
    ) -> ExpiryResult | None:
        """Compute spread for a single back-month contract."""
        f = sd.bbg_field
        key1 = (leg1_ticker, f)
        if key1 not in prices:
            return None
        if sd.leg2 is not None and (sd.leg2, f) not in prices:
            return None

        leg1_raw = prices[key1]

        if sd.leg2 is None:
            spread = leg1_raw.copy()
            leg1_conv = spread.copy()
        else:
            leg2_raw = prices[(sd.leg2, f)]
            common_idx = leg1_raw.index.intersection(leg2_raw.index)
            if common_idx.empty:
                return None

            leg1 = leg1_raw.loc[common_idx].copy()
            leg2 = leg2_raw.loc[common_idx].copy()

            if sd.fx_divisor:
                fx_key = (sd.fx_divisor, f)
                if fx_key in prices:
                    fx = prices[fx_key].reindex(common_idx).ffill()
                    leg1 = leg1 / fx

            leg1_conv = leg1 * sd.multiplier

            if sd.computation == "ratio":
                spread = leg1_conv / leg2
            elif sd.invert:
                spread = leg2 - leg1_conv
            else:
                spread = leg1_conv - leg2

        spread = spread.dropna()
        if spread.empty:
            return None

        current = spread.iloc[-1]
        prev_1d = spread.iloc[-2] if len(spread) >= 2 else np.nan

        if sd.computation == "ratio":
            chg_1d = current / prev_1d - 1 if not np.isnan(prev_1d) and prev_1d != 0 else np.nan
        else:
            chg_1d = current - prev_1d if not np.isnan(prev_1d) else np.nan

        label = leg1_ticker.split()[0] if " " in leg1_ticker else leg1_ticker

        return ExpiryResult(
            label=label,
            ticker=leg1_ticker,
            series=spread,
            current_value=current,
            change_1d=chg_1d,
            leg1_series=leg1_conv,
        )

    # ------------------------------------------------------------------
    #  Synthetic calculations
    # ------------------------------------------------------------------
    def _compute_synthetic(
        self,
        sd: SpreadDef,
        prices: dict[tuple[str, str], pd.Series],
        lookback: int,
    ) -> SpreadResult | None:
        if sd.synthetic == "gold_lease_1m":
            return self._calc_lease_rate(sd, prices, lookback, "GC1 Comdty", "GC2 Comdty", "XAU Curncy")
        elif sd.synthetic == "silver_lease_1m":
            return self._calc_lease_rate(sd, prices, lookback, "SI1 Comdty", "SI2 Comdty", "XAG Curncy")
        return None

    def _calc_lease_rate(
        self,
        sd: SpreadDef,
        prices: dict[tuple[str, str], pd.Series],
        lookback: int,
        front_ticker: str,
        back_ticker: str,
        spot_ticker: str,
    ) -> SpreadResult | None:
        """
        Implied lease rate ≈ risk-free rate − futures basis annualised.
        Basis = (Futures / Spot − 1) × (365 / days_to_expiry).
        Approximate days_to_expiry from front–back spread tenor (~60 days).
        """
        sofr_ticker = "SOFRRATE Index"
        needed_keys = [
            (sofr_ticker, "PX_LAST"),
            (front_ticker, "PX_LAST"),
            (spot_ticker, "PX_LAST"),
        ]
        if any(k not in prices for k in needed_keys):
            return None

        sofr = prices[(sofr_ticker, "PX_LAST")]
        front = prices[(front_ticker, "PX_LAST")]
        spot = prices[(spot_ticker, "PX_LAST")]

        common = sofr.index.intersection(front.index).intersection(spot.index)
        if common.empty:
            return None

        sofr_a = sofr.loc[common]
        front_a = front.loc[common]
        spot_a = spot.loc[common]

        # Approximate annualised basis (assume ~60-day tenor for front month)
        basis_ann = (front_a / spot_a - 1) * (365 / 60) * 100  # in %

        # Lease rate = SOFR − basis
        lease = sofr_a - basis_ann
        lease = lease.dropna()

        if lease.empty:
            return None

        current = lease.iloc[-1]

        # Multi-window stats
        stats = self._trailing_stats(lease, current, [
            (5, "1w"), (21, "1m"), (lookback, "1y"),
        ])

        # Percentile bands
        cutoff_1y = lease.index.max() - pd.Timedelta(days=int(lookback * 1.5))
        trailing_1y = lease.loc[lease.index >= cutoff_1y].dropna()

        return SpreadResult(
            definition=sd,
            series=lease,
            current_value=current,
            change_1d=current - lease.iloc[-2] if len(lease) >= 2 else np.nan,
            change_1w=current - lease.shift(5).iloc[-1] if len(lease) >= 6 else np.nan,
            change_1m=current - lease.shift(21).iloc[-1] if len(lease) >= 22 else np.nan,
            percentile_1w=stats["percentile_1w"],
            z_score_1w=stats["z_score_1w"],
            mean_1w=stats["mean_1w"],
            std_1w=stats["std_1w"],
            percentile_1m=stats["percentile_1m"],
            z_score_1m=stats["z_score_1m"],
            mean_1m=stats["mean_1m"],
            std_1m=stats["std_1m"],
            percentile_1y=stats["percentile_1y"],
            z_score_1y=stats["z_score_1y"],
            mean_1y=stats["mean_1y"],
            std_1y=stats["std_1y"],
            min_1y=trailing_1y.min() if len(trailing_1y) > 0 else np.nan,
            max_1y=trailing_1y.max() if len(trailing_1y) > 0 else np.nan,
            pct_10=np.nanpercentile(trailing_1y, 10) if len(trailing_1y) > 1 else np.nan,
            pct_25=np.nanpercentile(trailing_1y, 25) if len(trailing_1y) > 1 else np.nan,
            pct_50=np.nanpercentile(trailing_1y, 50) if len(trailing_1y) > 1 else np.nan,
            pct_75=np.nanpercentile(trailing_1y, 75) if len(trailing_1y) > 1 else np.nan,
            pct_90=np.nanpercentile(trailing_1y, 90) if len(trailing_1y) > 1 else np.nan,
        )
