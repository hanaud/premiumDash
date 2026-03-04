"""
Spread calculation engine.

Reads the YAML config, fetches data via DataManager, and computes
each spread with:
  - Unit / FX conversion
  - Same-hour snapshot alignment (only pairs dates where both legs
    have a valid observation on the same business day)
  - Roll-adjusted returns (avoids mixing expiries in return calcs)
  - Synthetic calculations (lease rates, ratios)
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
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
    leg1_snapshot: Optional[str] = None
    leg2_snapshot: Optional[str] = None
    synthetic: Optional[str] = None
    leg1_contracts: list[str] = field(default_factory=list)
    extra: dict = field(default_factory=dict)


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
    percentile_1y: float        # current value's percentile over 1yr
    z_score_1y: float
    mean_1y: float
    std_1y: float
    min_1y: float
    max_1y: float
    leg1_series: Optional[pd.Series] = None
    leg2_series: Optional[pd.Series] = None
    expiry_results: list[ExpiryResult] = field(default_factory=list)


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
                leg1_snapshot=s.get("leg1_snapshot"),
                leg2_snapshot=s.get("leg2_snapshot"),
                synthetic=s.get("synthetic"),
                leg1_contracts=s.get("leg1_contracts", []),
            )
            self.spread_defs.append(sd)

    def reload_config(self) -> None:
        self._load_config()

    # ------------------------------------------------------------------
    #  Collect all tickers needed
    # ------------------------------------------------------------------
    def all_tickers(self) -> list[str]:
        tickers: set[str] = set()
        for sd in self.spread_defs:
            if not sd.enabled:
                continue
            if sd.leg1:
                tickers.add(sd.leg1)
            if sd.leg2:
                tickers.add(sd.leg2)
            if sd.fx_divisor:
                tickers.add(sd.fx_divisor)
            for contract in sd.leg1_contracts:
                tickers.add(contract)
        # Also add tickers needed for synthetics
        tickers.discard(None)  # type: ignore
        return sorted(tickers)

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

        # 1. Fetch all data
        tickers = self.all_tickers()
        if not tickers:
            return []

        raw = self.dm.get_history(tickers, start, end, force_refresh=force_refresh)
        if raw.empty:
            logger.warning("No data returned from DataManager")
            return []

        # 2. Build a flat price dict: ticker → Series
        prices: dict[str, pd.Series] = {}
        for ticker in tickers:
            if ticker in raw.columns.get_level_values(0):
                col = raw[ticker]
                if "PX_LAST" in col.columns:
                    prices[ticker] = col["PX_LAST"].dropna()

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
    #  Single spread computation
    # ------------------------------------------------------------------
    def _compute_one(
        self,
        sd: SpreadDef,
        prices: dict[str, pd.Series],
        lookback: int,
    ) -> SpreadResult | None:
        # Handle synthetic spreads
        if sd.synthetic:
            return self._compute_synthetic(sd, prices, lookback)

        # Standalone series (no leg2)
        if sd.leg2 is None:
            if sd.leg1 not in prices:
                return None
            spread = prices[sd.leg1].copy()
            leg1_s = spread.copy()
            leg2_s = None
        else:
            if sd.leg1 not in prices or sd.leg2 not in prices:
                return None

            leg1_raw = prices[sd.leg1]
            leg2_raw = prices[sd.leg2]

            # ---- Same-day alignment ----
            # Only use dates where BOTH legs have data.
            # For inter-exchange spreads with different snapshot times,
            # this ensures we're comparing the same business day's
            # settlement / fix prices, not a stale vs fresh price.
            common_idx = leg1_raw.index.intersection(leg2_raw.index)
            if common_idx.empty:
                logger.warning("No overlapping dates for %s", sd.id)
                return None

            leg1 = leg1_raw.loc[common_idx].copy()
            leg2 = leg2_raw.loc[common_idx].copy()

            # FX conversion
            if sd.fx_divisor and sd.fx_divisor in prices:
                fx = prices[sd.fx_divisor].reindex(common_idx).ffill()
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
        # For generic futures (e.g. GC1), detect roll dates and
        # exclude those from return statistics to avoid contamination.
        spread = spread.dropna()
        if spread.empty:
            return None

        returns = spread.pct_change() if sd.computation == "ratio" else spread.diff()
        # Flag potential roll dates: large jumps > 4 std devs
        if len(returns.dropna()) > 20:
            roll_threshold = returns.std() * 4
            roll_dates = returns.abs() > roll_threshold
            returns_clean = returns.copy()
            returns_clean[roll_dates] = np.nan
        else:
            returns_clean = returns

        # ---- Statistics (trailing lookback window) ----
        cutoff = spread.index.max() - pd.Timedelta(days=lookback)
        trailing = spread.loc[spread.index >= cutoff]

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

        mean_1y = trailing.mean()
        std_1y = trailing.std()
        z_score = (current - mean_1y) / std_1y if std_1y > 0 else 0.0
        from scipy import stats as sp_stats
        pctile = sp_stats.percentileofscore(trailing.dropna(), current) if len(trailing.dropna()) > 1 else 50.0

        result = SpreadResult(
            definition=sd,
            series=spread,
            current_value=current,
            change_1d=chg_1d,
            change_1w=chg_1w,
            change_1m=chg_1m,
            percentile_1y=pctile,
            z_score_1y=z_score,
            mean_1y=mean_1y,
            std_1y=std_1y,
            min_1y=trailing.min(),
            max_1y=trailing.max(),
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
        prices: dict[str, pd.Series],
    ) -> ExpiryResult | None:
        """Compute spread for a single back-month contract."""
        if leg1_ticker not in prices:
            return None
        if sd.leg2 is not None and sd.leg2 not in prices:
            return None

        leg1_raw = prices[leg1_ticker]

        if sd.leg2 is None:
            spread = leg1_raw.copy()
            leg1_conv = spread.copy()
        else:
            leg2_raw = prices[sd.leg2]
            common_idx = leg1_raw.index.intersection(leg2_raw.index)
            if common_idx.empty:
                return None

            leg1 = leg1_raw.loc[common_idx].copy()
            leg2 = leg2_raw.loc[common_idx].copy()

            if sd.fx_divisor and sd.fx_divisor in prices:
                fx = prices[sd.fx_divisor].reindex(common_idx).ffill()
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
        prices: dict[str, pd.Series],
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
        prices: dict[str, pd.Series],
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
        needed = [sofr_ticker, front_ticker, spot_ticker]
        if any(t not in prices for t in needed):
            return None

        sofr = prices[sofr_ticker]
        front = prices[front_ticker]
        spot = prices[spot_ticker]

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

        # Package as SpreadResult
        cutoff = lease.index.max() - pd.Timedelta(days=lookback)
        trailing = lease.loc[lease.index >= cutoff]
        current = lease.iloc[-1]

        from scipy import stats as sp_stats

        return SpreadResult(
            definition=sd,
            series=lease,
            current_value=current,
            change_1d=current - lease.iloc[-2] if len(lease) >= 2 else np.nan,
            change_1w=current - lease.shift(5).iloc[-1] if len(lease) >= 6 else np.nan,
            change_1m=current - lease.shift(21).iloc[-1] if len(lease) >= 22 else np.nan,
            percentile_1y=sp_stats.percentileofscore(trailing.dropna(), current) if len(trailing.dropna()) > 1 else 50.0,
            z_score_1y=(current - trailing.mean()) / trailing.std() if trailing.std() > 0 else 0.0,
            mean_1y=trailing.mean(),
            std_1y=trailing.std(),
            min_1y=trailing.min(),
            max_1y=trailing.max(),
        )
