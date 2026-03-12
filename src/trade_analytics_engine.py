"""
Trade analytics engine for Dubai gold premium analysis.

Loads UAE gold trade data from cowork/ Excel files and computes metrics
for the Dubai Trade Analytics dashboard tab:
  - Premium dislocations (z-scores, divergences)
  - Trade flow drivers (Swiss supply, India duty regimes)
  - Supply-demand dynamics (import/export, source composition)
  - Macro correlations
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .gold_trade_loader import (
    load_dubai_premium_data,
    load_annual_trade_flows,
    load_trade_partner_flows,
    load_india_duty_timeline,
    load_swiss_gold_exports,
    compute_premium_zscore,
)

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
COWORK_DIR = PROJECT_ROOT / "cowork"


@pd.api.extensions.register_dataframe_accessor("ta")
class TradeAnalyticsAccessor:
    """Pandas accessor for trade analytics computations."""

    def __init__(self, pandas_obj):
        self._obj = pandas_obj


class TradeAnalyticsEngine:
    """Compute and cache trade analytics metrics."""

    def __init__(self):
        self.premium_data: Optional[pd.DataFrame] = None
        self.annual_data: Optional[pd.DataFrame] = None
        self.partner_data: Optional[pd.DataFrame] = None
        self.duty_timeline: Optional[pd.DataFrame] = None
        self.swiss_exports: Optional[pd.DataFrame] = None
        self._load_all_data()

    def _load_all_data(self) -> None:
        """Load all data sources."""
        logger.info("Loading trade analytics data...")
        try:
            self.premium_data = load_dubai_premium_data()
            logger.info(f"Loaded premium data: {len(self.premium_data)} rows")
        except Exception as e:
            logger.warning(f"Failed to load premium data: {e}")

        try:
            self.annual_data = load_annual_trade_flows()
            logger.info(f"Loaded annual data: {len(self.annual_data)} rows")
        except Exception as e:
            logger.warning(f"Failed to load annual data: {e}")

        try:
            self.partner_data = load_trade_partner_flows()
            logger.info(f"Loaded partner data: {len(self.partner_data)} rows")
        except Exception as e:
            logger.warning(f"Failed to load partner data: {e}")

        try:
            self.duty_timeline = load_india_duty_timeline()
            logger.info(f"Loaded duty timeline: {len(self.duty_timeline)} rows")
        except Exception as e:
            logger.warning(f"Failed to load duty timeline: {e}")

        try:
            self.swiss_exports = load_swiss_gold_exports()
            logger.info(f"Loaded Swiss exports: {len(self.swiss_exports)} rows")
        except Exception as e:
            logger.warning(f"Failed to load Swiss exports: {e}")

    # ========================================================================
    #  Premium Metrics
    # ========================================================================
    def get_premium_with_zscore(self) -> pd.DataFrame:
        """Return monthly premium data with computed z-scores."""
        if self.premium_data is None:
            return pd.DataFrame()

        df = self.premium_data.copy()
        df['Dubai_Premium_ZScore'] = compute_premium_zscore(df, window=252)
        return df

    def get_premium_divergence(self) -> pd.DataFrame:
        """Compute Dubai vs SGE premium divergence."""
        if self.premium_data is None:
            return pd.DataFrame()

        df = self.premium_data.copy()
        df['Divergence'] = df['Dubai_Premium_USD_oz'] - df['SGE_Premium_USD_oz']
        return df[['Dubai_Premium_USD_oz', 'SGE_Premium_USD_oz', 'Divergence']].dropna()

    # ========================================================================
    #  Trade Flow Metrics
    # ========================================================================
    def get_annual_trade_summary(self) -> pd.DataFrame:
        """Annual import/export volumes and net trade."""
        if self.annual_data is None:
            return pd.DataFrame()

        df = self.annual_data.copy()
        if 'Year' in df.columns:
            df = df.set_index('Year')
        return df

    def get_import_source_composition(self) -> pd.DataFrame:
        """African vs non-African import share over time."""
        if self.partner_data is None:
            return pd.DataFrame()

        df = self.partner_data.copy()

        # Identify African countries
        africa_cols = [c for c in df.columns if any(
            country in c for country in [
                'Guinea', 'Mali', 'Ghana', 'Sudan', 'Uganda',
                'Zimbabwe', 'Niger', 'Libya', 'Egypt', 'South_Africa'
            ]
        ) and c.startswith('Imp_')]

        non_africa_cols = [c for c in df.columns if c.startswith('Imp_') and c not in africa_cols]

        df['Africa_Imports'] = df[africa_cols].sum(axis=1) if africa_cols else 0
        df['Non_Africa_Imports'] = df[non_africa_cols].sum(axis=1) if non_africa_cols else 0
        df['Total_Imports'] = df['Africa_Imports'] + df['Non_Africa_Imports']
        df['Africa_Share_%'] = (df['Africa_Imports'] / df['Total_Imports'] * 100).replace([np.inf, -np.inf], np.nan)

        return df[['Year', 'Africa_Imports', 'Non_Africa_Imports', 'Africa_Share_%']]

    def get_india_export_trends(self) -> pd.DataFrame:
        """India as top export destination."""
        if self.partner_data is None:
            return pd.DataFrame()

        df = self.partner_data.copy()
        if 'Exp_India' in df.columns:
            return df[['Year', 'Exp_India']].copy()
        return pd.DataFrame()

    # ========================================================================
    #  Supply Indicators
    # ========================================================================
    def get_swiss_supply_with_premium(self) -> pd.DataFrame:
        """Swiss gold exports vs Dubai premium (monthly alignment)."""
        if self.swiss_exports is None or self.premium_data is None:
            return pd.DataFrame()

        swiss_m = self.swiss_exports[['net_weight_tonnes', 'value_usd']].copy()
        swiss_m.columns = ['Swiss_Export_Tonnes', 'Swiss_Export_Value_USD']

        # Get premium data (already date-indexed)
        prem = self.premium_data[['Dubai_Premium_USD_oz']].copy()

        # Join on date index
        merged = prem.join(swiss_m, how='inner')
        return merged.dropna(subset=['Dubai_Premium_USD_oz'])

    # ========================================================================
    #  Macro & Duty Regime Analysis
    # ========================================================================
    def get_duty_regime_premium_stats(self) -> pd.DataFrame:
        """Premium statistics grouped by India duty regime."""
        if self.duty_timeline is None or self.premium_data is None:
            return pd.DataFrame()

        prem = self.premium_data[['Dubai_Premium_USD_oz']].copy()

        # Merge premium data with duty timeline
        duty = self.duty_timeline[['Date', 'India_Gold_Total_Duty_Pct']].copy()
        duty = duty.set_index('Date')

        # Forward-fill duty to match monthly data
        combined = prem.join(duty, how='left')
        combined['Duty_Pct'] = combined['India_Gold_Total_Duty_Pct'].ffill()

        # Group by duty regime
        if combined['Duty_Pct'].notna().sum() > 0:
            stats = combined.groupby('Duty_Pct')['Dubai_Premium_USD_oz'].agg([
                'count', 'mean', 'median', 'std', 'min', 'max'
            ]).round(2)
            return stats
        return pd.DataFrame()

    def get_duty_timeline_events(self) -> pd.DataFrame:
        """Return India duty change events with dates."""
        if self.duty_timeline is None:
            return pd.DataFrame()

        df = self.duty_timeline.copy()
        df = df.sort_values('Date')

        # Identify changes
        df['Duty_Change'] = df['India_Gold_Total_Duty_Pct'].diff()
        events = df[df['Duty_Change'].notna() & (df['Duty_Change'] != 0)].copy()

        return events[['Date', 'India_Gold_Total_Duty_Pct', 'Duty_Change']].copy()

    # ========================================================================
    #  Signal Generation
    # ========================================================================
    def detect_dislocation_signals(self, zscore_threshold: float = 1.5) -> pd.DataFrame:
        """Detect premium dislocation signals (high |z-score|)."""
        df = self.get_premium_with_zscore()
        if df.empty:
            return pd.DataFrame()

        signals = df[df['Dubai_Premium_ZScore'].abs() > zscore_threshold].copy()
        signals['Signal_Type'] = signals['Dubai_Premium_ZScore'].apply(
            lambda z: 'Potential Reversion (Premium High)' if z > 0 else 'Potential Reversion (Discount Deep)'
        )
        return signals.reset_index()

    def detect_supply_demand_imbalance(self) -> dict:
        """Simple indicator: large Swiss supply spike followed by premium compression."""
        if self.swiss_exports is None:
            return {}

        swiss = self.swiss_exports[['net_weight_tonnes']].copy()

        # Compute rolling mean and detect spikes
        swiss['Rolling_Mean'] = swiss['net_weight_tonnes'].rolling(window=6, center=True).mean()
        swiss['Z_Score'] = (swiss['net_weight_tonnes'] - swiss['Rolling_Mean']) / swiss['net_weight_tonnes'].std()

        spikes = swiss[swiss['Z_Score'] > 1.5].copy()

        return {
            'total_spikes': len(spikes),
            'recent_spike': spikes.index[-1] if not spikes.empty else None,
            'avg_tonnes_spike': spikes['net_weight_tonnes'].mean() if not spikes.empty else 0,
        }

    # ========================================================================
    #  Correlation Analysis
    # ========================================================================
    def compute_macro_correlations(self) -> dict:
        """Compute correlations between Dubai premium and macro variables."""
        if self.premium_data is None or self.premium_data.empty:
            return {}

        df = self.premium_data[['Dubai_Premium_USD_oz', 'DXY_Index', 'VIX', 'US_10Y_Yield',
                                 'USD_INR', 'WTI_Crude_USD', 'Gold_Silver_Ratio']].dropna()

        if df.empty:
            return {}

        macro_cols = ['DXY_Index', 'VIX', 'US_10Y_Yield', 'USD_INR', 'WTI_Crude_USD', 'Gold_Silver_Ratio']
        corrs = {}
        for col in macro_cols:
            if col in df.columns:
                corrs[col] = df['Dubai_Premium_USD_oz'].corr(df[col])

        return corrs


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    )

    engine = TradeAnalyticsEngine()

    # Test: Dislocation signals
    signals = engine.detect_dislocation_signals()
    print(f"\nDislocation signals ({len(signals)} events):")
    if not signals.empty:
        print(signals[['Date', 'Dubai_Premium_USD_oz', 'Dubai_Premium_ZScore', 'Signal_Type']].tail())

    # Test: Duty regime stats
    duty_stats = engine.get_duty_regime_premium_stats()
    print(f"\nPremium stats by India duty regime:")
    print(duty_stats)

    # Test: Macro correlations
    corrs = engine.compute_macro_correlations()
    print(f"\nMacro correlations with Dubai premium:")
    for var, corr in sorted(corrs.items(), key=lambda x: abs(x[1]), reverse=True):
        print(f"  {var:20s}: {corr:+.3f}")
