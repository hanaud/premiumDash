"""
Synthetic data generator for offline development.

Produces realistic price series for all tickers used in the spread config.
Each pair of legs is co-simulated so that the resulting spread has
plausible mean-reversion, volatility clustering, and occasional regime shifts
(e.g. supply disruptions widening the arb).
"""

from __future__ import annotations

import datetime as dt
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Realistic parameter sets per ticker
# ---------------------------------------------------------------------------
#  base   : starting price level
#  vol    : annualised volatility (fraction)
#  mr     : mean-reversion speed (higher = faster revert)
#  drift  : annualised drift
TICKER_PARAMS: dict[str, dict] = {
    # ── LME base metals ($/mt) ──
    "LMCADY03 Comdty": dict(base=8800, vol=0.20, mr=0.02, drift=0.02),
    "LMAHDY03 Comdty": dict(base=2350, vol=0.18, mr=0.02, drift=0.01),
    "LMZSDY03 Comdty": dict(base=2650, vol=0.22, mr=0.02, drift=0.0),
    "LMNIDY03 Comdty": dict(base=17000, vol=0.30, mr=0.02, drift=0.0),

    # ── COMEX copper (¢/lb) ──
    "HG1 Comdty": dict(base=400, vol=0.20, mr=0.02, drift=0.02),
    "HG2 Comdty": dict(base=401, vol=0.20, mr=0.02, drift=0.02),
    "HG3 Comdty": dict(base=402, vol=0.20, mr=0.02, drift=0.02),

    # ── SHFE metals (CNY/mt) ──
    "SHFCCOM1 Index": dict(base=69000, vol=0.18, mr=0.02, drift=0.02),
    "SHFCCOM2 Index": dict(base=69200, vol=0.18, mr=0.02, drift=0.02),
    "SHFCCOM3 Index": dict(base=69400, vol=0.18, mr=0.02, drift=0.02),
    "SHFACOM1 Index": dict(base=19500, vol=0.16, mr=0.02, drift=0.01),
    "SHFZCOM1 Index": dict(base=22000, vol=0.20, mr=0.02, drift=0.0),
    "SHFNICOM1 Index": dict(base=135000, vol=0.28, mr=0.02, drift=0.0),

    # ── FX ──
    "USDCNY Curncy": dict(base=7.25, vol=0.04, mr=0.05, drift=0.0),

    # ── Physical premiums ──
    "MWAP Index": dict(base=24, vol=0.35, mr=0.05, drift=0.0),
    "EUAP Index": dict(base=240, vol=0.30, mr=0.05, drift=0.0),
    "CUPPSHBI Index": dict(base=55, vol=0.25, mr=0.05, drift=0.0),

    # ── COMEX precious metals ($/oz) ──
    "GC1 Comdty": dict(base=2050, vol=0.14, mr=0.01, drift=0.04),
    "GC2 Comdty": dict(base=2058, vol=0.14, mr=0.01, drift=0.04),
    "GC3 Comdty": dict(base=2066, vol=0.14, mr=0.01, drift=0.04),
    "SI1 Comdty": dict(base=24.5, vol=0.25, mr=0.01, drift=0.02),
    "SI2 Comdty": dict(base=24.6, vol=0.25, mr=0.01, drift=0.02),
    "SI3 Comdty": dict(base=24.7, vol=0.25, mr=0.01, drift=0.02),

    # ── London spot ──
    "XAU Curncy": dict(base=2040, vol=0.14, mr=0.01, drift=0.04),
    "XAG Curncy": dict(base=24.0, vol=0.25, mr=0.01, drift=0.02),
    "XPT Curncy": dict(base=945, vol=0.20, mr=0.02, drift=0.0),
    "XPD Curncy": dict(base=1050, vol=0.30, mr=0.02, drift=-0.05),

    # ── Rates ──
    "SOFRRATE Index": dict(base=5.33, vol=0.10, mr=0.10, drift=-0.02),
}

# Co-movement groups: tickers that should be correlated
# (e.g. COMEX gold and London gold spot should track closely)
CORRELATION_GROUPS = [
    # Gold cluster (very tight: futures ≈ spot + small carry)
    ["GC1 Comdty", "GC2 Comdty", "GC3 Comdty", "XAU Curncy"],
    # Silver cluster
    ["SI1 Comdty", "SI2 Comdty", "SI3 Comdty", "XAG Curncy"],
    # Copper cluster (different units but correlated moves)
    ["HG1 Comdty", "HG2 Comdty", "HG3 Comdty", "LMCADY03 Comdty",
     "SHFCCOM1 Index", "SHFCCOM2 Index", "SHFCCOM3 Index"],
    # Aluminium cluster
    ["LMAHDY03 Comdty", "SHFACOM1 Index"],
    # Zinc cluster
    ["LMZSDY03 Comdty", "SHFZCOM1 Index"],
    # Nickel cluster
    ["LMNIDY03 Comdty", "SHFNICOM1 Index"],
]

# Carry pairs: back-month = front-month + small carry (basis).
# These are generated as front_price * (1 + carry_bps/10000) + noise
# to avoid divergence that breaks lease-rate calculations.
CARRY_PAIRS: dict[str, dict] = {
    # Back-month futures = front-month + carry
    "GC2 Comdty": dict(base_ticker="GC1 Comdty", carry_bps=40, noise_vol=0.5),
    "GC3 Comdty": dict(base_ticker="GC1 Comdty", carry_bps=85, noise_vol=0.7),
    "SI2 Comdty": dict(base_ticker="SI1 Comdty", carry_bps=50, noise_vol=0.02),
    "SI3 Comdty": dict(base_ticker="SI1 Comdty", carry_bps=105, noise_vol=0.03),
    "HG2 Comdty": dict(base_ticker="HG1 Comdty", carry_bps=30, noise_vol=0.3),
    "HG3 Comdty": dict(base_ticker="HG1 Comdty", carry_bps=65, noise_vol=0.5),
    "SHFCCOM2 Index": dict(base_ticker="SHFCCOM1 Index", carry_bps=25, noise_vol=30),
    "SHFCCOM3 Index": dict(base_ticker="SHFCCOM1 Index", carry_bps=55, noise_vol=50),
    # Spot = front-month - small EFP basis (spot trades slightly below futures)
    "XAU Curncy": dict(base_ticker="GC1 Comdty", carry_bps=-15, noise_vol=0.3),
    "XAG Curncy": dict(base_ticker="SI1 Comdty", carry_bps=-20, noise_vol=0.01),
}


def generate_demo_data(
    tickers: list[str],
    start: dt.date,
    end: dt.date,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generate synthetic daily price data for all requested tickers.

    Returns DataFrame with MultiIndex columns (ticker, field) and
    DatetimeIndex.  The series are co-simulated so that spreads
    exhibit realistic behaviour (mean-reverting, correlated legs,
    occasional regime breaks).
    """
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(start, end)
    n = len(dates)
    dt_annual = 1 / 252  # daily step in years

    # Build correlated noise for each group
    group_noise = _build_group_noise(tickers, n, rng)

    frames: dict[str, pd.DataFrame] = {}
    for ticker in tickers:
        params = TICKER_PARAMS.get(ticker)
        if params is None:
            # Unknown ticker: generate generic mean-reverting series
            params = dict(base=100, vol=0.20, mr=0.03, drift=0.0)

        base = params["base"]
        vol = params["vol"]
        mr = params["mr"]
        drift = params["drift"]

        # Mean-reverting log-price (Ornstein-Uhlenbeck)
        log_base = np.log(base)
        log_price = np.zeros(n)
        log_price[0] = log_base

        # Get correlated noise for this ticker
        noise = group_noise.get(ticker, rng.standard_normal(n))

        for i in range(1, n):
            revert = mr * (log_base - log_price[i - 1]) * dt_annual * 252
            log_price[i] = (
                log_price[i - 1]
                + (drift + revert) * dt_annual
                + vol * np.sqrt(dt_annual) * noise[i]
            )

        prices = np.exp(log_price)

        # Add occasional regime shifts for physical premiums
        if ticker in ("MWAP Index", "EUAP Index", "CUPPSHBI Index"):
            prices = _add_regime_shifts(prices, rng, magnitude=0.3)

        # For rate-like series, keep positive and bounded
        if "RATE" in ticker or "SOFR" in ticker:
            prices = np.clip(prices, 0.01, 10.0)

        frames[ticker] = pd.DataFrame(
            {"PX_LAST": prices}, index=dates
        )

    # ── Post-process carry pairs ──
    # Overwrite back-month tickers to be front-month + small carry
    for carry_ticker, cp in CARRY_PAIRS.items():
        if carry_ticker in tickers and cp["base_ticker"] in frames:
            base_prices = frames[cp["base_ticker"]]["PX_LAST"].values
            carry_frac = cp["carry_bps"] / 10_000
            noise = rng.normal(0, cp["noise_vol"], n)
            derived = base_prices * (1 + carry_frac) + noise
            frames[carry_ticker] = pd.DataFrame(
                {"PX_LAST": derived}, index=dates
            )

    combined = pd.concat(frames, axis=1)
    combined.columns.names = ["ticker", "field"]
    return combined


def _build_group_noise(
    tickers: list[str], n: int, rng: np.random.Generator
) -> dict[str, np.ndarray]:
    """
    Build correlated Gaussian noise so that legs in the same group
    share a common factor (ρ ≈ 0.85) plus an idiosyncratic component.
    """
    noise: dict[str, np.ndarray] = {}
    used = set()

    for group in CORRELATION_GROUPS:
        members = [t for t in group if t in tickers]
        if len(members) < 2:
            continue

        common_factor = rng.standard_normal(n)
        rho = 0.85
        for t in members:
            idio = rng.standard_normal(n)
            noise[t] = rho * common_factor + np.sqrt(1 - rho**2) * idio
            used.add(t)

    # Remaining tickers get independent noise
    for t in tickers:
        if t not in used:
            noise[t] = rng.standard_normal(n)

    return noise


def _add_regime_shifts(
    prices: np.ndarray, rng: np.random.Generator, magnitude: float = 0.3
) -> np.ndarray:
    """
    Inject 2–4 regime shifts (sudden jumps/drops) to simulate
    supply disruptions, tariff changes, etc.
    """
    n = len(prices)
    n_shifts = rng.integers(2, 5)
    shift_points = sorted(rng.integers(n // 5, n - n // 5, size=n_shifts))

    for pt in shift_points:
        direction = rng.choice([-1, 1])
        jump = direction * magnitude * prices[pt]
        # Shift everything after the break
        prices[pt:] += jump
        # Gradual mean-reversion back over ~60 days
        decay_len = min(60, n - pt)
        decay = np.linspace(jump * 0.3, 0, decay_len)
        prices[pt : pt + decay_len] += decay

    # Ensure positive
    prices = np.maximum(prices, prices[0] * 0.2)
    return prices
