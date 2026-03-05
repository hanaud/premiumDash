"""
Data manager – parquet-based caching layer.

Handles:
  - Incremental fetches: only queries Bloomberg for dates after the last
    cached date.
  - Per-ticker parquet storage for fast columnar reads.
  - Staleness detection and forced refresh.
"""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

import pandas as pd

from .bbg_client import BloombergClient

logger = logging.getLogger(__name__)


class DataManager:
    """Cache Bloomberg data locally in parquet files."""

    def __init__(self, cache_dir: str | Path, client: BloombergClient):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.client = client

    # ------------------------------------------------------------------
    #  Public API
    # ------------------------------------------------------------------
    def get_history(
        self,
        tickers: list[str],
        start: dt.date,
        end: dt.date | None = None,
        fields: list[str] | None = None,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Return historical data for *tickers*, using cache where possible.

        For each ticker:
          1. Load existing parquet if present.
          2. Determine the gap between cached range and requested range.
          3. Fetch only the missing portion from Bloomberg.
          4. Merge, de-dup, and re-save the parquet.
        """
        if fields is None:
            fields = ["PX_LAST"]
        if end is None:
            end = dt.date.today()

        to_fetch: dict[str, tuple[dt.date, dt.date]] = {}
        cached_frames: dict[str, pd.DataFrame] = {}

        for ticker in tickers:
            cached = self._load_cache(ticker)

            if cached is not None and not force_refresh:
                cached_frames[ticker] = cached
                # Check if all requested fields are present in cache
                missing_fields = [f for f in fields if f not in cached.columns]
                if missing_fields:
                    # Need to re-fetch full range for the missing fields
                    to_fetch[ticker] = (start, end)
                else:
                    last_cached = cached.index.max().date()
                    # Need data after the last cached date?
                    next_day = last_cached + dt.timedelta(days=1)
                    if next_day <= end:
                        to_fetch[ticker] = (next_day, end)
                    # Need data before the first cached date?
                    first_cached = cached.index.min().date()
                    if start < first_cached:
                        to_fetch[ticker] = (start, first_cached - dt.timedelta(days=1))
            else:
                to_fetch[ticker] = (start, end)

        # Batch-fetch all missing data from Bloomberg
        if to_fetch:
            # Group by date range to minimise API calls
            range_groups: dict[tuple[dt.date, dt.date], list[str]] = {}
            for tkr, (s, e) in to_fetch.items():
                range_groups.setdefault((s, e), []).append(tkr)

            for (s, e), tkrs in range_groups.items():
                logger.info("Fetching %d tickers from BBG: %s → %s", len(tkrs), s, e)
                new_data = self.client.fetch_history(tkrs, fields, s, e)
                if new_data.empty:
                    continue

                for tkr in tkrs:
                    if tkr in new_data.columns.get_level_values(0):
                        new_slice = new_data[tkr]
                    else:
                        continue

                    # Merge with existing cache
                    if tkr in cached_frames and cached_frames[tkr] is not None:
                        merged = pd.concat([cached_frames[tkr], new_slice])
                        merged = merged[~merged.index.duplicated(keep="last")]
                        merged.sort_index(inplace=True)
                    else:
                        merged = new_slice.copy()
                        merged.sort_index(inplace=True)

                    cached_frames[tkr] = merged
                    self._save_cache(tkr, merged)

        # Assemble final output
        frames = {}
        for ticker in tickers:
            df = cached_frames.get(ticker)
            if df is not None:
                mask = (df.index >= pd.Timestamp(start)) & (df.index <= pd.Timestamp(end))
                frames[ticker] = df.loc[mask]

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, axis=1)
        combined.columns.names = ["ticker", "field"]
        return combined

    def invalidate(self, ticker: str | None = None) -> None:
        """Delete cache for a specific ticker or all tickers."""
        if ticker:
            path = self._cache_path(ticker)
            if path.exists():
                path.unlink()
                logger.info("Cache invalidated for %s", ticker)
        else:
            for f in self.cache_dir.glob("*.parquet"):
                f.unlink()
            logger.info("All cache invalidated")

    # ------------------------------------------------------------------
    #  Internal helpers
    # ------------------------------------------------------------------
    def _cache_path(self, ticker: str) -> Path:
        safe_name = ticker.replace(" ", "_").replace("/", "_")
        return self.cache_dir / f"{safe_name}.parquet"

    def _load_cache(self, ticker: str) -> pd.DataFrame | None:
        path = self._cache_path(ticker)
        if not path.exists():
            return None
        try:
            df = pd.read_parquet(path)
            df.index = pd.to_datetime(df.index)
            logger.debug("Cache hit for %s (%d rows)", ticker, len(df))
            return df
        except Exception as exc:
            logger.warning("Corrupt cache for %s, will re-fetch: %s", ticker, exc)
            path.unlink(missing_ok=True)
            return None

    def _save_cache(self, ticker: str, df: pd.DataFrame) -> None:
        path = self._cache_path(ticker)
        df.to_parquet(path, engine="pyarrow")
        logger.debug("Cache saved for %s (%d rows)", ticker, len(df))
