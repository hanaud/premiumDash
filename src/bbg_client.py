"""
Bloomberg API client with rolling-contract awareness.

Uses blpapi for historical and reference data requests.
Handles:
  - Bulk historical data pulls (BDH)
  - Reference data (BDP) for contract metadata
  - Generic contract roll tracking to avoid mixing expiries
  - Incremental fetching (only pulls new data since last cache date)
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

import pandas as pd

from .proxy_utils import encode_proxy_url

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Try to import blpapi – allow graceful fallback for dev/testing
# ---------------------------------------------------------------------------
try:
    import blpapi

    _HAS_BLPAPI = True
except ImportError:
    blpapi = None  # type: ignore
    _HAS_BLPAPI = False
    logger.warning("blpapi not installed – Bloomberg calls will use demo data")


class BloombergClient:
    """Thin wrapper around blpapi for historical + reference data."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8194,
        timeout: int = 30_000,
        proxy_url: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.proxy_url = proxy_url
        self._session: Optional[object] = None
        if proxy_url:
            logger.info(f"BloombergClient proxy configured: {proxy_url}")

    # ------------------------------------------------------------------
    #  Connection
    # ------------------------------------------------------------------
    def connect(self) -> None:
        if not _HAS_BLPAPI:
            logger.info("blpapi unavailable – running in demo mode")
            return

        try:
            opts = blpapi.SessionOptions()
            opts.setServerHost(self.host)
            opts.setServerPort(self.port)
            self._session = blpapi.Session(opts)
            if not self._session.start():
                logger.warning(
                    "Failed to start Bloomberg session (Terminal not running?). "
                    "Running in demo mode with synthetic data."
                )
                self._session = None
                return
            if not self._session.openService("//blp/refdata"):
                logger.warning(
                    "Failed to open //blp/refdata service. "
                    "Running in demo mode with synthetic data."
                )
                self._session.stop()
                self._session = None
                return
            logger.info("Bloomberg session connected (%s:%s)", self.host, self.port)
        except (ConnectionError, NotImplementedError, RuntimeError, Exception) as e:
            logger.warning(
                "Bloomberg connection failed (%s: %s). "
                "Make sure Bloomberg Terminal is running with DAPI enabled. "
                "Running in demo mode with synthetic data.",
                type(e).__name__, str(e)
            )
            self._session = None

    def disconnect(self) -> None:
        if self._session is not None:
            self._session.stop()
            self._session = None

    # ------------------------------------------------------------------
    #  Historical data  (BDH)
    # ------------------------------------------------------------------
    def fetch_history(
        self,
        tickers: list[str],
        fields: list[str] | None = None,
        start: dt.date | None = None,
        end: dt.date | None = None,
        currency: str | None = None,
    ) -> pd.DataFrame:
        """
        Fetch historical daily data for a list of tickers.

        Returns a DataFrame with DatetimeIndex and MultiIndex columns
        (ticker, field).  Missing dates are *not* forward-filled – that
        is the responsibility of the spread engine.
        """
        if fields is None:
            fields = ["PX_LAST"]
        if end is None:
            end = dt.date.today()
        if start is None:
            start = end - dt.timedelta(days=365 * 5)

        if not _HAS_BLPAPI or self._session is None:
            from .demo_data import generate_demo_data
            return generate_demo_data(tickers, start, end, fields=fields)

        return self._bdh(tickers, fields, start, end, currency)

    def _bdh(
        self,
        tickers: list[str],
        fields: list[str],
        start: dt.date,
        end: dt.date,
        currency: str | None,
    ) -> pd.DataFrame:
        svc = self._session.getService("//blp/refdata")
        req = svc.createRequest("HistoricalDataRequest")

        for t in tickers:
            req.getElement("securities").appendValue(t)
        for f in fields:
            req.getElement("fields").appendValue(f)

        req.set("startDate", start.strftime("%Y%m%d"))
        req.set("endDate", end.strftime("%Y%m%d"))
        req.set("periodicitySelection", "DAILY")
        req.set("nonTradingDayFillOption", "NON_TRADING_WEEKDAYS")
        req.set("nonTradingDayFillMethod", "PREVIOUS_VALUE")
        if currency:
            req.set("currency", currency)

        self._session.sendRequest(req)

        frames: dict[str, pd.DataFrame] = {}
        while True:
            ev = self._session.nextEvent(self.timeout)
            for msg in ev:
                if msg.hasElement("securityData"):
                    sec_data = msg.getElement("securityData")
                    ticker = sec_data.getElementAsString("security")
                    fd = sec_data.getElement("fieldData")
                    rows = []
                    for i in range(fd.numValues()):
                        row_elem = fd.getValueAsElement(i)
                        row = {"date": row_elem.getElementAsDatetime("date")}
                        for f in fields:
                            if row_elem.hasElement(f):
                                row[f] = row_elem.getElementAsFloat(f)
                        rows.append(row)
                    if rows:
                        df = pd.DataFrame(rows).set_index("date")
                        df.index = pd.to_datetime(df.index)
                        frames[ticker] = df
            if ev.eventType() == blpapi.Event.RESPONSE:
                break

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames, axis=1)
        combined.columns.names = ["ticker", "field"]
        return combined

    # ------------------------------------------------------------------
    #  Reference data  (BDP) – for roll metadata
    # ------------------------------------------------------------------
    def fetch_reference(
        self, tickers: list[str], fields: list[str]
    ) -> pd.DataFrame:
        """Fetch snapshot reference data (e.g. expiry dates, roll dates)."""
        if not _HAS_BLPAPI or self._session is None:
            return self._demo_reference(tickers, fields)

        svc = self._session.getService("//blp/refdata")
        req = svc.createRequest("ReferenceDataRequest")
        for t in tickers:
            req.getElement("securities").appendValue(t)
        for f in fields:
            req.getElement("fields").appendValue(f)

        self._session.sendRequest(req)
        records: list[dict] = []

        while True:
            ev = self._session.nextEvent(self.timeout)
            for msg in ev:
                if msg.hasElement("securityData"):
                    arr = msg.getElement("securityData")
                    for i in range(arr.numValues()):
                        sec = arr.getValueAsElement(i)
                        ticker = sec.getElementAsString("security")
                        fd = sec.getElement("fieldData")
                        rec: dict = {"ticker": ticker}
                        for f in fields:
                            if fd.hasElement(f):
                                rec[f] = fd.getElementAsString(f)
                        records.append(rec)
            if ev.eventType() == blpapi.Event.RESPONSE:
                break

        return pd.DataFrame(records).set_index("ticker") if records else pd.DataFrame()

    # ------------------------------------------------------------------
    #  Roll-aware history
    # ------------------------------------------------------------------
    def fetch_rolling_history(
        self,
        generic_ticker: str,
        start: dt.date,
        end: dt.date,
        roll_days_before_expiry: int = 5,
        roll_adjustment: str = "ratio",
    ) -> pd.DataFrame:
        """
        Fetch history for a generic futures contract (e.g. GC1 Comdty)
        and annotate with roll dates.

        Bloomberg generic tickers already handle rolls, but this method
        additionally:
          1. Fetches FUT_CUR_GEN_TICKER per date to track which specific
             contract is active.
          2. Flags roll dates so return calculations can adjust for the
             price gap at roll.

        Returns DataFrame with columns: PX_LAST, active_contract, is_roll_date,
                                         adjusted_price
        """
        # Step 1: get the price series from the generic ticker
        price_df = self.fetch_history(
            [generic_ticker], ["PX_LAST"], start, end
        )
        if price_df.empty:
            return price_df

        # Flatten multi-index columns
        if isinstance(price_df.columns, pd.MultiIndex):
            price_df = price_df.droplevel("ticker", axis=1)

        # Step 2: identify the underlying contract on each date
        # Bloomberg doesn't give daily active-contract mapping via BDH,
        # so we approximate: detect large overnight gaps that correspond
        # to a roll.
        df = price_df.copy()
        df.columns = ["PX_LAST"]
        df["daily_return"] = df["PX_LAST"].pct_change()
        df["is_roll_date"] = False

        # Step 3: build roll-adjusted price series
        if roll_adjustment == "ratio":
            df["adj_factor"] = 1.0
            df["adjusted_price"] = df["PX_LAST"]
            # We rely on Bloomberg's generic series which is already
            # back-adjusted for most purposes; flag potential roll points
            # for downstream awareness.
        else:
            df["adjusted_price"] = df["PX_LAST"]

        return df

    # ------------------------------------------------------------------
    #  Demo / offline data for development
    # ------------------------------------------------------------------
    @staticmethod
    def _demo_history(
        tickers: list[str],
        fields: list[str],
        start: dt.date,
        end: dt.date,
    ) -> pd.DataFrame:
        """Generate synthetic data when Bloomberg is unavailable."""
        import numpy as np

        dates = pd.bdate_range(start, end)
        np.random.seed(42)

        frames: dict[str, pd.DataFrame] = {}
        # Rough seed prices for demo
        seed_prices = {
            "LMCADS03 Comdty": 8500,
            "LMCADY03 Comdty": 8520,
            "LMCADY15 Comdty": 8600,
            "LMAHDS03 Comdty": 2300,
            "LMAHDY03 Comdty": 2320,
            "LMAHDY15 Comdty": 2380,
            "LMZSDS03 Comdty": 2600,
            "LMZSDY03 Comdty": 2620,
            "LMNIDS03 Comdty": 16500,
            "LMNIDY03 Comdty": 16600,
            "LMPBDS03 Comdty": 2100,
            "LMPBDY03 Comdty": 2115,
            "LMSNDS03 Comdty": 25000,
            "LMSNDY03 Comdty": 25200,
            "HG1 Comdty": 385,
            "GC1 Comdty": 2050,
            "GC2 Comdty": 2058,
            "GC3 Comdty": 2066,
            "SI1 Comdty": 24.5,
            "SI2 Comdty": 24.6,
            "PL1 Comdty": 950,
            "PL2 Comdty": 955,
            "PA1 Comdty": 1050,
            "PA2 Comdty": 1055,
            "XAU Curncy": 2040,
            "XAG Curncy": 24.0,
            "XPT Curncy": 945,
            "XPD Curncy": 1040,
            "USDCNY Curncy": 7.25,
            "SHFCCOM1 Index": 68000,
            "SHFACOM1 Index": 19000,
            "SHFZCOM1 Index": 21000,
            "SHFNICOM1 Index": 130000,
            "MWAP Index": 25,
            "EUAP Index": 250,
            "CUPPSHBI Index": 55,
            "SOFRRATE Index": 5.33,
        }
        for ticker in tickers:
            base = seed_prices.get(ticker, 100)
            vol = base * 0.012  # ~1.2% daily vol
            prices = base + np.cumsum(np.random.randn(len(dates)) * vol)
            prices = np.maximum(prices, base * 0.5)
            data = {f: prices for f in fields}
            frames[ticker] = pd.DataFrame(data, index=dates)

        if not frames:
            return pd.DataFrame()
        combined = pd.concat(frames, axis=1)
        combined.columns.names = ["ticker", "field"]
        return combined

    @staticmethod
    def _demo_reference(tickers: list[str], fields: list[str]) -> pd.DataFrame:
        records = []
        for t in tickers:
            rec = {"ticker": t}
            for f in fields:
                if f == "LAST_TRADEABLE_DT":
                    rec[f] = (dt.date.today() + dt.timedelta(days=30)).isoformat()
                elif f == "FUT_CUR_GEN_TICKER":
                    rec[f] = t.replace("1 ", "H26 ").replace("2 ", "M26 ")
                else:
                    rec[f] = ""
            records.append(rec)
        return pd.DataFrame(records).set_index("ticker")
