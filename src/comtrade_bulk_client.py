"""
Multi-commodity Comtrade bulk data client.

Fetches precious-metals trade data (gold, silver, PGM) for all major
trading countries via the UN Comtrade API, with parquet caching.

HS codes
--------
  7106  Silver (unwrought, semi-manufactured, powder)
  7108  Gold   (unwrought, semi-manufactured, powder)
  7110  Platinum-group metals (Pt, Pd, Rh, Ir, Os, Ru)
"""

from __future__ import annotations

import datetime as dt
import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  HS commodity codes
# ---------------------------------------------------------------------------
HS_CODES: dict[str, str] = {
    "Gold": "7108",
    "Silver": "7106",
    "PGM": "7110",
}

# ---------------------------------------------------------------------------
#  Reporter countries (M49)
# ---------------------------------------------------------------------------
REPORTER_COUNTRIES: dict[str, str] = {
    "784": "United Arab Emirates",
    "757": "Switzerland",
    "699": "India",
    "156": "China",
    "792": "Turkey",
    "826": "United Kingdom",
    "344": "Hong Kong",
    "842": "United States",
    "710": "South Africa",
    "643": "Russia",
    "036": "Australia",
    "764": "Thailand",
    "608": "Philippines",
    "360": "Indonesia",
    "682": "Saudi Arabia",
}

# ---------------------------------------------------------------------------
#  M49 → country-name lookup (for fixing null partnerDesc)
# ---------------------------------------------------------------------------
M49_COUNTRY_NAMES: dict[int, str] = {
    0: "World",
    4: "Afghanistan", 8: "Albania", 12: "Algeria", 20: "Andorra",
    24: "Angola", 28: "Antigua and Barbuda", 31: "Azerbaijan",
    32: "Argentina", 36: "Australia", 40: "Austria",
    48: "Bahrain", 50: "Bangladesh", 51: "Armenia", 52: "Barbados",
    56: "Belgium", 64: "Bhutan", 68: "Bolivia", 70: "Bosnia Herzegovina",
    72: "Botswana", 76: "Brazil", 84: "Belize", 96: "Brunei",
    100: "Bulgaria", 104: "Myanmar", 108: "Burundi",
    112: "Belarus", 116: "Cambodia", 120: "Cameroon",
    124: "Canada", 144: "Sri Lanka", 148: "Chad",
    152: "Chile", 156: "China", 158: "Taiwan",
    170: "Colombia", 174: "Comoros", 178: "Congo",
    180: "DR Congo", 188: "Costa Rica", 191: "Croatia",
    192: "Cuba", 196: "Cyprus", 203: "Czech Republic",
    208: "Denmark", 214: "Dominican Republic",
    218: "Ecuador", 222: "El Salvador", 226: "Equatorial Guinea",
    231: "Ethiopia", 233: "Estonia", 242: "Fiji",
    246: "Finland", 250: "France", 266: "Gabon",
    268: "Georgia", 276: "Germany", 288: "Ghana",
    300: "Greece", 320: "Guatemala", 324: "Guinea",
    328: "Guyana", 332: "Haiti", 340: "Honduras",
    344: "Hong Kong", 348: "Hungary", 352: "Iceland",
    356: "India", 360: "Indonesia", 364: "Iran",
    368: "Iraq", 372: "Ireland", 376: "Israel",
    380: "Italy", 384: "Côte d'Ivoire", 388: "Jamaica",
    392: "Japan", 398: "Kazakhstan", 400: "Jordan",
    404: "Kenya", 410: "South Korea", 414: "Kuwait",
    417: "Kyrgyzstan", 418: "Laos", 422: "Lebanon",
    426: "Lesotho", 428: "Latvia", 430: "Liberia",
    434: "Libya", 440: "Lithuania", 442: "Luxembourg",
    446: "Macao", 450: "Madagascar", 454: "Malawi",
    458: "Malaysia", 462: "Maldives", 466: "Mali",
    470: "Malta", 480: "Mauritius", 484: "Mexico",
    496: "Mongolia", 498: "Moldova", 499: "Montenegro",
    504: "Morocco", 508: "Mozambique", 512: "Oman",
    516: "Namibia", 524: "Nepal", 528: "Netherlands",
    540: "New Caledonia", 548: "Vanuatu",
    554: "New Zealand", 558: "Nicaragua", 562: "Niger",
    566: "Nigeria", 578: "Norway", 586: "Pakistan",
    591: "Panama", 598: "Papua New Guinea", 600: "Paraguay",
    604: "Peru", 608: "Philippines", 616: "Poland",
    620: "Portugal", 634: "Qatar", 642: "Romania",
    643: "Russia", 646: "Rwanda", 682: "Saudi Arabia",
    686: "Senegal", 688: "Serbia", 694: "Sierra Leone",
    699: "India", 702: "Singapore", 703: "Slovakia",
    704: "Vietnam", 705: "Slovenia", 710: "South Africa",
    716: "Zimbabwe", 724: "Spain", 736: "Sudan",
    740: "Suriname", 752: "Sweden", 756: "Switzerland",
    757: "Switzerland", 760: "Syria", 762: "Tajikistan",
    764: "Thailand", 768: "Togo", 780: "Trinidad and Tobago",
    784: "United Arab Emirates", 788: "Tunisia",
    792: "Turkey", 800: "Uganda", 804: "Ukraine",
    807: "North Macedonia", 818: "Egypt", 826: "United Kingdom",
    834: "Tanzania", 840: "United States", 842: "United States",
    854: "Burkina Faso", 858: "Uruguay", 860: "Uzbekistan",
    862: "Venezuela", 887: "Yemen", 894: "Zambia",
}

FLOW_CODE_NAMES = {"M": "Import", "X": "Export", "RM": "Re-import", "RX": "Re-export"}

# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------
DEFAULT_CACHE_DIR = Path("data/gold_trade")
COMTRADE_FULL_URL = "https://comtradeapi.un.org/data/v1/get/C/M/HS"
COMTRADE_PREVIEW_URL = "https://comtradeapi.un.org/public/v1/preview/C/M/HS"
REQUEST_TIMEOUT = 30
RATE_LIMIT_PAUSE = 2.0
MAX_RETRIES = 3

# Column rename map for Comtrade JSON → tidy DataFrame
_RENAME_MAP = {
    "period": "period",
    "reporterCode": "reporter_code",
    "reporterDesc": "reporter",
    "partnerCode": "partner_code",
    "partnerDesc": "partner",
    "flowCode": "flow_code",
    "flowDesc": "flow",
    "cmdCode": "hs_code",
    "cmdDescE": "description",
    "primaryValue": "value_usd",
    "netWgt": "net_weight_kg",
    "fobvalue": "fob_usd",
    "cifvalue": "cif_usd",
}


class ComtradeBulkClient:
    """Fetch & cache multi-commodity Comtrade trade data."""

    def __init__(
        self,
        cache_dir: str | Path = DEFAULT_CACHE_DIR,
        api_key: str | None = None,
        proxy_url: str | None = None,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.api_key = api_key or os.environ.get("COMTRADE_API_KEY")
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "PremiumDash/1.0 (precious-metals-research)",
            "Accept": "application/json",
        })
        if proxy_url:
            try:
                from .proxy_utils import encode_proxy_url
                proxy_url = encode_proxy_url(proxy_url)
            except ImportError:
                pass
            self._session.proxies.update({"http": proxy_url, "https": proxy_url})

    # ==================================================================
    #  Public API
    # ==================================================================
    def fetch_all(
        self,
        start_year: int = 2018,
        end_year: int | None = None,
        commodities: list[str] | None = None,
        countries: list[str] | None = None,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch trade data for all commodity × country combinations.

        Returns the merged DataFrame of everything fetched.
        """
        if end_year is None:
            end_year = dt.date.today().year

        hs_list = commodities or list(HS_CODES.values())
        country_list = countries or list(REPORTER_COUNTRIES.keys())

        frames: list[pd.DataFrame] = []
        total = len(hs_list) * len(country_list)
        done = 0

        for hs_code in hs_list:
            for country_code in country_list:
                done += 1
                commodity_name = self._hs_label(hs_code)
                country_name = REPORTER_COUNTRIES.get(country_code, country_code)
                logger.info(
                    "[%d/%d] Fetching %s for %s (%s-%s)",
                    done, total, commodity_name, country_name, start_year, end_year,
                )
                df = self.fetch_commodity(
                    hs_code=hs_code,
                    country_code=country_code,
                    start_year=start_year,
                    end_year=end_year,
                    force_refresh=force_refresh,
                )
                if not df.empty:
                    frames.append(df)

        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()

    def fetch_commodity(
        self,
        hs_code: str,
        country_code: str,
        start_year: int = 2018,
        end_year: int | None = None,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """Fetch imports + exports for one commodity × country, with caching."""
        if end_year is None:
            end_year = dt.date.today().year

        cache_key = f"comtrade_{hs_code}_{country_code}_MX_{start_year}_{end_year}"
        if not force_refresh:
            cached = self._load_cache(cache_key)
            if cached is not None:
                return cached

        # Choose endpoint based on API key availability
        if self.api_key:
            base_url = COMTRADE_FULL_URL
            headers = {"Ocp-Apim-Subscription-Key": self.api_key}
        else:
            base_url = COMTRADE_PREVIEW_URL
            headers = {}

        use_full_api = self.api_key is not None
        all_records: list[dict] = []

        for year in range(start_year, end_year + 1):
            if use_full_api:
                # Full API: batch all 12 months
                period = ",".join(f"{year}{m:02d}" for m in range(1, 13))
                for fc in ("M", "X"):
                    params = {
                        "reporterCode": country_code,
                        "cmdCode": hs_code,
                        "flowCode": fc,
                        "period": period,
                    }
                    records = self._request_with_retry(
                        base_url, headers, params,
                        label=f"[{country_code}] {hs_code} {fc} {year}",
                    )
                    if records:
                        all_records.extend(records)
                    time.sleep(RATE_LIMIT_PAUSE)
            else:
                # Preview API: one month at a time
                for month in range(1, 13):
                    period = f"{year}{month:02d}"
                    for fc in ("M", "X"):
                        params = {
                            "reporterCode": country_code,
                            "cmdCode": hs_code,
                            "flowCode": fc,
                            "period": period,
                        }
                        records = self._request_with_retry(
                            base_url, headers, params,
                            label=f"[{country_code}] {hs_code} {fc} {period}",
                        )
                        if records:
                            all_records.extend(records)
                        time.sleep(RATE_LIMIT_PAUSE)

        if not all_records:
            logger.warning(
                "No records for %s / %s (%d-%d)",
                hs_code, country_code, start_year, end_year,
            )
            return pd.DataFrame()

        df = self._parse_records(all_records)

        # Add commodity label column for easy pivot filtering
        df["commodity"] = self._hs_label(df["hs_code"].iloc[0] if "hs_code" in df.columns else hs_code)

        self._save_cache(cache_key, df)
        return df

    def load_all_cached(
        self,
        commodities: list[str] | None = None,
        countries: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load all cached parquet files matching the given filters."""
        hs_list = commodities or list(HS_CODES.values())
        country_list = countries or list(REPORTER_COUNTRIES.keys())

        frames: list[pd.DataFrame] = []
        for path in sorted(self.cache_dir.glob("comtrade_*.parquet")):
            # Parse filename — two conventions:
            #   NEW: comtrade_{hs}_{country}_MX_{start}_{end}.parquet
            #   OLD: comtrade_{country}_MX_{start}_{end}.parquet  (gold-only)
            parts = path.stem.split("_")
            if len(parts) < 4:
                continue

            # Detect which convention: if parts[1] is a known HS code → new format
            if parts[1] in HS_CODES.values():
                hs = parts[1]
                country = parts[2]
            elif parts[1] in REPORTER_COUNTRIES:
                # Legacy gold-only files
                hs = "7108"
                country = parts[1]
            else:
                continue

            if hs not in hs_list or country not in country_list:
                continue

            try:
                df = pd.read_parquet(path)
                # Ensure commodity column
                if "commodity" not in df.columns:
                    df["commodity"] = self._hs_label(hs)
                frames.append(df)
            except Exception as exc:
                logger.warning("Failed to read %s: %s", path, exc)

        if frames:
            combined = pd.concat(frames, ignore_index=True)
            # De-duplicate (same record from overlapping cache files)
            dedup_cols = [c for c in ("period", "reporter_code", "partner_code", "flow_code", "hs_code")
                          if c in combined.columns]
            if dedup_cols:
                combined = combined.drop_duplicates(subset=dedup_cols)
            return combined
        return pd.DataFrame()

    def get_available_cache_info(self) -> list[dict]:
        """Return summary of cached files for the UI."""
        info = []
        for path in sorted(self.cache_dir.glob("comtrade_*.parquet")):
            parts = path.stem.split("_")
            if len(parts) >= 5:
                hs = parts[1]
                country = parts[2]
                info.append({
                    "file": path.name,
                    "hs_code": hs,
                    "commodity": self._hs_label(hs),
                    "country_code": country,
                    "country": REPORTER_COUNTRIES.get(country, country),
                    "size_kb": round(path.stat().st_size / 1024, 1),
                })
        return info

    # ==================================================================
    #  Private helpers
    # ==================================================================
    def _request_with_retry(
        self, url: str, headers: dict, params: dict, label: str = "",
    ) -> list[dict]:
        """Comtrade API request with exponential backoff on 429."""
        for attempt in range(MAX_RETRIES):
            try:
                resp = self._session.get(
                    url, headers=headers, params=params, timeout=REQUEST_TIMEOUT,
                )
                if resp.status_code == 429:
                    wait = RATE_LIMIT_PAUSE * (2 ** (attempt + 1))
                    logger.info("Rate-limited %s, waiting %.1fs (%d/%d)", label, wait, attempt + 1, MAX_RETRIES)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                if "data" in data and data["data"]:
                    records = data["data"]
                    logger.info("Comtrade %s: %d records", label, len(records))
                    return records
                else:
                    logger.debug("Comtrade %s: no data", label)
                    return []
            except requests.RequestException as exc:
                logger.warning("Comtrade %s failed: %s", label, exc)
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RATE_LIMIT_PAUSE * (2 ** attempt))
                    continue
                return []
        logger.warning("Comtrade %s: exhausted retries", label)
        return []

    def _parse_records(self, records: list[dict]) -> pd.DataFrame:
        """Parse raw Comtrade JSON records into a tidy DataFrame."""
        df = pd.DataFrame(records)
        available = {k: v for k, v in _RENAME_MAP.items() if k in df.columns}
        df = df.rename(columns=available)
        keep_cols = [c for c in _RENAME_MAP.values() if c in df.columns]
        df = df[keep_cols]

        # Fix missing partner names
        if "partner_code" in df.columns and "partner" in df.columns:
            mask = df["partner"].isna() | (df["partner"] == "")
            if mask.any():
                df.loc[mask, "partner"] = (
                    df.loc[mask, "partner_code"].astype(int).map(M49_COUNTRY_NAMES)
                )
        elif "partner_code" in df.columns and "partner" not in df.columns:
            df["partner"] = df["partner_code"].astype(int).map(M49_COUNTRY_NAMES)

        # Fix missing flow names
        if "flow_code" in df.columns and "flow" in df.columns:
            mask = df["flow"].isna() | (df["flow"] == "")
            if mask.any():
                df.loc[mask, "flow"] = df.loc[mask, "flow_code"].map(FLOW_CODE_NAMES)

        # Parse date
        if "period" in df.columns:
            df["date"] = pd.to_datetime(df["period"].astype(str), format="%Y%m")
            df = df.sort_values("date").reset_index(drop=True)

        return df

    @staticmethod
    def _hs_label(hs_code: str) -> str:
        """Map HS code to human-friendly label."""
        for label, code in HS_CODES.items():
            if hs_code.startswith(code):
                return label
        return f"HS {hs_code}"

    # -- Cache helpers -----
    def _cache_path(self, key: str) -> Path:
        safe = key.replace(" ", "_").replace("/", "_").replace(",", "_")
        return self.cache_dir / f"{safe}.parquet"

    def _load_cache(self, key: str) -> pd.DataFrame | None:
        path = self._cache_path(key)
        if not path.exists():
            return None
        try:
            df = pd.read_parquet(path)
            logger.debug("Cache hit: %s (%d rows)", key, len(df))
            return df
        except Exception as exc:
            logger.warning("Corrupt cache %s: %s", key, exc)
            path.unlink(missing_ok=True)
            return None

    def _save_cache(self, key: str, df: pd.DataFrame) -> None:
        path = self._cache_path(key)
        df.to_parquet(path, engine="pyarrow")
        logger.debug("Cache saved: %s (%d rows)", key, len(df))
