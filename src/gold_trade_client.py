"""
Gold trade data client — fetches UAE gold flow data from free Tier 1 sources.

Sources:
  1. UN Comtrade API — UAE gold imports/exports by partner, monthly (HS 7108)
  2. Swiss-Impex (opendata.swiss) — Switzerland ↔ UAE gold monthly
  3. World Gold Council GoldHub — India gold premium/discount
  4. CBUAE Statistics — Central bank gold reserves (monthly bulletins)
  5. TrendEconomy — UAE trade data via SDMX REST API

All methods return pandas DataFrames and cache results to parquet.
"""

from __future__ import annotations

import datetime as dt
import io
import logging
import time
import zipfile
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Country / HS code constants
# ---------------------------------------------------------------------------
UAE_M49 = "784"
SWITZERLAND_M49 = "757"
INDIA_M49 = "699"
TURKEY_M49 = "792"
CHINA_M49 = "156"
UK_M49 = "826"
HONG_KONG_M49 = "344"
RUSSIA_M49 = "643"

# Gold HS codes
HS_GOLD = "7108"                 # Gold unwrought / semi-manufactured / powder
HS_GOLD_UNWROUGHT = "710812"     # Unwrought non-monetary gold
SWISS_TARIFF_GOLD = "7108.1200"  # Swiss tariff heading for unwrought gold

# Key partner groups for UAE gold trade
PARTNER_GROUPS = {
    "major_importers": [SWITZERLAND_M49, INDIA_M49, TURKEY_M49, CHINA_M49,
                        UK_M49, HONG_KONG_M49],
    "african_sources": ["834", "646", "736", "818", "148", "434", "768"],
    # Uganda, Rwanda, Sudan, Egypt, Chad, Libya, Togo
    "cis_sources": [RUSSIA_M49, "051"],  # Russia, Armenia
}

DEFAULT_CACHE_DIR = Path("data/gold_trade")
REQUEST_TIMEOUT = 30
RATE_LIMIT_PAUSE = 1.2  # seconds between API calls


class GoldTradeDataClient:
    """Fetches UAE gold trade data from free public sources."""

    def __init__(
        self,
        cache_dir: str | Path = DEFAULT_CACHE_DIR,
        comtrade_api_key: Optional[str] = None,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.comtrade_api_key = comtrade_api_key
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "PremiumDash/1.0 (gold-trade-research)",
            "Accept": "application/json",
        })

    # ==================================================================
    #  1. UN Comtrade — UAE gold trade by partner (monthly)
    # ==================================================================
    def fetch_comtrade(
        self,
        flow: str = "M",
        partners: list[str] | None = None,
        start_year: int = 2020,
        end_year: int | None = None,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch UAE gold trade data from UN Comtrade API.

        Parameters
        ----------
        flow : str
            'M' for imports, 'X' for exports, 'MX' for both.
        partners : list[str] | None
            M49 partner codes. None = all partners (world).
        start_year : int
            First year to fetch.
        end_year : int | None
            Last year (defaults to current year).
        force_refresh : bool
            Ignore cached data.

        Returns
        -------
        pd.DataFrame with columns: period, reporter, partner, flow,
            hs_code, value_usd, net_weight_kg, description
        """
        if end_year is None:
            end_year = dt.date.today().year

        cache_key = f"comtrade_{flow}_{start_year}_{end_year}"
        if not force_refresh:
            cached = self._load_cache(cache_key)
            if cached is not None:
                return cached

        base_url = "https://comtradeapi.un.org/data/v1/get/C/M/HS"
        headers = {}
        if self.comtrade_api_key:
            headers["Ocp-Apim-Subscription-Key"] = self.comtrade_api_key

        all_records = []
        flow_codes = list(flow) if len(flow) > 1 else [flow]

        for year in range(start_year, end_year + 1):
            # Build period string: all months for the year
            periods = ",".join(f"{year}{m:02d}" for m in range(1, 13))

            for fc in flow_codes:
                params = {
                    "reporterCode": UAE_M49,
                    "cmdCode": HS_GOLD,
                    "flowCode": fc,
                    "period": periods,
                    "motCode": "0",
                    "partner2Code": "0",
                    "customsCode": "C00",
                    "includeDesc": "TRUE",
                }
                if partners:
                    params["partnerCode"] = ",".join(partners)

                try:
                    resp = self._session.get(
                        base_url, headers=headers, params=params,
                        timeout=REQUEST_TIMEOUT,
                    )
                    resp.raise_for_status()
                    data = resp.json()

                    if "data" in data and data["data"]:
                        all_records.extend(data["data"])
                        logger.info(
                            "Comtrade: %d records for %s %d",
                            len(data["data"]), fc, year,
                        )
                    else:
                        logger.debug("Comtrade: no data for %s %d", fc, year)

                except requests.RequestException as exc:
                    logger.warning("Comtrade request failed for %s %d: %s", fc, year, exc)

                time.sleep(RATE_LIMIT_PAUSE)

        if not all_records:
            logger.warning("Comtrade returned no records")
            return pd.DataFrame()

        df = pd.DataFrame(all_records)
        # Normalise column names
        rename_map = {
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
        available = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(columns=available)

        # Keep only useful columns
        keep_cols = [c for c in rename_map.values() if c in df.columns]
        df = df[keep_cols]

        # Parse period to datetime
        if "period" in df.columns:
            df["date"] = pd.to_datetime(df["period"].astype(str), format="%Y%m")
            df = df.sort_values("date").reset_index(drop=True)

        self._save_cache(cache_key, df)
        return df

    def fetch_uae_gold_imports(
        self, start_year: int = 2020, force_refresh: bool = False,
    ) -> pd.DataFrame:
        """Convenience: UAE gold imports from all partners."""
        return self.fetch_comtrade(
            flow="M", start_year=start_year, force_refresh=force_refresh,
        )

    def fetch_uae_gold_exports(
        self, start_year: int = 2020, force_refresh: bool = False,
    ) -> pd.DataFrame:
        """Convenience: UAE gold exports to all partners."""
        return self.fetch_comtrade(
            flow="X", start_year=start_year, force_refresh=force_refresh,
        )

    def fetch_uae_africa_gold(
        self, start_year: int = 2020, force_refresh: bool = False,
    ) -> pd.DataFrame:
        """UAE gold imports from African source countries."""
        return self.fetch_comtrade(
            flow="M",
            partners=PARTNER_GROUPS["african_sources"],
            start_year=start_year,
            force_refresh=force_refresh,
        )

    # ==================================================================
    #  2. Swiss-Impex — Switzerland gold imports from UAE
    # ==================================================================
    def fetch_swiss_gold_imports(
        self, force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch Switzerland gold import data from opendata.swiss (CKAN).

        Returns monthly gold import data filtered for UAE as partner.
        Falls back to BAZG historical Excel if bulk CSV unavailable.
        """
        cache_key = "swiss_impex_gold"
        if not force_refresh:
            cached = self._load_cache(cache_key)
            if cached is not None:
                return cached

        # Also available via Comtrade (Switzerland as reporter, UAE as partner)
        base_url = "https://comtradeapi.un.org/data/v1/get/C/M/HS"
        headers = {}
        if self.comtrade_api_key:
            headers["Ocp-Apim-Subscription-Key"] = self.comtrade_api_key

        all_records = []
        current_year = dt.date.today().year

        for year in range(2018, current_year + 1):
            periods = ",".join(f"{year}{m:02d}" for m in range(1, 13))
            params = {
                "reporterCode": SWITZERLAND_M49,
                "cmdCode": HS_GOLD,
                "flowCode": "M",
                "partnerCode": UAE_M49,
                "period": periods,
                "motCode": "0",
                "partner2Code": "0",
                "customsCode": "C00",
                "includeDesc": "TRUE",
            }

            try:
                resp = self._session.get(
                    base_url, headers=headers, params=params,
                    timeout=REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                data = resp.json()
                if "data" in data and data["data"]:
                    all_records.extend(data["data"])
                    logger.info("Swiss-Impex (Comtrade): %d records for %d", len(data["data"]), year)
            except requests.RequestException as exc:
                logger.warning("Swiss-Impex request failed for %d: %s", year, exc)

            time.sleep(RATE_LIMIT_PAUSE)

        if not all_records:
            logger.warning("Swiss-Impex returned no records, trying opendata.swiss fallback")
            return self._fetch_swiss_opendata(force_refresh)

        df = pd.DataFrame(all_records)
        rename_map = {
            "period": "period",
            "partnerDesc": "partner",
            "primaryValue": "value_usd",
            "netWgt": "net_weight_kg",
            "cmdDescE": "description",
        }
        available = {k: v for k, v in rename_map.items() if k in df.columns}
        df = df.rename(columns=available)
        keep_cols = [c for c in rename_map.values() if c in df.columns]
        df = df[keep_cols]

        if "period" in df.columns:
            df["date"] = pd.to_datetime(df["period"].astype(str), format="%Y%m")
            df = df.sort_values("date").reset_index(drop=True)

        if "net_weight_kg" in df.columns:
            df["net_weight_tonnes"] = df["net_weight_kg"] / 1000.0

        self._save_cache(cache_key, df)
        return df

    def _fetch_swiss_opendata(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        Fallback: fetch gold trade data from opendata.swiss CKAN API.
        The bulk dataset covers Swiss trade by tariff number and country.
        """
        cache_key = "swiss_opendata_gold"
        if not force_refresh:
            cached = self._load_cache(cache_key)
            if cached is not None:
                return cached

        ckan_url = "https://ckan.opendata.swiss/api/3/action/package_show"
        dataset_id = (
            "schweizerische-exporte-und-importe-nach-tarifnummer-und-land"
            "-monatliche-daten-ab-1988"
        )

        try:
            resp = self._session.get(
                ckan_url, params={"id": dataset_id}, timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            resources = resp.json().get("result", {}).get("resources", [])

            csv_resources = [
                r for r in resources
                if r.get("format", "").upper() in ("CSV", "ZIP")
            ]

            if not csv_resources:
                logger.warning("No CSV/ZIP resources found on opendata.swiss")
                return pd.DataFrame()

            # Download the most recent resource
            resource_url = csv_resources[-1]["url"]
            logger.info("Downloading Swiss trade data from: %s", resource_url)

            resp = self._session.get(resource_url, timeout=120)
            resp.raise_for_status()

            if resource_url.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                    csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
                    df = pd.read_csv(zf.open(csv_name), sep=";", low_memory=False)
            else:
                df = pd.read_csv(io.BytesIO(resp.content), sep=";", low_memory=False)

            # Filter for gold (tariff 7108) and UAE as partner
            tariff_col = [c for c in df.columns if "tarif" in c.lower()][0] if any(
                "tarif" in c.lower() for c in df.columns
            ) else None

            if tariff_col:
                df = df[df[tariff_col].astype(str).str.startswith("7108")]

            country_col = [c for c in df.columns if "land" in c.lower() or "country" in c.lower()]
            if country_col:
                df = df[df[country_col[0]].astype(str).str.contains(
                    "AE|ARE|784|Emirat|UAE", case=False, na=False,
                )]

            self._save_cache(cache_key, df)
            return df

        except requests.RequestException as exc:
            logger.warning("opendata.swiss request failed: %s", exc)
            return pd.DataFrame()

    # ==================================================================
    #  3. World Gold Council — India gold premium/discount
    # ==================================================================
    def fetch_india_gold_premium(
        self, force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch India (and China) gold premium/discount vs London from WGC.

        Attempts to download the GoldHub premium XLSX. Falls back to
        scraping the GoldHub chart data endpoint.

        Returns DataFrame with columns: date, india_premium_usd,
            china_premium_usd (where available).
        """
        cache_key = "wgc_gold_premium"
        if not force_refresh:
            cached = self._load_cache(cache_key)
            if cached is not None:
                return cached

        # Try direct XLSX download (may require login/session)
        xlsx_urls = [
            "https://www.gold.org/goldhub/data/gold-premium",
            "https://www.gold.org/download/file/gold-premiums.xlsx",
        ]

        for url in xlsx_urls:
            try:
                resp = self._session.get(url, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200 and (
                    "spreadsheet" in resp.headers.get("content-type", "")
                    or "excel" in resp.headers.get("content-type", "")
                    or url.endswith(".xlsx")
                ):
                    df = pd.read_excel(io.BytesIO(resp.content))
                    if not df.empty:
                        df = self._parse_wgc_premium(df)
                        self._save_cache(cache_key, df)
                        logger.info("WGC premium data: %d rows", len(df))
                        return df
            except Exception as exc:
                logger.debug("WGC download attempt failed (%s): %s", url, exc)

        # Fallback: try the chart data exporter
        try:
            export_url = "https://chart-data-exporter.gold.org/export"
            resp = self._session.get(
                export_url,
                params={"chartId": "gold-premium", "format": "csv"},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                df = pd.read_csv(io.BytesIO(resp.content))
                df = self._parse_wgc_premium(df)
                self._save_cache(cache_key, df)
                return df
        except Exception as exc:
            logger.debug("WGC chart exporter failed: %s", exc)

        logger.warning(
            "Could not auto-download WGC premium data. "
            "Download manually from https://www.gold.org/goldhub/data/gold-premium "
            "and place as data/gold_trade/gold-premiums.xlsx"
        )

        # Check for manually downloaded file
        manual_path = self.cache_dir / "gold-premiums.xlsx"
        if manual_path.exists():
            df = pd.read_excel(manual_path)
            df = self._parse_wgc_premium(df)
            self._save_cache(cache_key, df)
            return df

        return pd.DataFrame()

    @staticmethod
    def _parse_wgc_premium(df: pd.DataFrame) -> pd.DataFrame:
        """Normalise WGC premium data to standard format."""
        # Try to find date column
        date_cols = [c for c in df.columns if "date" in c.lower() or "time" in c.lower()]
        if date_cols:
            df = df.rename(columns={date_cols[0]: "date"})
        elif df.columns[0] != "date":
            df = df.rename(columns={df.columns[0]: "date"})

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])

        # Normalise premium columns
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if "india" in cl:
                col_map[c] = "india_premium_usd"
            elif "china" in cl:
                col_map[c] = "china_premium_usd"
        if col_map:
            df = df.rename(columns=col_map)

        return df.sort_values("date").reset_index(drop=True)

    # ==================================================================
    #  4. CBUAE — Central bank gold reserves
    # ==================================================================
    def fetch_cbuae_reserves(
        self, force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch CBUAE gold reserve data.

        Tries the CBUAE open-data portal first, then falls back to
        scraping the statistics landing page for bulletin links.

        Returns DataFrame with columns: date, reserves_aed, reserves_usd
        """
        cache_key = "cbuae_reserves"
        if not force_refresh:
            cached = self._load_cache(cache_key)
            if cached is not None:
                return cached

        # Try CBUAE open data API
        open_data_urls = [
            "https://www.centralbank.ae/en/open-data-landing/",
            "https://www.centralbank.ae/api/v1/statistics/gold-reserves",
        ]

        for url in open_data_urls:
            try:
                resp = self._session.get(url, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    content_type = resp.headers.get("content-type", "")
                    if "json" in content_type:
                        data = resp.json()
                        df = self._parse_cbuae_json(data)
                        if not df.empty:
                            self._save_cache(cache_key, df)
                            return df
            except Exception as exc:
                logger.debug("CBUAE open data attempt failed (%s): %s", url, exc)

        # Try scraping the statistics page for bulletin XLSX links
        try:
            stats_url = "https://www.centralbank.ae/en/research-and-statistics/latest-statistics/"
            resp = self._session.get(stats_url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                df = self._parse_cbuae_bulletins_page(resp.text)
                if not df.empty:
                    self._save_cache(cache_key, df)
                    return df
        except Exception as exc:
            logger.debug("CBUAE statistics page scrape failed: %s", exc)

        # Check for manually downloaded bulletins
        manual_files = sorted(self.cache_dir.glob("statistical-bulletin*.xlsx"))
        if manual_files:
            frames = []
            for f in manual_files:
                try:
                    bulletin_df = self._parse_cbuae_bulletin_xlsx(f)
                    if not bulletin_df.empty:
                        frames.append(bulletin_df)
                except Exception as exc:
                    logger.debug("Failed to parse %s: %s", f.name, exc)
            if frames:
                df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["date"])
                df = df.sort_values("date").reset_index(drop=True)
                self._save_cache(cache_key, df)
                return df

        logger.warning(
            "Could not auto-fetch CBUAE reserves. Download monthly bulletins "
            "(XLSX) from https://www.centralbank.ae/en/research-and-statistics/ "
            "and place in %s/", self.cache_dir,
        )
        return pd.DataFrame()

    @staticmethod
    def _parse_cbuae_json(data: dict) -> pd.DataFrame:
        """Parse CBUAE open data JSON response."""
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict) and "data" in data:
            records = data["data"]
        else:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        if df.empty:
            return df

        # Try to find date and gold reserve columns
        date_cols = [c for c in df.columns if "date" in c.lower() or "period" in c.lower()]
        gold_cols = [c for c in df.columns if "gold" in c.lower() or "reserve" in c.lower()]

        if date_cols:
            df = df.rename(columns={date_cols[0]: "date"})
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        if gold_cols:
            df = df.rename(columns={gold_cols[0]: "reserves_aed"})

        return df

    def _parse_cbuae_bulletins_page(self, html: str) -> pd.DataFrame:
        """Extract bulletin download links and fetch XLSX files."""
        import re

        # Find all XLSX bulletin links
        pattern = r'href="([^"]*statistical-bulletin[^"]*\.xlsx[^"]*)"'
        links = re.findall(pattern, html, re.IGNORECASE)

        if not links:
            return pd.DataFrame()

        frames = []
        for link in links[:12]:  # Last 12 months max
            full_url = link if link.startswith("http") else f"https://www.centralbank.ae{link}"
            try:
                resp = self._session.get(full_url, timeout=REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    xlsx_path = self.cache_dir / full_url.split("/")[-1]
                    xlsx_path.write_bytes(resp.content)
                    bulletin_df = self._parse_cbuae_bulletin_xlsx(xlsx_path)
                    if not bulletin_df.empty:
                        frames.append(bulletin_df)
            except Exception as exc:
                logger.debug("Failed to fetch bulletin %s: %s", full_url, exc)
            time.sleep(RATE_LIMIT_PAUSE)

        if not frames:
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["date"])
        return df.sort_values("date").reset_index(drop=True)

    @staticmethod
    def _parse_cbuae_bulletin_xlsx(path: Path) -> pd.DataFrame:
        """Extract gold reserve data from a CBUAE statistical bulletin XLSX."""
        try:
            xls = pd.ExcelFile(path)
            # Look for sheets with gold/reserve data
            target_sheets = [
                s for s in xls.sheet_names
                if any(kw in s.lower() for kw in ["gold", "reserve", "asset", "balance"])
            ]
            if not target_sheets:
                target_sheets = xls.sheet_names[:3]

            for sheet in target_sheets:
                df = pd.read_excel(xls, sheet_name=sheet, header=None)
                # Search for "gold" in any cell
                for i, row in df.iterrows():
                    for j, val in enumerate(row):
                        if isinstance(val, str) and "gold" in val.lower():
                            # Found gold row — extract the value
                            # The value is usually in the next columns
                            numeric_vals = pd.to_numeric(
                                row.iloc[j + 1:], errors="coerce",
                            ).dropna()
                            if not numeric_vals.empty:
                                # Extract month from filename
                                month_str = path.stem.lower()
                                return pd.DataFrame([{
                                    "date": pd.Timestamp.now(),
                                    "reserves_aed": numeric_vals.iloc[0],
                                    "source_file": path.name,
                                }])
        except Exception as exc:
            logger.debug("CBUAE XLSX parse error for %s: %s", path.name, exc)

        return pd.DataFrame()

    # ==================================================================
    #  5. TrendEconomy — SDMX REST API
    # ==================================================================
    def fetch_trendeconomy(
        self,
        start_year: int = 2018,
        end_year: int | None = None,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Fetch UAE gold trade data from TrendEconomy SDMX API.

        Returns annual/periodic trade data for HS 7108 reported by UAE.
        """
        if end_year is None:
            end_year = dt.date.today().year

        cache_key = f"trendeconomy_{start_year}_{end_year}"
        if not force_refresh:
            cached = self._load_cache(cache_key)
            if cached is not None:
                return cached

        base_url = "http://trendeconomy.com/rest/data"

        # Step 1: discover dataflows
        try:
            meta_resp = self._session.get(
                "http://trendeconomy.com/rest/meta/dataflow",
                timeout=REQUEST_TIMEOUT,
            )
            if meta_resp.status_code != 200:
                logger.warning("TrendEconomy metadata request failed: %d", meta_resp.status_code)
                return self._fetch_trendeconomy_xlsx(start_year, end_year)
        except requests.RequestException as exc:
            logger.warning("TrendEconomy metadata request failed: %s", exc)
            return self._fetch_trendeconomy_xlsx(start_year, end_year)

        # Step 2: try to query trade data via SDMX
        # The key structure varies — try common patterns
        sdmx_patterns = [
            f"TE,TRADE_HS2,1.0/A.ARE.7108...",
            f"TE,TRADE,1.0/A.784.7108...",
        ]

        for pattern in sdmx_patterns:
            try:
                data_url = f"{base_url}/{pattern}"
                resp = self._session.get(
                    data_url,
                    params={
                        "startPeriod": str(start_year),
                        "endPeriod": str(end_year),
                        "format": "xlsx_flat",
                    },
                    timeout=REQUEST_TIMEOUT,
                )
                if resp.status_code == 200 and len(resp.content) > 500:
                    content_type = resp.headers.get("content-type", "")
                    if "spreadsheet" in content_type or "excel" in content_type:
                        df = pd.read_excel(io.BytesIO(resp.content))
                    else:
                        df = self._parse_sdmx_xml(resp.text)

                    if not df.empty:
                        self._save_cache(cache_key, df)
                        logger.info("TrendEconomy: %d records via SDMX", len(df))
                        return df
            except Exception as exc:
                logger.debug("TrendEconomy SDMX pattern %s failed: %s", pattern, exc)

        # Fallback to XLSX export
        return self._fetch_trendeconomy_xlsx(start_year, end_year)

    def _fetch_trendeconomy_xlsx(
        self, start_year: int, end_year: int,
    ) -> pd.DataFrame:
        """Fallback: try the TrendEconomy web export endpoint."""
        cache_key = f"trendeconomy_xlsx_{start_year}_{end_year}"
        cached = self._load_cache(cache_key)
        if cached is not None:
            return cached

        # Try the web page's data export
        export_url = "http://trendeconomy.com/data/h2/UnitedArabEmirates/7108"
        try:
            resp = self._session.get(
                export_url,
                params={"output": "xlsx"},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200 and len(resp.content) > 500:
                content_type = resp.headers.get("content-type", "")
                if "spreadsheet" in content_type or "excel" in content_type:
                    df = pd.read_excel(io.BytesIO(resp.content))
                    if not df.empty:
                        self._save_cache(cache_key, df)
                        return df
        except Exception as exc:
            logger.debug("TrendEconomy XLSX export failed: %s", exc)

        logger.warning(
            "TrendEconomy auto-fetch failed. Visit "
            "http://trendeconomy.com/data/h2/UnitedArabEmirates/7108 "
            "and export manually."
        )
        return pd.DataFrame()

    @staticmethod
    def _parse_sdmx_xml(xml_text: str) -> pd.DataFrame:
        """Parse SDMX 2.0 compact XML response into a DataFrame."""
        import xml.etree.ElementTree as ET

        try:
            root = ET.fromstring(xml_text)
            # SDMX compact format stores observations as attributes
            ns = {"ns": root.tag.split("}")[0].strip("{")} if "}" in root.tag else {}

            records = []
            for obs in root.iter():
                if obs.tag.endswith("Obs") or "Obs" in obs.tag:
                    records.append(dict(obs.attrib))

            if records:
                return pd.DataFrame(records)
        except ET.ParseError as exc:
            logger.debug("SDMX XML parse failed: %s", exc)

        return pd.DataFrame()

    # ==================================================================
    #  Unified fetch — all sources
    # ==================================================================
    def fetch_all(
        self,
        start_year: int = 2020,
        force_refresh: bool = False,
    ) -> dict[str, pd.DataFrame]:
        """
        Fetch data from all Tier 1 sources.

        Returns a dict keyed by source name.
        """
        results = {}
c
        logger.info("=" * 60)
        logger.info("Fetching from all Tier 1 gold trade data sources")
        logger.info("=" * 60)

        # 1. UN Comtrade — UAE gold imports & exports
        logger.info("[1/5] UN Comtrade — UAE gold imports...")
        results["comtrade_imports"] = self.fetch_uae_gold_imports(
            start_year=start_year, force_refresh=force_refresh,
        )
        logger.info("[1/5] UN Comtrade — UAE gold exports...")
        results["comtrade_exports"] = self.fetch_uae_gold_exports(
            start_year=start_year, force_refresh=force_refresh,
        )

        # 2. Swiss-Impex — Switzerland gold from UAE
        logger.info("[2/5] Swiss-Impex — Switzerland gold imports from UAE...")
        results["swiss_imports_from_uae"] = self.fetch_swiss_gold_imports(
            force_refresh=force_refresh,
        )

        # 3. WGC — India gold premium/discount
        logger.info("[3/5] World Gold Council — India gold premium...")
        results["india_premium"] = self.fetch_india_gold_premium(
            force_refresh=force_refresh,
        )

        # 4. CBUAE — Central bank reserves
        logger.info("[4/5] CBUAE — Gold reserves...")
        results["cbuae_reserves"] = self.fetch_cbuae_reserves(
            force_refresh=force_refresh,
        )

        # 5. TrendEconomy — UAE trade overview
        logger.info("[5/5] TrendEconomy — UAE gold trade data...")
        results["trendeconomy"] = self.fetch_trendeconomy(
            start_year=start_year, force_refresh=force_refresh,
        )

        # Summary
        logger.info("=" * 60)
        for name, df in results.items():
            rows = len(df) if not df.empty else 0
            status = "OK" if rows > 0 else "EMPTY (may need manual download)"
            logger.info("  %-30s %5d rows  [%s]", name, rows, status)
        logger.info("=" * 60)

        return results

    # ==================================================================
    #  Cache helpers (parquet)
    # ==================================================================
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
            logger.warning("Corrupt cache %s, will re-fetch: %s", key, exc)
            path.unlink(missing_ok=True)
            return None

    def _save_cache(self, key: str, df: pd.DataFrame) -> None:
        path = self._cache_path(key)
        df.to_parquet(path, engine="pyarrow")
        logger.debug("Cache saved: %s (%d rows)", key, len(df))

    def invalidate(self, key: str | None = None) -> None:
        """Delete cache for a specific key or all cached data."""
        if key:
            path = self._cache_path(key)
            if path.exists():
                path.unlink()
                logger.info("Cache invalidated: %s", key)
        else:
            for f in self.cache_dir.glob("*.parquet"):
                f.unlink()
            logger.info("All gold trade cache invalidated")
