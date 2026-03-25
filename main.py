#!/usr/bin/env python3
"""
Entry point for the Commodity Premium/Discount Dashboard.

Usage:
    python main.py                   # launch dashboard
    python main.py --refresh-only    # refresh cache, don't start server
    python main.py --port 8051       # custom port
    python main.py --debug           # Dash debug mode with hot-reload
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
from pathlib import Path

# Load .env file if present (before any imports that need env vars)
_env_path = Path(__file__).resolve().parent / "config" / ".env"
if _env_path.exists():
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

# Ensure project root is on the path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("premiumDash")


def _load_dashboard_defaults() -> dict:
    """Read host/port defaults from config/spreads.yaml if available."""
    import yaml
    import os

    config_path = PROJECT_ROOT / "config" / "spreads.yaml"
    defaults = {
        "host": "127.0.0.1",
        "port": 8050,
        "proxy_url": None,
    }

    try:
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        dash_cfg = cfg.get("settings", {}).get("dashboard", {})
        defaults["host"] = dash_cfg.get("host", "127.0.0.1")
        defaults["port"] = dash_cfg.get("port", 8050)

        # Read proxy from config (can be overridden by env var)
        net_cfg = cfg.get("settings", {}).get("network", {})
        defaults["proxy_url"] = net_cfg.get("proxy_url")
    except Exception:
        pass

    # Environment variable takes precedence
    env_proxy = os.environ.get("PREMIUM_DASH_PROXY")
    if env_proxy:
        defaults["proxy_url"] = env_proxy

    return defaults


def main():
    import os

    # Disable Jupyter notebook mode detection
    # This prevents Dash/IPython from trying to use Jupyter's comm protocol in terminal mode
    os.environ.setdefault('DASH_HOT_RELOAD', 'False')
    os.environ.setdefault('PYTHONUNBUFFERED', '1')

    # Tell IPython we are NOT in a Jupyter kernel environment
    # This prevents IPython from trying to create a kernel comm
    os.environ['PYDEVD_WARN_SLOW_RESOLVE_TIMEOUT'] = '2'
    os.environ['JPY_PARENT_PID'] = '-1'

    defaults = _load_dashboard_defaults()

    parser = argparse.ArgumentParser(description="Commodity Premium/Discount Dashboard")
    parser.add_argument("--port", type=int, default=defaults["port"],
                        help=f"Dashboard port (default {defaults['port']})")
    parser.add_argument("--host", default=defaults["host"],
                        help=f"Dashboard host (default {defaults['host']})")
    parser.add_argument("--proxy", default=defaults.get("proxy_url"),
                        help="Proxy URL for all HTTP requests (e.g., http://proxy.corp.com:3128, socks5://127.0.0.1:1080)")
    parser.add_argument("--debug", action="store_true", help="Enable Dash debug mode")
    parser.add_argument("--refresh-only", action="store_true", help="Refresh data cache and exit")
    parser.add_argument("--force-refresh", action="store_true", help="Ignore cache, refetch everything")
    args = parser.parse_args()

    if args.refresh_only:
        _refresh_cache(args.force_refresh, args.proxy)
        return

    from dashboard.app import create_app

    app = create_app(proxy_url=args.proxy)
    logger.info("Starting dashboard on http://%s:%d", args.host, args.port)
    if args.proxy:
        logger.info("Using proxy: %s", args.proxy)

    # Run in standalone server mode, not Jupyter mode
    app.run(host=args.host, port=args.port, debug=args.debug)


def _refresh_cache(force: bool = False, proxy_url: str | None = None):
    """Pull latest data from Bloomberg into parquet cache without starting the dashboard."""
    import yaml
    from src.bbg_client import BloombergClient
    from src.data_manager import DataManager
    from src.spread_engine import SpreadEngine

    config_path = PROJECT_ROOT / "config" / "spreads.yaml"
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    settings = cfg.get("settings", {})
    bbg_cfg = settings.get("bloomberg", {})
    cache_dir = PROJECT_ROOT / settings.get("cache", {}).get("directory", "data")

    client = BloombergClient(
        host=bbg_cfg.get("host", "localhost"),
        port=bbg_cfg.get("port", 8194),
        timeout=bbg_cfg.get("timeout", 30000),
        proxy_url=proxy_url,
    )
    client.connect()
    dm = DataManager(cache_dir, client)
    engine = SpreadEngine(config_path, dm)

    end = dt.date.today()
    start = end - dt.timedelta(days=365 * 5)

    # Group tickers by Bloomberg field to minimise API calls
    tfm = engine.ticker_field_map()
    field_to_tickers: dict[str, list[str]] = {}
    for ticker, fields_needed in tfm.items():
        for f in fields_needed:
            field_to_tickers.setdefault(f, []).append(ticker)

    for bbg_field, field_tickers in field_to_tickers.items():
        logger.info(
            "Refreshing cache for %d tickers, field=%s (%s → %s)",
            len(field_tickers), bbg_field, start, end,
        )
        dm.get_history(field_tickers, start, end, fields=[bbg_field], force_refresh=force)

    logger.info("Cache refresh complete. Files in: %s", cache_dir)


if __name__ == "__main__":
    main()
