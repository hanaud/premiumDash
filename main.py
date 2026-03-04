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
import sys
from pathlib import Path

# Ensure project root is on the path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("premiumDash")


def main():
    parser = argparse.ArgumentParser(description="Commodity Premium/Discount Dashboard")
    parser.add_argument("--port", type=int, default=8050, help="Dashboard port (default 8050)")
    parser.add_argument("--host", default="127.0.0.1", help="Dashboard host (default 127.0.0.1)")
    parser.add_argument("--debug", action="store_true", help="Enable Dash debug mode")
    parser.add_argument("--refresh-only", action="store_true", help="Refresh data cache and exit")
    parser.add_argument("--force-refresh", action="store_true", help="Ignore cache, refetch everything")
    args = parser.parse_args()

    if args.refresh_only:
        _refresh_cache(args.force_refresh)
        return

    from dashboard.app import create_app

    app = create_app()
    logger.info("Starting dashboard on http://%s:%d", args.host, args.port)
    app.run(host=args.host, port=args.port, debug=args.debug)


def _refresh_cache(force: bool = False):
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
    )
    client.connect()
    dm = DataManager(cache_dir, client)
    engine = SpreadEngine(config_path, dm)

    tickers = engine.all_tickers()
    end = dt.date.today()
    start = end - dt.timedelta(days=365 * 5)

    logger.info("Refreshing cache for %d tickers (%s → %s)", len(tickers), start, end)
    dm.get_history(tickers, start, end, force_refresh=force)
    logger.info("Cache refresh complete. Files in: %s", cache_dir)


if __name__ == "__main__":
    main()
