# Proxy Configuration & Data Caching

This guide explains how to configure proxy support and understand the caching strategy for the Premium Dashboard.

## Proxy Configuration

### Overview
The dashboard supports proxy configuration for all HTTP requests across:
- **Bloomberg API** (via blpapi connection)
- **yfinance** (market data downloads)
- **UN Comtrade API** (UAE gold trade data)
- **World Gold Council** (India gold premiums)
- **CBUAE** (Central bank data)
- **TrendEconomy** (Trade statistics)
- **opendata.swiss** (Swiss trade data)

### How to Configure Proxy

#### Option 1: Configuration File (Persistent)
Edit `config/spreads.yaml` and set the proxy URL:

```yaml
settings:
  network:
    proxy_url: "http://proxy.corp.com:3128"
    # or: "socks5://127.0.0.1:1080"
    # or: "http://user:password@proxy.corp.com:3128"
```

#### Option 2: Command Line (Temporary Override)
```bash
python main.py --proxy "http://proxy.corp.com:3128"
python main.py --proxy "socks5://127.0.0.1:1080"
```

#### Option 3: Environment Variable (Highest Priority)
```bash
export PREMIUM_DASH_PROXY="http://proxy.corp.com:3128"
python main.py
```

**Priority Order:** Environment Variable > Command Line > Config File

### Proxy URL Formats

| Format | Use Case |
|--------|----------|
| `http://proxy.corp.com:3128` | HTTP proxy (most common) |
| `https://proxy.corp.com:3128` | HTTPS proxy |
| `socks5://127.0.0.1:1080` | SOCKS5 proxy |
| `http://user:pass@proxy:3128` | Authenticated proxy |
| `socks5://user:pass@proxy:1080` | Authenticated SOCKS5 |

### Testing Proxy Connection
```bash
# Test with --refresh-only to verify proxy works before starting dashboard
python main.py --proxy "http://proxy.corp.com:3128" --refresh-only
```

---

## Data Caching Strategy

### All Caching Locations
All data is stored in `data/` directory with the following structure:

```
data/
├── gold_trade/                    # Gold trade & premium data
│   ├── monthly_premiums.parquet   # Dubai/SGE premiums + macro data
│   ├── swiss_impex_gold.parquet   # Swiss-UAE gold flows
│   ├── comtrade_*.parquet         # UN Comtrade UAE trade data
│   ├── wgc_gold_premium.parquet   # India gold premium (WGC)
│   ├── cbuae_reserves.parquet     # Central bank reserves
│   ├── trendeconomy_*.parquet     # UAE trade statistics
│   └── *.parquet                  # API cache files (by endpoint)
│
└── [tickers]/                     # Bloomberg ticker data
    ├── Z7A Comdty.parquet         # Dubai gold
    ├── GCA Comdty.parquet         # COMEX gold
    ├── USDAED Curncy.parquet      # USD/AED exchange rate
    └── [other-tickers].parquet    # All other configured spreads
```

### Cache Behavior

#### 1. **Excel Data Caching** (`src/gold_trade_loader.py`)
- **Source:** `cowork/UAE_Gold_Trade_Historical_Data.xlsx`
- **Cache:** `data/gold_trade/monthly_premiums.parquet`
- **Strategy:** Read once, cache to parquet for fast load
- **Refresh:** Add `--force-refresh` flag or manually delete parquet file

```python
# Load from cache on subsequent runs
df = load_dubai_premium_data()  # Uses cache if available

# Force re-read from Excel
df = load_dubai_premium_data(force_refresh=True)
```

#### 2. **Bloomberg Data Caching** (`src/data_manager.py`)
- **Source:** Bloomberg API (Z7A, GCA, USDAED, etc.)
- **Cache:** `data/[ticker].parquet` per ticker
- **Strategy:** Incremental fetch - only fetches dates after last cached date
- **Refresh:** Add `--force-refresh` flag

```bash
# Normal run (uses cache, fetches only new dates)
python main.py

# Force refresh from Bloomberg (full history re-fetch)
python main.py --force-refresh

# Refresh-only mode (don't start dashboard)
python main.py --refresh-only --force-refresh
```

#### 3. **API Data Caching** (`src/gold_trade_client.py`)
Multiple external data sources with automatic parquet caching:

| Source | Cache File | Frequency |
|--------|-----------|-----------|
| UN Comtrade | `comtrade_*.parquet` | Monthly (on demand) |
| Swiss-Impex | `swiss_impex_gold.parquet` | Monthly (on demand) |
| World Gold Council | `wgc_gold_premium.parquet` | Daily (on demand) |
| CBUAE Reserves | `cbuae_reserves.parquet` | Monthly (on demand) |
| TrendEconomy | `trendeconomy_*.parquet` | Annual (on demand) |

Each method respects `force_refresh=True` to bypass cache.

#### 4. **Trade Analytics Caching** (`src/trade_analytics_engine.py`)
- **Sources:** Excel files + cached Bloomberg data
- **Strategy:** No separate cache (uses underlying caches from above)
- **Load Time:** <1 second (all data already cached)

### Manual Cache Invalidation

#### Clear Specific Cache
```bash
# Delete specific parquet files
rm data/gold_trade/monthly_premiums.parquet
rm data/[ticker-name].parquet

# Next run will re-fetch and re-cache
python main.py
```

#### Clear All Cache
```bash
# Remove entire data directory
rm -rf data/

# Next run will re-fetch everything (takes longer)
python main.py --refresh-only
python main.py  # Then start dashboard
```

### Cache Performance Impact

| Scenario | Time | Notes |
|----------|------|-------|
| First run (no cache) | 2-5 min | Fetches all Bloomberg data + APIs |
| Incremental run (daily) | 10-30 sec | Only fetches new data since yesterday |
| Using cache only | <1 sec | All data in parquet, no network calls |
| Force refresh | 2-5 min | Ignores cache, re-fetches everything |

### Bandwidth Optimization

The caching system is designed to minimize network traffic:

1. **Bloomberg Data**
   - Incremental fetch: Only pulls data after last cached date
   - Reduces 5 years of history to ~1 day of new data per run
   - Example: 250+ trading days → 1 new data point/day

2. **API Data**
   - Cached to parquet on first fetch
   - Subsequent loads from local disk only
   - API calls only needed when `force_refresh=True`

3. **Excel Data**
   - One-time parquet conversion from Excel
   - Subsequent loads from fast parquet format
   - 10-50x faster than reading Excel directly

### Firewall Scenarios

#### Scenario A: Firewall blocks direct Bloomberg connection
**Solution:** Use proxy that can reach Bloomberg port 8194
```bash
python main.py --proxy "http://proxy.internal.net:3128"
```

#### Scenario B: Firewall blocks external APIs (Comtrade, WGC, etc.)
**Solution:** Use proxy with outbound HTTPS access
```bash
python main.py --proxy "socks5://bastion.internal:1080"
```

#### Scenario C: Offline mode (no network at all)
**Solution:** Pre-cache all data, then run offline
```bash
# On online machine:
python main.py --refresh-only
# Copy data/ directory to offline machine

# On offline machine:
python main.py  # Uses only cached data
```

### Cache Verification

Check cache status:
```bash
# List all cached parquet files
ls -lh data/**/*.parquet

# Check cache size
du -sh data/

# Verify parquet integrity
python -c "
import pandas as pd
from pathlib import Path
for f in Path('data').rglob('*.parquet'):
    try:
        df = pd.read_parquet(f)
        print(f'✓ {f.name}: {len(df)} rows')
    except Exception as e:
        print(f'✗ {f.name}: {e}')
"
```

---

## Example Configurations

### Corporate Firewall
```yaml
settings:
  network:
    proxy_url: "http://user:password@proxy.corp.com:3128"
```

### SOCKS5 Tunnel
```yaml
settings:
  network:
    proxy_url: "socks5://bastion.internal:1080"
```

### No Proxy (Default)
```yaml
settings:
  network:
    proxy_url: null  # No proxy
```

---

## Troubleshooting

### Proxy Connection Failed
```
ERROR: RequestException: Failed to connect through proxy
```
**Solution:**
1. Verify proxy URL format: `protocol://host:port`
2. Check proxy is running and accessible
3. Test: `curl -x http://proxy:3128 https://www.google.com`
4. Check credentials in proxy URL if authenticated

### Data Not Updating
```
INFO: Using cached data from [timestamp]
```
**Solution:**
```bash
# Force refresh to bypass cache
python main.py --force-refresh

# Or delete specific cache
rm data/[ticker-name].parquet
```

### Slow First Load
```
INFO: Fetching 15 tickers from Bloomberg...
```
**Solution:** This is normal for first run. Subsequent loads will be faster.
- Use `--refresh-only` to cache data in background
- First dashboard access uses cached data only

### Out of Memory on Large Refreshes
```
MemoryError: Unable to allocate X GB
```
**Solution:**
1. Use incremental caching (normal mode, no `--force-refresh`)
2. Reduce lookback period in `config/spreads.yaml`
3. Run `--refresh-only` instead of full dashboard

---

## Advanced: Custom Caching Strategy

To implement custom cache locations or add new data sources:

```python
# Example: Add custom data source with caching
from pathlib import Path
import pandas as pd

class CustomDataCache:
    def __init__(self, cache_dir="data/custom"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch_and_cache(self, source_name, fetch_func, force_refresh=False):
        cache_file = self.cache_dir / f"{source_name}.parquet"

        if cache_file.exists() and not force_refresh:
            return pd.read_parquet(cache_file)

        df = fetch_func()  # Call your fetch function
        df.to_parquet(cache_file)
        return df

# Usage
cache = CustomDataCache()
df = cache.fetch_and_cache("my_data", my_fetch_function, force_refresh=False)
```

---

## Environment Variables

| Variable | Purpose | Example |
|----------|---------|---------|
| `PREMIUM_DASH_PROXY` | Override proxy URL | `http://proxy:3128` |
| `PREMIUM_DASH_HOST` | Dashboard host | `0.0.0.0` |
| `PREMIUM_DASH_PORT` | Dashboard port | `8050` |

---

## Related Files
- `config/spreads.yaml` - Main configuration
- `src/gold_trade_client.py` - External API client with caching
- `src/data_manager.py` - Bloomberg data cache manager
- `src/gold_trade_loader.py` - Excel data loader with caching
- `main.py` - Proxy configuration reader
