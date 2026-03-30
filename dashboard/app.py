"""
Main Dash application – commodity premium / discount dashboard.

Layout:
  ┌───────────────────────────────────────────────────┐
  │  HEADER  (title, refresh btn, last-update stamp)  │
  ├───────────────────────────────────────────────────┤
  │  CONTROLS  (category filter, lookback, refresh)   │
  ├───────────────────────────────────────────────────┤
  │  KPI ROW  (top-level metrics for selected group)  │
  ├───────────────────────────────────────────────────┤
  │  Z-SCORE HEATMAP  (all spreads at a glance)       │
  ├───────────────────────────────────────────────────┤
  │  SUMMARY TABLE  (sortable stats grid)             │
  ├───────────────────────────────────────────────────┤
  │  SPREAD CHARTS  (time-series per spread, 2-col)   │
  └───────────────────────────────────────────────────┘
"""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

import dash
from dash import html, dcc, Input, Output, State, callback_context
import plotly.io as pio

from . import theme as T
from .components import kpi_card, spread_chart, legs_chart, zscore_heatmap, summary_table
from .trade_analytics import build_trade_analytics_tab
from .seasonality import build_seasonality_tab, build_seasonality_charts
from .pivot_table import build_pivot_tab, build_pivot_content
from src.bbg_client import BloombergClient
from src.data_manager import DataManager
from src.spread_engine import SpreadEngine, SpreadResult

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "spreads.yaml"

# Register Plotly template
pio.templates["premium_dash"] = T.PLOTLY_TEMPLATE
pio.templates.default = "premium_dash"


def create_app(proxy_url: str | None = None) -> dash.Dash:
    # ------------------------------------------------------------------
    #  Bootstrap services
    # ------------------------------------------------------------------
    engine = SpreadEngine(CONFIG_PATH, _build_data_manager(proxy_url))

    app = dash.Dash(
        __name__,
        title="Commodity Premium/Discount Monitor",
        external_stylesheets=T.EXTERNAL_STYLESHEETS,
        suppress_callback_exceptions=True,
    )

    # Explicitly run in standalone server mode, not Jupyter notebook mode
    app.config.suppress_callback_exceptions = True
    app.config.update({'suppress_callback_exceptions': True})

    # Store results in server-side cache
    app._spread_results: list[SpreadResult] = []
    app._last_refresh: dt.datetime | None = None
    app._engine = engine

    # ------------------------------------------------------------------
    #  Layout
    # ------------------------------------------------------------------
    categories = _get_categories(engine)

    app.layout = html.Div(
        style=T.GLOBAL_CSS,
        children=[
            # Interval for auto-refresh
            dcc.Interval(
                id="auto-refresh",
                interval=engine.config.get("settings", {}).get("dashboard", {}).get("refresh_minutes", 15) * 60_000,
                n_intervals=0,
            ),
            dcc.Store(id="results-store"),

            # ── Header ──
            html.Div(
                style=T.HEADER_STYLE,
                children=[
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "16px"},
                        children=[
                            html.H1(
                                "Commodity Premium / Discount Monitor",
                                style={"fontSize": "18px", "fontWeight": "600", "margin": "0"},
                            ),
                            html.Span(
                                "Base & Precious Metals",
                                style={"fontSize": "12px", "color": T.TEXT_SECONDARY},
                            ),
                        ],
                    ),
                    html.Div(
                        style={"display": "flex", "alignItems": "center", "gap": "12px"},
                        children=[
                            html.Span(id="last-update", style={"fontSize": "11px", "color": T.TEXT_MUTED}),
                            html.Button(
                                "Refresh",
                                id="refresh-btn",
                                style={
                                    "backgroundColor": T.ACCENT_BLUE,
                                    "color": "#fff",
                                    "border": "none",
                                    "borderRadius": "6px",
                                    "padding": "6px 16px",
                                    "fontSize": "12px",
                                    "fontWeight": "500",
                                    "cursor": "pointer",
                                },
                            ),
                        ],
                    ),
                ],
            ),

            # ── Tabs ──
            dcc.Tabs(
                id="main-tabs",
                value="tab-monitor",
                children=[
                    # Tab 1: Premium Monitor (original dashboard)
                    dcc.Tab(
                        label="Premium Monitor",
                        value="tab-monitor",
                        children=[
                            # Controls for this tab
                            html.Div(
                                style={"padding": "12px 24px", "display": "flex", "gap": "16px", "flexWrap": "wrap", "alignItems": "center"},
                                children=[
                                    html.Div([
                                        html.Label("Category", style={"fontSize": "11px", "color": T.TEXT_SECONDARY, "marginBottom": "4px", "display": "block"}),
                                        dcc.Dropdown(
                                            id="category-filter",
                                            options=[{"label": "All", "value": "ALL"}] + [{"label": c, "value": c} for c in categories],
                                            value="ALL",
                                            style={"width": "320px", "fontSize": "12px"},
                                            className="dash-dropdown-dark",
                                        ),
                                    ]),
                                    html.Div([
                                        html.Label("Lookback", style={"fontSize": "11px", "color": T.TEXT_SECONDARY, "marginBottom": "4px", "display": "block"}),
                                        dcc.Dropdown(
                                            id="lookback-select",
                                            options=[
                                                {"label": "3M", "value": 90},
                                                {"label": "6M", "value": 180},
                                                {"label": "1Y", "value": 365},
                                                {"label": "2Y", "value": 730},
                                                {"label": "5Y", "value": 1825},
                                            ],
                                            value=365,
                                            style={"width": "100px", "fontSize": "12px"},
                                            className="dash-dropdown-dark",
                                        ),
                                    ]),
                                ],
                            ),
                            # Content
                            html.Div(id="main-content", style={"padding": "0 24px 40px"}),
                        ],
                    ),

                    # Tab 2: Dubai Trade Analytics
                    dcc.Tab(
                        label="Dubai Trade Analytics",
                        value="tab-analytics",
                        children=[
                            html.Div(id="analytics-content"),
                        ],
                    ),

                    # Tab 3: Trade Seasonality
                    dcc.Tab(
                        label="Trade Seasonality",
                        value="tab-seasonality",
                        children=[
                            html.Div(id="seasonality-tab-content"),
                        ],
                    ),

                    # Tab 4: Precious Metals Trade Pivot
                    dcc.Tab(
                        label="Trade Pivot",
                        value="tab-pivot",
                        children=[
                            html.Div(id="pivot-tab-content"),
                        ],
                    ),
                ],
                style={"borderBottom": f"1px solid {T.BG_TERTIARY}"},
            ),
        ],
    )

    # ------------------------------------------------------------------
    #  Callbacks
    # ------------------------------------------------------------------
    @app.callback(
        Output("main-content", "children"),
        Output("last-update", "children"),
        Input("auto-refresh", "n_intervals"),
        Input("refresh-btn", "n_clicks"),
        Input("category-filter", "value"),
        Input("lookback-select", "value"),
        prevent_initial_call=False,
    )
    def update_dashboard(n_intervals, n_clicks, category, lookback_days):
        import traceback as tb
        try:
            return _do_update(n_intervals, n_clicks, category, lookback_days)
        except Exception:
            tb.print_exc()
            raise

    def _do_update(n_intervals, n_clicks, category, lookback_days):
        ctx = callback_context
        trigger = ctx.triggered[0]["prop_id"] if ctx.triggered else ""

        if lookback_days is None:
            lookback_days = 365

        # Re-compute on refresh or first load
        need_recompute = (
            app._last_refresh is None
            or "refresh-btn" in trigger
            or "auto-refresh" in trigger
        )

        if need_recompute:
            end = dt.date.today()
            start = end - dt.timedelta(days=int(lookback_days) + 90)
            force = "refresh-btn" in trigger
            try:
                app._spread_results = app._engine.compute_all(start, end, force_refresh=force)
            except Exception:
                logger.exception("Failed to compute spreads")
            app._last_refresh = dt.datetime.now()

        results = app._spread_results

        # Filter by category
        if category and category != "ALL":
            results = [r for r in results if r.definition.category == category]

        if not results:
            return html.Div(
                "No spread data available. Check Bloomberg connection and config.",
                style={"color": T.TEXT_MUTED, "padding": "40px", "textAlign": "center"},
            ), _stamp(app._last_refresh)

        # Trim series to lookback window and recompute percentile bands
        cutoff = pd.Timestamp(dt.date.today() - dt.timedelta(days=int(lookback_days)))
        for r in results:
            r.series = r.series.loc[r.series.index >= cutoff]
            for exp in r.expiry_results:
                exp.series = exp.series.loc[exp.series.index >= cutoff]
            # Recompute percentile bands from visible window
            vals = r.series.dropna().values
            if len(vals) > 1:
                r.pct_10 = float(np.nanpercentile(vals, 10))
                r.pct_25 = float(np.nanpercentile(vals, 25))
                r.pct_50 = float(np.nanpercentile(vals, 50))
                r.pct_75 = float(np.nanpercentile(vals, 75))
                r.pct_90 = float(np.nanpercentile(vals, 90))

        # ── Build layout sections ──
        sections: list = []

        # KPI row
        kpi_cards = []
        for r in results[:12]:  # limit row to first 12
            fmt = ".1f" if abs(r.current_value) > 100 else ".2f"
            kpi_cards.append(kpi_card(
                r.definition.name, r.current_value, r.definition.unit,
                r.change_1d, r.percentile_1y, fmt,
            ))
        sections.append(html.Div(
            style={"display": "flex", "gap": "12px", "overflowX": "auto", "paddingBottom": "4px", "marginBottom": "16px"},
            children=kpi_cards,
        ))

        # Heatmap
        sections.append(html.Div(
            style=T.CARD_STYLE,
            children=[zscore_heatmap(results, height=160 if len(results) <= 10 else 200)],
        ))

        # Summary table
        sections.append(html.Div(
            style={**T.CARD_STYLE, "overflowX": "auto"},
            children=[
                html.H3("Summary", style={"fontSize": "14px", "marginBottom": "12px", "fontWeight": "600"}),
                summary_table(results),
            ],
        ))

        # Spread charts – 2-column CSS grid
        chart_grid = []
        for r in results:
            chart_grid.append(html.Div(
                style=T.CARD_STYLE,
                children=[
                    spread_chart(r, height=250),
                    legs_chart(r, height=190) or html.Div(),
                ],
            ))
        sections.append(html.Div(
            style={
                "display": "grid",
                "gridTemplateColumns": "repeat(2, 1fr)",
                "gap": "12px",
            },
            children=chart_grid,
        ))

        return sections, _stamp(app._last_refresh)

    # Callback for analytics tab
    @app.callback(
        Output("analytics-content", "children"),
        Input("main-tabs", "value"),
    )
    def update_analytics_tab(active_tab):
        if active_tab != "tab-analytics":
            return html.Div()
        try:
            return build_trade_analytics_tab()
        except Exception:
            logger.exception("Failed to build analytics tab")
            return html.Div(
                "Failed to load trade analytics. Check data sources.",
                style={"color": T.TEXT_MUTED, "padding": "40px", "textAlign": "center"},
            )

    # Callback for seasonality tab — render controls on tab switch
    @app.callback(
        Output("seasonality-tab-content", "children"),
        Input("main-tabs", "value"),
    )
    def update_seasonality_tab(active_tab):
        if active_tab != "tab-seasonality":
            return html.Div()
        try:
            return build_seasonality_tab()
        except Exception:
            logger.exception("Failed to build seasonality tab")
            return html.Div(
                "Failed to load seasonality tab. Check data sources.",
                style={"color": T.TEXT_MUTED, "padding": "40px", "textAlign": "center"},
            )

    # Callback for seasonality charts — update on control changes
    @app.callback(
        Output("seasonality-content", "children"),
        Input("seasonality-country", "value"),
        Input("seasonality-flow", "value"),
        Input("seasonality-metric", "value"),
        Input("seasonality-topn", "value"),
        Input("seasonality-recent", "value"),
        Input("seasonality-zscore-mode", "value"),
        prevent_initial_call=False,
    )
    def update_seasonality_charts(country, flow, metric, top_n, recent_months, zscore_mode):
        if not country:
            return html.Div(
                "Select a country to view seasonality data.",
                style={"color": T.TEXT_MUTED, "padding": "40px", "textAlign": "center"},
            )
        try:
            return build_seasonality_charts(
                country_code=country,
                flow_code=flow or "M",
                metric=metric or "value_usd",
                top_n=top_n or 15,
                recent_months=recent_months or 6,
                zscore_mode=zscore_mode or "row",
            )
        except Exception:
            logger.exception("Failed to build seasonality charts")
            return html.Div(
                "Failed to load seasonality data. The data may not be cached yet — "
                "check network connectivity or try again.",
                style={"color": T.TEXT_MUTED, "padding": "40px", "textAlign": "center"},
            )

    # Callback for pivot tab — render controls on tab switch
    @app.callback(
        Output("pivot-tab-content", "children"),
        Input("main-tabs", "value"),
    )
    def update_pivot_tab(active_tab):
        if active_tab != "tab-pivot":
            return html.Div()
        try:
            return build_pivot_tab()
        except Exception:
            logger.exception("Failed to build pivot tab")
            return html.Div(
                "Failed to load pivot table. Check data sources.",
                style={"color": T.TEXT_MUTED, "padding": "40px", "textAlign": "center"},
            )

    # Callback for pivot content — update on control changes
    @app.callback(
        Output("pivot-content", "children"),
        Input("pivot-commodity", "value"),
        Input("pivot-reporter", "value"),
        Input("pivot-flow", "value"),
        Input("pivot-source", "value"),
        Input("pivot-date-range", "start_date"),
        Input("pivot-date-range", "end_date"),
        Input("pivot-rows", "value"),
        Input("pivot-cols", "value"),
        Input("pivot-values", "value"),
        Input("pivot-agg", "value"),
        Input("pivot-refresh-btn", "n_clicks"),
        prevent_initial_call=False,
    )
    def update_pivot_content(
        commodities, reporters, flow, source, start_date, end_date,
        row_axis, col_axis, value_col, agg_func, n_clicks,
    ):
        from dash import callback_context as ctx
        force = False
        if ctx.triggered:
            trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
            force = trigger_id == "pivot-refresh-btn" and (n_clicks or 0) > 0

        if not commodities or not reporters:
            return html.Div(
                "Select at least one commodity and one reporter country.",
                style={"color": T.TEXT_MUTED, "padding": "40px", "textAlign": "center"},
            )
        try:
            return build_pivot_content(
                commodities=commodities if isinstance(commodities, list) else [commodities],
                reporters=reporters if isinstance(reporters, list) else [reporters],
                flow=flow or "MX",
                start_date=start_date,
                end_date=end_date,
                row_axis=row_axis or "reporter",
                col_axis=col_axis or "commodity",
                value_col=value_col or "value_usd",
                agg_func=agg_func or "sum",
                force_refresh=force,
                source=source or "all",
            )
        except Exception:
            logger.exception("Failed to build pivot content")
            return html.Div(
                "Failed to build pivot table. Check data sources or try refreshing.",
                style={"color": T.TEXT_MUTED, "padding": "40px", "textAlign": "center"},
            )

    return app


# ======================================================================
#  Helpers
# ======================================================================
def _build_data_manager(proxy_url: str | None = None) -> DataManager:
    import yaml
    import os

    # Proxy from function parameter, config, or environment variable (in order of precedence)
    if proxy_url is None:
        proxy_url = os.environ.get("PREMIUM_DASH_PROXY")

    if proxy_url is None:
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
        net_cfg = cfg.get("settings", {}).get("network", {})
        proxy_url = net_cfg.get("proxy_url")

    with open(CONFIG_PATH) as f:
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
    return DataManager(cache_dir, client)


def _get_categories(engine: SpreadEngine) -> list[str]:
    seen = []
    for sd in engine.spread_defs:
        if sd.enabled and sd.category not in seen:
            seen.append(sd.category)
    return seen


def _stamp(last: dt.datetime | None) -> str:
    if last is None:
        return ""
    return f"Last refresh: {last.strftime('%H:%M:%S')}"


# Need pandas/numpy for cutoff trimming and percentile recalc
import numpy as np
import pandas as pd
