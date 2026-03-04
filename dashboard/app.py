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
from src.bbg_client import BloombergClient
from src.data_manager import DataManager
from src.spread_engine import SpreadEngine, SpreadResult

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "spreads.yaml"

# Register Plotly template
pio.templates["premium_dash"] = T.PLOTLY_TEMPLATE
pio.templates.default = "premium_dash"


def create_app() -> dash.Dash:
    # ------------------------------------------------------------------
    #  Bootstrap services
    # ------------------------------------------------------------------
    engine = SpreadEngine(CONFIG_PATH, _build_data_manager())

    app = dash.Dash(
        __name__,
        title="Commodity Premium/Discount Monitor",
        external_stylesheets=T.EXTERNAL_STYLESHEETS,
        suppress_callback_exceptions=True,
    )

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

            # ── Controls ──
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

            # ── Main content ──
            html.Div(id="main-content", style={"padding": "0 24px 40px"}),
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

        # Trim series to lookback window for display
        cutoff = pd.Timestamp(dt.date.today() - dt.timedelta(days=int(lookback_days)))
        for r in results:
            r.series = r.series.loc[r.series.index >= cutoff]
            for exp in r.expiry_results:
                exp.series = exp.series.loc[exp.series.index >= cutoff]

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

    return app


# ======================================================================
#  Helpers
# ======================================================================
def _build_data_manager() -> DataManager:
    import yaml
    with open(CONFIG_PATH) as f:
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


# Need pandas for cutoff trimming
import pandas as pd
