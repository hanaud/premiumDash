"""
Precious Metals Trade Pivot Table — dashboard tab.

Full-flexibility pivot: user picks row/column/value axes, filters
by commodity, reporter, flow, and date range.  Data is loaded from
cached Comtrade parquet files.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import html, dcc, dash_table

from . import theme as T
from src.comtrade_bulk_client import (
    ComtradeBulkClient,
    HS_CODES,
    REPORTER_COUNTRIES,
)

logger = logging.getLogger(__name__)

# Lazy-loaded client
_client: ComtradeBulkClient | None = None


def _get_client() -> ComtradeBulkClient:
    global _client
    if _client is None:
        _client = ComtradeBulkClient()
    return _client


def _base_layout(**overrides) -> dict:
    base = dict(T.PLOTLY_TEMPLATE["layout"])
    base.update(overrides)
    return base


# ======================================================================
#  Dropdown option builders
# ======================================================================
_COMMODITY_OPTIONS = [{"label": name, "value": code} for name, code in HS_CODES.items()]

_COUNTRY_OPTIONS = sorted(
    [{"label": name, "value": code} for code, name in REPORTER_COUNTRIES.items()],
    key=lambda x: x["label"],
)

_FLOW_OPTIONS = [
    {"label": "Both", "value": "MX"},
    {"label": "Import", "value": "M"},
    {"label": "Export", "value": "X"},
]

_AXIS_OPTIONS = [
    {"label": "Reporter", "value": "reporter"},
    {"label": "Partner", "value": "partner"},
    {"label": "Commodity", "value": "commodity"},
    {"label": "Year", "value": "year"},
    {"label": "Month", "value": "month"},
    {"label": "Flow", "value": "flow"},
]

_VALUE_OPTIONS = [
    {"label": "Value (USD)", "value": "value_usd"},
    {"label": "Net Weight (kg)", "value": "net_weight_kg"},
    {"label": "Avg Price (USD/kg)", "value": "avg_price"},
]

_AGG_OPTIONS = [
    {"label": "Sum", "value": "sum"},
    {"label": "Mean", "value": "mean"},
    {"label": "Count", "value": "count"},
]

_YEAR_OPTIONS = [{"label": str(y), "value": y} for y in range(2018, 2027)]

# -- Dropdown style helper ------------------------------------------------
_DD_STYLE = {"width": "160px", "fontSize": "12px"}
_LABEL_STYLE = {
    "fontSize": "11px",
    "color": T.TEXT_SECONDARY,
    "marginBottom": "4px",
    "display": "block",
}


def _dropdown_block(label: str, dd_id: str, options, value, multi=False, width="160px"):
    """Reusable labelled dropdown wrapper."""
    return html.Div([
        html.Label(label, style=_LABEL_STYLE),
        dcc.Dropdown(
            id=dd_id,
            options=options,
            value=value,
            multi=multi,
            style={**_DD_STYLE, "width": width},
            className="dash-dropdown-dark",
        ),
    ])


# ======================================================================
#  Tab shell (controls + placeholder)
# ======================================================================
def build_pivot_tab() -> html.Div:
    """Return the static shell with controls; content is filled by callback."""
    return html.Div(
        style={"padding": "16px 24px 40px"},
        children=[
            # -- Header --
            html.H2(
                "Precious Metals Trade Pivot",
                style={"margin": "0 0 16px", "fontSize": "18px", "fontWeight": "600"},
            ),

            # -- Filter row --
            html.Div(
                style={
                    "display": "flex", "gap": "12px", "flexWrap": "wrap",
                    "alignItems": "flex-end", "marginBottom": "12px",
                    **T.CARD_STYLE,
                },
                children=[
                    _dropdown_block("Commodity", "pivot-commodity", _COMMODITY_OPTIONS,
                                    [c["value"] for c in _COMMODITY_OPTIONS], multi=True, width="220px"),
                    _dropdown_block("Reporter", "pivot-reporter", _COUNTRY_OPTIONS,
                                    [c["value"] for c in _COUNTRY_OPTIONS], multi=True, width="260px"),
                    _dropdown_block("Flow", "pivot-flow", _FLOW_OPTIONS, "MX"),
                    _dropdown_block("From", "pivot-start-year", _YEAR_OPTIONS, 2018, width="90px"),
                    _dropdown_block("To", "pivot-end-year", _YEAR_OPTIONS, 2026, width="90px"),
                ],
            ),

            # -- Pivot axes row --
            html.Div(
                style={
                    "display": "flex", "gap": "12px", "flexWrap": "wrap",
                    "alignItems": "flex-end", "marginBottom": "16px",
                    **T.CARD_STYLE,
                },
                children=[
                    _dropdown_block("Rows", "pivot-rows", _AXIS_OPTIONS, "reporter"),
                    _dropdown_block("Columns", "pivot-cols", _AXIS_OPTIONS, "commodity"),
                    _dropdown_block("Values", "pivot-values", _VALUE_OPTIONS, "value_usd"),
                    _dropdown_block("Aggregation", "pivot-agg", _AGG_OPTIONS, "sum"),
                    html.Div([
                        html.Label(" ", style=_LABEL_STYLE),
                        html.Button(
                            "Refresh Data",
                            id="pivot-refresh-btn",
                            n_clicks=0,
                            style={
                                "backgroundColor": T.ACCENT_BLUE,
                                "color": "#fff",
                                "border": "none",
                                "borderRadius": "4px",
                                "padding": "8px 16px",
                                "fontSize": "12px",
                                "fontWeight": "500",
                                "cursor": "pointer",
                            },
                        ),
                    ]),
                ],
            ),

            # -- Dynamic content --
            dcc.Loading(
                type="dot",
                color=T.ACCENT_BLUE,
                children=html.Div(id="pivot-content"),
            ),
        ],
    )


# ======================================================================
#  Content builder (called from callback)
# ======================================================================
def build_pivot_content(
    commodities: list[str],
    reporters: list[str],
    flow: str,
    start_year: int,
    end_year: int,
    row_axis: str,
    col_axis: str,
    value_col: str,
    agg_func: str,
    force_refresh: bool = False,
) -> list:
    """Build the pivot table + summary chart from cached data."""
    client = _get_client()

    if force_refresh:
        client.fetch_all(
            start_year=start_year,
            end_year=end_year,
            commodities=commodities or None,
            countries=reporters or None,
            force_refresh=True,
        )

    # Load cached data
    df = client.load_all_cached(
        commodities=commodities or None,
        countries=reporters or None,
    )

    if df.empty:
        return [html.Div(
            "No cached data found.  Click 'Refresh Data' to fetch from Comtrade API.",
            style={"color": T.TEXT_MUTED, "padding": "40px", "textAlign": "center"},
        )]

    # -- Apply filters --
    # Date range
    if "date" in df.columns:
        df = df[
            (df["date"] >= pd.Timestamp(start_year, 1, 1))
            & (df["date"] <= pd.Timestamp(end_year, 12, 31))
        ]

    # Flow filter
    if flow != "MX" and "flow_code" in df.columns:
        df = df[df["flow_code"] == flow]

    if df.empty:
        return [html.Div(
            "No data for the selected filters.",
            style={"color": T.TEXT_MUTED, "padding": "40px", "textAlign": "center"},
        )]

    # -- Derived columns for pivot axes --
    if "date" in df.columns:
        df["year"] = df["date"].dt.year
        df["month"] = df["date"].dt.month

    # Computed value
    if value_col == "avg_price":
        df["avg_price"] = np.where(
            df["net_weight_kg"].fillna(0) > 0,
            df["value_usd"] / df["net_weight_kg"],
            np.nan,
        )

    # -- Build pivot --
    aggfunc = agg_func if agg_func != "count" else "size"
    try:
        pivot = pd.pivot_table(
            df,
            index=row_axis,
            columns=col_axis,
            values=value_col if value_col != "avg_price" or "avg_price" in df.columns else "value_usd",
            aggfunc=aggfunc,
            fill_value=0,
        )
    except Exception as exc:
        logger.exception("Pivot table failed")
        return [html.Div(
            f"Pivot error: {exc}",
            style={"color": T.ACCENT_RED, "padding": "40px", "textAlign": "center"},
        )]

    # Flatten multi-level column names if needed
    if isinstance(pivot.columns, pd.MultiIndex):
        pivot.columns = [" | ".join(str(c) for c in col) for col in pivot.columns]

    # Sort by total descending
    pivot["_total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("_total", ascending=False)
    pivot = pivot.drop(columns=["_total"])

    # -- Format for DataTable --
    display_df = pivot.reset_index()
    display_df.columns = [str(c) for c in display_df.columns]

    # Format numeric columns
    numeric_cols = [c for c in display_df.columns if c != row_axis]
    for col in numeric_cols:
        if value_col == "value_usd":
            display_df[col] = display_df[col].apply(_fmt_usd)
        elif value_col == "net_weight_kg":
            display_df[col] = display_df[col].apply(_fmt_weight)
        elif value_col == "avg_price":
            display_df[col] = display_df[col].apply(_fmt_price)
        elif agg_func == "count":
            display_df[col] = display_df[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "")

    # DataTable columns
    table_columns = [{"name": c, "id": c} for c in display_df.columns]

    # -- Summary bar chart (top 20 by row total) --
    bar_df = pivot.head(20)
    bar_fig = go.Figure()
    for col in bar_df.columns:
        bar_fig.add_trace(go.Bar(
            name=str(col),
            x=bar_df.index.astype(str),
            y=bar_df[col],
            text=[_fmt_compact(v) for v in bar_df[col]],
            textposition="none",
        ))
    bar_fig.update_layout(
        **_base_layout(
            barmode="stack",
            title=f"Top 20 by {_value_label(value_col)} ({agg_func})",
            xaxis_title=row_axis.title(),
            yaxis_title=_value_label(value_col),
            height=420,
            margin=dict(l=60, r=20, t=50, b=100),
            xaxis=dict(tickangle=-45),
        ),
    )

    # -- Stats line --
    total_records = len(df)
    cache_info = client.get_available_cache_info()
    total_cache_kb = sum(c["size_kb"] for c in cache_info)

    return [
        # Stats bar
        html.Div(
            style={"display": "flex", "gap": "24px", "marginBottom": "12px", "fontSize": "12px", "color": T.TEXT_SECONDARY},
            children=[
                html.Span(f"Total records: {total_records:,}"),
                html.Span(f"Pivot rows: {len(pivot):,}"),
                html.Span(f"Cache files: {len(cache_info)} ({total_cache_kb:,.0f} KB)"),
            ],
        ),
        # Pivot table
        html.Div(
            style=T.CARD_STYLE,
            children=[
                dash_table.DataTable(
                    id="pivot-datatable",
                    columns=table_columns,
                    data=display_df.to_dict("records"),
                    sort_action="native",
                    filter_action="native",
                    page_size=30,
                    style_table={"overflowX": "auto"},
                    style_header={
                        "backgroundColor": T.BG_TERTIARY,
                        "color": T.TEXT_PRIMARY,
                        "fontWeight": "600",
                        "fontSize": "11px",
                        "textTransform": "uppercase",
                        "border": f"1px solid {T.BORDER}",
                    },
                    style_cell={
                        "backgroundColor": T.BG_SECONDARY,
                        "color": T.TEXT_PRIMARY,
                        "border": f"1px solid {T.BORDER}",
                        "fontSize": "12px",
                        "padding": "6px 10px",
                        "textAlign": "right",
                        "minWidth": "90px",
                    },
                    style_cell_conditional=[
                        {"if": {"column_id": row_axis}, "textAlign": "left", "fontWeight": "500"},
                    ],
                    style_data_conditional=[
                        {"if": {"row_index": "odd"}, "backgroundColor": T.BG_PRIMARY},
                    ],
                    style_filter={
                        "backgroundColor": T.BG_TERTIARY,
                        "color": T.TEXT_PRIMARY,
                        "border": f"1px solid {T.BORDER}",
                    },
                ),
            ],
        ),
        # Summary chart
        html.Div(
            style={**T.CARD_STYLE, "marginTop": "12px"},
            children=[
                dcc.Graph(figure=bar_fig, config={"displayModeBar": False}),
            ],
        ),
    ]


# ======================================================================
#  Formatting helpers
# ======================================================================
def _fmt_usd(v) -> str:
    if pd.isna(v) or v == 0:
        return ""
    if abs(v) >= 1e9:
        return f"${v / 1e9:,.2f}B"
    if abs(v) >= 1e6:
        return f"${v / 1e6:,.1f}M"
    if abs(v) >= 1e3:
        return f"${v / 1e3:,.0f}K"
    return f"${v:,.0f}"


def _fmt_weight(v) -> str:
    if pd.isna(v) or v == 0:
        return ""
    if abs(v) >= 1e6:
        return f"{v / 1e3:,.0f}t"
    if abs(v) >= 1e3:
        return f"{v / 1e3:,.1f}t"
    return f"{v:,.0f}kg"


def _fmt_price(v) -> str:
    if pd.isna(v) or v == 0:
        return ""
    return f"${v:,.0f}"


def _fmt_compact(v) -> str:
    if pd.isna(v) or v == 0:
        return ""
    if abs(v) >= 1e9:
        return f"{v / 1e9:.1f}B"
    if abs(v) >= 1e6:
        return f"{v / 1e6:.0f}M"
    if abs(v) >= 1e3:
        return f"{v / 1e3:.0f}K"
    return f"{v:.0f}"


def _value_label(col: str) -> str:
    return {"value_usd": "Value (USD)", "net_weight_kg": "Net Weight (kg)", "avg_price": "Avg Price (USD/kg)"}.get(col, col)
