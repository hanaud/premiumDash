"""
Trade Seasonality dashboard tab.

Heatmap of monthly gold trade patterns by partner country, with
seasonality breakdown detection overlay.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import html, dcc, Input, Output, callback

from . import theme as T
from src.seasonality_engine import SeasonalityEngine, MONTH_NAMES

logger = logging.getLogger(__name__)

# Lazy-loaded engine instance
_engine: SeasonalityEngine | None = None


def _get_engine() -> SeasonalityEngine:
    global _engine
    if _engine is None:
        _engine = SeasonalityEngine()
    return _engine


def _base_layout(**overrides) -> dict:
    base = dict(T.PLOTLY_TEMPLATE["layout"])
    base.update(overrides)
    return base


# ======================================================================
#  Heatmap builders
# ======================================================================
def _build_seasonality_heatmap(
    data: pd.DataFrame,
    title: str,
    colorscale: str = "YlOrRd",
    value_format: str = ".1f",
) -> go.Figure:
    """Build a heatmap from a partner×month DataFrame."""
    if data.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data available", showarrow=False)
        return fig

    # Scale values for readability (USD millions or kg→tonnes)
    display = data.copy()
    max_val = display.values[~np.isnan(display.values)].max() if display.size > 0 else 1
    scale_label = ""
    if max_val > 1e9:
        display = display / 1e9
        scale_label = " (USD Bn)"
        value_format = ".2f"
    elif max_val > 1e6:
        display = display / 1e6
        scale_label = " (USD M)"
        value_format = ".1f"
    elif max_val > 1e3:
        display = display / 1e3
        scale_label = " (USD K)"
        value_format = ".0f"

    # Build text annotations
    text_matrix = []
    for _, row in display.iterrows():
        row_text = []
        for v in row:
            if pd.isna(v) or v == 0:
                row_text.append("")
            else:
                row_text.append(f"{v:{value_format}}")
        text_matrix.append(row_text)

    fig = go.Figure(data=go.Heatmap(
        z=display.values,
        x=display.columns.tolist(),
        y=display.index.tolist(),
        text=text_matrix,
        texttemplate="%{text}",
        textfont=dict(size=10),
        colorscale=colorscale,
        hoverongaps=False,
        hovertemplate="Partner: %{y}<br>Month: %{x}<br>Value: %{text}<extra></extra>",
        colorbar=dict(
            title=dict(text=scale_label.strip(" ()"), font=dict(size=10)),
            tickfont=dict(size=9),
            len=0.8,
        ),
    ))

    n_partners = len(display)
    height = max(300, min(700, 80 + n_partners * 28))

    fig.update_layout(
        **_base_layout(
            title=title + scale_label,
            xaxis=dict(
                side="top",
                tickfont=dict(size=11),
                dtick=1,
            ),
            yaxis=dict(
                autorange="reversed",
                tickfont=dict(size=11),
            ),
            height=height,
            margin=dict(l=140, r=40, t=60, b=20),
        )
    )

    return fig


def _build_breakdown_heatmap(
    data: pd.DataFrame,
    title: str = "Seasonality Breakdown (Z-Score)",
) -> go.Figure:
    """Build a diverging heatmap showing deviation from seasonal norms."""
    if data.empty:
        fig = go.Figure()
        fig.add_annotation(text="No recent data for breakdown analysis", showarrow=False)
        return fig

    # Diverging colorscale: blue (below norm) → white → red (above norm)
    colorscale = [
        [0.0, "#2166ac"],
        [0.25, "#67a9cf"],
        [0.5, "#f7f7f7"],
        [0.75, "#ef8a62"],
        [1.0, "#b2182b"],
    ]

    # Clamp z-scores for display
    display = data.clip(-3, 3)

    # Get latest month from DataFrame attrs (set by engine)
    latest_month = data.attrs.get("latest_month")
    latest_date = data.attrs.get("latest_date", "")

    # Build text annotations — mark latest month cells with ◄ indicator
    text_matrix = []
    for _, row in display.iterrows():
        row_text = []
        for col_name, v in row.items():
            if pd.isna(v):
                row_text.append("")
            elif col_name == latest_month:
                row_text.append(f"{v:+.1f}σ ◄")
            else:
                row_text.append(f"{v:+.1f}σ")
        text_matrix.append(row_text)

    fig = go.Figure(data=go.Heatmap(
        z=display.values,
        x=display.columns.tolist(),
        y=display.index.tolist(),
        text=text_matrix,
        texttemplate="%{text}",
        textfont=dict(size=10),
        colorscale=colorscale,
        zmid=0,
        zmin=-3,
        zmax=3,
        hoverongaps=False,
        hovertemplate="Partner: %{y}<br>Month: %{x}<br>Deviation: %{text}<extra></extra>",
        colorbar=dict(
            title=dict(text="Z-Score", font=dict(size=10)),
            tickfont=dict(size=9),
            len=0.8,
            tickvals=[-3, -2, -1, 0, 1, 2, 3],
            ticktext=["-3σ", "-2σ", "-1σ", "0", "+1σ", "+2σ", "+3σ"],
        ),
    ))

    n_partners = len(display)
    height = max(300, min(700, 80 + n_partners * 28))

    # Highlight the latest month column with a vertical line + annotation
    if latest_month and latest_month in display.columns:
        col_idx = display.columns.tolist().index(latest_month)
        fig.add_vline(
            x=col_idx, line_width=2, line_dash="solid",
            line_color=T.ACCENT_CYAN, opacity=0.8,
        )
        fig.add_annotation(
            x=col_idx, y=-0.12, yref="paper",
            text=f"▲ Latest: {latest_date}",
            showarrow=False,
            font=dict(size=10, color=T.ACCENT_CYAN, weight="bold"),
        )

    fig.update_layout(
        **_base_layout(
            title=title,
            xaxis=dict(side="top", tickfont=dict(size=11), dtick=1),
            yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
            height=height,
            margin=dict(l=140, r=40, t=60, b=30),
        )
    )

    return fig


# ======================================================================
#  Tab Layout (static shell with callback placeholders)
# ======================================================================
def build_seasonality_tab() -> html.Div:
    """Build the Trade Seasonality tab layout."""
    engine = _get_engine()
    countries = engine.available_countries()
    country_options = [
        {"label": name, "value": code}
        for code, name in sorted(countries.items(), key=lambda x: x[1])
    ]

    return html.Div(
        style={"padding": "24px", "backgroundColor": T.BG_PRIMARY},
        children=[
            html.H2(
                "Gold Trade Seasonality",
                style={"fontSize": "20px", "fontWeight": "600", "marginBottom": "20px"},
            ),

            # ── Controls ──
            html.Div(
                style={
                    "display": "flex", "gap": "16px", "flexWrap": "wrap",
                    "alignItems": "flex-end", "marginBottom": "20px",
                },
                children=[
                    html.Div([
                        html.Label(
                            "Focus Country",
                            style={"fontSize": "11px", "color": T.TEXT_SECONDARY,
                                   "marginBottom": "4px", "display": "block"},
                        ),
                        dcc.Dropdown(
                            id="seasonality-country",
                            options=country_options,
                            value="784",  # UAE default
                            style={"width": "240px", "fontSize": "12px"},
                            className="dash-dropdown-dark",
                        ),
                    ]),
                    html.Div([
                        html.Label(
                            "Flow",
                            style={"fontSize": "11px", "color": T.TEXT_SECONDARY,
                                   "marginBottom": "4px", "display": "block"},
                        ),
                        dcc.Dropdown(
                            id="seasonality-flow",
                            options=[
                                {"label": "Imports", "value": "M"},
                                {"label": "Exports", "value": "X"},
                            ],
                            value="M",
                            style={"width": "120px", "fontSize": "12px"},
                            className="dash-dropdown-dark",
                        ),
                    ]),
                    html.Div([
                        html.Label(
                            "Metric",
                            style={"fontSize": "11px", "color": T.TEXT_SECONDARY,
                                   "marginBottom": "4px", "display": "block"},
                        ),
                        dcc.Dropdown(
                            id="seasonality-metric",
                            options=[
                                {"label": "Trade Value (USD)", "value": "value_usd"},
                                {"label": "Net Weight (kg)", "value": "net_weight_kg"},
                            ],
                            value="value_usd",
                            style={"width": "180px", "fontSize": "12px"},
                            className="dash-dropdown-dark",
                        ),
                    ]),
                    html.Div([
                        html.Label(
                            "Top Partners",
                            style={"fontSize": "11px", "color": T.TEXT_SECONDARY,
                                   "marginBottom": "4px", "display": "block"},
                        ),
                        dcc.Dropdown(
                            id="seasonality-topn",
                            options=[
                                {"label": "10", "value": 10},
                                {"label": "15", "value": 15},
                                {"label": "20", "value": 20},
                            ],
                            value=15,
                            style={"width": "80px", "fontSize": "12px"},
                            className="dash-dropdown-dark",
                        ),
                    ]),
                    html.Div([
                        html.Label(
                            "Recent Window",
                            style={"fontSize": "11px", "color": T.TEXT_SECONDARY,
                                   "marginBottom": "4px", "display": "block"},
                        ),
                        dcc.Dropdown(
                            id="seasonality-recent",
                            options=[
                                {"label": "3 months", "value": 3},
                                {"label": "6 months", "value": 6},
                                {"label": "12 months", "value": 12},
                            ],
                            value=6,
                            style={"width": "120px", "fontSize": "12px"},
                            className="dash-dropdown-dark",
                        ),
                    ]),
                    html.Div([
                        html.Label(
                            "Z-Score Scope",
                            style={"fontSize": "11px", "color": T.TEXT_SECONDARY,
                                   "marginBottom": "4px", "display": "block"},
                        ),
                        dcc.Dropdown(
                            id="seasonality-zscore-mode",
                            options=[
                                {"label": "Per Partner (row)", "value": "row"},
                                {"label": "Global (table)", "value": "table"},
                            ],
                            value="row",
                            style={"width": "160px", "fontSize": "12px"},
                            className="dash-dropdown-dark",
                        ),
                    ]),
                ],
            ),

            # ── Charts (filled by callback, with loading spinner) ──
            dcc.Loading(
                id="seasonality-loading",
                type="default",
                color=T.ACCENT_BLUE,
                children=html.Div(id="seasonality-content", children=[
                    html.Div(
                        "Select a country to load seasonality data.",
                        style={"color": T.TEXT_MUTED, "padding": "40px", "textAlign": "center"},
                    ),
                ]),
                overlay_style={
                    "visibility": "visible",
                    "opacity": 0.6,
                    "backgroundColor": T.BG_PRIMARY,
                },
                custom_spinner=html.Div([
                    html.Div(
                        style={
                            "width": "40px", "height": "40px",
                            "border": f"4px solid {T.BG_TERTIARY}",
                            "borderTop": f"4px solid {T.ACCENT_BLUE}",
                            "borderRadius": "50%",
                            "animation": "spin 1s linear infinite",
                            "margin": "0 auto 12px auto",
                        },
                    ),
                    html.Div(
                        "Fetching trade data from UN Comtrade...",
                        style={
                            "color": T.TEXT_SECONDARY, "fontSize": "13px",
                            "textAlign": "center",
                        },
                    ),
                    html.Div(
                        "First load per country takes 15-30s (cached afterwards)",
                        style={
                            "color": T.TEXT_MUTED, "fontSize": "11px",
                            "textAlign": "center", "marginTop": "4px",
                        },
                    ),
                ]),
            ),
        ],
    )


def build_seasonality_charts(
    country_code: str,
    flow_code: str,
    metric: str,
    top_n: int,
    recent_months: int,
    zscore_mode: str = "row",
) -> list:
    """Build the seasonality heatmap and breakdown charts."""
    engine = _get_engine()
    countries = engine.available_countries()
    country_name = countries.get(country_code, country_code)
    flow_label = "Import" if flow_code == "M" else "Export"

    sections = []

    # Section 1: Seasonality heatmap (historical averages)
    seasonality = engine.compute_seasonality(
        reporter_code=country_code,
        flow_code=flow_code,
        metric=metric,
        top_n=top_n,
    )

    fig_season = _build_seasonality_heatmap(
        seasonality,
        title=f"{country_name} — {flow_label} Seasonality by Partner (Historical Avg)",
    )

    sections.append(
        html.Div(
            style={**T.CARD_STYLE, "marginBottom": "20px"},
            children=[
                html.H3(
                    f"{flow_label} Seasonality Map",
                    style={"fontSize": "14px", "marginBottom": "12px", "fontWeight": "600"},
                ),
                dcc.Graph(figure=fig_season, id="seasonality-heatmap"),
                html.Div(
                    style={
                        "marginTop": "12px", "padding": "12px",
                        "backgroundColor": T.BG_TERTIARY, "borderRadius": "4px",
                    },
                    children=[
                        html.P(
                            f"Average monthly {flow_label.lower()} values across all available years. "
                            "Higher values show peak trading months for each partner.",
                            style={"fontSize": "12px", "margin": "0"},
                        ),
                    ],
                ),
            ],
        )
    )

    # Section 2: Breakdown heatmap (z-score deviations)
    zscore_label = "per partner" if zscore_mode == "row" else "global"
    breakdown = engine.compute_breakdown(
        reporter_code=country_code,
        flow_code=flow_code,
        metric=metric,
        top_n=top_n,
        recent_months=recent_months,
        zscore_mode=zscore_mode,
    )

    fig_breakdown = _build_breakdown_heatmap(
        breakdown,
        title=f"{country_name} — Recent {flow_label} Deviation from Seasonal Norm (last {recent_months}mo, {zscore_label})",
    )

    # Get latest data point info from breakdown attrs
    latest_date_str = breakdown.attrs.get("latest_date", "")
    latest_badge = ""
    if latest_date_str:
        latest_badge = f"  Latest data point: {latest_date_str} (marked with ◄ and cyan line)."

    sections.append(
        html.Div(
            style={**T.CARD_STYLE, "marginBottom": "20px"},
            children=[
                html.H3(
                    "Seasonality Breakdown Detection",
                    style={"fontSize": "14px", "marginBottom": "12px", "fontWeight": "600"},
                ),
                dcc.Graph(figure=fig_breakdown, id="breakdown-heatmap"),
                html.Div(
                    style={
                        "marginTop": "12px", "padding": "12px",
                        "backgroundColor": T.BG_TERTIARY, "borderRadius": "4px",
                    },
                    children=[
                        html.P(
                            "Z-scores show how recent months deviate from historical seasonal averages. "
                            "Red (positive): unusually high trade. Blue (negative): unusually low. "
                            "Values beyond ±2σ suggest a potential breakdown in the seasonal pattern."
                            + latest_badge,
                            style={"fontSize": "12px", "margin": "0"},
                        ),
                    ],
                ),
            ],
        )
    )

    # Section 3: Summary of notable anomalies
    if not breakdown.empty:
        anomalies = _extract_anomalies(breakdown, country_name, flow_label)
        if anomalies:
            sections.append(
                html.Div(
                    style={**T.CARD_STYLE, "marginBottom": "20px"},
                    children=[
                        html.H3(
                            "Notable Anomalies",
                            style={"fontSize": "14px", "marginBottom": "12px", "fontWeight": "600"},
                        ),
                        _build_anomaly_table(anomalies),
                    ],
                )
            )

    return sections


def _extract_anomalies(
    breakdown: pd.DataFrame,
    country_name: str,
    flow_label: str,
    threshold: float = 1.5,
) -> list[dict]:
    """Extract notable anomalies from the breakdown matrix."""
    anomalies = []
    for partner in breakdown.index:
        for month in breakdown.columns:
            z = breakdown.loc[partner, month]
            if pd.notna(z) and abs(z) >= threshold:
                direction = "above" if z > 0 else "below"
                anomalies.append({
                    "partner": partner,
                    "month": month,
                    "zscore": z,
                    "direction": direction,
                    "severity": "Extreme" if abs(z) >= 2.5 else "Strong" if abs(z) >= 2.0 else "Moderate",
                })

    # Sort by absolute z-score descending
    anomalies.sort(key=lambda x: abs(x["zscore"]), reverse=True)
    return anomalies[:15]  # Limit to top 15


def _build_anomaly_table(anomalies: list[dict]) -> html.Table:
    """Build an HTML table of notable anomalies."""
    header_style = {
        "borderBottom": f"1px solid {T.BG_TERTIARY}",
        "padding": "8px",
        "fontWeight": "600",
        "textAlign": "left",
        "fontSize": "12px",
    }
    cell_style = {
        "borderBottom": f"1px solid {T.BG_TERTIARY}",
        "padding": "8px",
        "fontSize": "12px",
    }

    rows = []
    for a in anomalies:
        z_color = T.ACCENT_RED if a["zscore"] > 0 else T.ACCENT_BLUE
        severity_color = (
            T.ACCENT_RED if a["severity"] == "Extreme"
            else T.ACCENT_ORANGE if a["severity"] == "Strong"
            else T.TEXT_SECONDARY
        )
        rows.append(
            html.Tr([
                html.Td(a["partner"], style=cell_style),
                html.Td(a["month"], style={**cell_style, "textAlign": "center"}),
                html.Td(
                    f"{a['zscore']:+.1f}σ",
                    style={**cell_style, "textAlign": "center", "color": z_color, "fontWeight": "bold"},
                ),
                html.Td(
                    a["direction"],
                    style={**cell_style, "textAlign": "center"},
                ),
                html.Td(
                    a["severity"],
                    style={**cell_style, "textAlign": "center", "color": severity_color, "fontWeight": "600"},
                ),
            ])
        )

    thead = html.Thead(html.Tr([
        html.Th("Partner", style=header_style),
        html.Th("Month", style={**header_style, "textAlign": "center"}),
        html.Th("Z-Score", style={**header_style, "textAlign": "center"}),
        html.Th("Direction", style={**header_style, "textAlign": "center"}),
        html.Th("Severity", style={**header_style, "textAlign": "center"}),
    ]))

    return html.Table(
        [thead, html.Tbody(rows)],
        style={"width": "100%", "borderCollapse": "collapse"},
    )
