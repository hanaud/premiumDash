"""
Reusable Dash components: KPI cards, spread charts, heatmaps, summary table.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import html, dcc

from . import theme as T
from src.spread_engine import SpreadResult


def _base_layout(**overrides) -> dict:
    """Merge the Plotly template layout with per-chart overrides (overrides win)."""
    base = dict(T.PLOTLY_TEMPLATE["layout"])
    base.update(overrides)
    return base


# ======================================================================
#  KPI card
# ======================================================================
def kpi_card(label: str, value: float, unit: str, change: float,
             pct_rank: float, fmt: str = ".2f") -> html.Div:
    """Small KPI tile showing current value, 1-day change, percentile bar."""
    if np.isnan(value):
        val_str = "N/A"
        color = T.TEXT_MUTED
    else:
        val_str = f"{value:{fmt}}"
        color = T.ACCENT_GREEN if value >= 0 else T.ACCENT_RED

    if np.isnan(change):
        chg_str = ""
        chg_color = T.TEXT_MUTED
    else:
        arrow = "\u25B2" if change > 0 else ("\u25BC" if change < 0 else "\u25CF")
        chg_str = f"{arrow} {abs(change):{fmt}}"
        chg_color = T.ACCENT_GREEN if change > 0 else (T.ACCENT_RED if change < 0 else T.TEXT_MUTED)

    pct_bar_width = max(0, min(100, pct_rank))
    bar_color = T.ACCENT_BLUE if 20 < pct_rank < 80 else (T.ACCENT_RED if pct_rank >= 80 else T.ACCENT_GREEN)

    return html.Div(
        style={**T.CARD_STYLE, "minWidth": "200px", "flex": "1"},
        children=[
            html.Div(label, style=T.KPI_LABEL_STYLE),
            html.Div(
                style={"display": "flex", "alignItems": "baseline", "gap": "8px"},
                children=[
                    html.Span(val_str, style={**T.KPI_VALUE_STYLE, "color": color}),
                    html.Span(unit, style={"fontSize": "12px", "color": T.TEXT_SECONDARY}),
                ],
            ),
            html.Div(
                style={"display": "flex", "alignItems": "center", "gap": "8px", "marginTop": "6px"},
                children=[
                    html.Span(chg_str, style={"fontSize": "12px", "color": chg_color}),
                    html.Span("1d", style={"fontSize": "10px", "color": T.TEXT_MUTED}),
                ],
            ),
            # Percentile bar
            html.Div(
                style={
                    "marginTop": "8px",
                    "height": "4px",
                    "backgroundColor": T.BG_TERTIARY,
                    "borderRadius": "2px",
                    "overflow": "hidden",
                },
                children=[
                    html.Div(style={
                        "width": f"{pct_bar_width}%",
                        "height": "100%",
                        "backgroundColor": bar_color,
                        "borderRadius": "2px",
                    })
                ],
            ),
            html.Div(
                f"{pct_rank:.0f}th pctile (1Y)",
                style={"fontSize": "10px", "color": T.TEXT_MUTED, "marginTop": "2px"},
            ),
        ],
    )


# ======================================================================
#  Time-series chart for a spread
# ======================================================================
def spread_chart(result: SpreadResult, height: int = 280) -> dcc.Graph:
    s = result.series
    sd = result.definition
    has_expiries = bool(result.expiry_results)

    fig = go.Figure()

    # Colour the area green/red only when no multi-expiry overlay
    if sd.computation != "ratio" and sd.unit != "%" and not has_expiries:
        pos = s.clip(lower=0)
        neg = s.clip(upper=0)
        fig.add_trace(go.Scatter(
            x=s.index, y=pos, fill="tozeroy",
            fillcolor="rgba(63,185,80,0.15)", line=dict(width=0),
            showlegend=False, hoverinfo="skip",
        ))
        fig.add_trace(go.Scatter(
            x=s.index, y=neg, fill="tozeroy",
            fillcolor="rgba(248,81,73,0.15)", line=dict(width=0),
            showlegend=False, hoverinfo="skip",
        ))

    # Front-month line
    front_label = sd.leg1.split()[0] if sd.leg1 and " " in sd.leg1 else sd.name
    fig.add_trace(go.Scatter(
        x=s.index, y=s.values,
        mode="lines",
        line=dict(color=T.ACCENT_BLUE, width=1.5),
        name=front_label,
        hovertemplate=f"%{{y:.2f}} {sd.unit}<extra></extra>",
    ))

    # Overlay back-month expiries
    for i, exp in enumerate(result.expiry_results):
        color = T.EXPIRY_COLORS[i % len(T.EXPIRY_COLORS)]
        fig.add_trace(go.Scatter(
            x=exp.series.index, y=exp.series.values,
            mode="lines",
            line=dict(color=color, width=1, dash="dot"),
            name=exp.label,
            hovertemplate=f"%{{y:.2f}} {sd.unit}<extra></extra>",
        ))

    # Mean + ±1σ bands
    fig.add_hline(y=result.mean_1y, line_dash="dot", line_color=T.TEXT_MUTED, opacity=0.5)
    fig.add_hline(y=result.mean_1y + result.std_1y, line_dash="dash", line_color=T.TEXT_MUTED, opacity=0.3)
    fig.add_hline(y=result.mean_1y - result.std_1y, line_dash="dash", line_color=T.TEXT_MUTED, opacity=0.3)

    # Zero line for difference spreads
    if sd.computation != "ratio":
        fig.add_hline(y=0, line_color=T.BORDER, line_width=1)

    fig.update_layout(**_base_layout(
        title=dict(text=sd.name, font=dict(size=13)),
        height=height,
        yaxis_title=sd.unit,
        showlegend=has_expiries,
        legend=dict(orientation="h", y=1.12, x=0) if has_expiries else dict(),
    ))

    return dcc.Graph(figure=fig, config={"displayModeBar": False}, style={"width": "100%"})


# ======================================================================
#  Dual-axis chart showing both legs
# ======================================================================
def legs_chart(result: SpreadResult, height: int = 260) -> Optional[dcc.Graph]:
    if result.leg1_series is None or result.leg2_series is None:
        return None

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=result.leg1_series.index, y=result.leg1_series.values,
        name="Leg 1", line=dict(color=T.ACCENT_CYAN, width=1.2),
    ))
    fig.add_trace(go.Scatter(
        x=result.leg2_series.index, y=result.leg2_series.values,
        name="Leg 2", line=dict(color=T.ACCENT_ORANGE, width=1.2),
        yaxis="y2",
    ))

    fig.update_layout(**_base_layout(
        title=dict(text=f"{result.definition.name} — Legs", font=dict(size=12)),
        height=height,
        yaxis=dict(title="Leg 1", **T.PLOTLY_TEMPLATE["layout"]["yaxis"]),
        yaxis2=dict(
            title="Leg 2", overlaying="y", side="right",
            gridcolor="rgba(0,0,0,0)",
            tickfont=dict(size=10, color=T.TEXT_SECONDARY),
        ),
        legend=dict(orientation="h", y=1.12, x=0),
    ))

    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ======================================================================
#  Summary heatmap: z-score grid across all spreads
# ======================================================================
def zscore_heatmap(results: list[SpreadResult], height: int = 400) -> dcc.Graph:
    names = [r.definition.name for r in results]
    z_scores = [r.z_score_1y for r in results]
    pctiles = [r.percentile_1y for r in results]

    # Colour scale: green (low/backwardation) → white (neutral) → red (high/contango)
    fig = go.Figure(go.Heatmap(
        z=[z_scores],
        x=names,
        y=["Z-Score"],
        colorscale=[
            [0.0, T.ACCENT_GREEN],
            [0.5, T.BG_TERTIARY],
            [1.0, T.ACCENT_RED],
        ],
        zmin=-3, zmax=3,
        text=[[f"z={z:.1f}<br>{p:.0f}th pct" for z, p in zip(z_scores, pctiles)]],
        texttemplate="%{text}",
        textfont=dict(size=10, color=T.TEXT_PRIMARY),
        hovertemplate="%{x}: z=%{z:.2f}<extra></extra>",
        colorbar=dict(title="z", tickfont=dict(color=T.TEXT_SECONDARY)),
    ))

    fig.update_layout(**_base_layout(
        height=height,
        title="Spread Z-Scores (1Y trailing)",
        xaxis=dict(tickangle=45, tickfont=dict(size=9)),
        margin=dict(l=50, r=20, t=50, b=120),
    ))

    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ======================================================================
#  Summary table
# ======================================================================
def summary_table(results: list[SpreadResult]) -> html.Table:
    header = html.Thead(html.Tr([
        html.Th("Spread", style=_th()),
        html.Th("Current", style=_th()),
        html.Th("Unit", style=_th()),
        html.Th("1D Chg", style=_th()),
        html.Th("1W Chg", style=_th()),
        html.Th("1M Chg", style=_th()),
        html.Th("1Y Mean", style=_th()),
        html.Th("1Y Std", style=_th()),
        html.Th("Z-Score", style=_th()),
        html.Th("Pctile", style=_th()),
        html.Th("1Y Min", style=_th()),
        html.Th("1Y Max", style=_th()),
    ]))

    rows = []
    for r in results:
        def _fmt(v, f=".2f"):
            return f"{v:{f}}" if not np.isnan(v) else "—"

        def _chg_cell(v, f=".2f"):
            if np.isnan(v):
                return html.Td("—", style=_td())
            color = T.ACCENT_GREEN if v > 0 else (T.ACCENT_RED if v < 0 else T.TEXT_SECONDARY)
            return html.Td(_fmt(v, f), style={**_td(), "color": color})

        z_color = T.ACCENT_GREEN if r.z_score_1y < -1 else (T.ACCENT_RED if r.z_score_1y > 1 else T.TEXT_PRIMARY)

        rows.append(html.Tr([
            html.Td(r.definition.name, style={**_td(), "fontWeight": "500", "whiteSpace": "nowrap"}),
            html.Td(_fmt(r.current_value), style={**_td(), "fontWeight": "600"}),
            html.Td(r.definition.unit, style={**_td(), "color": T.TEXT_MUTED}),
            _chg_cell(r.change_1d),
            _chg_cell(r.change_1w),
            _chg_cell(r.change_1m),
            html.Td(_fmt(r.mean_1y), style=_td()),
            html.Td(_fmt(r.std_1y), style=_td()),
            html.Td(_fmt(r.z_score_1y), style={**_td(), "color": z_color, "fontWeight": "600"}),
            html.Td(f"{r.percentile_1y:.0f}", style=_td()),
            html.Td(_fmt(r.min_1y), style=_td()),
            html.Td(_fmt(r.max_1y), style=_td()),
        ]))

        # Sub-rows for back-month expiries
        for exp in r.expiry_results:
            exp_chg_color = T.ACCENT_GREEN if exp.change_1d > 0 else (T.ACCENT_RED if exp.change_1d < 0 else T.TEXT_SECONDARY) if not np.isnan(exp.change_1d) else T.TEXT_MUTED
            rows.append(html.Tr([
                html.Td(f"  {exp.label}", style={**_td(), "color": T.TEXT_SECONDARY, "fontStyle": "italic", "paddingLeft": "24px"}),
                html.Td(_fmt(exp.current_value), style={**_td(), "color": T.TEXT_SECONDARY}),
                html.Td("", style=_td()),
                html.Td(_fmt(exp.change_1d) if not np.isnan(exp.change_1d) else "—", style={**_td(), "color": exp_chg_color}),
                html.Td("—", style={**_td(), "color": T.TEXT_MUTED}),
                html.Td("—", style={**_td(), "color": T.TEXT_MUTED}),
                html.Td("—", style={**_td(), "color": T.TEXT_MUTED}),
                html.Td("—", style={**_td(), "color": T.TEXT_MUTED}),
                html.Td("—", style={**_td(), "color": T.TEXT_MUTED}),
                html.Td("—", style={**_td(), "color": T.TEXT_MUTED}),
                html.Td("—", style={**_td(), "color": T.TEXT_MUTED}),
                html.Td("—", style={**_td(), "color": T.TEXT_MUTED}),
            ]))

    return html.Table(
        [header, html.Tbody(rows)],
        style={
            "width": "100%",
            "borderCollapse": "collapse",
            "fontSize": "12px",
        },
    )


def _th():
    return {
        "textAlign": "right",
        "padding": "8px 10px",
        "borderBottom": f"2px solid {T.BORDER}",
        "color": T.TEXT_SECONDARY,
        "fontSize": "10px",
        "textTransform": "uppercase",
        "letterSpacing": "0.5px",
        "whiteSpace": "nowrap",
        "position": "sticky",
        "top": "0",
        "backgroundColor": T.BG_SECONDARY,
    }


def _td():
    return {
        "textAlign": "right",
        "padding": "6px 10px",
        "borderBottom": f"1px solid {T.BG_TERTIARY}",
    }
