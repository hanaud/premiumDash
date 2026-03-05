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
#  Time-series chart for a spread (with percentile bands)
# ======================================================================
def spread_chart(result: SpreadResult, height: int = 280) -> dcc.Graph:
    s = result.series
    sd = result.definition
    has_expiries = bool(result.expiry_results)

    fig = go.Figure()

    # ---- Percentile bands (behind everything) ----
    has_bands = False
    if not np.isnan(result.pct_10) and not np.isnan(result.pct_90):
        has_bands = True
        x_range = [s.index.min(), s.index.max()]
        band_10_90 = "rgba(88,166,255,0.06)"
        band_25_75 = "rgba(88,166,255,0.12)"

        # 10th–90th band as filled area (supports legend)
        fig.add_trace(go.Scatter(
            x=x_range + x_range[::-1],
            y=[result.pct_90, result.pct_90, result.pct_10, result.pct_10],
            fill="toself", fillcolor=band_10_90,
            line=dict(width=0), mode="lines",
            name=f"P10–P90 ({result.pct_10:.1f}–{result.pct_90:.1f})",
            hoverinfo="skip", legendgroup="bands",
        ))
        # 25th–75th band
        if not np.isnan(result.pct_25) and not np.isnan(result.pct_75):
            fig.add_trace(go.Scatter(
                x=x_range + x_range[::-1],
                y=[result.pct_75, result.pct_75, result.pct_25, result.pct_25],
                fill="toself", fillcolor=band_25_75,
                line=dict(width=0), mode="lines",
                name=f"P25–P75 ({result.pct_25:.1f}–{result.pct_75:.1f})",
                hoverinfo="skip", legendgroup="bands",
            ))
        # Median line
        if not np.isnan(result.pct_50):
            fig.add_hline(
                y=result.pct_50,
                line_dash="dash", line_color=T.ACCENT_BLUE, line_width=0.8,
                opacity=0.4,
            )
            # Legend entry for median
            fig.add_trace(go.Scatter(
                x=[None], y=[None], mode="lines",
                line=dict(color=T.ACCENT_BLUE, width=1, dash="dash"),
                name=f"Median ({result.pct_50:.1f})",
                legendgroup="bands",
            ))

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

    # Zero line for difference spreads
    if sd.computation != "ratio":
        fig.add_hline(y=0, line_color=T.BORDER, line_width=1)

    show_legend = has_expiries or has_bands
    fig.update_layout(**_base_layout(
        title=dict(text=sd.name, font=dict(size=13)),
        height=height,
        yaxis_title=sd.unit,
        showlegend=show_legend,
        legend=dict(
            orientation="h", y=1.18, x=0,
            font=dict(size=9, color=T.TEXT_SECONDARY),
            tracegroupgap=0,
        ) if show_legend else dict(),
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
#  Percentile background shade for table cells
# ======================================================================
def _pctile_bg(pctile: float) -> str:
    """Return a subtle RGBA background colour: intense near 0/100, transparent near 50."""
    if np.isnan(pctile):
        return "transparent"
    # Distance from 50 (centre), normalised 0→1
    dist = abs(pctile - 50) / 50
    if pctile < 50:
        # Green tones for low percentiles
        alpha = dist * 0.25
        return f"rgba(63,185,80,{alpha:.2f})"
    else:
        # Red tones for high percentiles
        alpha = dist * 0.25
        return f"rgba(248,81,73,{alpha:.2f})"


def _z_color(z: float) -> tuple[str, str]:
    """Return (text_color, bg_color) for a z-score value.

    Gradient: deep green for very negative, deep red for very positive.
    Background alpha scales with magnitude (0 at z=0, max at |z|≥3).
    """
    if np.isnan(z):
        return T.TEXT_MUTED, "transparent"

    abs_z = min(abs(z), 3.0)
    # Background alpha: ramp from 0 → 0.30 over |z| 0→3
    bg_alpha = abs_z / 3.0 * 0.30
    # Text intensity: neutral for small, full colour for |z|>1
    if abs_z < 0.5:
        txt = T.TEXT_SECONDARY
    elif abs_z < 1.0:
        txt = T.ACCENT_GREEN if z < 0 else T.ACCENT_RED
    else:
        txt = T.ACCENT_GREEN if z < 0 else T.ACCENT_RED

    if z < 0:
        bg = f"rgba(63,185,80,{bg_alpha:.2f})"
    else:
        bg = f"rgba(248,81,73,{bg_alpha:.2f})"
    return txt, bg


# ======================================================================
#  Summary table
# ======================================================================
def summary_table(results: list[SpreadResult]) -> html.Table:
    header = html.Thead(html.Tr([
        html.Th("Spread", style=_th()),
        html.Th("Field", style=_th()),
        html.Th("Leg1", style=_th()),
        html.Th("Leg2", style=_th()),
        html.Th("Spread", style=_th()),
        html.Th("Unit", style=_th()),
        html.Th("1D Chg", style=_th()),
        html.Th("1W Chg", style=_th()),
        html.Th("1M Chg", style=_th()),
        html.Th("Z(1W)", style=_th()),
        html.Th("Z(1M)", style=_th()),
        html.Th("Z(1Y)", style=_th()),
        html.Th("Pct(1W)", style=_th()),
        html.Th("Pct(1M)", style=_th()),
        html.Th("Pct(1Y)", style=_th()),
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

        def _z_cell(z):
            if np.isnan(z):
                return html.Td("—", style=_td())
            txt, bg = _z_color(z)
            return html.Td(
                _fmt(z),
                style={**_td(), "color": txt, "backgroundColor": bg, "fontWeight": "600"},
            )

        def _pct_cell(p):
            if np.isnan(p):
                return html.Td("—", style=_td())
            return html.Td(
                f"{p:.0f}",
                style={**_td(), "backgroundColor": _pctile_bg(p)},
            )

        # Leg values (converted)
        leg1_val = r.leg1_series.iloc[-1] if r.leg1_series is not None and len(r.leg1_series) > 0 else np.nan
        leg2_val = r.leg2_series.iloc[-1] if r.leg2_series is not None and len(r.leg2_series) > 0 else np.nan

        rows.append(html.Tr([
            html.Td(r.definition.name, style={**_td(), "fontWeight": "500", "whiteSpace": "nowrap", "textAlign": "left"}),
            html.Td(r.definition.bbg_field, style={**_td(), "color": T.TEXT_MUTED, "fontSize": "10px"}),
            html.Td(_fmt(leg1_val), style={**_td(), "color": T.TEXT_SECONDARY}),
            html.Td(_fmt(leg2_val), style={**_td(), "color": T.TEXT_SECONDARY}),
            html.Td(_fmt(r.current_value), style={**_td(), "fontWeight": "600"}),
            html.Td(r.definition.unit, style={**_td(), "color": T.TEXT_MUTED}),
            _chg_cell(r.change_1d),
            _chg_cell(r.change_1w),
            _chg_cell(r.change_1m),
            _z_cell(r.z_score_1w),
            _z_cell(r.z_score_1m),
            _z_cell(r.z_score_1y),
            _pct_cell(r.percentile_1w),
            _pct_cell(r.percentile_1m),
            _pct_cell(r.percentile_1y),
            html.Td(_fmt(r.min_1y), style=_td()),
            html.Td(_fmt(r.max_1y), style=_td()),
        ]))

        # Sub-rows for back-month expiries
        for exp in r.expiry_results:
            exp_chg_color = (
                T.ACCENT_GREEN if exp.change_1d > 0
                else (T.ACCENT_RED if exp.change_1d < 0 else T.TEXT_SECONDARY)
            ) if not np.isnan(exp.change_1d) else T.TEXT_MUTED
            exp_leg1 = exp.leg1_series.iloc[-1] if exp.leg1_series is not None and len(exp.leg1_series) > 0 else np.nan
            rows.append(html.Tr([
                html.Td(f"  {exp.label}", style={**_td(), "color": T.TEXT_SECONDARY, "fontStyle": "italic", "paddingLeft": "24px", "textAlign": "left"}),
                html.Td("", style=_td()),  # field (same as parent)
                html.Td(_fmt(exp_leg1), style={**_td(), "color": T.TEXT_MUTED}),
                html.Td("", style=_td()),  # leg2 (same as parent)
                html.Td(_fmt(exp.current_value), style={**_td(), "color": T.TEXT_SECONDARY}),
                html.Td("", style=_td()),
                html.Td(
                    _fmt(exp.change_1d) if not np.isnan(exp.change_1d) else "—",
                    style={**_td(), "color": exp_chg_color},
                ),
                *[html.Td("—", style={**_td(), "color": T.TEXT_MUTED}) for _ in range(10)],
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
