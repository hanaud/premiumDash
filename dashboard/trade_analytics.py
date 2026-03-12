"""
Dubai Gold Trade Analytics dashboard components.

Builds the layout for the "Dubai Trade Analytics" tab with deep-dive charts
on premium dislocations, trade flows, supply indicators, and early signals.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import html, dcc

from . import theme as T
from src.trade_analytics_engine import TradeAnalyticsEngine

logger = logging.getLogger(__name__)

# Global engine instance (lazy-loaded)
_engine: TradeAnalyticsEngine | None = None


def _get_engine() -> TradeAnalyticsEngine:
    """Lazy load trade analytics engine."""
    global _engine
    if _engine is None:
        _engine = TradeAnalyticsEngine()
    return _engine


def _base_layout(**overrides) -> dict:
    """Merge the Plotly template layout with per-chart overrides."""
    base = dict(T.PLOTLY_TEMPLATE["layout"])
    base.update(overrides)
    return base


# ======================================================================
#  SECTION 1: Premium Dislocation Signals
# ======================================================================
def chart_dubai_sge_premium_comparison() -> dcc.Graph:
    """Dubai vs SGE premium with divergence highlighting."""
    engine = _get_engine()
    df = engine.get_premium_with_zscore()
    if df.empty:
        return dcc.Graph(figure=go.Figure().add_annotation(text="No data available"))

    fig = go.Figure()

    # Dubai premium
    fig.add_trace(go.Scatter(
        x=df.index, y=df['Dubai_Premium_USD_oz'],
        name='Dubai Premium',
        mode='lines',
        line=dict(color='#1f77b4', width=2),
    ))

    # SGE premium
    fig.add_trace(go.Scatter(
        x=df.index, y=df['SGE_Premium_USD_oz'],
        name='SGE Premium',
        mode='lines',
        line=dict(color='#ff7f0e', width=2),
    ))

    # Zero line
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

    fig.update_layout(
        **_base_layout(
            title="Dubai vs Shanghai (SGE) Gold Premium",
            xaxis_title="Date",
            yaxis_title="Premium (USD/oz)",
            hovermode="x unified",
            height=350,
        )
    )

    return dcc.Graph(figure=fig)


def chart_dubai_premium_zscore() -> dcc.Graph:
    """Dubai premium z-score with dislocation threshold."""
    engine = _get_engine()
    df = engine.get_premium_with_zscore()
    if df.empty:
        return dcc.Graph(figure=go.Figure().add_annotation(text="No data available"))

    fig = go.Figure()

    # Z-score
    colors = ['#d62728' if z > 0 else '#2ca02c' for z in df['Dubai_Premium_ZScore']]
    fig.add_trace(go.Bar(
        x=df.index, y=df['Dubai_Premium_ZScore'],
        name='Z-Score',
        marker=dict(color=colors),
    ))

    # Threshold lines
    fig.add_hline(y=1.5, line_dash="dash", line_color="red", opacity=0.5,
                  annotation_text="Extreme Premium")
    fig.add_hline(y=-1.5, line_dash="dash", line_color="green", opacity=0.5,
                  annotation_text="Extreme Discount")
    fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

    fig.update_layout(
        **_base_layout(
            title="Dubai Premium Z-Score (1Y Rolling)",
            xaxis_title="Date",
            yaxis_title="Z-Score",
            hovermode="x unified",
            height=300,
        )
    )

    return dcc.Graph(figure=fig)


# ======================================================================
#  SECTION 2: Trade Flow Drivers
# ======================================================================
def chart_swiss_supply_vs_premium() -> dcc.Graph:
    """Swiss gold exports to UAE vs Dubai premium (dual-axis)."""
    engine = _get_engine()
    merged = engine.get_swiss_supply_with_premium()
    if merged.empty:
        return dcc.Graph(figure=go.Figure().add_annotation(text="No data available"))

    fig = go.Figure()

    # Swiss tonnes (left axis)
    fig.add_trace(go.Bar(
        x=merged.index, y=merged['Swiss_Export_Tonnes'],
        name='Swiss Export (tonnes)',
        marker=dict(color='#2ca02c', opacity=0.6),
        yaxis='y1',
    ))

    # Dubai premium (right axis)
    fig.add_trace(go.Scatter(
        x=merged.index, y=merged['Dubai_Premium_USD_oz'],
        name='Dubai Premium',
        mode='lines+markers',
        line=dict(color='#d62728', width=2),
        yaxis='y2',
    ))

    fig.update_layout(
        **_base_layout(
            title="Swiss Gold Exports to UAE vs Dubai Premium",
            xaxis_title="Date",
            yaxis=dict(title="Tonnes", side='left'),
            yaxis2=dict(title="Premium (USD/oz)", overlaying='y', side='right'),
            hovermode="x unified",
            height=350,
        )
    )

    return dcc.Graph(figure=fig)


def chart_india_duty_timeline() -> dcc.Graph:
    """India import duty changes with premium overlay."""
    engine = _get_engine()
    prem = engine.get_premium_with_zscore()
    duty = engine.get_duty_timeline_events()

    if prem.empty:
        return dcc.Graph(figure=go.Figure().add_annotation(text="No data available"))

    fig = go.Figure()

    # Premium line
    fig.add_trace(go.Scatter(
        x=prem.index, y=prem['Dubai_Premium_USD_oz'],
        name='Dubai Premium',
        mode='lines',
        line=dict(color='#1f77b4', width=2),
        yaxis='y1',
    ))

    # Duty change vertical lines
    if not duty.empty:
        for _, row in duty.iterrows():
            fig.add_vline(
                x=row['Date'],
                line_dash="dash",
                line_color="orange",
                opacity=0.5,
            )
            # Annotate duty level
            fig.add_annotation(
                x=row['Date'],
                y=prem['Dubai_Premium_USD_oz'].max() * 0.9,
                text=f"{row['India_Gold_Total_Duty_Pct']:.0f}%",
                showarrow=False,
                font=dict(color="orange", size=10),
                bgcolor="white",
                bordercolor="orange",
                borderwidth=1,
            )

    fig.update_layout(
        **_base_layout(
            title="Dubai Premium vs India Duty Regime Changes",
            xaxis_title="Date",
            yaxis_title="Premium (USD/oz)",
            hovermode="x unified",
            height=350,
        )
    )

    return dcc.Graph(figure=fig)


# ======================================================================
#  SECTION 3: Supply-Demand Dynamics
# ======================================================================
def chart_annual_import_export() -> dcc.Graph:
    """Annual UAE import/export volumes."""
    engine = _get_engine()
    annual = engine.get_annual_trade_summary()
    if annual.empty or 'Import_Tonnes' not in annual.columns:
        return dcc.Graph(figure=go.Figure().add_annotation(text="No data available"))

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=annual.index, y=annual['Import_Tonnes'],
        name='Imports',
        marker=dict(color='#1f77b4'),
    ))

    fig.add_trace(go.Bar(
        x=annual.index, y=annual['Export_Tonnes'],
        name='Exports',
        marker=dict(color='#ff7f0e'),
    ))

    fig.update_layout(
        **_base_layout(
            title="UAE Annual Gold Import/Export Volumes",
            xaxis_title="Year",
            yaxis_title="Tonnes",
            barmode='group',
            height=350,
        )
    )

    return dcc.Graph(figure=fig)


def chart_import_source_composition() -> dcc.Graph:
    """African vs non-African import source share."""
    engine = _get_engine()
    composition = engine.get_import_source_composition()
    if composition.empty:
        return dcc.Graph(figure=go.Figure().add_annotation(text="No data available"))

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=composition['Year'], y=composition['Africa_Imports'],
        name='African Sources',
        marker=dict(color='#e67e22'),
    ))

    fig.add_trace(go.Bar(
        x=composition['Year'], y=composition['Non_Africa_Imports'],
        name='Non-African Sources',
        marker=dict(color='#3498db'),
    ))

    fig.update_layout(
        **_base_layout(
            title="UAE Gold Import Source Composition",
            xaxis_title="Year",
            yaxis_title="Value (USD)",
            barmode='stack',
            height=350,
        )
    )

    return dcc.Graph(figure=fig)


def chart_india_demand() -> dcc.Graph:
    """India export demand vs Dubai premium."""
    engine = _get_engine()
    prem = engine.get_premium_with_zscore()
    india_exp = engine.get_india_export_trends()

    if prem.empty:
        return dcc.Graph(figure=go.Figure().add_annotation(text="No data available"))

    fig = go.Figure()

    # Dubai premium (main line)
    fig.add_trace(go.Scatter(
        x=prem.index, y=prem['Dubai_Premium_USD_oz'],
        name='Dubai Premium',
        mode='lines',
        line=dict(color='#d62728', width=2),
        yaxis='y1',
    ))

    # India export demand (if available, as bar or trace)
    if not india_exp.empty and 'Exp_India' in india_exp.columns:
        fig.add_trace(go.Bar(
            x=india_exp['Year'],
            y=india_exp['Exp_India'] / 1e9,  # Convert to billions
            name='India Exports (USD Bn)',
            marker=dict(color='#2ca02c', opacity=0.5),
            yaxis='y2',
        ))

    fig.update_layout(
        **_base_layout(
            title="India Gold Exports from UAE vs Dubai Premium",
            xaxis_title="Date",
            yaxis=dict(title="Premium (USD/oz)", side='left'),
            yaxis2=dict(title="Export Value (USD Bn)", overlaying='y', side='right') if not india_exp.empty else {},
            hovermode="x unified",
            height=300,
        )
    )

    return dcc.Graph(figure=fig)


# ======================================================================
#  SECTION 4: Macro Drivers Correlation
# ======================================================================
def chart_macro_correlations() -> dcc.Graph:
    """Bar chart of macro variable correlations with Dubai premium."""
    engine = _get_engine()
    prem = engine.get_premium_with_zscore()

    if prem.empty:
        return dcc.Graph(figure=go.Figure().add_annotation(text="No data available"))

    # Compute correlations with ALL available macro features
    macro_cols = [
        'DXY_Index', 'VIX', 'US_10Y_Yield', 'USD_INR', 'WTI_Crude_USD',
        'Gold_Silver_Ratio', 'USD_CNY', 'USD_TRY'
    ]

    df_analysis = prem[[c for c in macro_cols if c in prem.columns]].dropna(axis=1)
    df_analysis['Dubai_Premium'] = prem['Dubai_Premium_USD_oz']
    df_analysis = df_analysis.dropna()

    corrs = {}
    for col in macro_cols:
        if col in df_analysis.columns:
            corrs[col] = df_analysis['Dubai_Premium'].corr(df_analysis[col])

    if not corrs:
        return dcc.Graph(figure=go.Figure().add_annotation(text="No data available"))

    df_corr = pd.Series(corrs).sort_values()
    colors = ['#d62728' if v < 0 else '#2ca02c' for v in df_corr.values]

    # Format labels for better readability
    labels = [
        'DXY Index',
        'VIX',
        'US 10Y Yield',
        'USD/INR',
        'WTI Crude',
        'Gold/Silver Ratio',
        'USD/CNY',
        'USD/TRY',
    ]
    label_map = dict(zip(macro_cols, labels))
    formatted_labels = [label_map.get(c, c) for c in df_corr.index]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=df_corr.values,
        y=formatted_labels,
        orientation='h',
        marker=dict(color=colors),
        text=[f"{v:.3f}" for v in df_corr.values],
        textposition='auto',
    ))

    fig.add_vline(x=0, line_dash="dash", line_color="black")

    fig.update_layout(
        **_base_layout(
            title="Macro Variable Correlations with Dubai Premium",
            xaxis_title="Correlation Coefficient",
            yaxis_title="",
            height=450,
            margin=dict(l=150),  # More space for labels
        ),
        showlegend=False,
    )

    return dcc.Graph(figure=fig)


# ======================================================================
#  SECTION 5: Dislocation Signal Summary
# ======================================================================
def build_dislocation_signals_table() -> html.Div:
    """Build recent dislocation signals as a summary table."""
    engine = _get_engine()
    signals = engine.detect_dislocation_signals(zscore_threshold=1.5)

    if signals.empty:
        return html.Div(
            "No extreme dislocation signals (|Z-Score| > 1.5)",
            style={"color": T.TEXT_MUTED, "padding": "20px", "textAlign": "center"},
        )

    # Show last 10 signals
    signals_display = signals.tail(10)[['Date', 'Dubai_Premium_USD_oz', 'Dubai_Premium_ZScore', 'Signal_Type']].copy()
    signals_display['Date'] = signals_display['Date'].dt.strftime('%Y-%m-%d')
    signals_display.columns = ['Date', 'Premium (USD/oz)', 'Z-Score', 'Signal']

    # Style definitions
    table_style = {
        "width": "100%",
        "borderCollapse": "collapse",
        "fontSize": "12px",
    }

    header_style = {
        "borderBottom": f"1px solid {T.BG_TERTIARY}",
        "padding": "8px",
        "fontWeight": "600",
        "textAlign": "left",
    }

    cell_style = {
        "borderBottom": f"1px solid {T.BG_TERTIARY}",
        "padding": "8px",
    }

    # Build rows with proper styling applied directly
    rows = []
    for _, row in signals_display.iterrows():
        z_color = '#d62728' if row['Z-Score'] > 0 else '#2ca02c'
        rows.append(
            html.Tr([
                html.Td(row['Date'], style=cell_style),
                html.Td(f"{row['Premium (USD/oz)']:.2f}", style={**cell_style, "textAlign": "center"}),
                html.Td(f"{row['Z-Score']:+.2f}", style={**cell_style, "textAlign": "center", "color": z_color, "fontWeight": "bold"}),
                html.Td(row['Signal'], style={**cell_style, "fontSize": "12px"}),
            ])
        )

    thead = html.Thead(html.Tr([
        html.Th("Date", style=header_style),
        html.Th("Premium", style={**header_style, "textAlign": "center"}),
        html.Th("Z-Score", style={**header_style, "textAlign": "center"}),
        html.Th("Signal", style=header_style),
    ]))

    tbody = html.Tbody(rows)

    table = html.Table([thead, tbody], style=table_style)

    return table


# ======================================================================
#  Main Tab Layout
# ======================================================================
def build_trade_analytics_tab() -> html.Div:
    """Build the complete Dubai Trade Analytics tab."""
    return html.Div(
        style={"padding": "24px", "backgroundColor": T.BG_PRIMARY},
        children=[
            html.H2(
                "Dubai Gold Trade Analytics",
                style={"fontSize": "20px", "fontWeight": "600", "marginBottom": "24px"},
            ),

            # ── SECTION 1: Premium Dislocation Signals ──
            html.Div(
                style={**T.CARD_STYLE, "marginBottom": "20px"},
                children=[
                    html.H3("Premium Dislocation Signals", style={"fontSize": "14px", "marginBottom": "12px", "fontWeight": "600"}),
                    html.Div(
                        style={"display": "grid", "gridTemplateColumns": "repeat(2, 1fr)", "gap": "12px"},
                        children=[
                            chart_dubai_sge_premium_comparison(),
                            chart_dubai_premium_zscore(),
                        ],
                    ),
                    html.Div(
                        style={"marginTop": "12px", "padding": "12px", "backgroundColor": T.BG_TERTIARY, "borderRadius": "4px"},
                        children=[
                            html.P(
                                "🎯 When |Z-Score| > 1.5: Potential mean-reversion opportunity. "
                                "Premium near historical extremes may compress as supply/demand re-equilibrates.",
                                style={"fontSize": "12px", "margin": "0"},
                            ),
                        ],
                    ),
                ],
            ),

            # ── SECTION 2: Trade Flow Drivers ──
            html.Div(
                style={**T.CARD_STYLE, "marginBottom": "20px"},
                children=[
                    html.H3("Trade Flow Drivers", style={"fontSize": "14px", "marginBottom": "12px", "fontWeight": "600"}),
                    chart_swiss_supply_vs_premium(),
                    chart_india_duty_timeline(),
                    html.Div(
                        style={"marginTop": "12px", "padding": "12px", "backgroundColor": T.BG_TERTIARY, "borderRadius": "4px"},
                        children=[
                            html.P(
                                "💡 Swiss supply spikes often precede premium compression 4-6 weeks later. "
                                "India duty changes shift the Dubai-India arbitrage incentive and re-export flows.",
                                style={"fontSize": "12px", "margin": "0"},
                            ),
                        ],
                    ),
                ],
            ),

            # ── SECTION 3: Supply-Demand Dynamics ──
            html.Div(
                style={**T.CARD_STYLE, "marginBottom": "20px"},
                children=[
                    html.H3("Supply-Demand Dynamics", style={"fontSize": "14px", "marginBottom": "12px", "fontWeight": "600"}),
                    html.Div(
                        style={"display": "grid", "gridTemplateColumns": "repeat(2, 1fr)", "gap": "12px"},
                        children=[
                            chart_annual_import_export(),
                            chart_import_source_composition(),
                        ],
                    ),
                    chart_india_demand(),
                    html.Div(
                        style={"marginTop": "12px", "padding": "12px", "backgroundColor": T.BG_TERTIARY, "borderRadius": "4px"},
                        children=[
                            html.P(
                                "📊 Rising African sourcing share suggests cheaper artisanal supply entering the system. "
                                "High India export volumes + premium = strong re-export demand.",
                                style={"fontSize": "12px", "margin": "0"},
                            ),
                        ],
                    ),
                ],
            ),

            # ── SECTION 4: Macro Drivers ──
            html.Div(
                style={**T.CARD_STYLE, "marginBottom": "20px"},
                children=[
                    html.H3("Macro Driver Correlations", style={"fontSize": "14px", "marginBottom": "12px", "fontWeight": "600"}),
                    chart_macro_correlations(),
                    html.Div(
                        style={"marginTop": "12px", "padding": "12px", "backgroundColor": T.BG_TERTIARY, "borderRadius": "4px"},
                        children=[
                            html.P(
                                "🌍 Higher USD yields and crude oil prices support Dubai premium (demand-for-physical increases). "
                                "Rising DXY often tightens physical supply (import costs up).",
                                style={"fontSize": "12px", "margin": "0"},
                            ),
                        ],
                    ),
                ],
            ),

        ],
    )
