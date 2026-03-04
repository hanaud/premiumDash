"""
Dark-theme constants and Plotly template for the dashboard.
Bloomberg-terminal inspired colour palette.
"""

# -- Palette ---------------------------------------------------------------
BG_PRIMARY = "#0d1117"       # near-black
BG_SECONDARY = "#161b22"     # card backgrounds
BG_TERTIARY = "#21262d"      # hover / subtle
BORDER = "#30363d"
TEXT_PRIMARY = "#e6edf3"
TEXT_SECONDARY = "#8b949e"
TEXT_MUTED = "#484f58"
ACCENT_BLUE = "#58a6ff"
ACCENT_GREEN = "#3fb950"
ACCENT_RED = "#f85149"
ACCENT_ORANGE = "#d29922"
ACCENT_PURPLE = "#bc8cff"
ACCENT_CYAN = "#39d2c0"

# Spread-specific colours
BACKWARDATION_COLOR = ACCENT_GREEN   # physical premium
CONTANGO_COLOR = ACCENT_RED          # physical discount

# Multi-expiry overlay colours (front month uses ACCENT_BLUE)
EXPIRY_COLORS = [ACCENT_ORANGE, ACCENT_PURPLE, ACCENT_CYAN, ACCENT_GREEN]

# -- Plotly layout template ------------------------------------------------
PLOTLY_TEMPLATE = dict(
    layout=dict(
        paper_bgcolor=BG_SECONDARY,
        plot_bgcolor=BG_PRIMARY,
        font=dict(family="Inter, Segoe UI, Roboto, sans-serif", color=TEXT_PRIMARY, size=12),
        title=dict(font=dict(size=14, color=TEXT_PRIMARY), x=0.01, xanchor="left"),
        xaxis=dict(
            gridcolor=BG_TERTIARY,
            zerolinecolor=BORDER,
            linecolor=BORDER,
            tickfont=dict(size=10, color=TEXT_SECONDARY),
        ),
        yaxis=dict(
            gridcolor=BG_TERTIARY,
            zerolinecolor=BORDER,
            linecolor=BORDER,
            tickfont=dict(size=10, color=TEXT_SECONDARY),
        ),
        legend=dict(
            bgcolor="rgba(0,0,0,0)",
            font=dict(size=11, color=TEXT_SECONDARY),
        ),
        margin=dict(l=50, r=20, t=40, b=40),
        hovermode="x unified",
        hoverlabel=dict(bgcolor=BG_TERTIARY, font_size=11),
    )
)

# -- CSS -------------------------------------------------------------------
EXTERNAL_STYLESHEETS = [
    "https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap",
]

GLOBAL_CSS = {
    "fontFamily": "Inter, Segoe UI, Roboto, sans-serif",
    "backgroundColor": BG_PRIMARY,
    "color": TEXT_PRIMARY,
    "minHeight": "100vh",
}

CARD_STYLE = {
    "backgroundColor": BG_SECONDARY,
    "border": f"1px solid {BORDER}",
    "borderRadius": "8px",
    "padding": "16px",
    "marginBottom": "12px",
}

HEADER_STYLE = {
    "backgroundColor": BG_SECONDARY,
    "borderBottom": f"1px solid {BORDER}",
    "padding": "12px 24px",
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "space-between",
}

KPI_VALUE_STYLE = {
    "fontSize": "22px",
    "fontWeight": "600",
    "lineHeight": "1.2",
}

KPI_LABEL_STYLE = {
    "fontSize": "11px",
    "color": TEXT_SECONDARY,
    "textTransform": "uppercase",
    "letterSpacing": "0.5px",
    "marginBottom": "4px",
}
