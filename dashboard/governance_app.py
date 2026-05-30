"""
Governance Dashboard — Data quality KPIs for the music pipeline.
Run: python dashboard/governance_app.py
URL: http://localhost:8050
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import dash
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dash import Input, Output, State, dash_table, dcc, html

from utils import (
    COLORS,
    SOURCE_COLORS,
    SOURCE_LABELS,
    gold_state_key,
    last_updated,
    load_governance,
    severity_color,
)

# ── App init ──────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    title="Data Governance Dashboard",
    assets_folder="assets",
    suppress_callback_exceptions=True,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server  # expose Flask server for gunicorn


# ── Reusable components ───────────────────────────────────────────────────────

def kpi_card(title: str, value: str, subtitle: str = "", accent: str = COLORS["primary"]):
    return dbc.Card(
        dbc.CardBody([
            html.P(title, className="text-muted small mb-1 text-uppercase fw-semibold",
                   style={"letterSpacing": "0.06em", "fontSize": "11px"}),
            html.H3(value, className="fw-bold mb-1", style={"color": accent}),
            html.P(subtitle, className="text-muted mb-0", style={"fontSize": "12px"}),
        ]),
        className="shadow-sm h-100",
        style={"borderTop": f"4px solid {accent}", "borderRadius": "8px"},
    )


def section_card(title: str, graph_id: str):
    return dbc.Card(
        dbc.CardBody([
            html.H6(title, className="fw-semibold text-secondary mb-3",
                    style={"fontSize": "13px", "textTransform": "uppercase", "letterSpacing": "0.05em"}),
            dcc.Graph(id=graph_id, config={"displayModeBar": False}, style={"height": "340px"}),
        ]),
        className="shadow-sm h-100",
        style={"borderRadius": "8px"},
    )


# ── Layout ────────────────────────────────────────────────────────────────────

app.layout = dbc.Container(
    [
        # ── Header ────────────────────────────────────────────────────────────
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H2("Data Governance Dashboard", className="fw-bold mb-0",
                                style={"color": COLORS["primary"]}),
                        html.P("Music Artists & Albums Public Perception — Pipeline Quality Metrics",
                               className="text-muted mb-0", style={"fontSize": "14px"}),
                    ],
                    md=8,
                ),
                dbc.Col(
                    html.Div(
                        [
                            dbc.Button("↻  Refresh", id="gov-refresh-btn",
                                       color="primary", size="sm", outline=True, className="me-2"),
                            html.Span(id="gov-last-updated", className="text-muted",
                                      style={"fontSize": "12px"}),
                        ],
                        className="d-flex align-items-center justify-content-end h-100",
                    ),
                    md=4,
                ),
            ],
            className="py-3 mb-4 align-items-center",
            style={"borderBottom": f"2px solid {COLORS['primary']}"},
        ),

        # ── KPI Cards row ─────────────────────────────────────────────────────
        dbc.Row(id="gov-kpi-row", className="g-3 mb-4"),

        # ── Null rate + Outlier rate ───────────────────────────────────────────
        dbc.Row(
            [
                dbc.Col(section_card("Null Rate per Field", "gov-null-chart"), md=7),
                dbc.Col(section_card("Outlier Rate per Numeric Field (IQR ×1.5)", "gov-outlier-chart"), md=5),
            ],
            className="g-3 mb-4",
        ),

        # ── Volume by source + Text length ────────────────────────────────────
        dbc.Row(
            [
                dbc.Col(section_card("Total Records by Source", "gov-volume-chart"), md=5),
                dbc.Col(section_card("Mean Text Length per Field (chars)", "gov-textlen-chart"), md=7),
            ],
            className="g-3 mb-4",
        ),

        # ── Full KPI table ────────────────────────────────────────────────────
        dbc.Card(
            [
                dbc.CardHeader(
                    html.H6("Complete Data Quality KPI Table", className="mb-0 fw-semibold",
                            style={"fontSize": "13px", "textTransform": "uppercase",
                                   "letterSpacing": "0.05em", "color": COLORS["muted"]}),
                ),
                dbc.CardBody(html.Div(id="gov-kpi-table")),
            ],
            className="shadow-sm mb-4",
            style={"borderRadius": "8px"},
        ),

        # Poll gold folder every 15 s; re-render only when files change
        dcc.Interval(id="gov-interval", interval=15 * 1000, n_intervals=0),
        dcc.Store(id="gov-file-state", data=""),
    ],
    fluid=True,
    style={"backgroundColor": COLORS["background"], "minHeight": "100vh", "padding": "0 24px 32px"},
)


# ── Callback ──────────────────────────────────────────────────────────────────

@app.callback(
    Output("gov-kpi-row",       "children"),
    Output("gov-null-chart",    "figure"),
    Output("gov-outlier-chart", "figure"),
    Output("gov-volume-chart",  "figure"),
    Output("gov-textlen-chart", "figure"),
    Output("gov-kpi-table",     "children"),
    Output("gov-last-updated",  "children"),
    Output("gov-file-state",    "data"),
    Input("gov-interval",       "n_intervals"),
    Input("gov-refresh-btn",    "n_clicks"),
    State("gov-file-state",     "data"),
)
def update_governance(_interval, _clicks, stored_state):
    current_state = gold_state_key()

    # Skip re-render if the gold folder hasn't changed since last render
    # (always render on manual refresh or first load)
    if dash.ctx.triggered_id == "gov-interval" and current_state == stored_state:
        return (dash.no_update,) * 8
    df = load_governance()

    if df.empty:
        empty_fig = go.Figure()
        empty_fig.add_annotation(text="No data — run gold_pipeline DAG",
                                 x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False)
        empty_fig.update_layout(plot_bgcolor="white", paper_bgcolor="white")
        alert = dbc.Alert("No governance data found. Run the gold_pipeline DAG first.", color="warning")
        return [], empty_fig, empty_fig, empty_fig, empty_fig, alert, "N/A", current_state

    # ── KPI cards ─────────────────────────────────────────────────────────────
    def get_val(source, kpi, field="ALL"):
        sub = df[(df["source"] == source) & (df["kpi_type"] == kpi) & (df["field_name"] == field)]
        return sub["value"].iloc[0] if not sub.empty else None

    vol_reddit  = get_val("reddit",         "volume")
    vol_artists = get_val("lastfm_artists", "volume")
    vol_tracks  = get_val("lastfm_tracks",  "volume")
    compliance  = df[df["kpi_type"] == "schema_compliance"]["value"].mean()

    null_rates  = df[(df["kpi_type"] == "null_rate") & (df["value"] > 0)]
    max_null    = null_rates["value"].max() if not null_rates.empty else 0
    max_field   = (null_rates.loc[null_rates["value"].idxmax(), "field_name"]
                   if not null_rates.empty else "—")

    outlier_avg = df[df["kpi_type"] == "outlier_rate"]["value"].mean()

    kpi_cards = [
        dbc.Col(kpi_card("Reddit Comments",     f"{int(vol_reddit):,}" if vol_reddit else "—",
                         "total processed records", COLORS["reddit"]), md=3),
        dbc.Col(kpi_card("Last.fm Artists",     f"{int(vol_artists):,}" if vol_artists else "—",
                         "consolidated snapshots", COLORS["lastfm_artists"]), md=3),
        dbc.Col(kpi_card("Schema Compliance",   f"{compliance:.1f}%",
                         "all sources — required fields", COLORS["ok"]), md=3),
        dbc.Col(kpi_card("Highest Null Rate",   f"{max_null:.1f}%",
                         f"field: {max_field}",
                         severity_color(max_null)), md=3),
    ]

    # ── Null rate chart ───────────────────────────────────────────────────────
    null_df = (
        df[df["kpi_type"] == "null_rate"]
        .assign(full_field=lambda d: d["source"] + "." + d["field_name"])
        .sort_values("value", ascending=True)
    )
    fig_null = px.bar(
        null_df, x="value", y="full_field", orientation="h",
        color="value",
        color_continuous_scale=["#16a34a", "#d97706", "#dc2626"],
        range_color=[0, max(null_df["value"].max(), 1)],
        labels={"value": "Null Rate (%)", "full_field": ""},
        hover_data={"source": True, "field_name": True},
    )
    fig_null.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        coloraxis_showscale=False,
        margin=dict(l=0, r=20, t=10, b=30),
        xaxis_title="Null Rate (%)",
        font=dict(family="Inter, sans-serif", size=11),
    )

    # ── Outlier rate chart ────────────────────────────────────────────────────
    out_df = (
        df[df["kpi_type"] == "outlier_rate"]
        .assign(full_field=lambda d: d["source"] + "." + d["field_name"])
        .sort_values("value", ascending=True)
    )
    fig_out = go.Figure(go.Bar(
        x=out_df["value"],
        y=out_df["full_field"],
        orientation="h",
        marker_color=[SOURCE_COLORS.get(s, "#aaa") for s in out_df["source"]],
        hovertemplate="%{y}: %{x:.2f}%<extra></extra>",
        text=out_df["value"].round(1).astype(str) + "%",
        textposition="outside",
    ))
    fig_out.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        margin=dict(l=0, r=50, t=10, b=30),
        xaxis_title="Outlier Rate (%)",
        font=dict(family="Inter, sans-serif", size=11),
    )

    # ── Volume by source ──────────────────────────────────────────────────────
    vol_df = df[df["kpi_type"] == "volume"].copy()
    vol_df["label"] = vol_df["source"].map(SOURCE_LABELS).fillna(vol_df["source"])
    fig_vol = px.bar(
        vol_df, x="label", y="value",
        color="source", color_discrete_map=SOURCE_COLORS,
        text="value",
        labels={"value": "Records", "label": ""},
    )
    fig_vol.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig_vol.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        showlegend=False,
        margin=dict(l=0, r=20, t=10, b=60),
        yaxis_title="Total Records",
        xaxis_tickangle=-15,
        font=dict(family="Inter, sans-serif", size=11),
    )

    # ── Text length mean ──────────────────────────────────────────────────────
    tl_df = (
        df[df["kpi_type"] == "text_len_mean"]
        .assign(full_field=lambda d: d["source"] + "." + d["field_name"])
        .sort_values("value", ascending=True)
    )
    fig_tl = px.bar(
        tl_df, x="value", y="full_field", orientation="h",
        color="source", color_discrete_map=SOURCE_COLORS,
        labels={"value": "Mean Length (chars)", "full_field": ""},
        text=tl_df["value"].round(0).astype(int).astype(str) + " chars",
    )
    fig_tl.update_traces(textposition="outside")
    fig_tl.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        showlegend=True,
        legend=dict(orientation="h", y=1.08, x=0),
        margin=dict(l=0, r=80, t=30, b=10),
        xaxis_title="Mean Length (characters)",
        font=dict(family="Inter, sans-serif", size=11),
    )

    # ── Data quality table ────────────────────────────────────────────────────
    table_df = df[["source", "field_name", "kpi_type", "value", "unit", "data_date"]].copy()
    table_df["value"] = table_df["value"].round(3)
    table_df.columns = ["Source", "Field", "KPI", "Value", "Unit", "Data Date"]

    table = dash_table.DataTable(
        data=table_df.to_dict("records"),
        columns=[{"name": c, "id": c} for c in table_df.columns],
        filter_action="native",
        sort_action="native",
        page_size=15,
        style_table={"overflowX": "auto"},
        style_header={
            "backgroundColor": COLORS["primary"],
            "color": "white",
            "fontWeight": "600",
            "fontSize": "12px",
            "textTransform": "uppercase",
            "letterSpacing": "0.04em",
        },
        style_cell={
            "textAlign": "left",
            "padding": "8px 12px",
            "fontFamily": "Inter, sans-serif",
            "fontSize": "12px",
            "border": f"1px solid {COLORS['border']}",
        },
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#f8fafc"},
            {"if": {"filter_query": '{KPI} = "null_rate" && {Value} > 5'},
             "backgroundColor": "#fee2e2", "color": COLORS["error"]},
            {"if": {"filter_query": '{KPI} = "schema_compliance" && {Value} = 100'},
             "color": COLORS["ok"], "fontWeight": "600"},
            {"if": {"filter_query": '{KPI} = "outlier_rate" && {Value} > 15'},
             "backgroundColor": "#fef9c3"},
        ],
    )

    return (
        kpi_cards, fig_null, fig_out, fig_vol, fig_tl,
        table, f"Last updated: {last_updated(df)}", current_state,
    )


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8050)
