"""
Storytelling Dashboard — Music perception insights for the functional user.
Run: python dashboard/storytelling_app.py
URL: http://localhost:8051
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import dash
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dash import Input, Output, State, dcc, html

from utils import (
    COLORS,
    SENTIMENT_COLORS,
    SOURCE_COLORS,
    SOURCE_LABELS,
    gold_state_key,
    last_updated,
    load_storytelling,
)

# ── App init ──────────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.FLATLY],
    title="Music Perception Dashboard",
    assets_folder="assets",
    suppress_callback_exceptions=True,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server


# ── Layout ────────────────────────────────────────────────────────────────────

app.layout = dbc.Container(
    [
        # ── Header ────────────────────────────────────────────────────────────
        dbc.Row(
            [
                dbc.Col(
                    [
                        html.H2("Music Perception Dashboard", className="fw-bold mb-0",
                                style={"color": COLORS["primary"]}),
                        html.P(
                            "How the indie & hip-hop community reacts to new music — Reddit + Last.fm",
                            className="text-muted mb-0", style={"fontSize": "14px"},
                        ),
                    ],
                    md=8,
                ),
                dbc.Col(
                    html.Div(
                        [
                            dbc.Button("↻  Refresh", id="st-refresh-btn",
                                       color="primary", size="sm", outline=True, className="me-2"),
                            html.Span(id="st-last-updated", className="text-muted",
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

        # ── Narrative card ─────────────────────────────────────────────────────
        html.Div(id="st-narrative", className="mb-4"),

        # ── Sentiment donut + Trend ────────────────────────────────────────────
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.H6("Community Sentiment", className="fw-semibold text-secondary mb-3",
                                    style={"fontSize": "13px", "textTransform": "uppercase",
                                           "letterSpacing": "0.05em"}),
                            dcc.Graph(id="st-pie", config={"displayModeBar": False},
                                      style={"height": "320px"}),
                        ]),
                        className="shadow-sm h-100", style={"borderRadius": "8px"},
                    ),
                    md=4,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.H6("Sentiment Trend Over Time", className="fw-semibold text-secondary mb-3",
                                    style={"fontSize": "13px", "textTransform": "uppercase",
                                           "letterSpacing": "0.05em"}),
                            dcc.Graph(id="st-trend", config={"displayModeBar": False},
                                      style={"height": "320px"}),
                        ]),
                        className="shadow-sm h-100", style={"borderRadius": "8px"},
                    ),
                    md=8,
                ),
            ],
            className="g-3 mb-4",
        ),

        # ── Top keywords + Comment types ───────────────────────────────────────
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.H6("Top Keywords", className="fw-semibold text-secondary mb-3",
                                    style={"fontSize": "13px", "textTransform": "uppercase",
                                           "letterSpacing": "0.05em"}),
                            dcc.Graph(id="st-keywords", config={"displayModeBar": False},
                                      style={"height": "400px"}),
                        ]),
                        className="shadow-sm h-100", style={"borderRadius": "8px"},
                    ),
                    md=7,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.H6("Comment Type Breakdown", className="fw-semibold text-secondary mb-3",
                                    style={"fontSize": "13px", "textTransform": "uppercase",
                                           "letterSpacing": "0.05em"}),
                            dcc.Graph(id="st-comment-types", config={"displayModeBar": False},
                                      style={"height": "400px"}),
                        ]),
                        className="shadow-sm h-100", style={"borderRadius": "8px"},
                    ),
                    md=5,
                ),
            ],
            className="g-3 mb-4",
        ),

        # ── Volume activity + Source comparison ────────────────────────────────
        dbc.Row(
            [
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.H6("Volume Activity Over Time", className="fw-semibold text-secondary mb-3",
                                    style={"fontSize": "13px", "textTransform": "uppercase",
                                           "letterSpacing": "0.05em"}),
                            dcc.Graph(id="st-volume", config={"displayModeBar": False},
                                      style={"height": "320px"}),
                        ]),
                        className="shadow-sm h-100", style={"borderRadius": "8px"},
                    ),
                    md=6,
                ),
                dbc.Col(
                    dbc.Card(
                        dbc.CardBody([
                            html.H6("Top Artists — Last.fm vs. Reddit Mentions",
                                    className="fw-semibold text-secondary mb-3",
                                    style={"fontSize": "13px", "textTransform": "uppercase",
                                           "letterSpacing": "0.05em"}),
                            dcc.Graph(id="st-source-compare", config={"displayModeBar": False},
                                      style={"height": "320px"}),
                        ]),
                        className="shadow-sm h-100", style={"borderRadius": "8px"},
                    ),
                    md=6,
                ),
            ],
            className="g-3 mb-4",
        ),

        # Poll gold folder every 15 s; re-render only when files change
        dcc.Interval(id="st-interval", interval=15 * 1000, n_intervals=0),
        dcc.Store(id="st-file-state", data=""),
    ],
    fluid=True,
    style={"backgroundColor": COLORS["background"], "minHeight": "100vh", "padding": "0 24px 32px"},
)


# ── Callback ──────────────────────────────────────────────────────────────────

@app.callback(
    Output("st-narrative",      "children"),
    Output("st-pie",            "figure"),
    Output("st-trend",          "figure"),
    Output("st-keywords",       "figure"),
    Output("st-comment-types",  "figure"),
    Output("st-volume",         "figure"),
    Output("st-source-compare", "figure"),
    Output("st-last-updated",   "children"),
    Output("st-file-state",     "data"),
    Input("st-interval",        "n_intervals"),
    Input("st-refresh-btn",     "n_clicks"),
    State("st-file-state",      "data"),
)
def update_storytelling(_interval, _clicks, stored_state):
    current_state = gold_state_key()

    # Skip re-render if the gold folder hasn't changed since last render
    if dash.ctx.triggered_id == "st-interval" and current_state == stored_state:
        return (dash.no_update,) * 9

    df = load_storytelling()

    def empty_fig(msg="No data"):
        fig = go.Figure()
        fig.add_annotation(text=msg, x=0.5, y=0.5,
                           xref="paper", yref="paper", showarrow=False,
                           font=dict(size=13, color=COLORS["muted"]))
        fig.update_layout(plot_bgcolor="white", paper_bgcolor="white",
                          margin=dict(l=10, r=10, t=10, b=10))
        return fig

    if df.empty:
        alert = dbc.Alert("No storytelling data found. Run the gold_pipeline DAG first.", color="warning")
        ef = empty_fig("Run gold_pipeline DAG")
        return alert, ef, ef, ef, ef, ef, ef, "N/A", current_state

    # ── Narrative card ────────────────────────────────────────────────────────
    sent = df[df["metric_type"] == "sentiment_dist"]
    pos_row = sent[sent["dim1"] == "positive"]
    neg_row = sent[sent["dim1"] == "negative"]
    pos_pct   = f"{pos_row['pct'].iloc[0]:.1f}" if not pos_row.empty else "?"
    pos_score = f"{pos_row['avg_score'].iloc[0]:.3f}" if not pos_row.empty else "?"
    neg_pct   = f"{neg_row['pct'].iloc[0]:.1f}" if not neg_row.empty else "?"

    top_kw = (
        df[df["metric_type"] == "top_keyword"]
        .sort_values("record_count", ascending=False)["dim1"].head(3).tolist()
    )
    kw_str = ", ".join(f'"{k}"' for k in top_kw) if top_kw else "—"

    top_artist_row = (
        df[df["metric_type"] == "top_artist_lastfm"]
        .sort_values("avg_score", ascending=False)
    )
    top_artist = top_artist_row["dim1"].iloc[0] if not top_artist_row.empty else "—"
    top_listeners = (
        f"{int(top_artist_row['avg_score'].iloc[0]/1e6)}M"
        if not top_artist_row.empty else "—"
    )

    narrative = dbc.Card(
        dbc.CardBody([
            dbc.Row([
                dbc.Col(
                    html.Div([
                        html.Span("KEY INSIGHT", style={
                            "fontSize": "11px", "fontWeight": "700",
                            "letterSpacing": "0.1em", "color": COLORS["primary"],
                        }),
                        html.P(
                            f"{pos_pct}% of community comments are positive (avg score +{pos_score}), "
                            f"while only {neg_pct}% are negative. "
                            f"The conversation revolves around {kw_str}. "
                            f"On Last.fm, {top_artist} leads with {top_listeners}+ unique listeners — "
                            f"a quantitative giant whose community reception is worth tracking closely.",
                            className="mb-0 mt-1",
                            style={"fontSize": "14px", "lineHeight": "1.7", "color": COLORS["text"]},
                        ),
                    ]),
                    md=9,
                ),
                dbc.Col(
                    html.Div([
                        dbc.Row([
                            dbc.Col(html.Div([
                                html.P("Positive", className="text-muted mb-0",
                                       style={"fontSize": "11px", "textTransform": "uppercase"}),
                                html.H4(f"{pos_pct}%", className="fw-bold mb-0",
                                        style={"color": COLORS["positive"]}),
                            ]), width=4),
                            dbc.Col(html.Div([
                                html.P("Negative", className="text-muted mb-0",
                                       style={"fontSize": "11px", "textTransform": "uppercase"}),
                                html.H4(f"{neg_pct}%", className="fw-bold mb-0",
                                        style={"color": COLORS["negative"]}),
                            ]), width=4),
                            dbc.Col(html.Div([
                                html.P("Avg Score", className="text-muted mb-0",
                                       style={"fontSize": "11px", "textTransform": "uppercase"}),
                                html.H4(f"+{pos_score}", className="fw-bold mb-0",
                                        style={"color": COLORS["primary"]}),
                            ]), width=4),
                        ]),
                    ]),
                    md=3,
                    className="d-flex align-items-center",
                ),
            ]),
        ]),
        className="shadow-sm",
        style={
            "borderRadius": "8px",
            "borderLeft": f"5px solid {COLORS['primary']}",
            "backgroundColor": "#eff6ff",
        },
    )

    # ── Sentiment donut ───────────────────────────────────────────────────────
    total_comments = int(sent["record_count"].sum())
    fig_pie = go.Figure(go.Pie(
        labels=sent["dim1"],
        values=sent["record_count"],
        hole=0.58,
        marker_colors=[SENTIMENT_COLORS.get(l, "#aaa") for l in sent["dim1"]],
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>%{value} comments<br>%{percent}<extra></extra>",
        sort=False,
    ))
    fig_pie.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        showlegend=False,
        margin=dict(l=10, r=10, t=20, b=20),
        annotations=[dict(
            text=f"<b>{total_comments}</b><br><span style='font-size:11'>comments</span>",
            x=0.5, y=0.5, font_size=15, showarrow=False, align="center",
        )],
        font=dict(family="Inter, sans-serif", size=12),
    )

    # ── Sentiment trend ───────────────────────────────────────────────────────
    trend_df = df[df["metric_type"] == "sentiment_trend"].copy()
    trend_df["dim1"] = pd.to_datetime(trend_df["dim1"])
    trend_df = trend_df.sort_values("dim1")

    fig_trend = go.Figure()
    fig_trend.add_hrect(y0=0.05, y1=1, fillcolor=COLORS["positive"],
                        opacity=0.04, line_width=0, annotation_text="Positive zone",
                        annotation_position="top left",
                        annotation=dict(font_size=10, font_color=COLORS["positive"]))
    fig_trend.add_hrect(y0=-1, y1=-0.05, fillcolor=COLORS["negative"],
                        opacity=0.04, line_width=0, annotation_text="Negative zone",
                        annotation_position="bottom left",
                        annotation=dict(font_size=10, font_color=COLORS["negative"]))
    fig_trend.add_trace(go.Scatter(
        x=trend_df["dim1"], y=trend_df["avg_score"],
        mode="lines+markers",
        line=dict(color=COLORS["primary"], width=2.5),
        marker=dict(size=8, color=COLORS["primary"], line=dict(color="white", width=2)),
        fill="tozeroy",
        fillcolor="rgba(30,64,175,0.08)",
        hovertemplate="%{x|%Y-%m-%d}<br>Avg score: <b>%{y:.4f}</b><extra></extra>",
    ))
    fig_trend.add_hline(y=0, line_dash="dash", line_color="#94a3b8", line_width=1)
    fig_trend.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        xaxis_title="", yaxis_title="Avg Compound Score",
        yaxis=dict(range=[-1, 1], zeroline=False),
        margin=dict(l=10, r=10, t=10, b=30),
        showlegend=False,
        font=dict(family="Inter, sans-serif", size=11),
    )

    # ── Top keywords ──────────────────────────────────────────────────────────
    kw_df = (
        df[df["metric_type"] == "top_keyword"]
        .sort_values("record_count", ascending=False).head(20)
        .sort_values("record_count", ascending=True)
        .copy()
    )
    kw_df["sentiment_label"] = kw_df["avg_score"].apply(
        lambda s: "positive" if s >= 0.05 else ("negative" if s <= -0.05 else "neutral")
    )
    fig_kw = px.bar(
        kw_df, x="record_count", y="dim1", orientation="h",
        color="sentiment_label",
        color_discrete_map=SENTIMENT_COLORS,
        labels={"record_count": "Frequency", "dim1": "", "sentiment_label": "Sentiment"},
        hover_data={"avg_score": ":.4f", "pct": ":.2f"},
        custom_data=["avg_score"],
    )
    fig_kw.update_traces(
        hovertemplate="<b>%{y}</b><br>Frequency: %{x}<br>Avg sentiment: %{customdata[0]:.3f}<extra></extra>"
    )
    fig_kw.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        legend=dict(orientation="h", y=1.05, x=0, font_size=11),
        margin=dict(l=10, r=30, t=30, b=10),
        xaxis_title="Frequency",
        font=dict(family="Inter, sans-serif", size=11),
    )

    # ── Comment type breakdown ────────────────────────────────────────────────
    ct_df = df[df["metric_type"] == "comment_type_dist"].copy()
    type_colors = {
        "recommendation": COLORS["primary"],
        "opinion":        "#70ad47",
        "mixed":          "#ffc000",
        "other":          "#94a3b8",
    }
    type_labels = {
        "recommendation": "Recommendation",
        "opinion":        "Opinion",
        "mixed":          "Mixed",
        "other":          "Other",
    }
    ct_df["label"] = ct_df["dim1"].map(type_labels).fillna(ct_df["dim1"])
    fig_ct = go.Figure(go.Bar(
        x=ct_df["pct"],
        y=ct_df["label"],
        orientation="h",
        marker_color=[type_colors.get(t, "#aaa") for t in ct_df["dim1"]],
        text=ct_df["pct"].round(1).astype(str) + "%",
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>%{x:.1f}% (%{customdata} comments)<extra></extra>",
        customdata=ct_df["record_count"],
    ))
    fig_ct.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        xaxis_title="Share (%)",
        xaxis=dict(range=[0, ct_df["pct"].max() * 1.3]),
        margin=dict(l=10, r=60, t=10, b=10),
        font=dict(family="Inter, sans-serif", size=11),
    )

    # ── Volume activity ───────────────────────────────────────────────────────
    vol_df = df[df["metric_type"] == "volume_trend"].copy()
    if not vol_df.empty:
        vol_df["dim1"] = pd.to_datetime(vol_df["dim1"])
        vol_df["source_label"] = vol_df["dim2"].map(SOURCE_LABELS).fillna(vol_df["dim2"])
        fig_vol = px.bar(
            vol_df.sort_values("dim1"),
            x="dim1", y="record_count", color="dim2",
            color_discrete_map=SOURCE_COLORS,
            labels={"dim1": "Date", "record_count": "Records", "dim2": "Source"},
            barmode="group",
            hover_data={"source_label": True},
        )
        fig_vol.update_layout(
            plot_bgcolor="white", paper_bgcolor="white",
            legend=dict(orientation="h", y=1.05, x=0, font_size=11),
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis_title="", yaxis_title="Records",
            font=dict(family="Inter, sans-serif", size=11),
        )
    else:
        fig_vol = empty_fig("No volume data")

    # ── Source comparison: Last.fm top artists (listeners) ────────────────────
    artists_df = (
        df[df["metric_type"] == "top_artist_lastfm"]
        .sort_values("avg_score", ascending=False).head(10)
        .sort_values("avg_score", ascending=True)
    )
    reddit_art_df = (
        df[df["metric_type"] == "reddit_artist"]
        .sort_values("record_count", ascending=False).head(10)
    ) if "reddit_artist" in df["metric_type"].values else pd.DataFrame()

    fig_src = go.Figure()
    fig_src.add_trace(go.Bar(
        x=artists_df["avg_score"] / 1e6,
        y=artists_df["dim1"],
        orientation="h",
        name="Last.fm listeners (M)",
        marker_color=COLORS["lastfm_artists"],
        hovertemplate="<b>%{y}</b><br>Listeners: %{x:.1f}M<extra></extra>",
    ))
    if not reddit_art_df.empty:
        # Overlay Reddit mentions as a secondary axis marker
        matching = reddit_art_df[reddit_art_df["dim1"].isin(artists_df["dim1"])]
        if not matching.empty:
            fig_src.add_trace(go.Scatter(
                x=matching["record_count"] * (artists_df["avg_score"].max() / 1e6 / matching["record_count"].max()),
                y=matching["dim1"],
                mode="markers",
                name="Reddit mentions (scaled)",
                marker=dict(color=COLORS["reddit"], size=10, symbol="diamond"),
                hovertemplate="<b>%{y}</b><br>Reddit mentions: %{customdata}<extra></extra>",
                customdata=matching["record_count"],
            ))

    fig_src.update_layout(
        plot_bgcolor="white", paper_bgcolor="white",
        legend=dict(orientation="h", y=1.05, x=0, font_size=11),
        margin=dict(l=10, r=20, t=30, b=10),
        xaxis_title="Unique Listeners (millions)",
        font=dict(family="Inter, sans-serif", size=11),
    )

    return (
        narrative, fig_pie, fig_trend, fig_kw,
        fig_ct, fig_vol, fig_src,
        f"Last updated: {last_updated(df)}", current_state,
    )


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=8051)
