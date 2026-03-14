"""
Phase 7 — Flask application for LEO Satellite Risk Interactive Web UI.

Endpoints:
  GET  /              — Dashboard (map + search + panels)
  GET  /report        — Full findings report (Markdown → HTML)
  GET  /api/stats     — Overall tier distribution
  GET  /api/countries — Countries from dataset (Country dropdown)
  GET  /api/counties  — State + county names (County dropdown)
  GET  /api/map-data  — NC county GeoJSON with risk stats (choropleth)
  GET  /api/map-points — Sampled risk points by tier (for map + layer toggles)
  POST /api/analyze   — Single-location risk (address or lat/lon) → LLM narrative;
                        always runs; out_of_coverage: true when lat/lon outside NC (UI shows banner + full response)
  GET  /api/county/<name> — County briefing → LLM narrative
  GET  /admin         — Data team: summary stats, bar/pie charts, static map
  GET  /output/<path> — Serve generated files (charts, map PNG) from data/output/
  POST /api/chat      — Interactive chatbot: natural-language query → agent response + optional lat/lon
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import flask

# Project root and config
PROJECT_ROOT = Path(__file__).resolve().parent.parent

def _load_config():
    import sys
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))
    from src import config
    return config

def create_app():
    app = flask.Flask(
        __name__,
        static_folder=str(PROJECT_ROOT / "app" / "static"),
        template_folder=str(PROJECT_ROOT / "app" / "templates"),
        instance_relative_config=True,
    )
    app.config["JSONIFY_PRETTY_PRINT_REGULAR"] = True

    config = _load_config()
    scored_path = config.SCORED_LOCATIONS_PATH
    findings_path = config.FINDINGS_REPORT_PATH
    output_dir = config.OUTPUT_DIR

    # In-memory caches (populated on first use)
    _stats_cache: dict | None = None
    _county_geojson_cache: dict | None = None
    _map_points_cache: list | None = None
    _df_cache = None

    def get_scored_df():
        import pandas as pd
        nonlocal _df_cache
        if _df_cache is None:
            if not scored_path.exists():
                raise FileNotFoundError(
                    "locations_scored.csv not found. Run the full pipeline first: python -m src.main"
                )
            _df_cache = pd.read_csv(scored_path)
        return _df_cache

    # -------------------------------------------------------------------------
    # Pages
    # -------------------------------------------------------------------------

    @app.route("/")
    def index():
        return flask.render_template("index.html")

    @app.route("/report")
    def report_page():
        if not findings_path.exists():
            return flask.render_template("report.html", content="<p>Report not generated yet. Run the full pipeline first.</p>")
        content_md = findings_path.read_text(encoding="utf-8")
        try:
            import markdown
            content_html = markdown.markdown(content_md, extensions=["tables", "fenced_code"])
        except ImportError:
            content_html = f"<pre>{flask.escape(content_md)}</pre>"
        return flask.render_template("report.html", content=content_html)

    # -------------------------------------------------------------------------
    # API: stats
    # -------------------------------------------------------------------------

    @app.route("/api/countries")
    def api_countries():
        """Return list of countries present in the dataset (derived from state/geoid)."""
        fallback = [{"code": "US", "name": "United States"}]
        try:
            df = get_scored_df()
            if "state" not in df.columns:
                return flask.jsonify({"countries": fallback})
            states = df["state"].dropna().astype(str).str.upper().unique().tolist()
            countries = fallback if states else []
            return flask.jsonify({"countries": countries})
        except Exception:
            return flask.jsonify({"countries": fallback})

    def _nc_county_names_fallback():
        """Return sorted list of NC county names (NAMELSAD) from pygris when scored data unavailable."""
        try:
            import pygris
            gdf = pygris.counties(state="NC", cb=True, year=2022, cache=True)
            return sorted(gdf["NAMELSAD"].dropna().astype(str).tolist())
        except Exception:
            return []

    @app.route("/api/counties")
    def api_counties():
        """Return state and list of county names for dropdowns. Uses fallback from pygris if needed."""
        try:
            df = get_scored_df()
            if "state" in df.columns and "county" in df.columns:
                nc = df[df["state"].astype(str).str.upper() == "NC"]
                counties = sorted(nc["county"].dropna().astype(str).unique().tolist())
                if counties:
                    return flask.jsonify({"state": "NC", "counties": counties})
        except Exception:
            pass
        counties = _nc_county_names_fallback()
        return flask.jsonify({"state": "NC", "counties": counties})

    @app.route("/api/stats")
    def api_stats():
        try:
            df = get_scored_df()
        except FileNotFoundError as e:
            return flask.jsonify({"error": str(e)}), 503
        total = len(df)
        tier_counts = df["risk_tier"].value_counts()
        tiers = ["HIGH", "MODERATE", "LOW", "UNKNOWN"]
        distribution = {
            t: {"count": int(tier_counts.get(t, 0)), "pct": round(tier_counts.get(t, 0) / total * 100, 1)}
            for t in tiers
        }
        return flask.jsonify({"total": total, "tier_distribution": distribution})

    # -------------------------------------------------------------------------
    # API: map-data (NC counties GeoJSON with risk stats)
    # -------------------------------------------------------------------------

    @app.route("/api/map-data")
    def api_map_data():
        nonlocal _county_geojson_cache
        if _county_geojson_cache is not None:
            return flask.Response(
                json.dumps(_county_geojson_cache),
                mimetype="application/json",
            )
        try:
            df = get_scored_df()
        except FileNotFoundError as e:
            return flask.jsonify({"error": str(e)}), 503
        try:
            import pygris
            gdf = pygris.counties(state="NC", cb=True, year=2022, cache=True)
        except Exception as e:
            logging.exception("pygris counties failed")
            return flask.jsonify({"error": f"County boundaries failed: {e}"}), 500
        # Merge with our county stats (NAMELSAD = "Wake County" etc.)
        nc = df[df["state"].str.upper() == "NC"]
        by_county = nc.groupby("county").agg(
            total=("risk_tier", "count"),
            high=("risk_tier", lambda s: (s == "HIGH").sum()),
            moderate=("risk_tier", lambda s: (s == "MODERATE").sum()),
            low=("risk_tier", lambda s: (s == "LOW").sum()),
        ).reset_index()
        by_county["high_pct"] = (by_county["high"] / by_county["total"] * 100).round(1)
        merged = gdf.merge(by_county, left_on="NAMELSAD", right_on="county", how="left")
        merged["total"] = merged["total"].fillna(0).astype(int)
        merged["high_pct"] = merged["high_pct"].fillna(0)
        geojson = json.loads(merged.to_json())
        _county_geojson_cache = geojson
        return flask.jsonify(geojson)

    # -------------------------------------------------------------------------
    # API: map-points (sampled for performance)
    # -------------------------------------------------------------------------

    @app.route("/api/map-points")
    def api_map_points():
        nonlocal _map_points_cache
        if _map_points_cache is not None:
            return flask.jsonify(_map_points_cache)
        try:
            df = get_scored_df()
        except FileNotFoundError as e:
            return flask.jsonify({"error": str(e)}), 503
        nc = df[df["state"].str.upper() == "NC"]
        # Sample up to 8k per tier for responsive map (24k total)
        max_per_tier = 8000
        out = []
        for tier in ["HIGH", "MODERATE", "LOW"]:
            sub = nc[nc["risk_tier"] == tier]
            if len(sub) > max_per_tier:
                sub = sub.sample(n=max_per_tier, random_state=42)
            for _, row in sub.iterrows():
                out.append({
                    "lat": float(row["latitude"]),
                    "lon": float(row["longitude"]),
                    "tier": tier,
                })
        _map_points_cache = out
        return flask.jsonify(out)

    # -------------------------------------------------------------------------
    # API: analyze (single location → LLM narrative)
    # -------------------------------------------------------------------------

    # NC bounding box (data coverage) — same as download scripts
    NC_LAT_MIN, NC_LAT_MAX = 33.75, 36.65
    NC_LON_MIN, NC_LON_MAX = -84.50, -75.20

    @app.route("/api/analyze", methods=["POST"])
    def api_analyze():
        data = flask.request.get_json() or {}
        address = data.get("address", "").strip()
        lat = data.get("lat")
        lon = data.get("lon")
        out_of_coverage = False
        resolved_lat, resolved_lon = None, None
        if address:
            query = f"What is the LEO satellite connectivity risk at {address}?"
            try:
                from src.utils.geocoding_utils import resolve_location
                coords = resolve_location(address=address)
                if coords:
                    resolved_lat, resolved_lon = coords
            except Exception:
                pass
        elif lat is not None and lon is not None:
            try:
                la, lo = float(lat), float(lon)
                resolved_lat, resolved_lon = la, lo
                out_of_coverage = not (
                    NC_LAT_MIN <= la <= NC_LAT_MAX and NC_LON_MIN <= lo <= NC_LON_MAX
                )
            except (TypeError, ValueError):
                pass
            query = f"What is the risk at {lat}, {lon}?"
        else:
            return flask.jsonify({"error": "Provide 'address' or 'lat' and 'lon'."}), 400
        try:
            from src.agent import PipelineAgent
            agent = PipelineAgent(model=config.CLAUDE_MODEL)
            narrative = agent.run_interactive(query)
            return flask.jsonify({
                "markdown": narrative,
                "error": None,
                "out_of_coverage": out_of_coverage,
                "lat": resolved_lat,
                "lon": resolved_lon,
            })
        except Exception as e:
            logging.exception("analyze failed")
            return flask.jsonify({
                "markdown": None, "error": str(e), "out_of_coverage": out_of_coverage,
                "lat": resolved_lat, "lon": resolved_lon,
            }), 500

    # -------------------------------------------------------------------------
    # API: county briefing
    # -------------------------------------------------------------------------

    def _admin_charts(df):
        """Build interactive Plotly chart HTML for admin dashboard. Returns (chart_bar_html, chart_pie_html, chart_map_html)."""
        try:
            import plotly.express as px
            import plotly.graph_objects as go
        except ImportError:
            return None, None, None
        tier_order = ["HIGH", "MODERATE", "LOW", "UNKNOWN"]
        counts = df["risk_tier"].value_counts().reindex(tier_order, fill_value=0)
        total = counts.sum()
        if total == 0:
            return None, None, None
        colors = {"HIGH": "#f85149", "MODERATE": "#d29922", "LOW": "#3fb950", "UNKNOWN": "#8b949e"}
        # Bar chart
        fig_bar = go.Figure(data=[go.Bar(x=tier_order, y=counts.values, marker_color=[colors[t] for t in tier_order], text=counts.values, texttemplate="%{text:,}", textposition="outside")])
        fig_bar.update_layout(title="Risk tier distribution (count)", xaxis_title="Tier", yaxis_title="Locations", template="plotly_dark", margin=dict(t=40, b=60), height=320)
        chart_bar_html = fig_bar.to_html(full_html=False, include_plotlyjs="cdn", config={"displayModeBar": True, "responsive": True})
        # Pie chart
        fig_pie = go.Figure(data=[go.Pie(labels=tier_order, values=counts.values, marker_colors=[colors[t] for t in tier_order], hole=0.35, textinfo="label+percent", textposition="outside")])
        fig_pie.update_layout(title="Risk tier distribution (%)", template="plotly_dark", margin=dict(t=40), height=320, showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.2))
        chart_pie_html = fig_pie.to_html(full_html=False, include_plotlyjs=False, config={"displayModeBar": True, "responsive": True})
        # Scatter map (sample for performance)
        sample = df.sample(n=min(15000, len(df)), random_state=42) if len(df) > 15000 else df
        if "latitude" in sample.columns and "longitude" in sample.columns and "risk_tier" in sample.columns:
            fig_map = px.scatter(sample, x="longitude", y="latitude", color="risk_tier", color_discrete_map=colors, opacity=0.5)
            fig_map.update_traces(marker=dict(size=2))
            fig_map.update_layout(title="Locations by tier (sample, zoom/pan)", template="plotly_dark", margin=dict(t=40), height=400, xaxis_title="Longitude", yaxis_title="Latitude", legend=dict(orientation="h", yanchor="bottom", y=-0.15))
            chart_map_html = fig_map.to_html(full_html=False, include_plotlyjs=False, config={"displayModeBar": True, "scrollZoom": True, "responsive": True})
        else:
            chart_map_html = None
        return chart_bar_html, chart_pie_html, chart_map_html

    def _admin_monitoring_and_cost():
        """Load agent_monitoring_report.json and build cost chart HTML. Returns (monitoring_dict, chart_html)."""
        monitoring_path = config.MONITORING_REPORT_PATH
        if not monitoring_path.exists():
            return None, None
        try:
            data = json.loads(monitoring_path.read_text(encoding="utf-8"))
        except Exception:
            return None, None
        chart_html = None
        in_tok = data.get("input_tokens") or 0
        out_tok = data.get("output_tokens") or 0
        if in_tok or out_tok:
            try:
                import plotly.graph_objects as go
                fig = go.Figure(data=[
                    go.Bar(name="Input tokens", x=["Tokens"], y=[in_tok], marker_color="#58a6ff"),
                    go.Bar(name="Output tokens", x=["Tokens"], y=[out_tok], marker_color="#3fb950"),
                ])
                fig.update_layout(
                    title="Latest run: token usage",
                    barmode="stack",
                    template="plotly_dark",
                    margin=dict(t=40, b=30),
                    height=220,
                    showlegend=True,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )
                fig.update_yaxes(title_text="Count")
                chart_html = fig.to_html(full_html=False, include_plotlyjs="cdn", config={"displayModeBar": True, "responsive": True})
            except ImportError:
                pass
        return data, chart_html

    @app.route("/admin")
    def admin_page():
        """Data team: summary stats, interactive Plotly charts, and API usage/cost."""
        chart_bar_html = chart_pie_html = chart_map_html = None
        try:
            df = get_scored_df()
            total = len(df)
            tier_counts = df["risk_tier"].value_counts()
            tiers = ["HIGH", "MODERATE", "LOW", "UNKNOWN"]
            distribution = {
                t: {"count": int(tier_counts.get(t, 0)), "pct": round(tier_counts.get(t, 0) / total * 100, 1)}
                for t in tiers
            }
            top_counties = []
            if "county" in df.columns and "state" in df.columns:
                high_df = df[df["risk_tier"] == "HIGH"]
                if len(high_df) > 0:
                    top = high_df.groupby(["state", "county"]).size().reset_index(name="count").sort_values("count", ascending=False).head(15)
                    top_counties = [{"state": r["state"], "county": r["county"], "count": int(r["count"])} for _, r in top.iterrows()]
            chart_bar_html, chart_pie_html, chart_map_html = _admin_charts(df)
        except FileNotFoundError:
            distribution = {}
            total = 0
            top_counties = []
        monitoring_data, chart_cost_html = _admin_monitoring_and_cost()
        return flask.render_template(
            "admin.html",
            total=total,
            distribution=distribution,
            top_counties=top_counties,
            chart_bar_html=chart_bar_html,
            chart_pie_html=chart_pie_html,
            chart_map_html=chart_map_html,
            monitoring_data=monitoring_data,
            chart_cost_html=chart_cost_html,
        )

    @app.route("/output/<path:filename>")
    def serve_output(filename):
        """Serve generated files (e.g. risk_distribution.png) from data/output/."""
        path = (output_dir / filename).resolve()
        out_resolved = output_dir.resolve()
        if not path.exists() or out_resolved not in path.parents:
            return flask.jsonify({"error": "Not found"}), 404
        return flask.send_file(path, mimetype="image/png" if filename.endswith(".png") else "application/octet-stream")

    @app.route("/api/chat", methods=["POST"])
    def api_chat():
        """Interactive chatbot: run agent on natural-language query; return markdown and optional lat/lon for map zoom."""
        data = flask.request.get_json() or {}
        message = (data.get("message") or data.get("query") or "").strip()
        if not message:
            return flask.jsonify({"error": "Provide 'message' or 'query'."}), 400
        try:
            from src.agent import PipelineAgent
            agent = PipelineAgent(model=config.CLAUDE_MODEL)
            narrative = agent.run_interactive(message)
            # Try to parse lat/lon from message for map zoom (e.g. "35.78, -78.64" or "risk at 35.78 -78.64")
            import re
            numbers = re.findall(r"-?\d+\.?\d*", message)
            lat, lon = None, None
            if len(numbers) >= 2:
                try:
                    lat, lon = float(numbers[0]), float(numbers[1])
                    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                        lat, lon = None, None
                except ValueError:
                    pass
            return flask.jsonify({"markdown": narrative, "error": None, "lat": lat, "lon": lon})
        except Exception as e:
            logging.exception("chat failed")
            return flask.jsonify({"markdown": None, "error": str(e), "lat": None, "lon": None}), 500

    @app.route("/api/county/<path:name>")
    def api_county(name):
        # name may be "Buncombe County" or "Buncombe"
        county_name = name.strip()
        if county_name and not county_name.lower().endswith(" county"):
            county_name = county_name + " County"
        query = f"Give me a risk briefing for {county_name}, NC"
        try:
            from src.agent import PipelineAgent
            agent = PipelineAgent(model=config.CLAUDE_MODEL)
            narrative = agent.run_interactive(query)
            return flask.jsonify({"markdown": narrative, "error": None})
        except Exception as e:
            logging.exception("county briefing failed")
            return flask.jsonify({"markdown": None, "error": str(e)}), 500

    return app
