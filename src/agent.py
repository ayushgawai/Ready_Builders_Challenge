"""
Claude-powered agent orchestrator for the LEO Satellite Coverage Risk pipeline.

Design
------
The ``PipelineAgent`` class wraps the Anthropic ``messages.create`` API in an
agentic loop.  Claude reasons about the pipeline state, decides which tool to
call next, and interprets each tool's JSON response before making the next
decision.

Key architectural choices:

1. **Stateless data layer** — tools communicate via file paths, not in-memory
   DataFrames.  Claude never holds the raw data; it holds summaries and paths.
   This is the standard pattern for LLM-orchestrated data pipelines: the model
   reasons, the code acts.

2. **Separation of reasoning and execution** — Claude decides *what* to call;
   Python decides *how* to execute it.  The agent cannot invent tool results or
   skip validation checks; all real logic is in the underlying Python functions.

3. **Fail-loudly dispatcher** — ``handle_tool_call`` wraps every Python call in
   try/except and returns a structured JSON error if something goes wrong.  The
   agent then receives the error, reasons about it, and decides whether to retry
   or escalate.  We never silently swallow exceptions.

4. **Built-in monitoring** — every tool call records wall-clock execution time,
   success/failure, and result size.  Total token usage is tracked from the API
   responses.  This feeds the Phase 9 bonus monitoring report.

5. **Idempotency** — ``sample_environment`` checks whether enriched output
   already exists and is newer than the input before re-running raster sampling.
   This prevents the 20-60 minute sampling step from re-running on unchanged data.

Two run modes
-------------
``run_pipeline(csv_path)``
    Batch mode: processes the full locations CSV end-to-end.

``run_interactive(query)``
    On-demand mode: user asks about one or more specific locations.  The agent
    uses the same tools but works with a tiny ad-hoc DataFrame rather than the
    full 4.67M-row dataset.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import anthropic
import pandas as pd

import numpy as np

from src import config
from src.tools import ALL_TOOLS, ON_DEMAND_TOOLS

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are a geospatial data analysis agent for Ready.net's LEO satellite \
coverage risk assessment pipeline.

Your role: orchestrate a pipeline that analyses ~4.67 million US broadband locations \
and identifies which are at risk of Starlink satellite connectivity problems due to \
tree canopy cover, terrain slope, and land cover type.

You have two sets of tools:

BATCH PIPELINE — run in this exact order when asked to process the full dataset:
  1. ingest_locations     → load and validate the CSV, derive state/county from Census GEOID
  2. sample_environment   → download rasters (if needed) and sample values at each location
  3. score_risk           → apply weighted risk methodology, assign HIGH/MODERATE/LOW tiers
  4. validate_results     → anomaly detection (UNKNOWN rate, tier dominance, impossible values)
  5. generate_report      → produce summary statistics and findings report

ON-DEMAND TOOLS — use these when the user asks about a specific location or area:
  • analyze_location      → comprehensive single-point risk assessment
                            accepts address ("123 Main St, Seattle WA"), coordinate
                            string ("47.6, -122.3"), or lat/lon numbers.
                            Returns factor breakdown, nearby alternatives, seasonal context.
  • assess_area           → state or county level risk briefing
                            use when user names a state (e.g. "WA") or county
                            (e.g. "Whatcom County") instead of a single location.

ROUTING RULES:
- If the user asks about a SPECIFIC LOCATION (address, coordinates, place name):
    → call analyze_location immediately. Do NOT run the batch pipeline.
- If the user asks about a STATE or COUNTY:
    → call assess_area. Do NOT run the batch pipeline.
- If the user asks to run the pipeline / process all locations / generate the full report:
    → run all five batch pipeline tools in order.

BATCH PIPELINE DECISION RULES:
- After ingest_locations: if >20% of records were dropped, note the reason in your \
response (it is a data quality finding, not a pipeline failure). Continue.
- After sample_environment: if cache_hit=true, explicitly note that cached data was used.
- After validate_results:
    • If is_valid=False (CRITICAL anomaly): analyse the specific check that failed. \
Explain the likely root cause (e.g. raster download failure, CRS mismatch, threshold bug). \
Still call generate_report to produce whatever output is possible.
    • If is_valid=True with warnings only: note the warnings and proceed to generate_report.
- Never skip steps. Never call a later step before an earlier one completes successfully.
- If a tool returns an error key, report the error, explain what information is available \
to diagnose it, and do NOT call further tools that depend on the failed step.

ON-DEMAND RESPONSE FORMAT (after analyze_location):
Provide a conversational, technician-friendly response with:
  • Address or coordinates confirmed (with geocoded address if applicable)
  • Risk tier and what it means for installation
  • Primary risk driver (canopy / slope / land cover) with the actual value
  • Plain-English recommendation (what the installer should do)
  • Top 3-5 nearby lower-risk alternatives if found (with distance and tier)
  • Seasonal note if the location is in deciduous forest

BATCH OUTPUT FORMAT (after generate_report):
Provide a clear, structured summary with:
  • Total locations analysed and retention rate
  • Risk tier distribution (HIGH/MODERATE/LOW counts and percentages)
  • Top 3-5 states by HIGH-risk concentration
  • Top risk driver (which factor — canopy, slope, land cover — contributed most)
  • Data quality notes (dropped records, NaN rates if significant)
  • Paths to output files (report, chart, map)

Be concise. Use numbers. This summary will be read by a state broadband programme manager \
who needs to understand which locations need priority field assessment."""


# ---------------------------------------------------------------------------
# PipelineAgent
# ---------------------------------------------------------------------------

class PipelineAgent:
    """Orchestrates the LEO satellite risk pipeline using Claude tool_use.

    Parameters
    ----------
    api_key:
        Anthropic API key.  Defaults to ``config.ANTHROPIC_API_KEY``.
    model:
        Claude model identifier.  Defaults to ``config.CLAUDE_MODEL``.
    max_turns:
        Safety ceiling on the agentic loop.  Defaults to ``config.MAX_AGENT_TURNS``.
    """

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
        max_turns: int | None = None,
    ) -> None:
        self._api_key = api_key or config.ANTHROPIC_API_KEY
        if not self._api_key:
            raise ValueError(
                "ANTHROPIC_API_KEY is not set. "
                "Add it to your .env file or pass api_key= explicitly."
            )
        self._client = anthropic.Anthropic(api_key=self._api_key)
        self._model = model or config.CLAUDE_MODEL
        self._max_turns = max_turns or config.MAX_AGENT_TURNS
        self._monitoring: dict[str, Any] = {
            "model": self._model,
            "turns": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "tools": {},
        }
        # Lazy-loaded cache of the scored DataFrame for on-demand queries.
        # Populated on first call to analyze_location or assess_area to avoid
        # re-reading the 4.67M-row CSV for every interactive query.
        self._scored_df: pd.DataFrame | None = None

    # ------------------------------------------------------------------
    # Public: batch pipeline
    # ------------------------------------------------------------------

    def run_pipeline(self, csv_path: str | Path | None = None) -> str:
        """Run the full end-to-end batch pipeline.

        Parameters
        ----------
        csv_path:
            Path to the locations CSV.  Defaults to ``config.LOCATIONS_CSV``.

        Returns
        -------
        str
            The agent's final narrative response — a structured summary of
            findings, tier distribution, key metrics, and output file paths.
        """
        csv_path = str(csv_path or config.LOCATIONS_CSV)
        logger.info("Starting batch pipeline for: %s", csv_path)

        initial_message = (
            f"Run the full LEO satellite coverage risk pipeline on this locations file:\n"
            f"  {csv_path}\n\n"
            f"Call all five tools in order. After completing all steps, provide a clear "
            f"summary of findings including risk tier distribution and top high-risk areas."
        )

        return self._run_loop(initial_message, csv_path=csv_path)

    # ------------------------------------------------------------------
    # Public: interactive / on-demand
    # ------------------------------------------------------------------

    def run_interactive(self, query: str) -> str:
        """Analyse a specific location or answer a geospatial question.

        Parameters
        ----------
        query:
            Natural language query, e.g.:
              "What is the risk at 1600 Amphitheatre Pkwy, Mountain View, CA?"
              "Find low-risk alternatives near 47.6062, -122.3321 within 500m."
              "Give me a risk briefing for Whatcom County, WA."

        Returns
        -------
        str
            The agent's conversational response, driven by analyze_location or
            assess_area depending on what the user asked.
        """
        logger.info("Interactive query: %s", query)

        message = (
            f"Answer this query about LEO satellite connectivity risk:\n{query}\n\n"
            f"Use analyze_location for a specific location (address, coordinates, place name). "
            f"Use assess_area for a state or county briefing. "
            f"For a custom area (bounding box or polygon), use assess_polygon with min_lat, max_lat, "
            f"min_lon, max_lon or with coordinates (list of [lat, lon] points). "
            f"For questions like 'which county has the most LOW (or HIGH or MODERATE) risk "
            f"locations?', use query_top_counties with state and tier. "
            f"You have only on-demand tools; do not attempt to run the batch pipeline."
        )

        return self._run_loop(message, tools_override=ON_DEMAND_TOOLS)

    # ------------------------------------------------------------------
    # Agentic loop
    # ------------------------------------------------------------------

    def _run_loop(
        self,
        initial_message: str,
        tools_override: list[dict[str, Any]] | None = None,
        **_context: Any,
    ) -> str:
        """Core agentic loop: send → receive → tool_use → repeat until end_turn.

        When tools_override is set (e.g. ON_DEMAND_TOOLS), only those tools are
        available — used by run_interactive so chat never triggers batch pipeline.
        Returns the agent's final text response.
        """
        messages: list[dict[str, Any]] = [
            {"role": "user", "content": initial_message}
        ]
        tools = tools_override if tools_override is not None else ALL_TOOLS
        self._monitoring["mode"] = "interactive" if tools_override is not None else "batch"
        self._monitoring["tools_restricted"] = tools_override is not None

        final_response = ""
        loop_start = time.perf_counter()

        for turn in range(self._max_turns):
            self._monitoring["turns"] += 1
            logger.debug("Agent turn %d / %d", turn + 1, self._max_turns)

            response = self._client.messages.create(
                model=self._model,
                max_tokens=4096,
                temperature=config.CLAUDE_TEMPERATURE,
                system=_SYSTEM_PROMPT,
                tools=tools,
                messages=messages,
            )

            # Track token usage
            if hasattr(response, "usage") and response.usage:
                self._monitoring["input_tokens"] += getattr(response.usage, "input_tokens", 0)
                self._monitoring["output_tokens"] += getattr(response.usage, "output_tokens", 0)

            # Collect text blocks (may appear alongside tool_use blocks)
            text_parts = [
                block.text
                for block in response.content
                if block.type == "text"
            ]
            if text_parts:
                final_response = "\n".join(text_parts)

            # End of conversation
            if response.stop_reason == "end_turn":
                logger.info(
                    "Pipeline complete after %d turns. "
                    "Tokens: in=%d out=%d. Wall time: %.1fs.",
                    turn + 1,
                    self._monitoring["input_tokens"],
                    self._monitoring["output_tokens"],
                    time.perf_counter() - loop_start,
                )
                break

            # Process tool_use blocks
            if response.stop_reason != "tool_use":
                logger.warning(
                    "Unexpected stop_reason '%s' at turn %d.",
                    response.stop_reason, turn + 1,
                )
                break

            # Append the assistant's full message (including tool_use blocks)
            messages.append({"role": "assistant", "content": response.content})

            # Execute each tool call and collect results
            tool_results: list[dict[str, Any]] = []
            for block in response.content:
                if block.type != "tool_use":
                    continue

                tool_name: str = block.name
                tool_input: dict[str, Any] = block.input or {}
                tool_use_id: str = block.id

                logger.info("Tool call: %s  input=%s", tool_name, json.dumps(tool_input))

                result_json = self.handle_tool_call(tool_name, tool_input)
                result_dict = json.loads(result_json) if isinstance(result_json, str) else result_json

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": json.dumps(result_dict),
                })

                logger.info(
                    "Tool result: %s  success=%s",
                    tool_name,
                    str(result_dict.get("error") is None),
                )

            messages.append({"role": "user", "content": tool_results})

        else:
            logger.warning("Agent hit max_turns limit (%d).", self._max_turns)
            final_response = (
                final_response
                or "Pipeline reached the maximum number of turns without completing. "
                   "Check the logs for the last tool call result."
            )

        self._save_monitoring_report()
        return final_response

    # ------------------------------------------------------------------
    # Tool dispatcher
    # ------------------------------------------------------------------

    def handle_tool_call(
        self, tool_name: str, tool_input: dict[str, Any]
    ) -> str:
        """Execute a named tool and return its result as a JSON string.

        All exceptions are caught and returned as structured error JSON so
        that Claude can reason about the failure rather than crashing the loop.

        Parameters
        ----------
        tool_name:
            One of the five pipeline tool names.
        tool_input:
            Dict of parameters from Claude's tool_use block.

        Returns
        -------
        str
            JSON-encoded result dict.  Always has either a data payload or
            an ``error`` key with a descriptive message.
        """
        dispatch = {
            "ingest_locations":     self._tool_ingest_locations,
            "sample_environment":   self._tool_sample_environment,
            "score_risk":           self._tool_score_risk,
            "validate_results":    self._tool_validate_results,
            "generate_report":     self._tool_generate_report,
            "analyze_location":    self._tool_analyze_location,
            "assess_area":         self._tool_assess_area,
            "assess_polygon":      self._tool_assess_polygon,
            "query_top_counties":  self._tool_query_top_counties,
        }

        handler = dispatch.get(tool_name)
        if handler is None:
            return json.dumps({"error": f"Unknown tool '{tool_name}'."})

        t_start = time.perf_counter()
        try:
            result = handler(tool_input)
        except Exception as exc:  # pylint: disable=broad-except
            elapsed = round((time.perf_counter() - t_start) * 1000)
            logger.exception("Tool '%s' raised an exception.", tool_name)
            result = {
                "error": f"{type(exc).__name__}: {exc}",
                "tool": tool_name,
            }
            self._record_tool_metric(tool_name, elapsed, success=False)
            return json.dumps(result)

        elapsed = round((time.perf_counter() - t_start) * 1000)
        self._record_tool_metric(tool_name, elapsed, success="error" not in result)
        return json.dumps(result)

    # ------------------------------------------------------------------
    # Individual tool implementations
    # ------------------------------------------------------------------

    def _tool_ingest_locations(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """Load, validate, and save the locations CSV."""
        from src.ingest import load_locations, validate_locations, generate_quality_report

        file_path = inputs.get("file_path") or str(config.LOCATIONS_CSV)
        logger.info("ingest_locations: loading %s", file_path)

        df = load_locations(file_path)
        clean_df, quality_report = validate_locations(df)
        quality_text = generate_quality_report(quality_report)

        # Persist validated data for downstream tools
        config.VALIDATED_LOCATIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
        clean_df.to_csv(config.VALIDATED_LOCATIONS_PATH, index=False)
        logger.info(
            "ingest_locations: saved %d validated rows to %s",
            len(clean_df), config.VALIDATED_LOCATIONS_PATH,
        )

        return {
            "status": "success",
            "input_file": file_path,
            "total_records": quality_report["total_records"],
            "valid_records": quality_report["valid_records"],
            "dropped_records": quality_report["dropped_records"],
            "retention_rate_pct": quality_report["retention_rate_pct"],
            "drop_reasons": quality_report["drop_reasons"],
            "quality_report": quality_text,
            "output_path": str(config.VALIDATED_LOCATIONS_PATH),
        }

    def _tool_sample_environment(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """Download rasters (if needed) and enrich locations with environmental values."""
        from src.environment import (
            download_raster, enrich_locations,
        )
        from src.utils.pipeline_utils import is_scored_cache_valid, load_scored_cache, save_scored_cache

        force = bool(inputs.get("force_resample", False))

        # Idempotency: skip if enriched output is already up-to-date
        if (
            not force
            and config.ENRICHED_LOCATIONS_PATH.exists()
            and config.VALIDATED_LOCATIONS_PATH.exists()
            and is_scored_cache_valid(
                config.VALIDATED_LOCATIONS_PATH, config.ENRICHED_LOCATIONS_PATH
            )
        ):
            cached_df = load_scored_cache(config.ENRICHED_LOCATIONS_PATH)
            nan_rates = {
                col: round(float(cached_df[col].isna().mean() * 100), 2)
                for col in ["canopy_pct", "elevation_m", "slope_deg", "land_cover_code"]
                if col in cached_df.columns
            }
            logger.info("sample_environment: using cached enriched data.")
            return {
                "status": "success",
                "cache_hit": True,
                "total_enriched": len(cached_df),
                "nan_rates_pct": nan_rates,
                "output_path": str(config.ENRICHED_LOCATIONS_PATH),
                "note": "Cached enriched file is newer than validated input — skipped re-sampling.",
            }

        # Load validated locations
        if not config.VALIDATED_LOCATIONS_PATH.exists():
            return {
                "error": (
                    "Validated locations file not found. "
                    "Run ingest_locations first."
                )
            }

        df = pd.read_csv(config.VALIDATED_LOCATIONS_PATH, dtype={"geoid_cb": str, "county_fips": str})
        logger.info("sample_environment: loaded %d validated rows.", len(df))

        # Download rasters (skip if already present)
        logger.info("sample_environment: downloading rasters if needed...")
        canopy_path = download_raster(config.CANOPY_RASTER_URL, config.CANOPY_RASTER_PATH)

        # NLCD Land Cover has no stable direct download URL — must be placed manually.
        # See docs/data_download_guide.md Step 2 for MRLC Viewer instructions.
        if not config.LANDCOVER_RASTER_PATH.exists():
            return {
                "error": (
                    f"NLCD Land Cover raster not found at {config.LANDCOVER_RASTER_PATH}. "
                    "Download via the MRLC Web Viewer: https://www.mrlc.gov/viewer/ "
                    "Draw a rectangle around NC, select 'NLCD 2021 Land Cover L48', "
                    "enter your email, and download the clipped file. "
                    "See docs/data_download_guide.md Step 2 for full instructions. "
                    "Save as: data/raw/nlcd_landcover_conus_2021.tif"
                )
            }
        landcover_path = config.LANDCOVER_RASTER_PATH

        if not config.DEM_RASTER_PATH.exists():
            return {
                "error": (
                    f"DEM raster not found at {config.DEM_RASTER_PATH}. "
                    "Run: python scripts/download_nc_dem.py "
                    "This script automatically downloads and merges 5 NC DEM tiles "
                    "from the USGS 3DEP REST API (~250 MB, ~2-5 min). "
                    "See docs/data_download_guide.md Step 3 for details."
                )
            }

        # Enrich
        enriched_df = enrich_locations(
            df,
            canopy_path=canopy_path,
            dem_path=config.DEM_RASTER_PATH,
            landcover_path=landcover_path,
            slope_path=config.SLOPE_RASTER_PATH,
            batch_size=config.BATCH_SIZE,
        )

        save_scored_cache(enriched_df, config.ENRICHED_LOCATIONS_PATH)

        nan_rates = {
            col: round(float(enriched_df[col].isna().mean() * 100), 2)
            for col in ["canopy_pct", "elevation_m", "slope_deg", "land_cover_code"]
            if col in enriched_df.columns
        }

        return {
            "status": "success",
            "cache_hit": False,
            "total_enriched": len(enriched_df),
            "nan_rates_pct": nan_rates,
            "rasters_used": {
                "canopy": str(canopy_path),
                "dem": str(config.DEM_RASTER_PATH),
                "landcover": str(landcover_path),
                "slope": str(config.SLOPE_RASTER_PATH),
            },
            "output_path": str(config.ENRICHED_LOCATIONS_PATH),
        }

    def _tool_score_risk(self, _inputs: dict[str, Any]) -> dict[str, Any]:
        """Apply risk scoring methodology to enriched locations."""
        from src.risk_scoring import compute_composite_risk
        from src.utils.pipeline_utils import save_scored_cache

        if not config.ENRICHED_LOCATIONS_PATH.exists():
            return {
                "error": (
                    "Enriched locations file not found. "
                    "Run sample_environment first."
                )
            }

        df = pd.read_csv(
            config.ENRICHED_LOCATIONS_PATH,
            dtype={"geoid_cb": str, "county_fips": str},
        )
        logger.info("score_risk: scoring %d rows...", len(df))

        scored_df = compute_composite_risk(df)
        save_scored_cache(scored_df, config.SCORED_LOCATIONS_PATH)

        tier_counts = scored_df["risk_tier"].value_counts().to_dict()
        total = len(scored_df)
        tier_distribution = {
            tier: {
                "count": int(tier_counts.get(tier, 0)),
                "pct": round(tier_counts.get(tier, 0) / total * 100, 2),
            }
            for tier in [
                config.RISK_TIER_HIGH, config.RISK_TIER_MODERATE,
                config.RISK_TIER_LOW, config.RISK_TIER_UNKNOWN,
            ]
        }

        return {
            "status": "success",
            "total_scored": total,
            "tier_distribution": tier_distribution,
            "output_path": str(config.SCORED_LOCATIONS_PATH),
        }

    def _tool_validate_results(self, _inputs: dict[str, Any]) -> dict[str, Any]:
        """Run anomaly detection on scored results."""
        from src.validation import validate_results, generate_anomaly_report

        if not config.SCORED_LOCATIONS_PATH.exists():
            return {
                "error": (
                    "Scored locations file not found. "
                    "Run score_risk first."
                )
            }

        df = pd.read_csv(
            config.SCORED_LOCATIONS_PATH,
            dtype={"geoid_cb": str, "county_fips": str},
        )

        is_valid, validation_report = validate_results(df)
        anomaly_text = generate_anomaly_report(df, validation_report)

        # Determine overall severity
        severity = "OK"
        if validation_report.get("critical_checks"):
            severity = "CRITICAL"
        elif validation_report.get("warning_checks"):
            severity = "WARNING"

        return {
            "status": "success",
            "is_valid": is_valid,
            "severity": severity,
            "checks_passed": validation_report.get("passed_checks", []),
            "checks_failed": validation_report.get("critical_checks", []),
            "checks_warned": validation_report.get("warning_checks", []),
            "unknown_rate_pct": validation_report.get("unknown_rate_pct", 0.0),
            "anomaly_count": validation_report.get("anomaly_count", 0),
            "anomaly_report_path": str(config.ANOMALY_REPORT_PATH),
            "anomaly_report_preview": anomaly_text[:1500],
        }

    def _tool_generate_report(self, _inputs: dict[str, Any]) -> dict[str, Any]:
        """Generate summary statistics and findings report."""
        from src.reporting import (
            generate_summary_stats, write_findings_report,
            create_risk_distribution_chart, create_static_risk_map,
        )

        if not config.SCORED_LOCATIONS_PATH.exists():
            return {
                "error": (
                    "Scored locations file not found. "
                    "Run score_risk first."
                )
            }

        df = pd.read_csv(
            config.SCORED_LOCATIONS_PATH,
            dtype={"geoid_cb": str, "county_fips": str},
        )

        stats = generate_summary_stats(df)
        report_path = write_findings_report(stats)

        # Visualisations (stubs in Phase 6, full implementation in Phase 7)
        chart_path = create_risk_distribution_chart(df, config.RISK_DISTRIBUTION_CHART_PATH)
        map_path = create_static_risk_map(df, config.STATIC_MAP_PATH)

        output_files = {
            "findings_report": str(report_path),
            "scored_csv": str(config.SCORED_LOCATIONS_PATH),
        }
        if chart_path:
            output_files["risk_chart"] = str(chart_path)
        if map_path:
            output_files["risk_map"] = str(map_path)

        return {
            "status": "success",
            "summary": {
                "total_locations": stats["total_locations"],
                "tier_distribution": stats["tier_distribution"],
                "unknown_count": stats["unknown_count"],
                "unknown_pct": stats["unknown_pct"],
                "avg_canopy_by_tier": stats["avg_canopy_by_tier"],
                "avg_slope_by_tier": stats["avg_slope_by_tier"],
                "top_states": stats["state_breakdown"][:5],
                "top_counties_high_risk": stats["top_counties_high_risk"][:10],
            },
            "output_files": output_files,
        }

    # ------------------------------------------------------------------
    # On-demand tools: scored DataFrame cache helpers
    # ------------------------------------------------------------------

    def _load_scored_df(self) -> pd.DataFrame | None:
        """Load and cache the scored DataFrame.

        The 4.67M-row CSV is loaded once per PipelineAgent session and
        stored in self._scored_df.  Subsequent calls return the cached copy
        instantly (no re-read).  If the scored CSV does not exist, returns None.
        """
        if self._scored_df is not None:
            return self._scored_df

        if not config.SCORED_LOCATIONS_PATH.exists():
            logger.warning(
                "Scored locations CSV not found at %s. "
                "Run the batch pipeline first.",
                config.SCORED_LOCATIONS_PATH,
            )
            return None

        logger.info(
            "Loading scored locations into memory (once per session): %s",
            config.SCORED_LOCATIONS_PATH,
        )
        self._scored_df = pd.read_csv(
            config.SCORED_LOCATIONS_PATH,
            usecols=[
                "location_id", "latitude", "longitude", "risk_tier",
                "composite_score", "canopy_pct", "slope_deg", "land_cover_code",
                "canopy_risk", "slope_risk", "landcover_risk",
                "state", "county",
            ],
            dtype={"county_fips": str},
        )
        logger.info("Scored data loaded: %d rows.", len(self._scored_df))
        return self._scored_df

    # ------------------------------------------------------------------
    # On-demand tool: analyze_location
    # ------------------------------------------------------------------

    def _tool_analyze_location(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """Comprehensive single-point risk assessment (Tools A + B + D)."""
        from src.utils.geocoding_utils import resolve_location, find_nearest_scored, find_alternatives

        # ── Step 1: resolve coordinates ──────────────────────────────
        address = inputs.get("address")
        lat_raw = inputs.get("latitude")
        lon_raw = inputs.get("longitude")
        radius_m = float(inputs.get("radius_meters", 500))

        coords = resolve_location(
            address=address,
            latitude=lat_raw,
            longitude=lon_raw,
        )

        if coords is None:
            return {
                "error": (
                    "Could not resolve location. "
                    "Provide a US address, a coordinate string like '47.6, -122.3', "
                    "or explicit latitude and longitude numbers."
                )
            }

        lat, lon = coords
        input_type = (
            "coordinates" if (lat_raw is not None and lon_raw is not None) else "address"
        )
        logger.info("analyze_location: resolved to (%.6f, %.6f)", lat, lon)

        # ── Step 2: look up in pre-scored CSV ────────────────────────
        scored_df = self._load_scored_df()
        row: dict[str, Any] | None = None

        if scored_df is not None:
            row = find_nearest_scored(lat, lon, scored_df, max_distance_m=150)
            if row:
                row["source"] = "pre_scored_csv"
                logger.info(
                    "analyze_location: found scored location '%s' at %.1fm.",
                    row.get("location_id"), row.get("distance_m", 0),
                )

        # ── Step 3: on-the-fly scoring if not in CSV ─────────────────
        if row is None:
            row = self._score_single_point_otf(lat, lon)
            if "error" in row:
                return row

        # ── Step 4: factor breakdown (Tool A) ────────────────────────
        risk_breakdown = self._compute_factor_breakdown(row)

        # ── Step 5: nearby alternatives (Tool B) ─────────────────────
        alternatives: list[dict] = []
        if scored_df is not None:
            alternatives = find_alternatives(
                lat, lon, scored_df,
                radius_m=radius_m,
                current_tier=row.get("risk_tier", "UNKNOWN"),
                max_results=10,
            )
            logger.info(
                "analyze_location: found %d alternatives within %.0fm.",
                len(alternatives), radius_m,
            )

        # ── Step 6: seasonal context (Tool D) ────────────────────────
        seasonal_note = self._get_seasonal_context(row.get("land_cover_code"))

        return {
            "status": "success",
            "input": {
                "type": input_type,
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "address_queried": address,
            },
            "location": {
                "location_id": row.get("location_id"),
                "state": row.get("state"),
                "county": row.get("county"),
                "distance_from_query_m": row.get("distance_m", 0),
                "data_source": row.get("source", "unknown"),
            },
            "risk_assessment": risk_breakdown,
            "nearby_alternatives": {
                "search_radius_m": radius_m,
                "count_found": len(alternatives),
                "locations": alternatives[:5],
            },
            "seasonal_note": seasonal_note,
        }

    def _score_single_point_otf(self, lat: float, lon: float) -> dict[str, Any]:
        """Score a single point on-the-fly from rasters (not in scored CSV)."""
        rasters_needed = [
            config.CANOPY_RASTER_PATH,
            config.DEM_RASTER_PATH,
            config.LANDCOVER_RASTER_PATH,
        ]
        missing = [str(p) for p in rasters_needed if not p.exists()]
        if missing:
            return {
                "error": (
                    f"Location not found in scored dataset and rasters are not available "
                    f"for on-the-fly scoring. Missing: {missing}. "
                    "Run the batch pipeline first (ingest_locations → sample_environment "
                    "→ score_risk) to pre-score all locations, then retry."
                )
            }

        try:
            from src.environment import enrich_locations
            from src.risk_scoring import compute_composite_risk

            single_df = pd.DataFrame([{
                "location_id": f"ADHOC_{lat:.5f}_{lon:.5f}",
                "latitude": lat,
                "longitude": lon,
                "geoid_cb": None,
                "state": None,
                "county": None,
            }])

            enriched = enrich_locations(
                single_df,
                canopy_path=config.CANOPY_RASTER_PATH,
                dem_path=config.DEM_RASTER_PATH,
                landcover_path=config.LANDCOVER_RASTER_PATH,
                slope_path=config.SLOPE_RASTER_PATH if config.SLOPE_RASTER_PATH.exists() else None,
                batch_size=1,
            )
            scored = compute_composite_risk(enriched)
            row = scored.iloc[0].to_dict()
            row["source"] = "on_the_fly"
            row["distance_m"] = 0
            logger.info(
                "analyze_location: on-the-fly scored (%.6f, %.6f) → tier=%s",
                lat, lon, row.get("risk_tier"),
            )
            return row
        except Exception as exc:
            logger.exception("On-the-fly scoring failed for (%.6f, %.6f).", lat, lon)
            return {"error": f"On-the-fly scoring failed: {type(exc).__name__}: {exc}"}

    def _compute_factor_breakdown(self, row: dict[str, Any]) -> dict[str, Any]:
        """Compute factor contributions and produce a plain-English recommendation."""
        tier = row.get("risk_tier", "UNKNOWN")
        canopy = row.get("canopy_pct")
        slope = row.get("slope_deg")
        lc_code = row.get("land_cover_code")
        composite = row.get("composite_score")

        # Column names from compute_composite_risk: canopy_risk, slope_risk, landcover_risk
        canopy_score = float(row.get("canopy_risk") or row.get("canopy_score") or 0)
        slope_score = float(row.get("slope_risk") or row.get("slope_score") or 0)
        lc_score = float(row.get("landcover_risk") or row.get("landcover_score") or 0)

        # Weighted contributions (matching config weights)
        w_canopy = float(getattr(config, "WEIGHT_CANOPY", 0.50))
        w_slope = float(getattr(config, "WEIGHT_SLOPE", 0.30))
        w_lc = float(getattr(config, "WEIGHT_LANDCOVER", 0.20))

        canopy_contrib = canopy_score * w_canopy
        slope_contrib = slope_score * w_slope
        lc_contrib = lc_score * w_lc

        total_contrib = canopy_contrib + slope_contrib + lc_contrib or 1.0

        # Primary driver
        contributions = {
            "canopy": canopy_contrib,
            "slope": slope_contrib,
            "land_cover": lc_contrib,
        }
        primary_driver = max(contributions, key=contributions.get)

        # NLCD land cover names (CONUS codes)
        _LC_NAMES = {
            11: "Open Water", 12: "Perennial Snow/Ice", 21: "Developed (Open Space)",
            22: "Developed (Low Intensity)", 23: "Developed (Medium Intensity)",
            24: "Developed (High Intensity)", 31: "Barren Rock/Sand/Clay",
            41: "Deciduous Forest", 42: "Evergreen Forest", 43: "Mixed Forest",
            52: "Shrub/Scrub", 71: "Grassland/Herbaceous", 81: "Pasture/Hay",
            82: "Cultivated Crops", 90: "Woody Wetlands", 95: "Emergent Herbaceous Wetlands",
        }
        try:
            lc_int = int(lc_code) if lc_code is not None and not (isinstance(lc_code, float) and np.isnan(lc_code)) else None
        except (ValueError, TypeError):
            lc_int = None
        lc_name = _LC_NAMES.get(lc_int, f"Code {lc_int}") if lc_int is not None else "Unknown"

        canopy_label = (
            "HIGH" if canopy is not None and float(canopy) > float(getattr(config, "CANOPY_HIGH_THRESHOLD", 50))
            else "MODERATE" if canopy is not None and float(canopy) > float(getattr(config, "CANOPY_MOD_THRESHOLD", 20))
            else "LOW"
        )

        recommendation = self._generate_recommendation(primary_driver, canopy, slope, tier, lc_int)

        return {
            "risk_tier": tier,
            "composite_score": round(float(composite), 3) if composite is not None and not (isinstance(composite, float) and np.isnan(composite)) else None,
            "primary_driver": primary_driver,
            "factors": {
                "canopy": {
                    "value_pct": round(float(canopy), 1) if canopy is not None else None,
                    "normalised_score": round(canopy_score, 3),
                    "contribution_to_composite": round(canopy_contrib, 3),
                    "contribution_pct": round(canopy_contrib / total_contrib * 100, 1),
                    "risk_level": canopy_label,
                    "weight_in_model": f"{int(w_canopy * 100)}%",
                },
                "slope": {
                    "value_deg": round(float(slope), 1) if slope is not None else None,
                    "normalised_score": round(slope_score, 3),
                    "contribution_to_composite": round(slope_contrib, 3),
                    "contribution_pct": round(slope_contrib / total_contrib * 100, 1),
                    "weight_in_model": f"{int(w_slope * 100)}%",
                },
                "land_cover": {
                    "nlcd_code": lc_int,
                    "name": lc_name,
                    "normalised_score": round(lc_score, 3),
                    "contribution_to_composite": round(lc_contrib, 3),
                    "contribution_pct": round(lc_contrib / total_contrib * 100, 1),
                    "weight_in_model": f"{int(w_lc * 100)}%",
                },
            },
            "recommendation": recommendation,
        }

    @staticmethod
    def _generate_recommendation(
        primary_driver: str,
        canopy: Any,
        slope: Any,
        tier: str,
        lc_code: int | None,
    ) -> str:
        canopy_f = float(canopy) if canopy is not None else None
        slope_f = float(slope) if slope is not None else None

        if tier == "LOW":
            return (
                "LOW risk — standard Starlink installation should work well. "
                "A rooftop or clear-sky pole mount is sufficient."
            )

        if tier == "MODERATE":
            if primary_driver == "canopy" and canopy_f is not None:
                return (
                    f"MODERATE canopy obstruction ({canopy_f:.0f}% cover). "
                    "Rooftop mount recommended to clear vegetation. "
                    "Run the Starlink obstruction check before finalising placement."
                )
            if primary_driver == "slope" and slope_f is not None:
                return (
                    f"MODERATE terrain slope ({slope_f:.0f}°). "
                    "Ensure the dish has a clear northern sky view. "
                    "An elevated pole mount on the uphill side of the building may help."
                )
            return (
                "MODERATE risk. Roof mount recommended. "
                "On-site obstruction check advised before installation."
            )

        if tier == "HIGH":
            if primary_driver == "canopy" and canopy_f is not None:
                if canopy_f > 75:
                    return (
                        f"HIGH canopy risk ({canopy_f:.0f}% cover). "
                        "Dense tree cover likely blocks the required 100° FOV. "
                        "Tall pole mount above the canopy line, or an alternative nearby "
                        "site, is strongly recommended. Check alternatives below."
                    )
                return (
                    f"HIGH canopy risk ({canopy_f:.0f}% cover). "
                    "Elevated mount above the tree line is required. "
                    "Consider nearby lower-canopy locations listed below."
                )
            if primary_driver == "slope" and slope_f is not None:
                return (
                    f"HIGH terrain risk ({slope_f:.0f}° slope). "
                    "Steep terrain restricts horizon visibility. "
                    "A hilltop, ridgeline, or structure with open horizon access is "
                    "recommended. On-site assessment required."
                )
            if primary_driver == "land_cover":
                return (
                    "HIGH risk driven by land cover type. "
                    "Dense forest or developed obstruction likely. "
                    "On-site assessment required before installation."
                )
            return (
                "HIGH risk. On-site assessment required. "
                "Alternative mounting location strongly recommended."
            )

        return (
            "Risk assessment incomplete — some environmental data was unavailable. "
            "Run the full batch pipeline to ensure rasters are downloaded and sampled."
        )

    @staticmethod
    def _get_seasonal_context(land_cover_code: Any) -> dict[str, Any] | None:
        """Return seasonal guidance for deciduous/evergreen forest locations."""
        try:
            code = int(land_cover_code) if land_cover_code is not None else None
        except (ValueError, TypeError):
            return None

        if code == 41:  # Deciduous Forest
            return {
                "land_cover": "Deciduous Forest (NLCD code 41)",
                "nlcd_capture_season": "Peak summer (June–August)",
                "winter_adjustment": (
                    "Deciduous trees shed leaves November–March, reducing effective "
                    "canopy obstruction by an estimated 30–60%. "
                    "This location may qualify as MODERATE risk for winter-season "
                    "installations even if scored HIGH in summer."
                ),
                "recommendation": (
                    "If scheduling is flexible, a winter install (Nov–Mar) may "
                    "significantly improve Starlink connectivity at this site. "
                    "Re-run an obstruction check in-app after leaf drop."
                ),
                "winter_install_actionable": True,
            }
        if code == 42:  # Evergreen Forest
            return {
                "land_cover": "Evergreen Forest (NLCD code 42)",
                "seasonal_variation": "Minimal — evergreen canopy is consistent year-round.",
                "winter_install_actionable": False,
            }
        if code == 43:  # Mixed Forest
            return {
                "land_cover": "Mixed Forest (NLCD code 43)",
                "seasonal_variation": (
                    "Partial — mixed deciduous/evergreen. Some canopy reduction in winter "
                    "is expected, but the evergreen component remains. "
                    "Effect is less pronounced than pure deciduous forest."
                ),
                "winter_install_actionable": True,
            }
        return None

    # ------------------------------------------------------------------
    # On-demand tool: assess_area
    # ------------------------------------------------------------------

    def _tool_assess_area(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """State or county level risk briefing."""
        state = (inputs.get("state") or "").strip().upper()
        county = (inputs.get("county") or "").strip()
        top_n = int(inputs.get("top_n") or 10)

        if not state and not county:
            return {
                "error": (
                    "Provide at least 'state' (2-letter abbreviation, e.g. 'WA') "
                    "or 'county' (NAMELSAD name, e.g. 'Whatcom County')."
                )
            }

        scored_df = self._load_scored_df()
        if scored_df is None:
            return {
                "error": (
                    "Scored locations CSV not found. "
                    "Run the batch pipeline first (ingest_locations → sample_environment "
                    "→ score_risk) to generate the scored dataset."
                )
            }

        # Filter
        subset = scored_df.copy()
        if state:
            subset = subset[subset["state"].str.upper() == state]
        if county:
            subset = subset[subset["county"].str.lower() == county.lower()]

        if subset.empty:
            label = f"{county}, {state}" if county else state
            return {
                "error": (
                    f"No scored locations found for '{label}'. "
                    "Check that the state/county name matches the scored dataset. "
                    "County names use NAMELSAD format, e.g. 'Whatcom County'."
                )
            }

        area_label = f"{county}, {state}" if county else state
        total = len(subset)

        # Tier distribution
        _TIERS = [
            config.RISK_TIER_HIGH, config.RISK_TIER_MODERATE,
            config.RISK_TIER_LOW, config.RISK_TIER_UNKNOWN,
        ]
        tier_counts = subset["risk_tier"].value_counts()
        tier_distribution = {
            t: {
                "count": int(tier_counts.get(t, 0)),
                "pct": round(tier_counts.get(t, 0) / total * 100, 2),
            }
            for t in _TIERS
        }

        # Environmental averages by tier
        env_cols = [c for c in ["canopy_pct", "slope_deg"] if c in subset.columns]
        env_avgs: dict[str, Any] = {}
        for t in [config.RISK_TIER_HIGH, config.RISK_TIER_MODERATE, config.RISK_TIER_LOW]:
            t_df = subset[subset["risk_tier"] == t]
            if not t_df.empty:
                env_avgs[t] = {
                    col: round(float(t_df[col].mean()), 2)
                    for col in env_cols
                    if col in t_df.columns
                }

        # Primary risk driver (mean factor scores across HIGH-tier locations)
        high_df = subset[subset["risk_tier"] == config.RISK_TIER_HIGH]
        primary_driver: str | None = None
        if not high_df.empty:
            # Support both old column names (test fixtures) and actual pipeline output
            score_cols = {
                "canopy": next((c for c in ["canopy_risk", "canopy_score"] if c in high_df.columns), None),
                "slope": next((c for c in ["slope_risk", "slope_score"] if c in high_df.columns), None),
                "land_cover": next((c for c in ["landcover_risk", "landcover_score"] if c in high_df.columns), None),
            }
            score_cols = {k: v for k, v in score_cols.items() if v is not None}
            driver_means = {
                name: float(high_df[col].mean())
                for name, col in score_cols.items()
                if col in high_df.columns
            }
            if driver_means:
                primary_driver = max(driver_means, key=driver_means.get)

        # Top sub-areas (counties within state, or top-N locations within county)
        top_sub_areas: list[dict] = []
        if not county and "county" in subset.columns:
            county_stats = (
                subset[subset["risk_tier"] == config.RISK_TIER_HIGH]
                .groupby("county")
                .size()
                .sort_values(ascending=False)
                .head(top_n)
            )
            total_by_county = subset.groupby("county").size()
            for county_name, high_count in county_stats.items():
                county_total = int(total_by_county.get(county_name, 1))
                top_sub_areas.append({
                    "county": county_name,
                    "high_risk_count": int(high_count),
                    "total_locations": county_total,
                    "high_risk_pct": round(high_count / county_total * 100, 1),
                })

        # Plain-English briefing paragraph
        high_pct = tier_distribution[config.RISK_TIER_HIGH]["pct"]
        high_count = tier_distribution[config.RISK_TIER_HIGH]["count"]
        avg_canopy_high = env_avgs.get(config.RISK_TIER_HIGH, {}).get("canopy_pct")
        briefing = (
            f"{area_label} has {total:,} assessed locations. "
            f"{high_count:,} ({high_pct:.1f}%) are HIGH risk"
        )
        if primary_driver:
            briefing += f", driven primarily by {primary_driver} obstruction"
        if avg_canopy_high is not None:
            briefing += f" (average canopy cover {avg_canopy_high:.1f}% at HIGH-risk sites)"
        briefing += ". "
        if top_sub_areas:
            top_name = top_sub_areas[0]["county"]
            top_count = top_sub_areas[0]["high_risk_count"]
            briefing += (
                f"The highest concentration of HIGH-risk locations is in "
                f"{top_name} ({top_count:,} locations). "
            )
        briefing += (
            "Field assessment is recommended for HIGH-risk sites before scheduling "
            "Starlink installations."
        )

        return {
            "status": "success",
            "area": area_label,
            "total_locations": total,
            "tier_distribution": tier_distribution,
            "environmental_averages_by_tier": env_avgs,
            "primary_risk_driver": primary_driver,
            "top_high_risk_sub_areas": top_sub_areas,
            "briefing": briefing,
        }

    def _tool_assess_polygon(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """Risk briefing for a bounding box or polygon (read-only from scored CSV)."""
        scored_df = self._load_scored_df()
        if scored_df is None:
            return {
                "error": (
                    "Scored locations CSV not found. "
                    "Run the batch pipeline first (ingest_locations → sample_environment → score_risk)."
                )
            }
        coords = inputs.get("coordinates")
        if coords and len(coords) >= 3:
            try:
                from shapely.geometry import Polygon, Point
                import geopandas as gpd
                poly = Polygon([(float(p[1]), float(p[0])) for p in coords])  # shapely expects (x,y) = (lon,lat)
                if not poly.is_valid:
                    poly = poly.buffer(0)
                gdf = gpd.GeoDataFrame(
                    scored_df,
                    geometry=gpd.points_from_xy(scored_df["longitude"], scored_df["latitude"]),
                    crs="EPSG:4326",
                )
                subset = scored_df.loc[gdf.within(poly)].copy()
            except Exception as e:
                return {"error": f"Invalid polygon or geometry error: {e}"}
            area_label = (inputs.get("label") or "Custom polygon").strip() or "Custom polygon"
        else:
            try:
                min_lat = float(inputs.get("min_lat", -90))
                max_lat = float(inputs.get("max_lat", 90))
                min_lon = float(inputs.get("min_lon", -180))
                max_lon = float(inputs.get("max_lon", 180))
            except (TypeError, ValueError):
                return {"error": "Provide bbox (min_lat, max_lat, min_lon, max_lon) or coordinates (list of [lat, lon] with ≥3 points)."}
            subset = scored_df[
                (scored_df["latitude"] >= min_lat) & (scored_df["latitude"] <= max_lat)
                & (scored_df["longitude"] >= min_lon) & (scored_df["longitude"] <= max_lon)
            ].copy()
            area_label = inputs.get("label") or f"Bbox ({min_lat:.2f},{min_lon:.2f}) to ({max_lat:.2f},{max_lon:.2f})"

        if subset.empty:
            return {"error": f"No scored locations found inside the requested area ({area_label})."}

        total = len(subset)
        _TIERS = [
            config.RISK_TIER_HIGH, config.RISK_TIER_MODERATE,
            config.RISK_TIER_LOW, config.RISK_TIER_UNKNOWN,
        ]
        tier_counts = subset["risk_tier"].value_counts()
        tier_distribution = {
            t: {"count": int(tier_counts.get(t, 0)), "pct": round(tier_counts.get(t, 0) / total * 100, 2)}
            for t in _TIERS
        }
        env_cols = [c for c in ["canopy_pct", "slope_deg"] if c in subset.columns]
        env_avgs = {}
        for t in [config.RISK_TIER_HIGH, config.RISK_TIER_MODERATE, config.RISK_TIER_LOW]:
            t_df = subset[subset["risk_tier"] == t]
            if not t_df.empty:
                env_avgs[t] = {col: round(float(t_df[col].mean()), 2) for col in env_cols if col in t_df.columns}
        high_df = subset[subset["risk_tier"] == config.RISK_TIER_HIGH]
        primary_driver = None
        if not high_df.empty:
            score_cols = {
                "canopy": next((c for c in ["canopy_risk", "canopy_score"] if c in high_df.columns), None),
                "slope": next((c for c in ["slope_risk", "slope_score"] if c in high_df.columns), None),
                "land_cover": next((c for c in ["landcover_risk", "landcover_score"] if c in high_df.columns), None),
            }
            score_cols = {k: v for k, v in score_cols.items() if v is not None}
            driver_means = {name: float(high_df[col].mean()) for name, col in score_cols.items() if col in high_df.columns}
            if driver_means:
                primary_driver = max(driver_means, key=driver_means.get)
        top_sub_areas = []
        if "county" in subset.columns:
            county_stats = (
                subset[subset["risk_tier"] == config.RISK_TIER_HIGH]
                .groupby("county").size().sort_values(ascending=False).head(10)
            )
            total_by_county = subset.groupby("county").size()
            for county_name, high_count in county_stats.items():
                county_total = int(total_by_county.get(county_name, 1))
                top_sub_areas.append({
                    "county": county_name,
                    "high_risk_count": int(high_count),
                    "total_locations": county_total,
                    "high_risk_pct": round(high_count / county_total * 100, 1),
                })
        high_pct = tier_distribution[config.RISK_TIER_HIGH]["pct"]
        high_count = tier_distribution[config.RISK_TIER_HIGH]["count"]
        avg_canopy_high = env_avgs.get(config.RISK_TIER_HIGH, {}).get("canopy_pct")
        briefing = (
            f"{area_label} has {total:,} assessed locations. "
            f"{high_count:,} ({high_pct:.1f}%) are HIGH risk"
        )
        if primary_driver:
            briefing += f", driven primarily by {primary_driver} obstruction"
        if avg_canopy_high is not None:
            briefing += f" (average canopy cover {avg_canopy_high:.1f}% at HIGH-risk sites)"
        briefing += ". "
        if top_sub_areas:
            briefing += f"Top HIGH-risk county in area: {top_sub_areas[0]['county']} ({top_sub_areas[0]['high_risk_count']:,} locations). "
        briefing += "Field assessment is recommended for HIGH-risk sites before scheduling Starlink installations."
        return {
            "status": "success",
            "area": area_label,
            "total_locations": total,
            "tier_distribution": tier_distribution,
            "environmental_averages_by_tier": env_avgs,
            "primary_risk_driver": primary_driver,
            "top_high_risk_sub_areas": top_sub_areas,
            "briefing": briefing,
        }

    def _tool_query_top_counties(self, inputs: dict[str, Any]) -> dict[str, Any]:
        """Top N counties by count of locations in a given risk tier (read-only from scored CSV)."""
        state = (inputs.get("state") or "").strip().upper()
        tier = (inputs.get("tier") or "LOW").strip().upper()
        top_n = int(inputs.get("top_n") or 15)

        if not state:
            return {"error": "Missing required 'state' (2-letter abbreviation, e.g. 'NC')."}

        if tier not in (config.RISK_TIER_HIGH, config.RISK_TIER_MODERATE, config.RISK_TIER_LOW):
            return {"error": f"Invalid 'tier'. Use one of: HIGH, MODERATE, LOW (got '{tier}')."}

        scored_df = self._load_scored_df()
        if scored_df is None:
            return {
                "error": (
                    "Scored locations CSV not found. Run the batch pipeline first "
                    "(ingest_locations → sample_environment → score_risk)."
                )
            }

        subset = scored_df[scored_df["state"].str.upper() == state]
        if subset.empty:
            return {"error": f"No scored locations found for state '{state}'."}

        tier_subset = subset[subset["risk_tier"] == tier]
        total_tier = len(tier_subset)
        if "county" not in subset.columns:
            return {"error": "Scored dataset has no 'county' column."}

        county_counts = (
            tier_subset.groupby("county").size().sort_values(ascending=False).head(top_n)
        )
        total_state = len(subset)
        top_counties = [
            {
                "county": name,
                "count": int(count),
                "pct_of_state": round(count / total_state * 100, 2) if total_state else 0,
            }
            for name, count in county_counts.items()
        ]

        return {
            "status": "success",
            "state": state,
            "tier": tier,
            "total_locations_in_tier": total_tier,
            "total_locations_in_state": total_state,
            "top_counties": top_counties,
        }

    # ------------------------------------------------------------------
    # Monitoring
    # ------------------------------------------------------------------

    def _record_tool_metric(
        self, tool_name: str, elapsed_ms: int, success: bool
    ) -> None:
        tools = self._monitoring["tools"]
        if tool_name not in tools:
            tools[tool_name] = {"calls": 0, "failures": 0, "total_ms": 0}
        tools[tool_name]["calls"] += 1
        tools[tool_name]["total_ms"] += elapsed_ms
        if not success:
            tools[tool_name]["failures"] += 1
        logger.debug("Tool metric: %s | %d ms | success=%s", tool_name, elapsed_ms, success)

    def _save_monitoring_report(self) -> None:
        """Persist the monitoring dict to MONITORING_REPORT_PATH as JSON.

        In interactive mode, merges with existing report so token usage and cost
        accumulate across UI requests. In batch mode, overwrites (one run = one report).
        """
        import json as _json

        report = dict(self._monitoring)
        in_tok = report.get("input_tokens") or 0
        out_tok = report.get("output_tokens") or 0

        if report.get("mode") == "interactive" and config.MONITORING_REPORT_PATH.exists():
            try:
                existing = _json.loads(
                    config.MONITORING_REPORT_PATH.read_text(encoding="utf-8")
                )
                in_tok = (existing.get("input_tokens") or 0) + in_tok
                out_tok = (existing.get("output_tokens") or 0) + out_tok
                report["input_tokens"] = in_tok
                report["output_tokens"] = out_tok
                report["turns"] = (existing.get("turns") or 0) + (report.get("turns") or 0)
                existing_tools = existing.get("tools") or {}
                for name, stats in (report.get("tools") or {}).items():
                    if name not in existing_tools:
                        existing_tools[name] = {"calls": 0, "failures": 0, "total_ms": 0}
                    existing_tools[name]["calls"] += stats.get("calls", 0)
                    existing_tools[name]["failures"] += stats.get("failures", 0)
                    existing_tools[name]["total_ms"] += stats.get("total_ms", 0)
                report["tools"] = existing_tools
            except (OSError, ValueError) as exc:
                logger.warning("Could not merge existing monitoring report: %s", exc)

        report["estimated_cost_usd"] = round(
            (in_tok / 1_000_000 * config.ANTHROPIC_PRICE_INPUT_PER_1M_TOKENS)
            + (out_tok / 1_000_000 * config.ANTHROPIC_PRICE_OUTPUT_PER_1M_TOKENS),
            4,
        )

        try:
            config.MONITORING_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
            config.MONITORING_REPORT_PATH.write_text(
                _json.dumps(report, indent=2), encoding="utf-8"
            )
            logger.info("Monitoring report saved to %s", config.MONITORING_REPORT_PATH)
        except OSError as exc:
            logger.warning("Could not save monitoring report: %s", exc)

    def get_monitoring_report(self) -> dict[str, Any]:
        """Return the current monitoring metrics as a dict (includes estimated_cost_usd)."""
        out = dict(self._monitoring)
        in_tok = out.get("input_tokens") or 0
        out_tok = out.get("output_tokens") or 0
        out["estimated_cost_usd"] = round(
            (in_tok / 1_000_000 * config.ANTHROPIC_PRICE_INPUT_PER_1M_TOKENS)
            + (out_tok / 1_000_000 * config.ANTHROPIC_PRICE_OUTPUT_PER_1M_TOKENS),
            4,
        )
        return out
