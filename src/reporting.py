"""
Reporting and visualisation for the LEO Satellite Coverage Risk pipeline.

Summary stats, findings report (Markdown), risk distribution charts (bar + pie),
and static scatter map. Charts are saved to data/output/ and linked from /admin.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from src import config

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import seaborn as sns
    _HAS_MPL = True
except ImportError:
    _HAS_MPL = False

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------------

def generate_summary_stats(df: pd.DataFrame) -> dict[str, Any]:
    """Compute summary statistics from the scored locations DataFrame.

    Parameters
    ----------
    df:
        Scored DataFrame containing at minimum ``risk_tier``, ``state``,
        ``county``, ``canopy_pct``, and ``slope_deg`` columns.

    Returns
    -------
    dict with keys:
      total_locations         — int
      tier_distribution       — dict {tier: {count, pct}}
      unknown_count           — int
      unknown_pct             — float
      avg_canopy_by_tier      — dict {tier: avg_canopy_pct}
      avg_slope_by_tier       — dict {tier: avg_slope_deg}
      state_breakdown         — list of dicts sorted by HIGH count desc
      top_counties_high_risk  — list of top-20 counties by HIGH count
    """
    total = len(df)
    if total == 0:
        return {"total_locations": 0, "error": "empty dataframe"}

    # --- Tier distribution ---
    tier_counts = df["risk_tier"].value_counts().to_dict()
    tier_distribution: dict[str, dict[str, Any]] = {}
    for tier in [config.RISK_TIER_HIGH, config.RISK_TIER_MODERATE,
                 config.RISK_TIER_LOW, config.RISK_TIER_UNKNOWN]:
        n = int(tier_counts.get(tier, 0))
        tier_distribution[tier] = {
            "count": n,
            "pct": round(n / total * 100, 2),
        }

    unknown_count = int(tier_distribution[config.RISK_TIER_UNKNOWN]["count"])
    unknown_pct = round(unknown_count / total * 100, 2)

    # --- Environmental averages by tier ---
    scored_only = df[df["risk_tier"] != config.RISK_TIER_UNKNOWN]

    def _avg_by_tier(col: str) -> dict[str, float | None]:
        result: dict[str, float | None] = {}
        if col not in df.columns:
            return result
        for tier in [config.RISK_TIER_HIGH, config.RISK_TIER_MODERATE, config.RISK_TIER_LOW]:
            subset = scored_only.loc[scored_only["risk_tier"] == tier, col].dropna()
            result[tier] = round(float(subset.mean()), 2) if len(subset) > 0 else None
        return result

    avg_canopy_by_tier = _avg_by_tier("canopy_pct")
    avg_slope_by_tier = _avg_by_tier("slope_deg")

    # --- State breakdown ---
    state_breakdown: list[dict[str, Any]] = []
    if "state" in df.columns:
        state_groups = df.groupby("state")["risk_tier"].value_counts().unstack(fill_value=0)
        for tier in [config.RISK_TIER_HIGH, config.RISK_TIER_MODERATE,
                     config.RISK_TIER_LOW, config.RISK_TIER_UNKNOWN]:
            if tier not in state_groups.columns:
                state_groups[tier] = 0
        state_groups["total"] = state_groups.sum(axis=1)
        state_groups["high_pct"] = (
            state_groups.get(config.RISK_TIER_HIGH, 0) / state_groups["total"] * 100
        ).round(2)
        state_groups = state_groups.sort_values(config.RISK_TIER_HIGH, ascending=False)
        for state, row in state_groups.iterrows():
            state_breakdown.append({
                "state": str(state),
                "total": int(row["total"]),
                "HIGH": int(row.get(config.RISK_TIER_HIGH, 0)),
                "MODERATE": int(row.get(config.RISK_TIER_MODERATE, 0)),
                "LOW": int(row.get(config.RISK_TIER_LOW, 0)),
                "UNKNOWN": int(row.get(config.RISK_TIER_UNKNOWN, 0)),
                "high_pct": float(row["high_pct"]),
            })

    # --- Top-20 counties by HIGH risk count ---
    top_counties_high_risk: list[dict[str, Any]] = []
    if "county" in df.columns and "state" in df.columns:
        high_df = df[df["risk_tier"] == config.RISK_TIER_HIGH]
        if len(high_df) > 0:
            county_high = (
                high_df.groupby(["state", "county"])
                .size()
                .reset_index(name="high_count")
                .sort_values("high_count", ascending=False)
                .head(20)
            )
            for _, row in county_high.iterrows():
                top_counties_high_risk.append({
                    "state": str(row["state"]),
                    "county": str(row["county"]),
                    "high_count": int(row["high_count"]),
                })

    logger.info(
        "Summary stats: %d total | HIGH=%d (%.1f%%) | MOD=%d (%.1f%%) | "
        "LOW=%d (%.1f%%) | UNKNOWN=%d (%.1f%%)",
        total,
        tier_distribution[config.RISK_TIER_HIGH]["count"],
        tier_distribution[config.RISK_TIER_HIGH]["pct"],
        tier_distribution[config.RISK_TIER_MODERATE]["count"],
        tier_distribution[config.RISK_TIER_MODERATE]["pct"],
        tier_distribution[config.RISK_TIER_LOW]["count"],
        tier_distribution[config.RISK_TIER_LOW]["pct"],
        unknown_count, unknown_pct,
    )

    return {
        "total_locations": total,
        "tier_distribution": tier_distribution,
        "unknown_count": unknown_count,
        "unknown_pct": unknown_pct,
        "avg_canopy_by_tier": avg_canopy_by_tier,
        "avg_slope_by_tier": avg_slope_by_tier,
        "state_breakdown": state_breakdown,
        "top_counties_high_risk": top_counties_high_risk,
    }


# ---------------------------------------------------------------------------
# Findings report (Markdown)
# ---------------------------------------------------------------------------

def write_findings_report(stats: dict[str, Any], output_path: Path | None = None) -> Path:
    """Write a human-readable Markdown findings report from summary stats.

    Parameters
    ----------
    stats:
        Dict returned by :func:`generate_summary_stats`.
    output_path:
        Destination path.  Defaults to ``config.FINDINGS_REPORT_PATH``.

    Returns
    -------
    Path
        Path to the written report file.
    """
    if output_path is None:
        output_path = config.FINDINGS_REPORT_PATH
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total = stats.get("total_locations", 0)
    td = stats.get("tier_distribution", {})
    high = td.get("HIGH", {})
    mod = td.get("MODERATE", {})
    low = td.get("LOW", {})
    unk = td.get("UNKNOWN", {})

    lines: list[str] = [
        "# LEO Satellite Connectivity Risk — Analysis Findings",
        "",
        "## Overview",
        "",
        f"**Total locations analysed:** {total:,}",
        "",
        "## Risk Tier Distribution",
        "",
        "| Risk Tier | Count | Percentage |",
        "|-----------|------:|-----------:|",
        f"| HIGH      | {high.get('count', 0):,} | {high.get('pct', 0):.1f}% |",
        f"| MODERATE  | {mod.get('count', 0):,}  | {mod.get('pct', 0):.1f}% |",
        f"| LOW       | {low.get('count', 0):,}  | {low.get('pct', 0):.1f}% |",
        f"| UNKNOWN   | {unk.get('count', 0):,}  | {unk.get('pct', 0):.1f}% |",
        "",
        "UNKNOWN = raster nodata at location (outside raster extent, water body, etc.)",
        "",
        "## Environmental Averages by Risk Tier",
        "",
        "| Tier     | Avg Canopy (%) | Avg Slope (°) |",
        "|----------|---------------:|---------------:|",
    ]
    for tier in ["HIGH", "MODERATE", "LOW"]:
        c = stats.get("avg_canopy_by_tier", {}).get(tier)
        s = stats.get("avg_slope_by_tier", {}).get(tier)
        c_str = f"{c:.1f}" if c is not None else "N/A"
        s_str = f"{s:.1f}" if s is not None else "N/A"
        lines.append(f"| {tier:<8} | {c_str:>14} | {s_str:>14} |")

    lines += [
        "",
        "## State-Level Breakdown (sorted by HIGH risk count)",
        "",
        "| State | Total | HIGH | MODERATE | LOW | UNKNOWN | HIGH% |",
        "|-------|------:|-----:|---------:|----:|--------:|------:|",
    ]
    for row in stats.get("state_breakdown", [])[:20]:
        lines.append(
            f"| {row['state']} | {row['total']:,} | {row['HIGH']:,} | "
            f"{row['MODERATE']:,} | {row['LOW']:,} | {row['UNKNOWN']:,} | "
            f"{row['high_pct']:.1f}% |"
        )

    lines += [
        "",
        "## Top 20 Counties by HIGH Risk Count",
        "",
        "| State | County | HIGH Count |",
        "|-------|--------|----------:|",
    ]
    for row in stats.get("top_counties_high_risk", []):
        lines.append(
            f"| {row['state']} | {row['county']} | {row['high_count']:,} |"
        )

    lines += [
        "",
        "---",
        "",
        "## Methodology Notes",
        "",
        "**Risk scoring** uses a weighted composite of three factors:",
        "- Tree Canopy Cover (50% weight): >50% → HIGH, 20-50% → MODERATE, <20% → LOW",
        "- Terrain Slope (30% weight): >20° → HIGH, 10-20° → MODERATE, <10° → LOW",
        "- Land Cover Type (20% weight): Forest → HIGH, Developed → MODERATE, Open/Ag → LOW",
        "",
        "**Data sources:**",
        "- NLCD Tree Canopy Cover 2021 (USGS, 30m resolution)",
        "- USGS 3DEP DEM (10-30m resolution, slope derived via numpy gradient)",
        "- NLCD Land Cover Classification 2021 (USGS, 30m resolution)",
        "",
        "**Known limitations:** Building obstructions not modelled (no national dataset).",
        "Seasonal canopy variation not captured. Results are location-level estimates,",
        "not site surveys.",
        "",
        "_Generated by LEO Satellite Coverage Risk Analysis Pipeline_",
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Findings report written to %s", output_path)
    return output_path


# ---------------------------------------------------------------------------
# Visualisation — bar chart, pie chart, static map (Phase 7 / plan)
# ---------------------------------------------------------------------------

def create_risk_distribution_chart(
    df: pd.DataFrame,
    output_path: Path | None = None,
) -> Path | None:
    """Create bar chart and pie chart of risk tier distribution. Saves single PNG."""
    if not _HAS_MPL:
        logger.warning("matplotlib not available — skipping risk distribution chart")
        return None
    if output_path is None:
        output_path = config.RISK_DISTRIBUTION_CHART_PATH
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    tier_order = [config.RISK_TIER_HIGH, config.RISK_TIER_MODERATE, config.RISK_TIER_LOW, config.RISK_TIER_UNKNOWN]
    counts = df["risk_tier"].value_counts().reindex(tier_order, fill_value=0)
    labels = [t.replace("_", " ").title() for t in tier_order]
    colors = ["#f85149", "#d29922", "#3fb950", "#8b949e"]  # high, moderate, low, unknown

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    # Bar chart
    bars = ax1.bar(range(len(counts)), counts.values, color=colors, edgecolor="white", linewidth=0.5)
    ax1.set_xticks(range(len(labels)))
    ax1.set_xticklabels(labels, rotation=15, ha="right")
    ax1.set_ylabel("Number of locations")
    ax1.set_title("Risk tier distribution (count)")
    for b, v in zip(bars, counts.values):
        ax1.text(b.get_x() + b.get_width() / 2, b.get_height() + max(counts) * 0.01, f"{v:,}", ha="center", fontsize=9)

    # Pie chart
    total = counts.sum()
    if total > 0:
        sizes = counts.values
        ax2.pie(sizes, labels=labels, colors=colors, autopct=lambda p: f"{p:.1f}%" if p > 0 else "", startangle=90)
    ax2.set_title("Risk tier distribution (%)")

    plt.suptitle("LEO Satellite Connectivity Risk — Tier Distribution", fontsize=12, y=1.02)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close()
    logger.info("Risk distribution chart saved to %s", output_path)
    return output_path


def create_static_risk_map(
    df: pd.DataFrame,
    output_path: Path | None = None,
) -> Path | None:
    """Create a static scatter map of locations colour-coded by risk tier (sample for performance)."""
    if not _HAS_MPL:
        logger.warning("matplotlib not available — skipping static risk map")
        return None
    if output_path is None:
        output_path = config.STATIC_MAP_PATH
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if "latitude" not in df.columns or "longitude" not in df.columns or "risk_tier" not in df.columns:
        logger.warning("Missing lat/lon or risk_tier — skipping static map")
        return None

    # Sample up to 50k points for readable PNG
    sample = df.sample(n=min(50_000, len(df)), random_state=42) if len(df) > 50_000 else df
    tier_order = [config.RISK_TIER_HIGH, config.RISK_TIER_MODERATE, config.RISK_TIER_LOW, config.RISK_TIER_UNKNOWN]
    colors = {"HIGH": "#f85149", "MODERATE": "#d29922", "LOW": "#3fb950", "UNKNOWN": "#8b949e"}

    fig, ax = plt.subplots(figsize=(10, 8))
    for tier in tier_order:
        subset = sample[sample["risk_tier"] == tier]
        if len(subset) > 0:
            ax.scatter(
                subset["longitude"], subset["latitude"],
                c=colors.get(tier, "#888"), s=0.3, alpha=0.6, label=tier,
            )
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("LEO Satellite Risk — Locations by tier (sample)")
    ax.legend(loc="upper right", markerscale=10)
    ax.set_aspect("equal")
    plt.tight_layout()
    plt.savefig(output_path, dpi=120, bbox_inches="tight")
    plt.close()
    logger.info("Static risk map saved to %s", output_path)
    return output_path


def create_interactive_map(
    df: pd.DataFrame,
    output_path: Path | None = None,
) -> Path | None:
    """Create an interactive Folium map with per-location risk popups.

    Phase 7 / Bonus feature.
    """
    logger.info("create_interactive_map: visualisations implemented in Phase 7.")
    return None
