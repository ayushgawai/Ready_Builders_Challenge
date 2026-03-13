"""
Result validation and anomaly detection for the LEO Satellite Coverage Risk pipeline.

Responsibilities:
  - Validate the scored DataFrame for signs of pipeline failure or data corruption
  - Detect anomalous individual records for review
  - Produce a structured validation report consumed by the agent

Two-tier severity model:
  CRITICAL  → is_valid = False
    - >50% UNKNOWN tier (indicates raster sampling failure — can't trust results)
    - >90% same tier (statistically implausible without massive geographic bias)
    - Any impossible values (canopy>100, slope<0, etc.) — indicates raster corruption
  WARNING   → is_valid = True, logged in report
    - Forest land cover with <5% canopy (NLCD classification inconsistency)
    - Moderate UNKNOWN rate (<50% — some NaN is expected near raster edges)

Design decisions:
  - All thresholds imported from config.py — nothing hardcoded here.
  - Cross-validation anomalies are flagged, not dropped. They may be legitimate
    edge cases (e.g. recently logged areas, NLCD update lag).
  - The anomaly report includes up to N sample location_ids per anomaly type
    so the agent and reviewers can inspect specific rows.
  - generate_anomaly_report saves to disk so the artifact persists after the
    agent run completes.
"""

import logging
from pathlib import Path

import pandas as pd

from src.config import (
    ANOMALY_REPORT_PATH,
    CANOPY_IMPOSSIBLE_MAX,
    DOMINANT_TIER_THRESHOLD,
    FOREST_CODES,
    FOREST_LOW_CANOPY_THRESHOLD,
    RISK_TIER_UNKNOWN,
    SLOPE_IMPOSSIBLE_MAX,
    SLOPE_IMPOSSIBLE_MIN,
)

logger = logging.getLogger(__name__)

# Maximum number of sample location_ids to include per anomaly type in reports.
_MAX_ANOMALY_SAMPLE = 10

# Required columns in the scored DataFrame (output of compute_composite_risk).
_REQUIRED_COLUMNS = [
    "canopy_pct",
    "slope_deg",
    "land_cover_code",
    "composite_score",
    "risk_tier",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_results(df: pd.DataFrame) -> tuple[bool, dict]:
    """Validate a scored locations DataFrame for anomalies and pipeline failures.

    Runs four independent checks and aggregates results into a structured
    validation report. Returns ``(is_valid, report)`` where ``is_valid``
    indicates whether the results are trustworthy enough to proceed to reporting.

    Checks performed
    ----------------
    1. **UNKNOWN tier rate** — locations where composite_score is NaN (raster
       sampling returned no data). Rate >50% is a CRITICAL failure; it means
       more than half the locations were not scored.

    2. **Tier distribution dominance** — if any single risk tier accounts for
       >90% of all scored (non-UNKNOWN) locations, the distribution is
       statistically implausible and likely indicates a scoring bug or raster
       failure. CRITICAL failure.

    3. **Impossible values** — canopy_pct > 100, canopy_pct < 0, slope_deg < 0,
       slope_deg > 90. Any occurrence indicates raster data corruption. CRITICAL.

    4. **Cross-validation: forest/canopy consistency** — rows where
       land_cover_code is a forest code (41, 42, 43) but canopy_pct < 5%.
       NLCD classifies the pixel as forest ecosystem, yet almost no canopy
       is detected. This is a known inconsistency in NLCD data (logging
       lag, classification boundary artefacts). WARNING only — flagged for
       review but does not invalidate the run.

    Parameters
    ----------
    df:
        Scored DataFrame — must contain at minimum: canopy_pct, slope_deg,
        land_cover_code, composite_score, risk_tier.

    Returns
    -------
    is_valid : bool
        True if no CRITICAL issues detected. False if any critical issue found.
    validation_report : dict
        Structured report (see schema in module docstring). The ``is_valid``
        field inside the report mirrors the return value.

    Raises
    ------
    ValueError
        If any required column is missing from the DataFrame.
    """
    _check_required_columns(df)

    total = len(df)
    critical_issues: list[str] = []
    warnings: list[str] = []

    # --- Check 1: UNKNOWN tier rate ---
    tier_distribution = df["risk_tier"].value_counts().to_dict()
    unknown_count = int(tier_distribution.get(RISK_TIER_UNKNOWN, 0))
    unknown_pct = round(unknown_count / total * 100, 2) if total else 0.0

    tier_distribution_pct: dict[str, float] = {
        tier: round(count / total * 100, 2)
        for tier, count in tier_distribution.items()
    }

    if unknown_pct > 50.0:
        msg = (
            f"{unknown_pct:.1f}% of locations scored as UNKNOWN "
            f"({unknown_count:,} / {total:,}). "
            "Likely indicates raster sampling failure — check raster downloads."
        )
        critical_issues.append(msg)
        logger.error("CRITICAL: %s", msg)
    elif unknown_count > 0:
        warnings.append(
            f"{unknown_pct:.1f}% of locations scored as UNKNOWN ({unknown_count:,}). "
            "May indicate raster edge clipping or partial download."
        )

    # --- Check 2: Tier distribution dominance ---
    scored_df = df[df["risk_tier"] != RISK_TIER_UNKNOWN]
    scored_count = len(scored_df)
    dominant_tier: str | None = None

    if scored_count > 0:
        scored_tier_counts = scored_df["risk_tier"].value_counts()
        top_tier = scored_tier_counts.index[0]
        top_tier_pct = scored_tier_counts.iloc[0] / scored_count

        if top_tier_pct >= DOMINANT_TIER_THRESHOLD:
            dominant_tier = top_tier
            msg = (
                f"{top_tier_pct * 100:.1f}% of scored locations are '{top_tier}' tier "
                f"({scored_tier_counts.iloc[0]:,} / {scored_count:,}). "
                f"Threshold: {DOMINANT_TIER_THRESHOLD * 100:.0f}%. "
                "Verify raster data and scoring weights."
            )
            critical_issues.append(msg)
            logger.error("CRITICAL: %s", msg)

    # --- Check 3: Impossible values ---
    impossible: dict[str, int] = {}

    canopy = df["canopy_pct"].dropna()
    impossible["canopy_above_100"] = int((canopy > CANOPY_IMPOSSIBLE_MAX).sum())
    impossible["canopy_below_0"] = int((canopy < 0).sum())

    slope = df["slope_deg"].dropna()
    impossible["slope_below_0"] = int((slope < SLOPE_IMPOSSIBLE_MIN).sum())
    impossible["slope_above_90"] = int((slope > SLOPE_IMPOSSIBLE_MAX).sum())

    total_impossible = sum(impossible.values())

    if total_impossible > 0:
        detail = ", ".join(
            f"{k}={v}" for k, v in impossible.items() if v > 0
        )
        msg = (
            f"{total_impossible} impossible sensor values detected ({detail}). "
            "Indicates raster data corruption or unit conversion error."
        )
        critical_issues.append(msg)
        logger.error("CRITICAL: %s", msg)

    # --- Check 4: Cross-validation — Forest land cover vs low canopy ---
    forest_mask = df["land_cover_code"].isin(FOREST_CODES)
    low_canopy_mask = df["canopy_pct"].notna() & (df["canopy_pct"] < FOREST_LOW_CANOPY_THRESHOLD)
    forest_low_canopy_mask = forest_mask & low_canopy_mask

    forest_low_canopy_count = int(forest_low_canopy_mask.sum())
    forest_low_canopy_sample: list[str] = []

    if forest_low_canopy_count > 0:
        sample_col = "location_id" if "location_id" in df.columns else df.index
        forest_low_canopy_sample = (
            df.loc[forest_low_canopy_mask, "location_id"]
            .head(_MAX_ANOMALY_SAMPLE)
            .astype(str)
            .tolist()
            if "location_id" in df.columns
            else list(df.index[forest_low_canopy_mask][:_MAX_ANOMALY_SAMPLE])
        )
        warnings.append(
            f"{forest_low_canopy_count} rows have Forest land cover (NLCD 41/42/43) "
            f"but canopy_pct < {FOREST_LOW_CANOPY_THRESHOLD}%. "
            "Known NLCD inconsistency (logging lag, classification boundary). "
            "Flagged for review — not dropped."
        )
        logger.warning(
            "Cross-validation: %d forest-coded rows have canopy_pct < %d%%.",
            forest_low_canopy_count,
            FOREST_LOW_CANOPY_THRESHOLD,
        )

    is_valid = len(critical_issues) == 0

    validation_report: dict = {
        "total_scored": total,
        "tier_distribution": {k: int(v) for k, v in tier_distribution.items()},
        "tier_distribution_pct": tier_distribution_pct,
        "unknown_tier_count": unknown_count,
        "unknown_tier_pct": unknown_pct,
        "dominant_tier": dominant_tier,
        "impossible_values": impossible,
        "total_impossible": total_impossible,
        "cross_validation": {
            "forest_low_canopy_count": forest_low_canopy_count,
            "forest_low_canopy_sample_ids": forest_low_canopy_sample,
        },
        "critical_issues": critical_issues,
        "warnings": warnings,
        "is_valid": is_valid,
    }

    if is_valid:
        logger.info(
            "Validation passed: %d locations scored, %d UNKNOWN (%.1f%%), "
            "%d warnings.",
            total,
            unknown_count,
            unknown_pct,
            len(warnings),
        )
    else:
        logger.error(
            "Validation FAILED: %d critical issue(s), %d warning(s).",
            len(critical_issues),
            len(warnings),
        )

    return is_valid, validation_report


def generate_anomaly_report(df: pd.DataFrame, validation_report: dict) -> str:
    """Format the validation report and anomalous records into a human-readable string.

    Writes the report to ``data/output/anomaly_report.txt`` so it persists
    as a pipeline artifact.

    Parameters
    ----------
    df:
        Scored DataFrame — same one passed to :func:`validate_results`.
    validation_report:
        Dict produced by :func:`validate_results`.

    Returns
    -------
    str
        The formatted report text (also written to disk).
    """
    lines: list[str] = [
        "=" * 70,
        "  ANOMALY REPORT — Scored Locations Validation",
        "=" * 70,
        "",
        f"  Total locations scored   : {validation_report['total_scored']:>10,}",
        f"  Validation status        : {'✓ PASSED' if validation_report['is_valid'] else '✗ FAILED'}",
        f"  Critical issues          : {len(validation_report['critical_issues']):>10}",
        f"  Warnings                 : {len(validation_report['warnings']):>10}",
        "",
        "  Risk Tier Distribution:",
    ]

    for tier, count in sorted(validation_report["tier_distribution"].items()):
        pct = validation_report["tier_distribution_pct"].get(tier, 0.0)
        lines.append(f"    {tier:<12} : {count:>10,}  ({pct:.1f}%)")

    lines += [""]

    if validation_report["critical_issues"]:
        lines += ["  CRITICAL ISSUES:"]
        for i, issue in enumerate(validation_report["critical_issues"], 1):
            lines.append(f"    [{i}] {issue}")
        lines += [""]

    if validation_report["warnings"]:
        lines += ["  WARNINGS:"]
        for i, warn in enumerate(validation_report["warnings"], 1):
            lines.append(f"    [{i}] {warn}")
        lines += [""]

    # Impossible value detail rows
    impossible = validation_report.get("impossible_values", {})
    if validation_report["total_impossible"] > 0:
        lines += ["  IMPOSSIBLE VALUE DETAIL:"]
        for check, count in impossible.items():
            if count > 0:
                lines.append(f"    {check:<35}: {count:>8,}")
        lines += [""]

    # Cross-validation sample
    cv = validation_report.get("cross_validation", {})
    if cv.get("forest_low_canopy_count", 0) > 0:
        lines += [
            "  FOREST/CANOPY CROSS-VALIDATION ANOMALIES:",
            f"    Total flagged : {cv['forest_low_canopy_count']:,}",
            f"    Sample IDs    : {', '.join(cv['forest_low_canopy_sample_ids'][:5])}",
        ]
        if len(cv["forest_low_canopy_sample_ids"]) > 5:
            lines.append(
                f"    ... and {cv['forest_low_canopy_count'] - 5} more "
                f"(see data/output/anomaly_report.txt)"
            )
        lines += [""]

    lines += ["=" * 70]
    report_text = "\n".join(lines)

    try:
        ANOMALY_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        ANOMALY_REPORT_PATH.write_text(report_text, encoding="utf-8")
        logger.info("Anomaly report saved to %s", ANOMALY_REPORT_PATH)
    except OSError as exc:
        logger.warning("Could not write anomaly report to disk: %s", exc)

    return report_text


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_required_columns(df: pd.DataFrame) -> None:
    """Raise ValueError if any required column is absent from *df*."""
    missing = [col for col in _REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"validate_results() missing required columns: {missing}\n"
            f"Required: {_REQUIRED_COLUMNS}\n"
            f"Found:    {list(df.columns)}"
        )
