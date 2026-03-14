"""
Phase 7 — Interactive Web UI entry point.

Run from project root:
    python -m app.run

Then open http://127.0.0.1:5001

Requires:
  - data/processed/locations_scored.csv (from full pipeline run)
  - ANTHROPIC_API_KEY in .env (for on-demand analyze_location, assess_area, assess_polygon, query_top_counties)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on path when running as python -m app.run
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.app import create_app

app = create_app()

if __name__ == "__main__":
    # Use 5001 to avoid conflict with macOS AirPlay Receiver on 5000
    app.run(host="127.0.0.1", port=5001, debug=True, use_reloader=False)
