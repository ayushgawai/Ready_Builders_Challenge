# AI Tool Disclosure

This document lists every AI tool used in building this project, what each was used for, and cases where I diverged from AI-generated output.

---

## Tools Used

| Tool | Provider | Used For |
|---|---|---|
| **Claude (Anthropic API)** | Anthropic | Core agent in the pipeline — orchestrates tool calls, reasons about data quality, explains findings |
| **Claude Opus (via Cursor)** | Anthropic / Cursor | Architecture planning, master build plan design, methodology reasoning |
| **Cursor IDE** | Cursor AI | Code generation, file scaffolding, refactoring assistance |

---

## Detailed Usage

### Claude API (in-pipeline)

The Anthropic Claude API is the agent brain of this pipeline. It:
- Decides the order and parameters of tool calls
- Interprets tool results and reasons about data quality
- Handles failures and decides whether to retry or abort
- Generates the final natural-language findings summary

This is not a "wrapper around Claude to generate code" — Claude is an active runtime component of the system.

### Claude Opus (planning phase)

Used during the design phase to produce a detailed master build plan (`_personal/MASTER_BUILD_PLAN.md`) that outlined:
- Architecture decisions with tradeoffs
- Risk scoring methodology derived from the Starlink install guide
- Phased build plan with per-phase deliverables
- Production considerations and known limitations

### Cursor IDE

Used for implementation assistance during the coding phases:
- Generating boilerplate for Python modules
- Suggesting docstring formats
- Code review and linting assistance

---

## Cases Where I Diverged from AI Output

### 1. Model selection in config.py

**AI suggested:** `claude-sonnet-4-20250514`  
**What I did:** Changed to `claude-opus-4-5` based on current available model names and ensuring the agent loop has strong multi-step reasoning for the validation and failure-handling logic.  
**Why:** The suggested model name did not match a valid Anthropic API model identifier. Accuracy of the model name is critical — a wrong name causes a hard runtime failure.

### 2. Risk weight for Land Cover

**AI's initial suggestion:** Equal 33%/33%/33% weights for canopy, slope, and land cover.  
**What I did:** Used 50%/30%/20% as specified in the build plan.  
**Why:** The Starlink install guide text is explicit that tree branches are the most common cause of interruptions. Terrain slope matters but can often be mitigated by dish mounting height. Equal weighting would overstate the influence of land cover, which is fundamentally a corroborating signal rather than a direct obstruction measure.

### 3. PostGIS vs. rasterio

**AI suggested:** Including PostGIS as a geospatial processing option.  
**What I did:** Excluded PostGIS entirely; rasterio only.  
**Why:** The core operation is sampling raster cell values at point coordinates. This is a raster operation. PostGIS is designed for spatial joins between vector datasets. Adding a database server for a raster sampling task introduces infrastructure complexity with zero analytical benefit in this context.

---

*This document will be updated as additional AI tools are used in later phases.*
