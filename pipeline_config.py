"""
Config and path constants for the LLM categorization + attribute extraction pipeline.

Paths are relative to this workspace root.
ENV controls which environment folder (prod/ or dev/) is used.
OUTPUT_TIMESTAMP controls which timestamped subfolder pipeline outputs go into.
Both are set by workflow.py before importing pipeline functions; defaults point to prod/output.
"""
from pathlib import Path

# Workspace root (directory containing this file)
ROOT = Path(__file__).resolve().parent

# Environment: "prod" or "dev" — overridden by workflow.py at runtime
ENV: str = "prod"

# Timestamp for this pipeline run — overridden by workflow.py at runtime
OUTPUT_TIMESTAMP: str = ""


def _output_dir() -> Path:
    base = ROOT / ENV / "output"
    if OUTPUT_TIMESTAMP:
        return base / OUTPUT_TIMESTAMP
    return base


def _downloaded_dir() -> Path:
    return ROOT / ENV / "downloaded"


# Taxonomy source of truth — always at root, env-agnostic
PROPOSED_SUBCATEGORIES_JSON = ROOT / "proposed-subcategories.json"
PROPOSED_ATTRIBUTES_JSON = ROOT / "proposed-attributes.json"


def get_items_leaf_mapping_csv() -> Path:
    return _output_dir() / "items-leaf-mapping.csv"


def get_verified_mapping_csv() -> Path:
    return _output_dir() / "verified-mapping.csv"


def get_attribute_values_csv() -> Path:
    return _output_dir() / "attribute-values.csv"


def get_phase1_dir() -> Path:
    return _output_dir() / "phase1"


def get_phase2_dir() -> Path:
    return _output_dir() / "phase2"


def get_items_source_csv() -> Path:
    """Primary items source for step0 — latest manual download in prod/downloaded/."""
    return _downloaded_dir() / "items-with-store-data-2.csv"


# Legacy flat constants for backward-compat when running scripts standalone
# These point to the existing prod/output flat files (no timestamp subfolder)
ITEMS_LEAF_MAPPING_CSV = ROOT / "prod" / "output" / "items-leaf-mapping.csv"
VERIFIED_MAPPING_CSV = ROOT / "prod" / "output" / "verified-mapping.csv"
ATTRIBUTE_VALUES_CSV = ROOT / "prod" / "output" / "attribute-values.csv"
PHASE1_DIR = ROOT / "prod" / "output" / "phase1"
PHASE2_DIR = ROOT / "prod" / "output" / "phase2"

# Batch size for LLM calls
BATCH_SIZE = 30

# Retry / rate limit
MAX_RETRIES = 5
INITIAL_BACKOFF_SEC = 2
MAX_BACKOFF_SEC = 60
THROTTLE_AFTER_SUCCESS_SEC = 0.5

# Model
DEFAULT_MODEL = "gpt-4o-mini"
