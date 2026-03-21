"""
Load taxonomy (leaf category paths) and attribute definitions with inheritance.
Used by both phase 1 and phase 2 of the LLM pipeline.
"""
import json
from pathlib import Path
from typing import List, Optional, Tuple

from pipeline_config import PROPOSED_SUBCATEGORIES_JSON, PROPOSED_ATTRIBUTES_JSON


def load_taxonomy_leaf_paths(json_path: Optional[Path] = None) -> List[Tuple[str, str]]:
    """
    Flatten proposed-subcategories.json to a list of (category_path, display_label).
    Leaf = tier1/tier2 if no tier3, else tier1/tier2/tier3.
    Returns list of (path, label) for use in LLM prompt.
    """
    path = json_path or PROPOSED_SUBCATEGORIES_JSON
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    result: List[Tuple[str, str]] = []
    for tier1_slug, tier1_data in data.items():
        if not isinstance(tier1_data, dict):
            continue
        t1_name = tier1_data.get("display_name", tier1_slug)
        subcats = tier1_data.get("subcategories") or []
        if not subcats:
            result.append((tier1_slug, t1_name))
            continue
        for t2 in subcats:
            t2_slug = t2.get("slug", "")
            t2_name = t2.get("display_name", t2_slug)
            tier3_list = t2.get("tier3")
            if not tier3_list:
                result.append((f"{tier1_slug}/{t2_slug}", f"{t1_name} > {t2_name}"))
                continue
            for t3 in tier3_list:
                t3_slug = t3.get("slug", "")
                t3_name = t3.get("display_name", t3_slug)
                result.append(
                    (f"{tier1_slug}/{t2_slug}/{t3_slug}", f"{t1_name} > {t2_name} > {t3_name}")
                )
    return result


def load_category_attributes_with_inheritance(json_path: Optional[Path] = None) -> dict:
    """
    Load _category_attributes and for each path that appears (and its prefixes),
    build full attribute list by inheritance. Keys are full category_path;
    value is list of attr dicts (key, label, description, unit_symbol, value_type).
    """
    path = json_path or PROPOSED_ATTRIBUTES_JSON
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    raw = data.get("_category_attributes") or {}

    # For each path we might see (from raw keys and all prefix paths), collect inherited attrs
    all_paths: set[str] = set()
    for key in raw:
        all_paths.add(key)
        parts = key.split("/")
        for i in range(1, len(parts)):
            all_paths.add("/".join(parts[: i + 1]))

    result: dict = {}
    for path in sorted(all_paths):
        attrs = []
        seen_keys = set()
        parts = path.split("/")
        for i in range(1, len(parts) + 1):
            prefix = "/".join(parts[:i])
            for a in raw.get(prefix, []):
                k = a.get("key")
                if k and k not in seen_keys:
                    seen_keys.add(k)
                    attrs.append(dict(a))
        result[path] = attrs
    return result


def taxonomy_prompt_text(leaf_paths: List[Tuple[str, str]]) -> str:
    """Compact list of allowed category paths for the LLM."""
    lines = ["Allowed category paths (use exactly one of these slugs):"]
    for path, label in leaf_paths:
        lines.append(f"  - {path}  ({label})")
    return "\n".join(lines)
