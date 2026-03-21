#!/usr/bin/env python3
"""
step2_validate_outputs.py

Validate local marketplace categorization artifacts before DB import.

Checks:
- attribute paths exist in the taxonomy
- unit symbols are known
- no duplicate attribute keys within a single path
- no ancestor/descendant key collisions
- verified item mappings point only to taxonomy leaves
- no duplicate (item_id, category_path) pairs in verified mapping
- attribute-values category paths exist in taxonomy

Can be run standalone or called as a module from workflow.py.
"""

from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SUBCATEGORIES_JSON = ROOT / "proposed-subcategories.json"
ATTRIBUTES_JSON = ROOT / "proposed-attributes.json"
VERIFIED_MAPPING_CSV = ROOT / "prod" / "output" / "verified-mapping.csv"
ATTRIBUTE_VALUES_CSV = ROOT / "prod" / "output" / "attribute-values.csv"


def load_taxonomy_paths() -> tuple[set[str], set[str]]:
    with SUBCATEGORIES_JSON.open(encoding="utf-8") as f:
        data = json.load(f)

    all_paths: set[str] = set()
    leaf_paths: set[str] = set()

    for tier1_slug, tier1_data in data.items():
        all_paths.add(tier1_slug)
        subcats = tier1_data.get("subcategories") or []
        if not subcats:
            leaf_paths.add(tier1_slug)
            continue

        for tier2 in subcats:
            tier2_slug = tier2["slug"]
            path2 = f"{tier1_slug}/{tier2_slug}"
            all_paths.add(path2)

            tier3_list = tier2.get("tier3") or []
            if not tier3_list:
                leaf_paths.add(path2)
                continue

            for tier3 in tier3_list:
                path3 = f"{path2}/{tier3['slug']}"
                all_paths.add(path3)
                leaf_paths.add(path3)

    return all_paths, leaf_paths


def validate_attributes(all_paths: set[str]) -> list[str]:
    with ATTRIBUTES_JSON.open(encoding="utf-8") as f:
        data = json.load(f)

    raw = data.get("_category_attributes") or {}
    units_seeded = set(data.get("_notes", {}).get("units_already_seeded", []))
    units_to_add = {u["symbol"] for u in data.get("_units_to_add", []) if "symbol" in u}
    known_units = units_seeded | units_to_add

    issues: list[str] = []
    path_to_keys: dict[str, list[str]] = defaultdict(list)

    for path, attrs in raw.items():
        if path not in all_paths:
            issues.append(f"Missing attribute path: {path}")

        key_counts = Counter(attr.get("key") for attr in attrs)
        for key, count in key_counts.items():
            if key is not None and count > 1:
                issues.append(f"Duplicate attribute key on same path: {path} -> {key} ({count})")

        for attr in attrs:
            key = attr.get("key")
            if key:
                path_to_keys[path].append(key)

            unit_symbol = attr.get("unit_symbol")
            if unit_symbol is not None and unit_symbol not in known_units:
                issues.append(f"Unknown unit symbol: {path} -> {key} -> {unit_symbol}")

    for path, keys in path_to_keys.items():
        parts = path.split("/")
        ancestor_keys: set[str] = set()
        for i in range(1, len(parts)):
            ancestor_path = "/".join(parts[:i])
            ancestor_keys.update(path_to_keys.get(ancestor_path, []))
        for key in keys:
            if key in ancestor_keys:
                issues.append(f"Lineage key conflict: {path} -> {key}")

    return issues


def validate_verified_mapping(leaf_paths: set[str]) -> list[str]:
    issues: list[str] = []
    seen_pairs: set[tuple[str, str]] = set()

    with VERIFIED_MAPPING_CSV.open(newline="", encoding="utf-8") as f:
        rows = csv.DictReader(f)
        for row in rows:
            item_id = row["item_id"]
            category_path = row["category_path"]

            if category_path not in leaf_paths:
                issues.append(f"Verified mapping points to non-leaf or unknown category: {item_id} -> {category_path}")

            pair = (item_id, category_path)
            if pair in seen_pairs:
                issues.append(f"Duplicate verified mapping pair: {item_id} -> {category_path}")
            seen_pairs.add(pair)

    return issues


def validate_attribute_values(all_paths: set[str]) -> list[str]:
    issues: list[str] = []

    with ATTRIBUTE_VALUES_CSV.open(newline="", encoding="utf-8") as f:
        rows = csv.DictReader(f)
        for row in rows:
            category_path = row["category_path"]
            if category_path not in all_paths:
                issues.append(
                    f"Attribute value row points to unknown category path: {row['item_id']} -> {category_path}"
                )

    return issues


def run_validation(
    verified_mapping_csv: Path | None = None,
    attribute_values_csv: Path | None = None,
) -> list[str]:
    """
    Run all validation checks and return a list of issue strings (empty = clean).
    Optionally override the default CSV paths for use by workflow.py.
    """
    global VERIFIED_MAPPING_CSV, ATTRIBUTE_VALUES_CSV
    if verified_mapping_csv:
        VERIFIED_MAPPING_CSV = verified_mapping_csv
    if attribute_values_csv:
        ATTRIBUTE_VALUES_CSV = attribute_values_csv

    all_paths, leaf_paths = load_taxonomy_paths()
    issues: list[str] = []
    issues.extend(validate_attributes(all_paths))
    issues.extend(validate_verified_mapping(leaf_paths))
    issues.extend(validate_attribute_values(all_paths))
    return issues


def main() -> int:
    issues = run_validation()
    if issues:
        print("Validation failed:")
        for issue in issues:
            print(f"- {issue}")
        return 1
    print("Validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
