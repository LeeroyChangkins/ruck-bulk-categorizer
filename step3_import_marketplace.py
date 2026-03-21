#!/usr/bin/env python3
"""
step3_import_marketplace.py

DB push and pull module for marketplace categorization data.
Designed to be imported by workflow.py; contains no interactive prompts.

Push tables (in dependency order):
  1. marketplace_attribute_units
  2. marketplace_categories
  3. marketplace_attributes
  4. marketplace_item_categories
  5. marketplace_attribute_values

Pull tables (DB → local files):
  items, marketplace_categories, marketplace_attributes,
  marketplace_attribute_units, marketplace_item_categories,
  marketplace_attribute_values

No other tables are touched.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any

import psycopg2
from tqdm import tqdm  # noqa: F401 — used inside imported functions and by workflow.py

# ──────────────────────────────────────────────────────────────────────────────
# DB configs — user/password are passed in at call time, never stored here
# ──────────────────────────────────────────────────────────────────────────────

DEV_DB_CONFIG: dict[str, Any] = {
    "host": "127.0.0.1",
    "port": 15432,
    "dbname": "flightcontrol",
    "sslmode": "require",
}

PROD_DB_CONFIG: dict[str, Any] = {
    "host": "127.0.0.1",
    "port": 15433,          # start prod tunnel on this port
    "dbname": "flightcontrol",
    "sslmode": "require",
    # TODO: add ruck-db-prod tunnel helper in ~/.zshrc pointing to prod RDS + bastion
}

# ──────────────────────────────────────────────────────────────────────────────
# File paths
# ──────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent
SUBCATEGORIES_JSON = ROOT / "proposed-subcategories.json"
ATTRIBUTES_JSON = ROOT / "proposed-attributes.json"
# Push source files — callers may override by passing paths directly
VERIFIED_MAPPING_CSV = ROOT / "prod" / "output" / "verified-mapping.csv"
ATTRIBUTE_VALUES_CSV = ROOT / "prod" / "output" / "attribute-values.csv"

# ──────────────────────────────────────────────────────────────────────────────
# Connection helper
# ──────────────────────────────────────────────────────────────────────────────

DRY_RUN: bool = False  # set by workflow.py before calling push functions


def make_connection(db_config: dict, user: str, password: str):
    """Open and return a psycopg2 connection."""
    return psycopg2.connect(**db_config, user=user, password=password)

# ──────────────────────────────────────────────────────────────────────────────
# Data loaders
# ──────────────────────────────────────────────────────────────────────────────


def load_sources() -> tuple[dict, dict, list[dict], list[dict]]:
    with SUBCATEGORIES_JSON.open(encoding="utf-8") as f:
        subcategories = json.load(f)
    with ATTRIBUTES_JSON.open(encoding="utf-8") as f:
        attributes = json.load(f)

    verified_rows: list[dict] = []
    with VERIFIED_MAPPING_CSV.open(encoding="utf-8", newline="") as f:
        verified_rows = list(csv.DictReader(f))

    attr_value_rows: list[dict] = []
    with ATTRIBUTE_VALUES_CSV.open(encoding="utf-8", newline="") as f:
        attr_value_rows = list(csv.DictReader(f))

    return subcategories, attributes, verified_rows, attr_value_rows


def build_unit_rows(attributes: dict) -> list[dict]:
    """
    Build the full list of units to ensure are present in the DB.

    _units_to_add entries have full metadata (symbol, name, description, value_type).
    units_already_seeded entries are symbol-only stubs inserted only if somehow
    missing from the DB (safe to re-insert; value_type is inferred from symbol).
    """
    to_add: list[dict] = attributes.get("_units_to_add") or []
    seeded_symbols: list[str] = (attributes.get("_notes") or {}).get("units_already_seeded") or []

    rows: list[dict] = []
    for u in to_add:
        rows.append({
            "symbol": u["symbol"],
            "name": u.get("name"),
            "description": u.get("description"),
            "value_type": u["value_type"],
        })

    # Stubs for pre-seeded symbols not already covered above
    covered = {r["symbol"] for r in rows}
    text_units = {"color", "finish", "material", "grade"}
    for sym in seeded_symbols:
        if sym not in covered:
            rows.append({
                "symbol": sym,
                "name": sym,
                "description": None,
                "value_type": "text" if sym in text_units else "number",
            })

    return rows


def build_category_nodes(subcategories: dict) -> list[tuple[str, str, str | None]]:
    """
    Flatten the taxonomy into (slug_path, display_name, parent_slug_path) tuples.
    Returned in tier1 → tier2 → tier3 order so parent IDs are available when
    children are inserted.
    """
    nodes: list[tuple[str, str, str | None]] = []
    for tier1_slug, tier1_data in subcategories.items():
        nodes.append((tier1_slug, tier1_data["display_name"], None))
        for tier2 in tier1_data.get("subcategories") or []:
            t2_path = f"{tier1_slug}/{tier2['slug']}"
            nodes.append((t2_path, tier2["display_name"], tier1_slug))
            for tier3 in tier2.get("tier3") or []:
                t3_path = f"{t2_path}/{tier3['slug']}"
                nodes.append((t3_path, tier3["display_name"], t2_path))
    return nodes


# ──────────────────────────────────────────────────────────────────────────────
# Phase 1 — marketplace_attribute_units
# ──────────────────────────────────────────────────────────────────────────────


def import_units(cur: Any, unit_rows: list[dict]) -> dict[str, str]:
    """
    Insert units that don't already exist (matched by symbol).
    Returns symbol → id map for every symbol in unit_rows.
    """
    symbols = [r["symbol"] for r in unit_rows]

    if DRY_RUN:
        print(f"[units] would ensure {len(symbols)} unit symbols are present")
        print(f"  Sample (first 5): {symbols[:5]}")
        return {r["symbol"]: f"<dry:{r['symbol']}>" for r in unit_rows}

    cur.execute(
        "SELECT id, symbol FROM marketplace_attribute_units WHERE symbol = ANY(%s)",
        (symbols,),
    )
    existing: dict[str, str] = {row[1]: row[0] for row in cur.fetchall()}

    to_insert = [r for r in unit_rows if r["symbol"] not in existing]
    inserted: dict[str, str] = dict(existing)

    with tqdm(to_insert, desc="  units", unit="unit", leave=False) as bar:
        for u in to_insert:
            bar.set_postfix_str(u["symbol"])
            cur.execute(
                """
                INSERT INTO marketplace_attribute_units (symbol, name, description, value_type)
                VALUES (%s, %s, %s, %s::marketplace_attribute_value_type)
                RETURNING id
                """,
                (u["symbol"], u["name"], u["description"], u["value_type"]),
            )
            inserted[u["symbol"]] = cur.fetchone()[0]
            bar.update(1)

    tqdm.write(f"  [units] {len(existing)} already existed, {len(to_insert)} inserted")
    return inserted


# ──────────────────────────────────────────────────────────────────────────────
# Phase 2 — marketplace_categories
# ──────────────────────────────────────────────────────────────────────────────


def import_categories(cur: Any, nodes: list[tuple[str, str, str | None]]) -> dict[str, str]:
    """
    Insert categories in tier order (tier1 first, then tier2, then tier3).
    Skips categories that already exist (matched by slug + parent_id).
    Returns slug_path → id map.
    """
    if DRY_RUN:
        path_to_id: dict[str, str] = {}
        for slug_path, display_name, parent_path in nodes:
            path_to_id[slug_path] = f"<dry:{slug_path}>"
        print(f"[categories] would ensure {len(nodes)} category nodes are present")
        tiers = {p.count("/") + 1 for p, _, _ in nodes}
        for t in sorted(tiers):
            count = sum(1 for p, _, _ in nodes if p.count("/") + 1 == t)
            print(f"  tier{t}: {count} nodes")
        return path_to_id

    path_to_id = {}
    inserted_count = 0
    skipped_count = 0

    with tqdm(nodes, desc="  categories", unit="cat", leave=False) as bar:
        for slug_path, display_name, parent_path in nodes:
            slug = slug_path.split("/")[-1]
            parent_id = path_to_id.get(parent_path) if parent_path else None
            bar.set_postfix_str(slug_path)

            if parent_id is not None:
                cur.execute(
                    "SELECT id FROM marketplace_categories WHERE slug = %s AND parent_id = %s",
                    (slug, parent_id),
                )
            else:
                cur.execute(
                    "SELECT id FROM marketplace_categories WHERE slug = %s AND parent_id IS NULL",
                    (slug,),
                )

            existing_row = cur.fetchone()
            if existing_row:
                path_to_id[slug_path] = existing_row[0]
                skipped_count += 1
            else:
                cur.execute(
                    """
                    INSERT INTO marketplace_categories (name, slug, parent_id)
                    VALUES (%s, %s, %s)
                    RETURNING id
                    """,
                    (display_name, slug, parent_id),
                )
                path_to_id[slug_path] = cur.fetchone()[0]
                inserted_count += 1

            bar.update(1)

    tqdm.write(f"  [categories] {skipped_count} already existed, {inserted_count} inserted")
    return path_to_id


# ──────────────────────────────────────────────────────────────────────────────
# Phase 3 — marketplace_attributes
# ──────────────────────────────────────────────────────────────────────────────


def import_attributes(
    cur: Any,
    attributes: dict,
    path_to_id: dict[str, str],
) -> dict[tuple[str, str], str]:
    """
    Insert attribute definitions scoped to their category.
    Skips attributes that already exist (matched by category_id + key).
    Returns (category_path, attribute_key) → attribute_id map.
    """
    category_attrs: dict[str, list[dict]] = attributes.get("_category_attributes") or {}

    if DRY_RUN:
        attr_key_to_id: dict[tuple[str, str], str] = {}
        total_attrs = sum(len(v) for v in category_attrs.values())
        missing_paths = [p for p in category_attrs if p not in path_to_id]
        print(f"[attributes] would ensure {total_attrs} attribute definitions across {len(category_attrs)} category paths")
        if missing_paths:
            print(f"  WARNING: {len(missing_paths)} attribute paths not in category map (would skip):", file=sys.stderr)
            for p in missing_paths:
                print(f"    {p}", file=sys.stderr)
        for cat_path, attr_list in category_attrs.items():
            for attr in attr_list:
                attr_key_to_id[(cat_path, attr["key"])] = f"<dry:{cat_path}:{attr['key']}>"
        return attr_key_to_id

    attr_key_to_id = {}
    inserted_count = 0
    skipped_count = 0
    missing_paths: list[str] = []

    all_attr_items = [
        (cat_path, attr)
        for cat_path, attr_list in category_attrs.items()
        for attr in attr_list
    ]

    with tqdm(all_attr_items, desc="  attributes", unit="attr", leave=False) as bar:
        for cat_path, attr in all_attr_items:
            cat_id = path_to_id.get(cat_path)
            key = attr["key"]
            bar.set_postfix_str(f"{cat_path.split('/')[-1]}:{key}")

            if not cat_id:
                if cat_path not in missing_paths:
                    missing_paths.append(cat_path)
                bar.update(1)
                continue

            cur.execute(
                "SELECT id FROM marketplace_attributes WHERE category_id = %s AND key = %s",
                (cat_id, key),
            )
            existing_row = cur.fetchone()
            if existing_row:
                attr_key_to_id[(cat_path, key)] = existing_row[0]
                skipped_count += 1
            else:
                cur.execute(
                    """
                    INSERT INTO marketplace_attributes
                        (category_id, key, label, description, unit_required)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        cat_id,
                        key,
                        attr["label"],
                        attr.get("description"),
                        bool(attr.get("unit_required", False)),
                    ),
                )
                attr_key_to_id[(cat_path, key)] = cur.fetchone()[0]
                inserted_count += 1

            bar.update(1)

    if missing_paths:
        tqdm.write(
            f"  WARNING: {len(missing_paths)} attribute paths not in category map (skipped):",
        )
        for p in missing_paths:
            tqdm.write(f"    {p}")

    tqdm.write(f"  [attributes] {skipped_count} already existed, {inserted_count} inserted")
    return attr_key_to_id


# ──────────────────────────────────────────────────────────────────────────────
# Phase 4 — marketplace_item_categories
# ──────────────────────────────────────────────────────────────────────────────


def import_item_categories(
    cur: Any,
    verified_rows: list[dict],
    path_to_id: dict[str, str],
) -> None:
    """
    Insert item-to-category relationships.
    The partial unique index (unique_active_item_category) prevents active duplicates;
    ON CONFLICT DO NOTHING silently skips them.
    """
    if DRY_RUN:
        missing = [r for r in verified_rows if r["category_path"] not in path_to_id]
        print(f"[item_categories] would insert up to {len(verified_rows)} rows")
        if missing:
            print(f"  WARNING: {len(missing)} rows have unknown category paths (would skip)", file=sys.stderr)
        return

    inserted_count = 0
    skipped_count = 0
    missing_paths: list[str] = []

    with tqdm(verified_rows, desc="  item_categories", unit="row", leave=False) as bar:
        for row in verified_rows:
            item_id = row["item_id"]
            cat_path = row["category_path"]
            cat_id = path_to_id.get(cat_path)

            if not cat_id:
                missing_paths.append(f"{item_id} → {cat_path}")
                bar.update(1)
                continue

            cur.execute(
                """
                INSERT INTO marketplace_item_categories (item_id, category_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (item_id, cat_id),
            )
            if cur.rowcount > 0:
                inserted_count += 1
            else:
                skipped_count += 1

            bar.set_postfix(inserted=inserted_count, skipped=skipped_count)
            bar.update(1)

    if missing_paths:
        tqdm.write(f"  WARNING: {len(missing_paths)} item rows had unknown category paths (skipped):")
        for m in missing_paths[:20]:
            tqdm.write(f"    {m}")
        if len(missing_paths) > 20:
            tqdm.write(f"    … and {len(missing_paths) - 20} more")

    tqdm.write(f"  [item_categories] {skipped_count} already existed, {inserted_count} inserted")


# ──────────────────────────────────────────────────────────────────────────────
# Phase 5 — marketplace_attribute_values
# ──────────────────────────────────────────────────────────────────────────────


def _resolve_attr_id(
    cat_path: str,
    attr_key: str,
    attr_key_to_id: dict[tuple[str, str], str],
) -> str | None:
    """
    Look up attribute_id by walking up the category lineage from cat_path to tier1.
    Attributes may be defined on a parent node but referenced via a child path in
    attribute-values.csv, so we check the exact path first, then each ancestor.
    """
    parts = cat_path.split("/")
    for depth in range(len(parts), 0, -1):
        candidate = "/".join(parts[:depth])
        attr_id = attr_key_to_id.get((candidate, attr_key))
        if attr_id:
            return attr_id
    return None


def import_attribute_values(
    cur: Any,
    attr_value_rows: list[dict],
    attr_key_to_id: dict[tuple[str, str], str],
    unit_symbol_to_id: dict[str, str],
) -> None:
    """
    Insert per-item attribute values.
    The partial unique index (unique_active_attribute_value) prevents active duplicates;
    ON CONFLICT DO NOTHING silently skips them.
    Attribute ID resolution walks up the category lineage so inherited attributes
    (defined on a parent node) are matched correctly.
    Units that can't be resolved are warned about but the value row is still inserted
    without a unit rather than being dropped.
    """
    if DRY_RUN:
        missing_attrs = {
            (r["category_path"], r["attribute_key"])
            for r in attr_value_rows
            if _resolve_attr_id(r["category_path"], r["attribute_key"], attr_key_to_id) is None
        }
        all_unit_syms = {
            r["unit_symbol"]
            for r in attr_value_rows
            if r.get("unit_symbol") and r["unit_symbol"] not in ("", "null", "None")
        }
        missing_units = all_unit_syms - set(unit_symbol_to_id)
        print(f"[attribute_values] would insert up to {len(attr_value_rows)} rows")
        if missing_attrs:
            print(
                f"  WARNING: {len(missing_attrs)} unique (category_path, attribute_key) combos not resolvable via lineage (would skip those rows)",
                file=sys.stderr,
            )
            for combo in sorted(missing_attrs)[:20]:
                print(f"    {combo}", file=sys.stderr)
            if len(missing_attrs) > 20:
                print(f"    … and {len(missing_attrs) - 20} more", file=sys.stderr)
        if missing_units:
            print(
                f"  WARNING: {len(missing_units)} unit symbols not in unit map (would insert without unit): {sorted(missing_units)}",
                file=sys.stderr,
            )
        return

    inserted_count = 0
    skipped_count = 0
    missing_attrs: list[str] = []
    missing_units: list[str] = []

    with tqdm(attr_value_rows, desc="  attribute_values", unit="row", leave=False) as bar:
        for row in attr_value_rows:
            item_id = row["item_id"]
            cat_path = row["category_path"]
            attr_key = row["attribute_key"]
            value = row["value"] or None
            unit_sym = row.get("unit_symbol") or None

            attr_id = _resolve_attr_id(cat_path, attr_key, attr_key_to_id)
            if not attr_id:
                missing_attrs.append(f"{item_id} / {cat_path} / {attr_key}")
                bar.update(1)
                continue

            unit_id: str | None = None
            if unit_sym and unit_sym not in ("", "null", "None"):
                unit_id = unit_symbol_to_id.get(unit_sym)
                if not unit_id:
                    missing_units.append(unit_sym)

            cur.execute(
                """
                INSERT INTO marketplace_attribute_values
                    (item_id, attribute_id, attribute_unit_id, value)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (item_id, attr_id, unit_id, value),
            )
            if cur.rowcount > 0:
                inserted_count += 1
            else:
                skipped_count += 1

            bar.set_postfix(inserted=inserted_count, skipped=skipped_count)
            bar.update(1)

    if missing_attrs:
        unique_missing = sorted(set(missing_attrs))
        tqdm.write(f"  WARNING: {len(unique_missing)} unique (path, key) combos not in attribute map (skipped):")
        for m in unique_missing[:20]:
            tqdm.write(f"    {m}")
        if len(unique_missing) > 20:
            tqdm.write(f"    … and {len(unique_missing) - 20} more")

    if missing_units:
        tqdm.write(
            f"  WARNING: {len(set(missing_units))} unit symbol(s) not found in DB (rows inserted without unit): {sorted(set(missing_units))}"
        )

    tqdm.write(f"  [attribute_values] {skipped_count} already existed, {inserted_count} inserted")


# ──────────────────────────────────────────────────────────────────────────────
# Push orchestrator — called by workflow.py
# ──────────────────────────────────────────────────────────────────────────────


def push_taxonomy(conn, scope: str = "all") -> None:
    """
    Push taxonomy (units, categories, attributes) to DB.
    scope: "units" | "categories" | "attributes" | "all"
    """
    conn.autocommit = False
    cur = conn.cursor()
    try:
        with SUBCATEGORIES_JSON.open(encoding="utf-8") as f:
            subcategories = json.load(f)
        with ATTRIBUTES_JSON.open(encoding="utf-8") as f:
            attributes = json.load(f)
        category_nodes = build_category_nodes(subcategories)
        unit_rows = build_unit_rows(attributes)

        if scope in ("units", "all"):
            tqdm.write("  Pushing units…")
            import_units(cur, unit_rows)
            conn.commit()

        path_to_id: dict[str, str] = {}
        if scope in ("categories", "all"):
            tqdm.write("  Pushing categories…")
            path_to_id = import_categories(cur, category_nodes)
            conn.commit()

        if scope in ("attributes", "all"):
            if not path_to_id:
                # Need to build path_to_id from existing DB categories
                cur.execute("SELECT id, slug FROM marketplace_categories WHERE deleted_at IS NULL")
                rows = cur.fetchall()
                # We'll do a full categories push to rebuild the map
                path_to_id = import_categories(cur, category_nodes)
                conn.commit()
            tqdm.write("  Pushing attributes…")
            import_attributes(cur, attributes, path_to_id)
            conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def push_item_relationships(
    conn,
    verified_mapping_csv: Path | None = None,
    attribute_values_csv: Path | None = None,
) -> None:
    """
    Push item-category and attribute-value rows to DB.
    Uses provided paths or falls back to module-level defaults.
    """
    vm_csv = verified_mapping_csv or VERIFIED_MAPPING_CSV
    av_csv = attribute_values_csv or ATTRIBUTE_VALUES_CSV

    conn.autocommit = False
    cur = conn.cursor()
    try:
        with SUBCATEGORIES_JSON.open(encoding="utf-8") as f:
            subcategories = json.load(f)
        with ATTRIBUTES_JSON.open(encoding="utf-8") as f:
            attributes = json.load(f)

        verified_rows: list[dict] = []
        with vm_csv.open(encoding="utf-8", newline="") as f:
            verified_rows = list(csv.DictReader(f))

        attr_value_rows: list[dict] = []
        with av_csv.open(encoding="utf-8", newline="") as f:
            attr_value_rows = list(csv.DictReader(f))

        # Rebuild in-memory maps from DB
        category_nodes = build_category_nodes(subcategories)
        unit_rows = build_unit_rows(attributes)

        tqdm.write("  Building category map from DB…")
        path_to_id = import_categories(cur, category_nodes)
        conn.commit()

        tqdm.write("  Building unit map from DB…")
        unit_symbol_to_id = import_units(cur, unit_rows)
        conn.commit()

        tqdm.write("  Building attribute map from DB…")
        attr_key_to_id = import_attributes(cur, attributes, path_to_id)
        conn.commit()

        # Pre-filter to only item IDs that exist in the DB
        all_item_ids = {r["item_id"] for r in verified_rows} | {r["item_id"] for r in attr_value_rows}
        if all_item_ids:
            cur.execute(
                "SELECT id FROM items WHERE id = ANY(%s) AND deleted_at IS NULL",
                (list(all_item_ids),),
            )
            existing_item_ids = {row[0] for row in cur.fetchall()}
            missing_item_ids = all_item_ids - existing_item_ids
            if missing_item_ids:
                tqdm.write(
                    f"  WARNING: {len(missing_item_ids)} item ID(s) not found in items table — skipping those rows."
                )
            verified_rows = [r for r in verified_rows if r["item_id"] in existing_item_ids]
            attr_value_rows = [r for r in attr_value_rows if r["item_id"] in existing_item_ids]

        tqdm.write("  Pushing item-category relationships…")
        import_item_categories(cur, verified_rows, path_to_id)
        conn.commit()

        tqdm.write("  Pushing attribute values…")
        import_attribute_values(cur, attr_value_rows, attr_key_to_id, unit_symbol_to_id)
        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


# ──────────────────────────────────────────────────────────────────────────────
# Pull functions — called by workflow.py (download path)
# ──────────────────────────────────────────────────────────────────────────────


def pull_all(conn, out_dir: Path) -> dict[str, int]:
    """
    Pull all 6 tables from DB into timestamped out_dir.
    Returns dict of table_name → row_count.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    cur = conn.cursor()
    counts: dict[str, int] = {}

    # items
    cur.execute("""
        SELECT DISTINCT ON (i.id)
            i.id           AS id,
            i.title        AS title,
            i.description  AS description,
            ''             AS subtitle,
            s.name         AS store_name,
            i.category,
            i.subcategory
        FROM items i
        JOIN store_items si ON si.items_id = i.id
        JOIN stores s ON s.id = si.store_id
        WHERE i.deleted_at IS NULL
        ORDER BY i.id, s.name
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    _write_csv(out_dir / "items.csv", cols, rows)
    counts["items"] = len(rows)

    # marketplace_categories
    cur.execute("""
        SELECT id, parent_id, name, slug, created_at
        FROM marketplace_categories
        WHERE deleted_at IS NULL
        ORDER BY id
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    _write_json(out_dir / "marketplace_categories.json", cols, rows)
    counts["marketplace_categories"] = len(rows)

    # marketplace_attribute_units
    cur.execute("""
        SELECT id, symbol, name, description, value_type
        FROM marketplace_attribute_units
        WHERE deleted_at IS NULL
        ORDER BY symbol
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    _write_json(out_dir / "marketplace_attribute_units.json", cols, rows)
    counts["marketplace_attribute_units"] = len(rows)

    # marketplace_attributes
    cur.execute("""
        SELECT id, category_id, key, label, description, unit_required
        FROM marketplace_attributes
        WHERE deleted_at IS NULL
        ORDER BY category_id, key
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    _write_json(out_dir / "marketplace_attributes.json", cols, rows)
    counts["marketplace_attributes"] = len(rows)

    # marketplace_item_categories
    cur.execute("""
        SELECT id, item_id, category_id, created_at
        FROM marketplace_item_categories
        WHERE deleted_at IS NULL
        ORDER BY item_id
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    _write_csv(out_dir / "marketplace_item_categories.csv", cols, rows)
    counts["marketplace_item_categories"] = len(rows)

    # marketplace_attribute_values
    cur.execute("""
        SELECT id, item_id, attribute_id, attribute_unit_id, value
        FROM marketplace_attribute_values
        WHERE deleted_at IS NULL
        ORDER BY item_id
    """)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    _write_csv(out_dir / "marketplace_attribute_values.csv", cols, rows)
    counts["marketplace_attribute_values"] = len(rows)

    cur.close()
    return counts


def get_gap_counts(conn) -> dict[str, int]:
    """
    Query DB for uncategorized and missing-attribute item counts.
    Returns {"total": N, "uncategorized": X, "missing_attrs": Y}
    """
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM items WHERE deleted_at IS NULL")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM items i
        WHERE i.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM marketplace_item_categories mic
              WHERE mic.item_id = i.id AND mic.deleted_at IS NULL
          )
    """)
    uncategorized = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(DISTINCT mic.item_id)
        FROM marketplace_item_categories mic
        WHERE mic.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM marketplace_attribute_values mav
              WHERE mav.item_id = mic.item_id AND mav.deleted_at IS NULL
          )
    """)
    missing_attrs = cur.fetchone()[0]

    cur.close()
    return {"total": total, "uncategorized": uncategorized, "missing_attrs": missing_attrs}


def get_uncategorized_ids(conn) -> set[str]:
    """Return item IDs not yet in marketplace_item_categories."""
    cur = conn.cursor()
    cur.execute("""
        SELECT i.id FROM items i
        WHERE i.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM marketplace_item_categories mic
              WHERE mic.item_id = i.id AND mic.deleted_at IS NULL
          )
    """)
    ids = {row[0] for row in cur.fetchall()}
    cur.close()
    return ids


def get_missing_attrs_ids(conn) -> set[str]:
    """Return item IDs that have a category but zero attribute values."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT mic.item_id
        FROM marketplace_item_categories mic
        WHERE mic.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM marketplace_attribute_values mav
              WHERE mav.item_id = mic.item_id AND mav.deleted_at IS NULL
          )
    """)
    ids = {row[0] for row in cur.fetchall()}
    cur.close()
    return ids


def get_taxonomy_gaps(conn) -> dict:
    """
    Compare local proposed-*.json against DB.
    Returns counts and sample lists of missing categories, attributes, and units.
    """
    cur = conn.cursor()

    with SUBCATEGORIES_JSON.open(encoding="utf-8") as f:
        subcategories = json.load(f)
    with ATTRIBUTES_JSON.open(encoding="utf-8") as f:
        attributes = json.load(f)

    # Check categories — compare by leaf slug
    category_nodes = build_category_nodes(subcategories)
    local_slugs = {n[0].split("/")[-1] for n in category_nodes}
    cur.execute("SELECT slug FROM marketplace_categories WHERE deleted_at IS NULL")
    db_slugs = {row[0] for row in cur.fetchall() if row[0]}
    missing_cat_slugs = sorted(local_slugs - db_slugs)

    # Check units
    unit_rows = build_unit_rows(attributes)
    local_symbols = {u["symbol"] for u in unit_rows}
    cur.execute("SELECT symbol FROM marketplace_attribute_units WHERE deleted_at IS NULL")
    db_symbols = {row[0] for row in cur.fetchall()}
    missing_unit_symbols = sorted(local_symbols - db_symbols)

    # Check attributes — compare by (leaf_slug, key)
    category_attrs = attributes.get("_category_attributes") or {}
    # local: use leaf slug (last path segment) for comparison
    local_keys = {
        (path.split("/")[-1], a["key"]): path
        for path, attrs in category_attrs.items()
        for a in attrs
    }
    cur.execute("""
        SELECT mc.slug, ma.key
        FROM marketplace_attributes ma
        JOIN marketplace_categories mc ON mc.id = ma.category_id
        WHERE ma.deleted_at IS NULL
    """)
    db_keys = {(row[0], row[1]) for row in cur.fetchall()}
    missing_attr_pairs = sorted(
        f"{local_keys[k]}/{k[1]}" for k in local_keys if k not in db_keys
    )

    cur.close()
    return {
        "missing_categories": len(missing_cat_slugs),
        "missing_attributes": len(missing_attr_pairs),
        "missing_units": len(missing_unit_symbols),
        "_missing_cat_slugs": missing_cat_slugs,
        "_missing_attr_pairs": missing_attr_pairs,
        "_missing_unit_symbols": missing_unit_symbols,
    }


# ──────────────────────────────────────────────────────────────────────────────
# File write helpers
# ──────────────────────────────────────────────────────────────────────────────


def _write_csv(path: Path, cols: list[str], rows: list) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        writer.writerows(rows)


def _write_json(path: Path, cols: list[str], rows: list) -> None:
    data = [dict(zip(cols, row)) for row in rows]
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
