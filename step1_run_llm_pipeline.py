#!/usr/bin/env python3
"""
step1_run_llm_pipeline.py

Resumable two-phase LLM pipeline:
  Phase 1: Verify/re-assign item categories (batch by item, skip if batch file exists).
  Phase 2: Extract attribute values per item (batch by category_path, skip if batch file exists).

Usage:
  python step1_run_llm_pipeline.py --phase 1        # run phase 1 only
  python step1_run_llm_pipeline.py --phase 2        # run phase 2 only (needs verified-mapping.csv)
  python step1_run_llm_pipeline.py --merge-only 1   # merge phase 1 batch files only
  python step1_run_llm_pipeline.py --merge-only 2   # merge phase 2 batch files only
  python step1_run_llm_pipeline.py                  # run phase 1, merge 1, phase 2, merge 2

  # Delta mode (only process specific items):
  python step1_run_llm_pipeline.py --phase 1 --filter-file /path/to/uncategorized_ids.txt
  python step1_run_llm_pipeline.py --phase 2 --attrs-filter-file /path/to/missing_attrs_ids.txt

Requires: OPENAI_API_KEY in env. pip install openai.
"""
import argparse
import csv
import re
import json
import os
import sys
import time
from pathlib import Path
from decimal import Decimal

from tqdm import tqdm


_SEC_PER_BATCH = 20  # conservative estimate used for time display


def _sanitize(text: str) -> str:
    """Strip null bytes and control characters that break JSON serialization."""
    if not text:
        return ""
    # Remove null bytes and other ASCII control characters (except tab/newline/CR)
    return "".join(
        ch for ch in text
        if ch == "\t" or ch == "\n" or ch == "\r" or (ord(ch) >= 32 and ord(ch) != 127)
    )


def _fmt_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    m, s = divmod(seconds, 60)
    if m < 60:
        return f"{m}m {s}s"
    h, m = divmod(m, 60)
    return f"{h}h {m}m"


def _progress(phase_label: str, total: int, current: int, status: str = "", pbar=None) -> None:
    """Update tqdm bar description if available, else write a self-rewriting line."""
    if pbar is not None:
        pbar.set_description(f"  {phase_label} {status}")
        return
    pct = 100 * current // total if total else 0
    if status:
        line = f"\r  {phase_label} {status} — {current}/{total} ({pct}%)"
    else:
        line = f"\r  {phase_label} {current}/{total} ({pct}%)"
    sys.stdout.write(line + " " * 60)
    sys.stdout.flush()

from pipeline_config import (
    ATTRIBUTE_VALUES_CSV,
    BATCH_SIZE,
    INITIAL_BACKOFF_SEC,
    ITEMS_LEAF_MAPPING_CSV,
    MAX_BACKOFF_SEC,
    MAX_RETRIES,
    PHASE1_DIR,
    PHASE2_DIR,
    PROPOSED_ATTRIBUTES_JSON,
    PROPOSED_SUBCATEGORIES_JSON,
    THROTTLE_AFTER_SUCCESS_SEC,
    VERIFIED_MAPPING_CSV,
    DEFAULT_MODEL,
)
from pipeline_data import (
    load_category_attributes_with_inheritance,
    load_taxonomy_leaf_paths,
    taxonomy_prompt_text,
)


def _ensure_dirs():
    PHASE1_DIR.mkdir(parents=True, exist_ok=True)
    PHASE2_DIR.mkdir(parents=True, exist_ok=True)
    VERIFIED_MAPPING_CSV.parent.mkdir(parents=True, exist_ok=True)
    ATTRIBUTE_VALUES_CSV.parent.mkdir(parents=True, exist_ok=True)


def _normalize_divisions_for_json(text: str) -> str:
    """
    The model sometimes outputs numeric JSON values as arithmetic expressions, e.g.:
      "thickness": 5/8
      "blade_width": 18 / 25.4
    Those are not valid JSON numbers, so json.loads fails.

    This rewrites simple <number> / <number> tokens into a decimal number.
    It only rewrites cases that appear immediately after a JSON colon.
    """
    # Match: : <number> / <number>
    pattern = re.compile(r'(:\s*)(-?\d+(?:\.\d+)?)\s*/\s*(-?\d+(?:\.\d+)?)')

    def repl(match: re.Match) -> str:
        prefix = match.group(1)
        a = Decimal(match.group(2))
        b = Decimal(match.group(3))
        if b == 0:
            return match.group(0)
        val = a / b
        # Format without scientific notation and trim trailing zeros.
        sval = format(val, "f")
        if "." in sval:
            sval = sval.rstrip("0").rstrip(".")
        return prefix + sval

    return pattern.sub(repl, text)


# ---------- Phase 1: Categorization ----------


def load_items_grouped_by_id(csv_path: Path) -> list[dict]:
    """Load CSV and return one dict per unique item_id with name, description, store_name, list of category_paths."""
    by_id: dict[str, dict] = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            iid = (row.get("item_id") or "").strip()
            if not iid:
                continue
            if iid not in by_id:
                by_id[iid] = {
                    "item_id": iid,
                    "item_name": (row.get("item_name") or "").strip(),
                    "item_description": (row.get("item_description") or "").strip(),
                    "store_name": (row.get("store_name") or "").strip(),
                    "category_paths": [],
                }
            path = (row.get("category_path") or "").strip()
            if path and path not in by_id[iid]["category_paths"]:
                by_id[iid]["category_paths"].append(path)
    # Sort by item_id for deterministic batching
    return [by_id[k] for k in sorted(by_id)]


def run_phase1(client, model: str, filter_ids: "set[str] | None" = None) -> None:
    _ensure_dirs()
    print("Phase 1: Loading items and taxonomy...", flush=True)
    all_items = load_items_grouped_by_id(ITEMS_LEAF_MAPPING_CSV)
    if filter_ids is not None:
        items = [it for it in all_items if it["item_id"] in filter_ids]
        print(f"Phase 1: delta mode — {len(items)}/{len(all_items)} items after filter")
    else:
        items = all_items
    leaf_paths = load_taxonomy_leaf_paths(PROPOSED_SUBCATEGORIES_JSON)
    taxonomy_text = taxonomy_prompt_text(leaf_paths)
    leaf_path_slugs = [p[0] for p in leaf_paths]

    system = f"""You are a product categorization expert. Given an item's name and description (and optional store name), determine the single best leaf category from the list below.
{taxonomy_text}
Respond with a JSON array. Each element: {{ "item_id": "<id>", "confirmed": true|false, "category_paths": ["<one path from the list>"] }}.
- If the current category_paths are correct, set confirmed=true and keep category_paths as the best single path (or the same paths if multiple are valid; otherwise pick the single best).
- If wrong, set confirmed=false and set category_paths to exactly one path from the allowed list.
- Always return exactly one path in category_paths unless the product truly fits two categories; then you may return up to two. Use only paths from the list."""

    num_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE
    already_done = sum(1 for i in range(num_batches) if (PHASE1_DIR / f"batch_{i:04d}.json").exists())
    remaining = num_batches - already_done
    est_secs = remaining * _SEC_PER_BATCH

    print(f"\n  Phase 1: {len(items)} items → {num_batches} batches "
          f"({already_done} already done, {remaining} remaining)")
    print(f"  Estimated time: ~{_fmt_duration(est_secs)}")

    if remaining > 0:
        confirm = input("  Proceed with Phase 1? [Y/n]: ").strip().lower()
        if confirm in ("n", "no"):
            print("  Phase 1 skipped.")
            return

    with tqdm(
        total=num_batches,
        initial=already_done,
        desc="  Phase 1",
        unit="batch",
        bar_format="{desc}: {percentage:3.0f}%|{bar}| {n}/{total} batches [{elapsed}<{remaining}]",
        ncols=80,
    ) as pbar:
        for batch_i in range(num_batches):
            out_file = PHASE1_DIR / f"batch_{batch_i:04d}.json"
            if out_file.exists():
                continue
            pbar.set_description(f"  Phase 1 — batch {batch_i + 1}/{num_batches}")
            start = batch_i * BATCH_SIZE
            batch_items = items[start : start + BATCH_SIZE]
            user_content = json.dumps(
                [
                    {
                        "item_id": it["item_id"],
                        "item_name": _sanitize(it["item_name"]),
                        "item_description": _sanitize(it["item_description"]),
                        "store_name": _sanitize(it["store_name"]),
                        "current_category_paths": it["category_paths"],
                    }
                    for it in batch_items
                ],
                indent=2,
                ensure_ascii=True,
            )
            for attempt in range(MAX_RETRIES):
                try:
                    resp = client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": user_content},
                        ],
                        temperature=0.1,
                    )
                    text = (resp.choices[0].message.content or "").strip()
                    if not text:
                        raise ValueError("Empty response")
                    if "```" in text:
                        text = text.split("```")[1]
                        if text.startswith("json"):
                            text = text[4:]
                    parsed = json.loads(text)
                    if not isinstance(parsed, list):
                        raise ValueError("Expected JSON array")
                    for item in parsed:
                        paths = item.get("category_paths") or []
                        item["category_paths"] = [p for p in paths if p in leaf_path_slugs][:2] or (
                            [leaf_path_slugs[0]] if leaf_path_slugs else []
                        )
                    with open(out_file, "w", encoding="utf-8") as f:
                        json.dump(parsed, f, indent=2)
                    pbar.update(1)
                    time.sleep(THROTTLE_AFTER_SUCCESS_SEC)
                    break
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    raw = text if "text" in dir() else ""
                    if attempt == MAX_RETRIES - 1:
                        err_file = PHASE1_DIR / f"batch_{batch_i:04d}.error.json"
                        with open(err_file, "w", encoding="utf-8") as f:
                            json.dump({"raw": raw, "error": str(e)}, f)
                        tqdm.write(f"  Batch {batch_i} parse failed after {MAX_RETRIES} retries; see {err_file}")
                    else:
                        time.sleep(min(INITIAL_BACKOFF_SEC * (2 ** attempt), MAX_BACKOFF_SEC))
                except Exception as e:
                    backoff = min(INITIAL_BACKOFF_SEC * (2 ** attempt), MAX_BACKOFF_SEC)
                    tqdm.write(f"  Batch {batch_i} attempt {attempt + 1} error: {e}. Retry in {backoff}s")
                    time.sleep(backoff)
            else:
                tqdm.write(f"  Batch {batch_i} failed after {MAX_RETRIES} retries")

    print(f"  Phase 1 done. Batches written to {PHASE1_DIR}")


def merge_phase1() -> None:
    """Merge all phase1 batch JSONs into verified-mapping.csv (one row per item, primary category)."""
    print("Merge phase 1: reading batch files...", flush=True)
    rows: list[dict] = []
    for f in sorted(PHASE1_DIR.glob("batch_*.json")):
        if f.name.endswith(".error.json"):
            continue
        with open(f, encoding="utf-8") as fp:
            batch = json.load(fp)
        for item in batch:
            iid = item.get("item_id", "")
            paths = item.get("category_paths") or []
            primary = paths[0] if paths else ""
            rows.append({"item_id": iid, "category_path": primary})
    # Enrich with name/description from original CSV by item_id
    by_id = {r["item_id"]: r for r in rows}
    with open(ITEMS_LEAF_MAPPING_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            iid = (row.get("item_id") or "").strip()
            if iid in by_id:
                by_id[iid]["item_name"] = row.get("item_name", "")
                by_id[iid]["item_description"] = row.get("item_description", "")
                by_id[iid]["store_name"] = row.get("store_name", "")
    # Derive tier1, tier2, tier3 from category_path
    for r in rows:
        path = r.get("category_path", "")
        parts = path.split("/")
        r["tier1"] = parts[0] if len(parts) > 0 else ""
        r["tier2"] = parts[1] if len(parts) > 1 else ""
        r["tier3"] = parts[2] if len(parts) > 2 else ""
    with open(VERIFIED_MAPPING_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["item_id", "item_name", "item_description", "store_name", "category_path", "tier1", "tier2", "tier3"],
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"Merge phase 1: {len(rows)} rows -> {VERIFIED_MAPPING_CSV}")


# ---------- Phase 2: Attribute extraction ----------


def load_verified_mapping(csv_path: Path) -> list[dict]:
    with open(csv_path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run_phase2(client, model: str, filter_ids: "set[str] | None" = None) -> None:
    _ensure_dirs()
    print("Phase 2: Loading verified mapping and attributes...", flush=True)
    if not VERIFIED_MAPPING_CSV.exists():
        print("Run phase 1 and merge first to create", VERIFIED_MAPPING_CSV)
        return
    all_rows = load_verified_mapping(VERIFIED_MAPPING_CSV)
    if filter_ids is not None:
        rows = [r for r in all_rows if r.get("item_id") in filter_ids]
        print(f"Phase 2: delta mode — {len(rows)}/{len(all_rows)} items after filter")
    else:
        rows = all_rows
    path_to_attrs = load_category_attributes_with_inheritance(PROPOSED_ATTRIBUTES_JSON)
    # Skip miscellaneous and paths with no attributes
    by_path: dict[str, list[dict]] = {}
    for r in rows:
        path = (r.get("category_path") or "").strip()
        if not path or path.startswith("miscellaneous") or not path_to_attrs.get(path):
            continue
        by_path.setdefault(path, []).append(r)
    # Build deterministic batch list: (batch_index, category_path, list of item dicts)
    batches: list[tuple[int, str, list[dict]]] = []
    idx = 0
    for path in sorted(by_path.keys()):
        items_in_path = by_path[path]
        for start in range(0, len(items_in_path), BATCH_SIZE):
            chunk = items_in_path[start : start + BATCH_SIZE]
            batches.append((idx, path, chunk))
            idx += 1

    num_batches_p2 = len(batches)
    already_done_p2 = sum(1 for i, _, _ in batches if (PHASE2_DIR / f"batch_{i:05d}.json").exists())
    remaining_p2 = num_batches_p2 - already_done_p2
    est_secs_p2 = remaining_p2 * _SEC_PER_BATCH

    print(f"\n  Phase 2: {num_batches_p2} batches "
          f"({already_done_p2} already done, {remaining_p2} remaining)")
    print(f"  Estimated time: ~{_fmt_duration(est_secs_p2)}")

    if remaining_p2 > 0:
        confirm = input("  Proceed with Phase 2? [Y/n]: ").strip().lower()
        if confirm in ("n", "no"):
            print("  Phase 2 skipped.")
            return

    with tqdm(
        total=num_batches_p2,
        initial=already_done_p2,
        desc="  Phase 2",
        unit="batch",
        bar_format="{desc}: {percentage:3.0f}%|{bar}| {n}/{total} batches [{elapsed}<{remaining}]",
        ncols=80,
    ) as pbar:
        for batch_i, category_path, chunk in batches:
            out_file = PHASE2_DIR / f"batch_{batch_i:05d}.json"
            if out_file.exists():
                continue
            pbar.set_description(f"  Phase 2 — {category_path.split('/')[-1]}")
            attrs = path_to_attrs.get(category_path) or []
            attr_spec = [
                {"key": a["key"], "label": a["label"], "unit": a.get("unit_symbol") or "—", "value_type": a.get("value_type", "text")}
                for a in attrs
            ]
            system = f"""You extract product attribute values from item name and description.
Category path: {category_path}
Attributes to extract (use these keys in the output; use null if unknown):
{json.dumps(attr_spec, indent=2)}
Return a JSON array. Each element: {{ "item_id": "<id>", "attributes": {{ "<key>": <value or null>, ... }} }}.
IMPORTANT: For numeric attributes, output JSON numbers as decimals ONLY (no fractions like 5/8, no arithmetic like 18 / 25.4).
If you cannot determine the value, use null.
Use string for text attributes, boolean for boolean attributes. If unknown, use null."""

            user_content = json.dumps(
                [
                    {
                        "item_id": r["item_id"],
                        "item_name": _sanitize(r.get("item_name", "")),
                        "item_description": _sanitize(r.get("item_description", "")),
                    }
                    for r in chunk
                ],
                indent=2,
                ensure_ascii=True,
            )
            text = ""
            for attempt in range(MAX_RETRIES):
                try:
                    resp = client.chat.completions.create(
                        model=model,
                        messages=[
                            {"role": "system", "content": system},
                            {"role": "user", "content": user_content},
                        ],
                        temperature=0.1,
                    )
                    text = (resp.choices[0].message.content or "").strip()
                    if "```" in text:
                        text = text.split("```")[1]
                        if text.startswith("json"):
                            text = text[4:]
                    text = _normalize_divisions_for_json(text)
                    parsed = json.loads(text)
                    if not isinstance(parsed, list):
                        raise ValueError("Expected JSON array")
                    with open(out_file, "w", encoding="utf-8") as f:
                        json.dump(parsed, f, indent=2)
                    pbar.update(1)
                    time.sleep(THROTTLE_AFTER_SUCCESS_SEC)
                    break
                except (json.JSONDecodeError, ValueError) as e:
                    if attempt == MAX_RETRIES - 1:
                        err_file = PHASE2_DIR / f"batch_{batch_i:05d}.error.json"
                        with open(err_file, "w", encoding="utf-8") as f:
                            json.dump({"raw": text, "error": str(e)}, f)
                        tqdm.write(f"  Phase 2 batch {batch_i} parse failed after {MAX_RETRIES} retries; see {err_file}")
                    else:
                        time.sleep(min(INITIAL_BACKOFF_SEC * (2 ** attempt), MAX_BACKOFF_SEC))
                except Exception as e:
                    backoff = min(INITIAL_BACKOFF_SEC * (2 ** attempt), MAX_BACKOFF_SEC)
                    tqdm.write(f"  Phase 2 batch {batch_i} attempt {attempt + 1}: {e}. Retry in {backoff}s")
                    time.sleep(backoff)
            else:
                tqdm.write(f"  Phase 2 batch {batch_i} failed after {MAX_RETRIES} retries")

    print(f"  Phase 2 done. Batches written to {PHASE2_DIR}")


def merge_phase2() -> None:
    """Merge phase2 batch JSONs into attribute-values.csv (item_id, category_path, attribute_key, value, unit_symbol)."""
    path_to_attrs = load_category_attributes_with_inheritance(PROPOSED_ATTRIBUTES_JSON)
    out_rows: list[dict] = []
    for f in sorted(PHASE2_DIR.glob("batch_*.json")):
        if f.name.endswith(".error.json"):
            continue
        with open(f, encoding="utf-8") as fp:
            batch = json.load(fp)
        # We don't have category_path in the batch file; we need to get it from verified-mapping by item_id
        # So we need to load verified mapping and match. Actually each batch was built from one category_path.
        # We don't store category_path in the batch file. So we need to either store it in the batch file or
        # infer from which batch index we are. Batch index is global. So we need to store category_path in each batch output.
        # Simpler: when building batches we had (batch_i, path, chunk). So batch_00000.json might be path A, etc.
        # So we need to recompute the same batch order and assign path to batch index. Let me recompute.
        pass
    # Recompute batch index -> category_path
    if not VERIFIED_MAPPING_CSV.exists():
        print("No verified-mapping.csv; cannot merge phase 2")
        return
    rows = load_verified_mapping(VERIFIED_MAPPING_CSV)
    by_path: dict[str, list[dict]] = {}
    for r in rows:
        path = (r.get("category_path") or "").strip()
        if not path or path.startswith("miscellaneous") or not path_to_attrs.get(path):
            continue
        by_path.setdefault(path, []).append(r)
    print("Merge phase 2: reading batch files...", flush=True)
    batch_index_to_path = {}
    idx = 0
    for path in sorted(by_path.keys()):
        items_in_path = by_path[path]
        for start in range(0, len(items_in_path), BATCH_SIZE):
            batch_index_to_path[idx] = path
            idx += 1
    # Now read each batch file and emit rows
    for f in sorted(PHASE2_DIR.glob("batch_*.json")):
        if f.name.endswith(".error.json"):
            continue
        stem = f.stem  # batch_00000
        try:
            batch_i = int(stem.split("_")[1])
        except (IndexError, ValueError):
            continue
        category_path = batch_index_to_path.get(batch_i, "")
        with open(f, encoding="utf-8") as fp:
            batch = json.load(fp)
        for item in batch:
            iid = item.get("item_id", "")
            attrs = item.get("attributes") or {}
            for key, val in attrs.items():
                if val is None:
                    continue
                unit = ""
                for a in path_to_attrs.get(category_path, []):
                    if a.get("key") == key:
                        unit = a.get("unit_symbol") or ""
                        break
                out_rows.append({
                    "item_id": iid,
                    "category_path": category_path,
                    "attribute_key": key,
                    "value": val if not isinstance(val, (dict, list)) else json.dumps(val),
                    "unit_symbol": unit,
                })
    with open(ATTRIBUTE_VALUES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["item_id", "category_path", "attribute_key", "value", "unit_symbol"])
        writer.writeheader()
        writer.writerows(out_rows)
    print(f"Merge phase 2: {len(out_rows)} attribute value rows -> {ATTRIBUTE_VALUES_CSV}")


# ---------- CLI ----------


def _load_filter_file(path: str) -> "set[str]":
    with open(path, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def main():
    ap = argparse.ArgumentParser(description="Resumable LLM categorization + attribute extraction")
    ap.add_argument("--phase", type=int, choices=[1, 2], help="Run only phase 1 or 2")
    ap.add_argument("--merge-only", type=int, choices=[1, 2], help="Only merge phase 1 or 2 batch files")
    ap.add_argument("--model", default=DEFAULT_MODEL, help="OpenAI model (default: gpt-4o-mini)")
    ap.add_argument(
        "--filter-file",
        help="Text file with one item_id per line; phase 1 processes only these items (delta mode)",
    )
    ap.add_argument(
        "--attrs-filter-file",
        help="Text file with one item_id per line; phase 2 processes only these items (delta mode)",
    )
    args = ap.parse_args()

    if args.merge_only:
        if args.merge_only == 1:
            merge_phase1()
        else:
            merge_phase2()
        return

    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        print("Set OPENAI_API_KEY")
        return
    from openai import OpenAI
    client = OpenAI(api_key=key)

    p1_filter = _load_filter_file(args.filter_file) if args.filter_file else None
    p2_filter = _load_filter_file(args.attrs_filter_file) if args.attrs_filter_file else None

    if args.phase == 1:
        run_phase1(client, args.model, filter_ids=p1_filter)
        merge_phase1()
        return
    if args.phase == 2:
        merge_phase1()  # ensure CSV exists
        run_phase2(client, args.model, filter_ids=p2_filter)
        merge_phase2()
        return

    # Full run
    run_phase1(client, args.model, filter_ids=p1_filter)
    merge_phase1()
    run_phase2(client, args.model, filter_ids=p2_filter)
    merge_phase2()


if __name__ == "__main__":
    main()
