#!/usr/bin/env python3
"""
workflow.py — Full categorization workflow orchestrator.

Decision tree:
  1. Select environment: dev or prod
  2. Select operation: download or upload

  DOWNLOAD path:
    - Prompt for DB credentials
    - Pull all 6 tables into {env}/downloaded/{timestamp}/
    - Display gap analysis (uncategorized count, missing-attrs count)
    - Optionally: categorize uncategorized items via LLM pipeline
      - Check / prompt for OPENAI_API_KEY
      - Show cost estimate
      - Run step0 (map-to-leaf) then step1 (LLM phases 1+2) for delta items only
      - Output to {env}/output/{timestamp}/
    - Optionally: push the new output to the DB

  UPLOAD path:
    - Prompt for DB credentials
    - Prompt for output folder path (or list available timestamped folders)
    - Check taxonomy sync (categories/attributes/units) and offer to push if gaps found
    - Run step2 validation; warn on issues, allow user to abort
    - Push item-category and attribute-value rows (dry-run or live)
"""

from __future__ import annotations

import getpass
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ── Load .env if present ───────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent

_env_file = ROOT / ".env"
if _env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(_env_file)
    except ImportError:
        # Fallback: parse manually
        for _line in _env_file.read_text().splitlines():
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                if _v and not os.environ.get(_k.strip()):
                    os.environ[_k.strip()] = _v.strip()

# ── Helpers ────────────────────────────────────────────────────────────────────


def _hr(char: str = "─", width: int = 60) -> None:
    print(char * width)


def _section(title: str) -> None:
    print()
    _hr()
    print(f"  {title}")
    _hr()


def _ask(prompt: str, options: list[str]) -> str:
    """Prompt user to pick one of a numbered list; return the chosen option string."""
    while True:
        print()
        print(prompt)
        for i, opt in enumerate(options, 1):
            print(f"  [{i}] {opt}")
        raw = input("  → ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            chosen = options[int(raw) - 1]
            print(f"  Selected: {chosen}")
            return chosen
        print("  Invalid selection — please enter a number from the list.")


def _confirm(prompt: str, default_yes: bool = False) -> bool:
    hint = " [Y/n]" if default_yes else " [y/N]"
    while True:
        raw = input(f"  {prompt}{hint}: ").strip().lower()
        if raw == "" and default_yes:
            return True
        if raw == "" and not default_yes:
            return False
        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False
        print("  Please enter y or n.")


def _prompt_credentials() -> tuple[str, str]:
    env_user = os.environ.get("DB_USER", "")
    env_pass = os.environ.get("DB_PASSWORD", "")

    if env_user and env_pass:
        print(f"  DB credentials loaded from .env (user: {env_user})")
        return env_user, env_pass

    user = input(f"  DB username [{env_user}]: ").strip() or env_user
    password = getpass.getpass("  DB password: ") or env_pass
    return user, password


def _get_db_config(env: str) -> dict:
    import step3_import_marketplace as s3
    return s3.DEV_DB_CONFIG if env == "dev" else s3.PROD_DB_CONFIG


def _connect(env: str, user: str, password: str):
    import step3_import_marketplace as s3
    cfg = _get_db_config(env)
    try:
        conn = s3.make_connection(cfg, user, password)
        return conn
    except Exception as e:
        print(f"\n  ERROR: Could not connect to {env} database: {e}")
        print("  Make sure the SSM tunnel is running (ruck-db-staging / ruck-db-prod).")
        raise


def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _find_resumable_output(output_base: Path) -> "Path | None":
    """
    Return the most recent output folder that has batch files but no completed
    verified-mapping.csv — i.e. an in-progress run that can be resumed.
    Returns None if no such folder exists.
    """
    if not output_base.exists():
        return None
    for folder in sorted(output_base.iterdir(), reverse=True):
        if not folder.is_dir():
            continue
        # Has at least one phase1 batch file
        phase1_dir = folder / "phase1"
        has_batches = phase1_dir.exists() and any(phase1_dir.glob("batch_*.json"))
        # verified-mapping.csv either missing or empty
        vm = folder / "verified-mapping.csv"
        vm_incomplete = not vm.exists() or vm.stat().st_size < 10
        if has_batches and vm_incomplete:
            return folder
    return None


def _list_timestamped_dirs(base: Path) -> list[Path]:
    if not base.exists():
        return []
    return sorted(
        (p for p in base.iterdir() if p.is_dir()),
        reverse=True,
    )


def _check_openai_key() -> str | None:
    key = os.environ.get("OPENAI_API_KEY")
    if key:
        return key
    print()
    print("  OPENAI_API_KEY is not set in your environment.")
    if _confirm("Paste your API key now?", default_yes=True):
        key = getpass.getpass("  Paste OPENAI_API_KEY: ").strip()
        if key:
            os.environ["OPENAI_API_KEY"] = key
            return key
    return None


def _estimate_llm_cost(n_uncategorized: int, n_missing_attrs: int) -> None:
    """Rough cost estimate printed to terminal."""
    from pipeline_config import BATCH_SIZE
    p1_batches = (n_uncategorized + BATCH_SIZE - 1) // BATCH_SIZE if n_uncategorized else 0
    p2_batches = (n_missing_attrs + BATCH_SIZE - 1) // BATCH_SIZE if n_missing_attrs else 0
    # gpt-4o-mini: ~$0.0002/1K input + $0.0008/1K output (rough per-batch estimate of ~1K tokens each)
    p1_cost = p1_batches * 0.0004
    p2_cost = p2_batches * 0.0008
    print()
    print("  Estimated LLM cost (gpt-4o-mini):")
    print(f"    Phase 1 ({p1_batches} batches × ~{BATCH_SIZE} items): ~${p1_cost:.4f}")
    print(f"    Phase 2 ({p2_batches} batches × ~{BATCH_SIZE} items): ~${p2_cost:.4f}")
    print(f"    Total estimate: ~${p1_cost + p2_cost:.4f}")
    print("  (Batches already completed are skipped automatically.)")


# ── Download path ──────────────────────────────────────────────────────────────


def run_download(env: str) -> None:
    _section(f"DOWNLOAD — {env.upper()} database")

    print()
    user, password = _prompt_credentials()

    print("\n  Connecting…")
    conn = _connect(env, user, password)

    # ── Pull all 6 tables ──────────────────────────────────────────────────────
    ts = _timestamp()
    download_dir = ROOT / env / "downloaded" / ts
    print(f"\n  Pulling all tables into {download_dir.relative_to(ROOT)} …")

    import step3_import_marketplace as s3
    counts = s3.pull_all(conn, download_dir)

    print()
    for table, n in counts.items():
        print(f"    {table}: {n:,} rows")
    print(f"\n  Saved to: {download_dir}")

    # ── Gap analysis ──────────────────────────────────────────────────────────
    print("\n  Analyzing gaps…")
    gaps = s3.get_gap_counts(conn)
    print()
    print(f"    Total items:       {gaps['total']:,}")
    print(f"    Uncategorized:     {gaps['uncategorized']:,}  (not in marketplace_item_categories)")
    print(f"    Missing all attrs: {gaps['missing_attrs']:,}  (have category but no attribute values)")

    if gaps["uncategorized"] == 0 and gaps["missing_attrs"] == 0:
        print("\n  All items are categorized and have attributes. Nothing to do.")
        conn.close()
        return

    # ── Offer to run LLM pipeline ──────────────────────────────────────────────
    n_to_process = gaps["uncategorized"] + gaps["missing_attrs"]
    print()
    if not _confirm(
        f"Run LLM pipeline to categorize {n_to_process} items "
        f"({gaps['uncategorized']} uncategorized + {gaps['missing_attrs']} missing attrs)?",
        default_yes=False,
    ):
        print("\n  Skipping LLM pipeline. Run `python workflow.py` again when ready.")
        conn.close()
        return

    # ── OpenAI key check ──────────────────────────────────────────────────────
    api_key = _check_openai_key()
    if not api_key:
        print("\n  Cannot run LLM pipeline without OPENAI_API_KEY. Aborting.")
        conn.close()
        return

    # ── Cost estimate ─────────────────────────────────────────────────────────
    _estimate_llm_cost(gaps["uncategorized"], gaps["missing_attrs"])
    if not _confirm("Proceed with LLM pipeline?", default_yes=True):
        print("\n  LLM pipeline cancelled.")
        conn.close()
        return

    # ── Get item ID sets from DB ───────────────────────────────────────────────
    print("\n  Fetching uncategorized item IDs from DB…")
    uncategorized_ids = s3.get_uncategorized_ids(conn)
    missing_attrs_ids = s3.get_missing_attrs_ids(conn)
    conn.close()

    # ── Check for an in-progress output folder to resume ─────────────────────
    output_base = ROOT / env / "output"
    output_dir = _find_resumable_output(output_base)

    if output_dir:
        p1_done = len(list((output_dir / "phase1").glob("batch_*.json"))) if (output_dir / "phase1").exists() else 0
        p2_done = len(list((output_dir / "phase2").glob("batch_*.json"))) if (output_dir / "phase2").exists() else 0
        print(f"\n  Found in-progress run: {output_dir.name}")
        print(f"    Phase 1 batches completed: {p1_done}")
        print(f"    Phase 2 batches completed: {p2_done}")
        if not _confirm("Resume this run?", default_yes=True):
            output_dir = None

    if output_dir is None:
        output_ts = _timestamp()
        output_dir = output_base / output_ts
        output_dir.mkdir(parents=True, exist_ok=True)

        filter_file = output_dir / "uncategorized_ids.txt"
        attrs_filter_file = output_dir / "missing_attrs_ids.txt"
        filter_file.write_text("\n".join(sorted(uncategorized_ids)), encoding="utf-8")
        attrs_filter_file.write_text("\n".join(sorted(missing_attrs_ids)), encoding="utf-8")
        print(f"  {len(uncategorized_ids)} uncategorized IDs → {filter_file.relative_to(ROOT)}")
        print(f"  {len(missing_attrs_ids)} missing-attrs IDs → {attrs_filter_file.relative_to(ROOT)}")
    else:
        # Reload filter files from the existing run
        filter_file = output_dir / "uncategorized_ids.txt"
        attrs_filter_file = output_dir / "missing_attrs_ids.txt"
        if filter_file.exists():
            uncategorized_ids = {l.strip() for l in filter_file.read_text().splitlines() if l.strip()}
        if attrs_filter_file.exists():
            missing_attrs_ids = {l.strip() for l in attrs_filter_file.read_text().splitlines() if l.strip()}
        print(f"  Resuming with {len(uncategorized_ids)} uncategorized + {len(missing_attrs_ids)} missing-attrs IDs")

    # ── Step 0: map-to-leaf for uncategorized items ───────────────────────────
    if uncategorized_ids:
        items_source = ROOT / env / "downloaded" / ts / "items.csv"
        if not items_source.exists():
            # Fall back to the manually downloaded file
            items_source = ROOT / env / "downloaded" / "items-with-store-data-2.csv"

        if not items_source.exists():
            print(f"\n  WARNING: No items source CSV found at {items_source}.")
            print("  Cannot run step0 without item source. Skipping step0/phase1.")
        else:
            leaf_mapping_csv = output_dir / "items-leaf-mapping.csv"
            print(f"\n  Running step0 (map-to-leaf) for {len(uncategorized_ids)} items…")
            ret = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "step0_map_to_leaf.py"),
                    "--input", str(items_source),
                    "--output", str(leaf_mapping_csv),
                    "--filter-file", str(filter_file),
                ],
                cwd=ROOT,
            )
            if ret.returncode != 0:
                print("\n  step0 failed — check output above.")
                return

    # ── Step 1: LLM phase 1 + 2 ──────────────────────────────────────────────
    import pipeline_config
    pipeline_config.ENV = env
    pipeline_config.OUTPUT_TIMESTAMP = output_ts

    # Override module-level path constants so step1 uses our output dir
    import step1_run_llm_pipeline as s1
    s1.ITEMS_LEAF_MAPPING_CSV = output_dir / "items-leaf-mapping.csv"
    s1.VERIFIED_MAPPING_CSV = output_dir / "verified-mapping.csv"
    s1.ATTRIBUTE_VALUES_CSV = output_dir / "attribute-values.csv"
    s1.PHASE1_DIR = output_dir / "phase1"
    s1.PHASE2_DIR = output_dir / "phase2"

    from openai import OpenAI
    client = OpenAI(api_key=api_key)
    model = pipeline_config.DEFAULT_MODEL

    leaf_mapping_csv = output_dir / "items-leaf-mapping.csv"
    if uncategorized_ids and leaf_mapping_csv.exists():
        # Check the leaf mapping actually has rows
        import csv as _csv
        with leaf_mapping_csv.open(newline="", encoding="utf-8") as _f:
            leaf_row_count = sum(1 for _ in _csv.DictReader(_f))

        if leaf_row_count == 0:
            print("\n  step0 produced 0 mapped rows — no items could be matched to the taxonomy.")
            print("  This usually means the items source CSV has no matching content.")
            print("  Check the downloaded items.csv and re-run.")
            conn.close() if hasattr(conn, 'closed') else None
            return

        print(f"\n  Running LLM phase 1 (categorization) for {len(uncategorized_ids)} items…")
        s1.run_phase1(client, model, filter_ids=uncategorized_ids)
        s1.merge_phase1()

    all_attrs_ids = uncategorized_ids | missing_attrs_ids
    if all_attrs_ids:
        print(f"\n  Running LLM phase 2 (attribute extraction) for {len(all_attrs_ids)} items…")
        s1.run_phase2(client, model, filter_ids=all_attrs_ids)
        s1.merge_phase2()

    print(f"\n  LLM pipeline complete. Output: {output_dir}")

    # ── Check if there's anything to upload ──────────────────────────────────
    import csv as _csv
    vm_csv = output_dir / "verified-mapping.csv"
    vm_rows = 0
    if vm_csv.exists():
        with vm_csv.open(newline="", encoding="utf-8") as _f:
            vm_rows = sum(1 for _ in _csv.DictReader(_f))

    if vm_rows == 0:
        print("\n  No categorized rows were produced — nothing to upload. Exiting.")
        return

    # ── Offer to upload ───────────────────────────────────────────────────────
    print()
    if _confirm("Upload the new output to the database now?", default_yes=False):
        _do_upload(env, output_dir, user, password)
    else:
        print(f"\n  Output is at: {output_dir}")
        print("  Run `python workflow.py` and choose Upload to push it when ready.")


# ── Upload path ────────────────────────────────────────────────────────────────


def _do_upload(
    env: str,
    output_dir: Path,
    db_user: str | None = None,
    db_password: str | None = None,
    dry_run: bool | None = None,
) -> None:
    """Core upload logic, called from either the download continuation or standalone upload."""
    _section(f"UPLOAD — {env.upper()} database")

    # ── Confirm dry/live ──────────────────────────────────────────────────────
    if dry_run is None:
        dry_run = not _confirm("Write to the database? (No = dry-run preview)", default_yes=False)

    mode_label = "DRY RUN (no writes)" if dry_run else "LIVE RUN (writing to DB)"
    print(f"\n  Mode: {mode_label}")

    # ── Credentials ───────────────────────────────────────────────────────────
    if not db_user:
        db_user, db_password = _prompt_credentials()

    # ── Validate output files exist ───────────────────────────────────────────
    vm_csv = output_dir / "verified-mapping.csv"
    av_csv = output_dir / "attribute-values.csv"

    missing_files = [p for p in [vm_csv, av_csv] if not p.exists()]
    if missing_files:
        print("\n  ERROR: Required output files not found:")
        for p in missing_files:
            print(f"    {p}")
        return

    # ── Step 2: Validation ────────────────────────────────────────────────────
    print("\n  Running step2 validation…")
    from step2_validate_outputs import run_validation
    issues = run_validation(
        verified_mapping_csv=vm_csv,
        attribute_values_csv=av_csv,
    )

    if issues:
        print(f"\n  Validation found {len(issues)} issue(s):")
        for issue in issues[:30]:
            print(f"    - {issue}")
        if len(issues) > 30:
            print(f"    … and {len(issues) - 30} more")
        print()
        if not _confirm("Continue with upload despite validation issues?", default_yes=False):
            print("  Upload cancelled. Fix issues and re-run.")
            return
    else:
        print("  Validation passed.")

    # ── Taxonomy sync check ───────────────────────────────────────────────────
    import step3_import_marketplace as s3
    if not dry_run:
        print("\n  Checking taxonomy sync…")
        conn = _connect(env, db_user, db_password)
        try:
            tax_gaps = s3.get_taxonomy_gaps(conn)
        finally:
            conn.close()

        if any(v for k, v in tax_gaps.items() if not k.startswith("_")):
            # Build item-count-per-leaf-slug from verified-mapping
            import csv as _csv
            from collections import Counter as _Counter
            items_per_slug: "_Counter[str]" = _Counter()
            if vm_csv.exists():
                with vm_csv.open(newline="", encoding="utf-8") as _f:
                    for row in _csv.DictReader(_f):
                        path = (row.get("category_path") or "").strip()
                        if path:
                            items_per_slug[path.split("/")[-1]] += 1

            print()
            print("  Taxonomy gaps found in DB:")
            if tax_gaps["missing_categories"]:
                total_items_affected = sum(
                    items_per_slug.get(s, 0) for s in tax_gaps["_missing_cat_slugs"]
                )
                suffix = (
                    f"  ({total_items_affected} items tied to these categories)"
                    if total_items_affected
                    else "  (no items tied to these categories)"
                )
                print(f"    Categories missing: {tax_gaps['missing_categories']}{suffix}")
            if tax_gaps["missing_attributes"]:
                print(f"    Attributes missing: {tax_gaps['missing_attributes']}")
            if tax_gaps["missing_units"]:
                print(f"    Units missing:      {tax_gaps['missing_units']}")

            # Offer previews
            if tax_gaps["missing_categories"] and _confirm(
                "Preview missing categories (top 25)?", default_yes=False
            ):
                for slug in tax_gaps["_missing_cat_slugs"][:25]:
                    ref_count = items_per_slug.get(slug, 0)
                    suffix = (
                        f"  ({ref_count} items tied to this category)"
                        if ref_count
                        else "  (no items tied)"
                    )
                    print(f"      - {slug}{suffix}")
                if len(tax_gaps["_missing_cat_slugs"]) > 25:
                    print(f"      … and {len(tax_gaps['_missing_cat_slugs']) - 25} more")

            if tax_gaps["missing_attributes"] and _confirm(
                "Preview missing attributes (top 25)?", default_yes=False
            ):
                for item in tax_gaps["_missing_attr_pairs"][:25]:
                    print(f"      - {item}")
                if len(tax_gaps["_missing_attr_pairs"]) > 25:
                    print(f"      … and {len(tax_gaps['_missing_attr_pairs']) - 25} more")

            if tax_gaps["missing_units"] and _confirm(
                "Preview missing units (top 25)?", default_yes=False
            ):
                for item in tax_gaps["_missing_unit_symbols"][:25]:
                    print(f"      - {item}")

            print()
            if _confirm("Sync missing taxonomy to DB now? (No = skip and continue with upload)", default_yes=False):
                conn = _connect(env, db_user, db_password)
                try:
                    s3.push_taxonomy(conn, scope="all")
                    conn.commit()
                    print("  Taxonomy synced.")
                finally:
                    conn.close()
            else:
                print("  Skipping taxonomy sync — items referencing missing categories may fail FK checks.")
        else:
            print("  Taxonomy is in sync.")

    # ── Push item relationships ───────────────────────────────────────────────
    if dry_run:
        print("\n  [DRY RUN] Would push:")
        import csv as _csv
        with vm_csv.open(newline="", encoding="utf-8") as f:
            rows = list(_csv.DictReader(f))
        print(f"    {len(rows)} item-category rows from {vm_csv.name}")
        with av_csv.open(newline="", encoding="utf-8") as f:
            rows = list(_csv.DictReader(f))
        print(f"    {len(rows)} attribute-value rows from {av_csv.name}")
        print("\n  Dry run complete — no changes made.")
        return

    print("\n  Pushing item relationships to DB…")
    conn = _connect(env, db_user, db_password)
    try:
        s3.push_item_relationships(conn, vm_csv, av_csv)
        conn.commit()
        print("\n  Upload complete.")
    except Exception as e:
        conn.rollback()
        print(f"\n  ERROR during upload: {e}")
        print("  Transaction rolled back.")
        raise
    finally:
        conn.close()


def run_upload(env: str) -> None:
    _section(f"UPLOAD — {env.upper()} database")

    # ── Select output folder ──────────────────────────────────────────────────
    output_base = ROOT / env / "output"
    available = _list_timestamped_dirs(output_base)

    if not available:
        print(f"\n  No output folders found in {output_base.relative_to(ROOT)}.")
        print("  Run the Download + categorize workflow first to generate output.")
        return

    print("\n  Available output folders:")
    options = [
        f"{p.name} (latest)" if i == 0 else p.name
        for i, p in enumerate(available)
    ]
    chosen_name = _ask("Select output folder to upload:", options)
    chosen_name = chosen_name.replace(" (latest)", "")
    output_dir = output_base / chosen_name

    _do_upload(env, output_dir)


# ── Main menu ──────────────────────────────────────────────────────────────────


def main() -> None:
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║       Ruck Marketplace Categorization Workflow           ║")
    print("╚══════════════════════════════════════════════════════════╝")

    env = _ask("Target database:", ["dev (staging)", "prod"])
    env = "dev" if env.startswith("dev") else "prod"

    operation = _ask("Operation:", ["download (pull data from DB)", "upload (push data to DB)"])
    operation = "download" if operation.startswith("download") else "upload"

    if operation == "download":
        run_download(env)
    else:
        run_upload(env)


if __name__ == "__main__":
    main()
