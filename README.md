# ruck-bulk-categorizer

Bulk categorization and attribute extraction tooling for the Ruck marketplace.

Pulls items from the database, runs an LLM pipeline to assign leaf categories and extract attribute values, then pushes the results back.

---

## Setup

### 1. Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Credentials

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

```
DB_USER=flightcontrol
DB_PASSWORD=your_db_password
OPENAI_API_KEY=sk-...
```

### 3. SSM tunnel

The database is accessed via AWS SSM port forwarding. Add the helper to `~/.zshrc`:

```bash
ruck-db-staging() {
  aws sso login --profile ruck-sso
  aws ssm start-session \
    --profile ruck-sso \
    --region us-west-2 \
    --target <BASTION_INSTANCE_ID> \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters '{
      "host": ["<DEV_RDS_HOST>"],
      "portNumber": ["5432"],
      "localPortNumber": ["15432"]
    }'
}
```

Run `ruck-db-staging` in a separate terminal before using this tool.

---

## Usage

```bash
source .venv/bin/activate
python workflow.py
```

The interactive menu will guide you through:

1. **Select environment** — `dev` or `prod`
2. **Select operation** — `download` or `upload`

### Download path

- Pulls all 6 marketplace tables into `{env}/downloaded/{timestamp}/`
- Shows gap analysis (uncategorized items, missing attributes)
- Optionally runs the LLM pipeline for uncategorized items only
- Optionally uploads results to the DB

### Upload path

- Select a timestamped output folder to push
- Checks taxonomy sync (categories, attributes, units) against DB
- Runs validation (`step2`) before writing
- Dry-run mode available

---

## Inspection queries

Copy-paste SQL queries for visually inspecting the data are in [`queries.sql`](queries.sql).

Queries included:
1. Category tree (full hierarchy with depth)
2. Leaf categories with item + attribute counts
3. Parent/non-leaf categories
4. Items per leaf category sorted by count
5. Items with their assigned categories
6. Items with NO category assigned
7. Items with categories AND attribute values
8. Items missing all attribute values
9. All attributes per category
10. Attribute units
11. Category → attributes → units (full taxonomy)
12. Attribute value distribution for a single key
13. Most common attribute values across all items
14. Coverage — how many items have each attribute filled in
15. Items with a specific category path
16. Search items by title keyword
17. Summary dashboard (counts of everything)

---

## Script reference

| Script | Purpose |
|---|---|
| `workflow.py` | Main entry point — interactive orchestrator |
| `step0_map_to_leaf.py` | Rule-based mapping of items to leaf categories |
| `step1_run_llm_pipeline.py` | LLM categorization (phase 1) and attribute extraction (phase 2) |
| `step2_validate_outputs.py` | Pre-import data validation |
| `step3_import_marketplace.py` | DB push/pull module (called by workflow.py) |
| `pipeline_config.py` | Path constants and config |
| `pipeline_data.py` | Data loading utilities for the pipeline |

## Taxonomy source files

| File | Purpose |
|---|---|
| `proposed-subcategories.json` | Category hierarchy (tier1 / tier2 / tier3) — source of truth |
| `proposed-attributes.json` | Attribute definitions per category path — source of truth |

---

## Folder structure

```
.
├── workflow.py
├── step0_map_to_leaf.py
├── step1_run_llm_pipeline.py
├── step2_validate_outputs.py
├── step3_import_marketplace.py
├── pipeline_config.py
├── pipeline_data.py
├── proposed-subcategories.json
├── proposed-attributes.json
├── .env.example
├── dev/
│   ├── downloaded/{timestamp}/   ← pulled from dev DB
│   └── output/{timestamp}/       ← LLM pipeline output for dev
└── prod/
    ├── downloaded/{timestamp}/   ← pulled from prod DB
    └── output/{timestamp}/       ← LLM pipeline output for prod
```

`dev/` and `prod/` are gitignored — data stays local.
