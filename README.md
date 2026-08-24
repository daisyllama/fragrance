# Fragrance Intelligence Platform

A personal Databricks tool for fragrance analysis, semantic search, and recommendation using vector embeddings. Run manually, notebook by notebook — no scheduled jobs or always-on infrastructure.

## Overview

This project processes fragrance data and enables similarity search through:
- Data ingestion and cleaning from a raw fragrance dataset
- Vector embeddings generation for semantic search
- Fast in-memory similarity search (NumPy, no persistent search endpoint)
- A single-URL scraper and a manual entry tool for adding new fragrances

## Project Structure

```
sniffers/
├── README.md                    # Project documentation
├── PLAN.md                      # Current state / possible next steps
├── common/                      # Shared Python logic, imported by notebooks/ AND streamlit_app/
│   ├── matching.py              # Fuzzy name search (concentration normalization, scoring)
│   ├── similarity.py            # Cosine similarity top-N (NumPy, no Spark/Streamlit deps)
│   ├── cleaning.py              # Shared regex field-extraction SQL fragment
│   ├── scraping.py              # scrape_fragrantica(url) — pure Python, no Spark
│   ├── add_fragrance.py         # SQL-connector MERGE steps for adding one fragrance
│   └── databricks_jobs.py       # Trigger a one-time Databricks job run via the Jobs API
├── data/
│   ├── raw/                     # Original source data
│   │   └── frag_raw.csv        # Raw fragrance dataset (external source, not scraped)
│   ├── processed/               # Local exports of the cleaned table (may be stale — Delta table is the source of truth)
│   │   ├── frag_cleaned_sh.csv
│   │   └── all_unique_accords_and_notes.json
│   └── archive/
│       └── frag_cleaned_ref.csv # Unrelated reference dataset (different schema), not part of the pipeline
├── notebooks/                    # Flat, numbered by pipeline order — one Databricks notebook per step
│   ├── 01_clean_from_raw.py                     # frag_raw table → fragrance_cleaned table
│   ├── 02_generate_embeddings_job.py            # Batch embedding generation
│   ├── 03_perfume_search.py                     # Fuzzy name search + NumPy similarity search
│   ├── 04_add_update_fragrance.py               # Manual paste-in add/update tool
│   └── 05_generate_embeddings_for_new_frag.py   # Incremental embedding upsert (interactive/cell-by-cell version)
├── jobs/
│   └── generate_embeddings_for_new_frag.py      # Same logic as 05, as a plain script for automated job triggering
├── sql/
│   ├── create generate_perfume_string function.dbquery.ipynb  # UDF used by clean_from_raw
│   └── cardinality_of_embeddings.dbquery.ipynb               # Data quality check
├── scraping/
│   └── notebooks/
│       └── fragrantica_scraper.ipynb  # Scrape one Fragrantica URL, add it to frag_raw
└── streamlit_app/                # Local app: search + notes + recommendations, live-queries Databricks
    ├── app.py
    └── README.md                 # Setup (SQL warehouse connection, token)
```

`legacy/` (gitignored, local only) holds superseded notebooks/data kept for reference — not part of the tracked project.

Notebooks are experimentation/troubleshooting tools run cell-by-cell in Databricks; `streamlit_app/` is the actual user-facing app. Both call the same `common/` code for anything that isn't Spark-specific (matching, similarity, cleaning SQL) rather than duplicating logic — the notebooks only add the thin Spark wrapper (`pandas_udf`, `spark.sql(...)`) around it.

## How it fits together

```
scraping/notebooks/fragrantica_scraper.ipynb   (scrape 1 URL)
notebooks/04_add_update_fragrance.py           (manual paste-in entry)
        │  both MERGE the new row into the frag_raw table (keyed by url)
        ▼
frag_raw  ──[01_clean_from_raw.py]──▶  fragrance_cleaned
        │  (full re-run recomputes                (id, name, brand, gender,
        │   the whole table from                    release_year, perfumers,
        │   frag_raw, incl. any                      accords, notes,
        │   newly added rows)                        perfume_string, url)
        ▼
02_generate_embeddings_job.py (full/batch)  /  05_generate_embeddings_for_new_frag.py (incremental, run by hand)
                                             /  jobs/generate_embeddings_for_new_frag.py (same logic, triggered as
                                                a one-time job run by streamlit_app/app.py's "Add a fragrance" tab)
        ▼
fragrance_embeddings   (id, perfume_string, embedding — one table, float32)
        ▼
03_perfume_search.py (notebook)  /  streamlit_app/app.py (local app)
        rapidfuzz name resolve  →  NumPy cosine similarity
        (load all embeddings once, one matrix multiply — no UDF-per-row,
         no persistent search endpoint to keep running/pay for)
```

## Prerequisites
- Databricks workspace with Unity Catalog enabled (tables under `fragrance_db.default`)
- `frag_raw` table loaded from `data/raw/frag_raw.csv`
- `fragrance_db.default.generate_perfume_string` UDF created (see `sql/create generate_perfume_string function.dbquery.ipynb`)
- Serverless compute or a cluster with the ML runtime

## Running the pipeline

1. `notebooks/01_clean_from_raw.py` — cleans `frag_raw` into `fragrance_cleaned`
2. `notebooks/02_generate_embeddings_job.py` — generates embeddings into `fragrance_embeddings` for any `fragrance_cleaned` rows not yet embedded
3. `notebooks/03_perfume_search.py` — search by name (fuzzy match) or by `id`, get similar fragrances back (for actual day-to-day use, use `streamlit_app/` instead — see below)

## Adding a fragrance

Two entry points, both feed the same pipeline:
- **`scraping/notebooks/fragrantica_scraper.ipynb`** — give it one Fragrantica URL, it scrapes the page and MERGEs a row into `frag_raw`
- **`notebooks/04_add_update_fragrance.py`** — paste in accords/description/gender by hand (for pages that can't be scraped, or corrections); also handles the `fragrance_cleaned` MERGE and lets you delete a test entry

After either one, run `notebooks/05_generate_embeddings_for_new_frag.py` to upsert the embedding for just the new/changed row(s) — no need to re-run the full batch job.

## Using the app

`streamlit_app/` is the actual user-facing tool: search a perfume, see its notes, get recommendations with theirs. Runs locally, queries the live Databricks tables. See `streamlit_app/README.md` for setup.

## Data Quality

`frag_raw` is scraped/sourced text, so `01_clean_from_raw.py` (and its single-row equivalents in `04_add_update_fragrance.py` / `common/add_fragrance.py`) do more than reshape columns:

- **Dedup by url, keep the best-rated duplicate.** `frag_raw` can contain more than one row per perfume (re-scrapes, re-imports). Cleaning ranks duplicates with `row_number() over (partition by url order by rating_count desc)` and keeps only `rn = 1` — the copy with the most ratings, on the theory that it's the most complete/recent scrape.
- **Structured fields are regex-extracted from free text, not stored separately in the source.** `id`, `name`, and `brand` come from the Fragrantica URL slug (`common/cleaning.py`'s `EXTRACTED_FIELDS_SQL`); `release_year` and the three note tiers (`top_notes`/`mid_notes`/`base_notes`) come from pattern-matching the prose description (e.g. `'was launched in ([0-9]{4})'`, `'(?i)top note[s]? (is|are) ([^.;]*)'`). If a description doesn't use that exact phrasing — different tense, no "launched in", notes described in an unusual way — the field silently comes back `NULL` rather than erroring. There's no fallback extraction path; a `NULL` note tier just means the perfume string has less signal for that section.
- **`gender` is inferred, not sourced.** Derived from a suffix on the scraped title (`... for women and men` / `... for women` / `... for men`); anything that doesn't match one of those three suffixes becomes `NULL`. `04_add_update_fragrance.py`'s manual-entry flow bypasses this entirely with a dropdown, since there's no title text to pattern-match against for a hand-typed entry.
- **`rating` / `rating_count` are coerced, not trusted.** `TRY_CAST(... AS DECIMAL/INT)` after stripping thousands-separator commas from `rating_count` — a malformed or missing value becomes `NULL` instead of failing the whole batch.
- **List-shaped fields go through the same bracket-strip cleanup in three separate places.** `accords` and `perfumers` arrive as Python `str(list)` text (e.g. `"['rose', 'woody']"`) from the scraper, and get turned into a plain comma-separated string with `replace(replace(replace(x, '[', ''), ']', ''), "'", "")`. This logic is duplicated across `01_clean_from_raw.py`, `common/add_fragrance.py`, and (for accords only) `common/cleaning.py`'s shared fragment — `perfumers` cleaning specifically isn't in the shared fragment because `04_add_update_fragrance.py`'s manual flow never captures perfumers at all, so folding it into the fragment would force every caller to supply a field some of them don't have.
- **`perfume_string` (the text actually embedded) is a tagged bag-of-words, not the raw description.** Each note tier and accord list is split, prefixed (`accords_`, `top_notes_`, `mid_notes_`, `base_notes_`), space-joined, and concatenated — so a `NULL`/empty tier contributes nothing rather than a stray `None` token. Two fragrances with identical notes but differently-phrased descriptions still end up with the same `perfume_string`, and thus near-identical embeddings.
- **Search-time normalization compensates for inconsistent naming, not storage.** `common/matching.py`'s `normalize_concentration` maps `eau de parfum` → `edp`, `eau de toilette` → `edt`, etc. on both the query and the stored name at search time — the underlying `fragrance_cleaned.name` values are left as scraped (mixed "EDP"/"Eau de Parfum"/"eau de toilette" formatting and all), so this is a search-side patch, not a cleaning-side fix.
- **New rows added via the scraper inherit the scraper's own gaps.** `common/scraping.py` returns the literal string `"N/A"` (or `[]` for list fields) for anything its selectors can't find on the page — a 403/anti-bot response, a redesigned page, or a genuinely missing field (e.g. no listed perfumer) all look the same downstream: a field that cleans and merges fine but carries no real data. The Streamlit "Add a fragrance" tab surfaces this as a warning at the preview step, before you confirm the write — that's the point at which bad scrapes are meant to get caught, since nothing later in the pipeline flags an `"N/A"` field.

Run `sql/cardinality_of_embeddings.dbquery.ipynb` to sanity-check that every embedding has the expected dimension.

## Notes

- **Source of truth**: the Unity Catalog Delta tables (`frag_raw`, `fragrance_cleaned`, `fragrance_embeddings`). The CSV/JSON files under `data/processed/` are local exports and may lag behind the tables — don't treat them as current without regenerating.
- **`data/archive/frag_cleaned_ref.csv`** is a different, unrelated dataset (different schema, fewer rows) kept for reference only — it's not consumed by anything here.
- **No persistent search infra**: similarity search loads embeddings into memory and computes cosine similarity with NumPy inside the notebook you're already running. There's no Vector Search endpoint to provision, sync, or pay to keep running.
