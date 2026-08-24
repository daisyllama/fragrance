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
├── data/
│   ├── raw/                     # Original source data
│   │   └── frag_raw.csv        # Raw fragrance dataset (external source, not scraped)
│   ├── processed/               # Local exports of the cleaned table (may be stale — Delta table is the source of truth)
│   │   ├── frag_cleaned_sh.csv
│   │   └── all_unique_accords_and_notes.json
│   └── archive/
│       └── frag_cleaned_ref.csv # Unrelated reference dataset (different schema), not part of the pipeline
├── notebooks/
│   ├── 01_data_processing/
│   │   └── clean_from_raw      # frag_raw table → fragrance_cleaned table
│   ├── 02_embeddings/
│   │   └── generate_embeddings_job    # Batch embedding generation
│   ├── 03_search/
│   │   └── perfume_search      # Fuzzy name search + NumPy similarity search
│   └── 04_maintenance/
│       ├── add_update_fragrance             # Manual paste-in add/update tool
│       └── generate_embeddings_for_new_frag # Incremental embedding upsert
├── sql/
│   ├── create generate_perfume_string function.dbquery.ipynb  # UDF used by clean_from_raw
│   └── cardinality_of_embeddings.dbquery.ipynb               # Data quality check
└── scraping/
    └── notebooks/
        └── fragrantica_scraper.ipynb  # Scrape one Fragrantica URL, add it to frag_raw
```

`legacy/` (gitignored, local only) holds superseded notebooks/data kept for reference — not part of the tracked project.

## How it fits together

```
scraping/notebooks/fragrantica_scraper.ipynb   (scrape 1 URL)
notebooks/04_maintenance/add_update_fragrance.py   (manual paste-in entry)
        │  both MERGE the new row into the frag_raw table (keyed by url)
        ▼
frag_raw  ──[clean_from_raw.py]──▶  fragrance_cleaned
        │  (full re-run recomputes                    (id, name, brand, gender,
        │   the whole table from                        release_year, perfumers,
        │   frag_raw, incl. any                          accords, notes,
        │   newly added rows)                            perfume_string, url)
        ▼
generate_embeddings_job.py (full/batch)  /  generate_embeddings_for_new_frag.py (incremental upsert)
        ▼
fragrance_embeddings   (id, perfume_string, embedding — one table, float32)
        ▼
perfume_search.py:  rapidfuzz name resolve  →  NumPy cosine similarity
                     (load all embeddings once, one matrix multiply — no UDF-per-row,
                      no persistent search endpoint to keep running/pay for)
```

## Prerequisites
- Databricks workspace with Unity Catalog enabled (tables under `fragrance_db.default`)
- `frag_raw` table loaded from `data/raw/frag_raw.csv`
- `fragrance_db.default.generate_perfume_string` UDF created (see `sql/create generate_perfume_string function.dbquery.ipynb`)
- Serverless compute or a cluster with the ML runtime

## Running the pipeline

1. `notebooks/01_data_processing/clean_from_raw` — cleans `frag_raw` into `fragrance_cleaned`
2. `notebooks/02_embeddings/generate_embeddings_job` — generates embeddings into `fragrance_embeddings` for any `fragrance_cleaned` rows not yet embedded
3. `notebooks/03_search/perfume_search` — search by name (fuzzy match) or by `id`, get similar fragrances back

## Adding a fragrance

Two entry points, both feed the same pipeline:
- **`scraping/notebooks/fragrantica_scraper.ipynb`** — give it one Fragrantica URL, it scrapes the page and MERGEs a row into `frag_raw`
- **`notebooks/04_maintenance/add_update_fragrance`** — paste in accords/description/gender by hand (for pages that can't be scraped, or corrections); also handles the `fragrance_cleaned` MERGE and lets you delete a test entry

After either one, run `notebooks/04_maintenance/generate_embeddings_for_new_frag` to upsert the embedding for just the new/changed row(s) — no need to re-run the full batch job.

## Data Quality

Run `sql/cardinality_of_embeddings.dbquery.ipynb` to sanity-check that every embedding has the expected dimension.

## Notes

- **Source of truth**: the Unity Catalog Delta tables (`frag_raw`, `fragrance_cleaned`, `fragrance_embeddings`). The CSV/JSON files under `data/processed/` are local exports and may lag behind the tables — don't treat them as current without regenerating.
- **`data/archive/frag_cleaned_ref.csv`** is a different, unrelated dataset (different schema, fewer rows) kept for reference only — it's not consumed by anything here.
- **No persistent search infra**: similarity search loads embeddings into memory and computes cosine similarity with NumPy inside the notebook you're already running. There's no Vector Search endpoint to provision, sync, or pay to keep running.
