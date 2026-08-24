# Fragrance pipeline — current state

## Status: single validated pipeline, personal/manual tool

This project was previously split across three parallel, inconsistent implementations
(a manual notebook path, an unfinished declarative Lakeflow bronze/silver/gold pipeline,
and a dead Vector Search endpoint setup). Those have been consolidated into one path:

```
scraping/notebooks/fragrantica_scraper.ipynb  \
notebooks/04_add_update_fragrance.py            } → frag_raw
                                                          │
                                          notebooks/01_clean_from_raw.py
                                                          ▼
                                                  fragrance_cleaned
                                                          │
        notebooks/02_generate_embeddings_job.py (batch)
        notebooks/05_generate_embeddings_for_new_frag.py (incremental, run by hand)
        jobs/generate_embeddings_for_new_frag.py (same logic, triggered as a Databricks
                                                   job run by streamlit_app/app.py)
                                                          ▼
                                                fragrance_embeddings
                                                          │
                                    notebooks/03_perfume_search.py  /  streamlit_app/app.py
                                    (rapidfuzz name resolve + NumPy cosine similarity —
                                     shared via common/, not duplicated between the two)
```

`notebooks/` is flat and numbered by pipeline order (`01_`–`05_`), not nested folders — one file per step, meant to be run interactively/cell-by-cell for troubleshooting. `jobs/` holds plain-script twins of the notebooks that need to be triggered programmatically (currently just the embedding generator, triggered by the Streamlit app's "Add a fragrance" tab via the Databricks Jobs API as a one-time serverless run). The actual matching/cleaning/scraping logic that both the notebooks and the Streamlit app need lives in `common/` (`matching.py`, `similarity.py`, `cleaning.py`, `scraping.py`, `add_fragrance.py`, `databricks_jobs.py`) as plain importable Python, not copy-pasted between them.

See `README.md` for the full structure and how to run each step.

## Deliberately not doing

- **No declarative Lakeflow pipeline.** The earlier `pipeline/bronze|silver|gold` attempt
  called Unity Catalog functions that were never defined anywhere and was missing the
  dedup-by-url step the notebook path has — it was never finished or run. This is a
  personal, run-manually tool; a scheduled/declarative pipeline isn't needed.
- **No Databricks Vector Search endpoint.** It requires a persistently provisioned,
  billed endpoint. `perfume_search.py` instead loads all embeddings once per run and
  computes cosine similarity with a single NumPy matrix multiply — fast enough at this
  data size (~70K rows) with zero ongoing cost or infrastructure to maintain.

## Possible future work (not started)

- If the dataset grows past what comfortably fits in driver memory for the NumPy
  approach, a self-hosted ANN index (e.g. FAISS, loaded once per notebook session)
  would be the next step — still no persistent endpoint required.
- `data/processed/*` and `data/archive/*` are local snapshots that can drift from the
  live Delta tables; regenerate from `fragrance_cleaned` if you need a fresh export
  rather than trusting the files on disk.
