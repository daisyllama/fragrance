# Fragrance pipeline — current state

## Status: single validated pipeline, personal/manual tool

This project was previously split across three parallel, inconsistent implementations
(a manual notebook path, an unfinished declarative Lakeflow bronze/silver/gold pipeline,
and a dead Vector Search endpoint setup). Those have been consolidated into one path:

```
scraping/notebooks/fragrantica_scraper.ipynb  \
notebooks/04_maintenance/add_update_fragrance.py  } → frag_raw
                                                          │
                                    notebooks/01_data_processing/clean_from_raw.py
                                                          ▼
                                                  fragrance_cleaned
                                                          │
        notebooks/02_embeddings/generate_embeddings_job.py (batch)
        notebooks/04_maintenance/generate_embeddings_for_new_frag.py (incremental)
                                                          ▼
                                                fragrance_embeddings
                                                          │
                                    notebooks/03_search/perfume_search.py
                                    (rapidfuzz name resolve + NumPy cosine similarity)
```

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
