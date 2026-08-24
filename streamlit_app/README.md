# Fragrance Search — Streamlit app

Local app with two tabs:
- **Search** — search a perfume by name, see its notes, get recommended similar perfumes with theirs.
- **Add a fragrance** — paste a Fragrantica URL, scrape it, review, and add it to the database end-to-end.

Runs entirely on your machine and queries `fragrance_cleaned` / `fragrance_embeddings` live from Databricks — same tables and logic as the `notebooks/`, `scraping/`, and `common/` code they're built from (the matching/similarity/cleaning/scraping logic itself lives in `common/` and is imported by both the notebooks and this app, not duplicated).

## Setup (one-time)

1. **Get a SQL Warehouse connection.** In your Databricks workspace: SQL Warehouses → (create one if needed; Serverless is fine, it auto-suspends when idle) → open it → **Connection details** tab. Note the **Server hostname** and **HTTP path**.
2. **Generate a personal access token.** User Settings → Developer → Access tokens → Generate new token.
3. **Find the embedding job script's workspace path.** In the Databricks workspace file browser, locate `jobs/generate_embeddings_for_new_frag.py` and copy its full path (e.g. `/Workspace/Repos/<you>/sniffers/jobs/generate_embeddings_for_new_frag.py`, including the `.py`). Needed for the "Add a fragrance" tab, which triggers this script as a one-time job run instead of running the embedding model locally.
4. **Fill in secrets.** Copy the template and edit it with the values from steps 1–3:
   ```bash
   cp .streamlit/secrets.toml.example .streamlit/secrets.toml
   ```
   `secrets.toml` is gitignored — never commit it.
5. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Run

```bash
streamlit run app.py
```

**Search tab:** type a perfume name, pick the match you meant, see its notes, then its top similar perfumes (adjust the count in the sidebar).

**Add a fragrance tab:** paste a Fragrantica perfume page URL, click Scrape, review the extracted fields (scraping can partially fail and return `"N/A"` — a warning flags this), then Confirm & add. This runs, in order: MERGE into `frag_raw` → clean + MERGE into `fragrance_cleaned` → trigger `jobs/generate_embeddings_for_new_frag.py` as a Databricks job run and wait for it to finish. The embedding step submits a *one-time* job run on serverless compute (see below) rather than running the model on your machine — expect it to take a bit longer than the other steps, especially if compute needs to spin up.

## Notes

- Data is cached for an hour (`st.cache_data(ttl=3600)`) after first load, so repeated searches don't re-query Databricks — restart the app or wait out the TTL to pick up fragrances added outside this app. Fragrances added *through* this app show up immediately (the cache is cleared automatically after a successful add).
- If a selected perfume shows "hasn't been embedded yet," run `notebooks/02_generate_embeddings_job.py` or `notebooks/05_generate_embeddings_for_new_frag.py` in Databricks, then reload.
- The "Add a fragrance" tab needs `embedding_job_python_file` set in `secrets.toml`. If the job fails with a cluster-related error, your workspace may not have serverless job compute enabled for `spark_python_task` — set `job_cluster_id` in `secrets.toml` to an existing all-purpose cluster's ID instead.
