"""
Fragrance search + recommendations + add-a-fragrance.

Local Streamlit app: search a perfume by name, see its notes, get similar
perfumes with theirs; or add a new one by pasting a Fragrantica URL. Queries
fragrance_cleaned / fragrance_embeddings live from Databricks via a SQL
Warehouse (see README.md for setup) — same tables and same search/similarity/
add logic as notebooks/03_perfume_search.py and friends, just running outside
Databricks (the actual matching/similarity/cleaning/scraping logic is shared
via common/, imported by both, not duplicated).
"""

import sys
from pathlib import Path

import databricks.sql as dbsql
import pandas as pd
import streamlit as st

# Repo root, so `common` (shared with the notebooks/) is importable
# regardless of the working directory `streamlit run` is launched from.
sys.path.append(str(Path(__file__).resolve().parent.parent))
from common.add_fragrance import clean_and_merge_fragrance, merge_frag_raw  # noqa: E402
from common.databricks_jobs import trigger_embedding_job  # noqa: E402
from common.matching import normalize_concentration, fuzzy_score  # noqa: E402
from common.scraping import scrape_fragrantica  # noqa: E402
from common.similarity import build_embedding_matrix, top_n_similar  # noqa: E402

CATALOG_SCHEMA = "fragrance_db.default"


@st.cache_resource
def get_connection():
    creds = st.secrets["databricks"]
    return dbsql.connect(
        server_hostname=creds["server_hostname"],
        http_path=creds["http_path"],
        access_token=creds["access_token"],
    )


@st.cache_data(ttl=3600)
def load_data():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT id, name, brand, gender, accords, top_notes, mid_notes, base_notes, url
            FROM {CATALOG_SCHEMA}.fragrance_cleaned
        """)
        cleaned_df = cur.fetchall_arrow().to_pandas()

        cur.execute(f"SELECT id, embedding FROM {CATALOG_SCHEMA}.fragrance_embeddings")
        embeddings_df = cur.fetchall_arrow().to_pandas()

    # id's actual stored type can't be trusted to be numeric (Delta keeps
    # whatever type the table was first created with) — normalize to str
    # on both sides so lookups/joins are consistent regardless.
    cleaned_df["id"] = cleaned_df["id"].astype(str)
    embeddings_df["id"] = embeddings_df["id"].astype(str)

    matrix, norms = build_embedding_matrix(embeddings_df["embedding"].to_numpy())
    embedding_ids = embeddings_df["id"].to_numpy()

    return cleaned_df, embedding_ids, matrix, norms


def render_notes(row):
    st.write(f"**Accords:** {row['accords'] or '—'}")
    st.write(f"**Top notes:** {row['top_notes'] or '—'}")
    st.write(f"**Mid notes:** {row['mid_notes'] or '—'}")
    st.write(f"**Base notes:** {row['base_notes'] or '—'}")


def render_search_tab():
    cleaned_df, embedding_ids, matrix, norms = load_data()

    search_term = st.text_input("Search for a perfume by name", "")
    if not search_term.strip():
        return

    term_norm = normalize_concentration(search_term.strip().lower())
    scores = cleaned_df["name"].apply(lambda n: fuzzy_score(n, term_norm))
    candidates = cleaned_df.assign(score=scores)
    candidates = candidates[candidates["score"] > 70].sort_values("score", ascending=False).head(20)

    if candidates.empty:
        st.info("No matches found — try a different search term.")
        return

    labels = [f"{r['name']} — {r['brand']}" for _, r in candidates.iterrows()]
    label_to_id = dict(zip(labels, candidates["id"]))
    choice = st.selectbox("Select the perfume you meant", labels)
    selected_id = label_to_id[choice]

    target = cleaned_df[cleaned_df["id"] == selected_id].iloc[0]

    st.header(f"{target['name']} — {target['brand']}")
    render_notes(target)

    st.divider()

    top_n = st.sidebar.slider("Number of recommendations", min_value=1, max_value=10, value=3)

    try:
        rec_ids, rec_similarities = top_n_similar(embedding_ids, matrix, norms, selected_id, top_n)
    except ValueError:
        st.error(
            "This fragrance hasn't been embedded yet. Run "
            "notebooks/02_generate_embeddings_job.py or "
            "notebooks/05_generate_embeddings_for_new_frag.py in "
            "Databricks first, then reload this page."
        )
        return

    recs_df = pd.DataFrame({"id": rec_ids, "similarity": rec_similarities.astype(float)})
    recs_df = recs_df.merge(cleaned_df, on="id", how="left")

    st.subheader(f"Top {len(recs_df)} similar perfumes")

    columns = st.columns(len(recs_df)) if len(recs_df) > 0 else []
    for col, (_, row) in zip(columns, recs_df.iterrows()):
        with col:
            st.markdown(f"**{row['name']}**  \n{row['brand']}")
            st.caption(f"similarity: {row['similarity']:.3f}")
            render_notes(row)


def render_add_tab():
    st.write(
        "Paste a Fragrantica perfume page URL. This scrapes it, then (after "
        "you confirm) merges it into `frag_raw` and `fragrance_cleaned`, and "
        "triggers a Databricks job to generate its embedding."
    )

    url = st.text_input(
        "Fragrantica URL",
        placeholder="https://www.fragrantica.com/perfume/Brand/Name-12345.html",
        key="add_url",
    )

    if st.button("Scrape", disabled=not url.strip()):
        with st.spinner("Scraping..."):
            st.session_state["scraped"] = scrape_fragrantica(url.strip())

    scraped = st.session_state.get("scraped")
    if not scraped:
        return

    if "error" in scraped:
        st.error(f"Scrape failed: {scraped['error']}")
        return

    st.subheader("Preview")
    na_fields = [k for k in ("name", "gender", "rating", "rating_count", "description") if scraped.get(k) == "N/A"]
    if na_fields:
        st.warning(
            f"These fields came back as \"N/A\" — the scraper may not have parsed this page correctly "
            f"(anti-bot measures or a page layout it doesn't recognize): {', '.join(na_fields)}. "
            "Review before confirming."
        )
    st.json(scraped)

    if st.button("Confirm & add"):
        conn = get_connection()
        try:
            with st.status("Adding fragrance...", expanded=True) as status:
                with conn.cursor() as cur:
                    st.write("Merging into `frag_raw`...")
                    merge_frag_raw(cur, scraped)

                    st.write("Cleaning and merging into `fragrance_cleaned`...")
                    cleaned_row = clean_and_merge_fragrance(cur, scraped)
                    st.write(f"→ id `{cleaned_row['id']}`: {cleaned_row['name']} — {cleaned_row['brand']}")

                    st.write("Triggering Databricks job to generate the embedding (this can take a minute, especially if compute needs to spin up)...")
                    creds = st.secrets["databricks"]
                    trigger_embedding_job(
                        host=creds["server_hostname"],
                        token=creds["access_token"],
                        notebook_path=creds["notebook_path"],
                        cluster_id=creds.get("job_cluster_id"),
                    )

                status.update(label="Done", state="complete")

            load_data.clear()
            st.success(f"Added: {cleaned_row['name']} — {cleaned_row['brand']} (id {cleaned_row['id']}). It's searchable now.")
            del st.session_state["scraped"]
        except Exception as e:
            st.error(f"Failed: {e}")


st.set_page_config(page_title="Fragrance Search", layout="wide")
st.title("Fragrance Search")

search_tab, add_tab = st.tabs(["Search", "Add a fragrance"])
with search_tab:
    render_search_tab()
with add_tab:
    render_add_tab()
