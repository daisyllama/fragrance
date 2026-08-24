# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Perfume Search
# MAGIC
# MAGIC Run cell by cell:
# MAGIC 1. Set `search_term` and run the text-search cell → prints a table of candidate `id`s
# MAGIC 2. Copy the `id` you want into `selected_id` and run the recommendation cells → top-N similar perfumes, with notes

# COMMAND ----------

# DBTITLE 1,Setup
import sys
sys.path.append("..")  # repo root, so `common` (shared with streamlit_app/app.py) is importable

import numpy as np
import pandas as pd
from pyspark.sql.functions import col, lit, pandas_udf
from pyspark.sql.types import DoubleType

from common.matching import normalize_concentration, fuzzy_score

fragrance_cleaned = spark.table("fragrance_db.default.fragrance_cleaned")

# COMMAND ----------

# DBTITLE 1,Step 1 — search by text, get an id
search_term = "miss dior edp"


@pandas_udf(DoubleType())
def smart_fuzzy_score(names: pd.Series, search_term_series: pd.Series) -> pd.Series:
    term = normalize_concentration(search_term_series.iloc[0].lower())
    return names.apply(lambda name: fuzzy_score(name, term))

search_results = (
    fragrance_cleaned
    .withColumn("score", smart_fuzzy_score(col("name"), lit(search_term)))
    .filter(col("score") > 70)
    .orderBy(col("score").desc(), col("release_year").desc())
    .limit(20)
)

display(search_results.select("id", "name", "brand", "release_year", "score", "url"))

# COMMAND ----------

# DBTITLE 1,Step 2 — pick the id and how many recommendations you want
selected_id = search_results.first().id  # or set manually, e.g. selected_id = 72944
top_n = 3

target = fragrance_cleaned.filter(col("id") == selected_id).select(
    "id", "name", "brand", "accords", "top_notes", "mid_notes", "base_notes"
).collect()[0]

print(f"Selected: {target['name']} by {target['brand']} (id={target['id']})")
display(spark.createDataFrame([target]))

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Compute similarity
# Load every embedding once and compute cosine similarity as a single
# vectorized matrix-vector product (one BLAS call over an in-memory NumPy
# matrix), instead of a per-row UDF + a global Spark sort. At this table
# size (~70K x 384 floats, ~100MB) this comfortably fits in driver memory.
from common.similarity import build_embedding_matrix, top_n_similar

try:
    selected_id = int(selected_id)
except (TypeError, ValueError):
    raise ValueError(f"selected_id must be numeric, got {selected_id!r}")

embeddings_pdf = spark.table("fragrance_db.default.fragrance_embeddings") \
    .select("id", "embedding").toPandas()
ids = embeddings_pdf["id"].to_numpy()
matrix, norms = build_embedding_matrix(embeddings_pdf["embedding"].to_numpy())

try:
    rec_ids, rec_similarities = top_n_similar(ids, matrix, norms, selected_id, top_n)
except ValueError:
    raise ValueError(
        f"id {selected_id} has no row in fragrance_embeddings — it may not be "
        "embedded yet. Run generate_embeddings_job.py or "
        "generate_embeddings_for_new_frag.py for this fragrance first."
    )

recs_pdf = pd.DataFrame({
    "id": rec_ids.astype("int64"),
    "similarity": rec_similarities.astype("float64"),
})

# COMMAND ----------

# DBTITLE 1,check embeddings
# MAGIC %sql
# MAGIC select * from fragrance_db.default.fragrance_embeddings 
# MAGIC where id = '68905'
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fragrance_db.default.fragrance_cleaned
# MAGIC where id = '68905'
# MAGIC ;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Step 3 — top-N recommendations, with notes
recommendations = (
    fragrance_cleaned
    .select("id", "name", "brand", "gender", "accords", "top_notes", "mid_notes", "base_notes", "url")
    .join(spark.createDataFrame(recs_pdf), on="id", how="inner")
    .orderBy(col("similarity").desc())
)

print(f"Top {top_n} similar perfumes to {target['name']} by {target['brand']}:")
display(recommendations)