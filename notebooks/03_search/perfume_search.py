# Databricks notebook source
# MAGIC %md
# MAGIC # Perfume Search
# MAGIC
# MAGIC Run cell by cell:
# MAGIC 1. Set `search_term` and run the text-search cell → prints a table of candidate `id`s
# MAGIC 2. Copy the `id` you want into `selected_id` and run the recommendation cells → top-N similar perfumes, with notes

# COMMAND ----------

# DBTITLE 1,Setup
from rapidfuzz import fuzz
import numpy as np
import pandas as pd
from pyspark.sql.functions import col, lit, pandas_udf
from pyspark.sql.types import DoubleType

fragrance_cleaned = spark.table("fragrance_db.default.fragrance_cleaned")

# COMMAND ----------

# DBTITLE 1,Step 1 — search by text, get an id
search_term = "miss dior eau de parfum"

# Perfume names store the concentration inconsistently ("edp" vs "eau de
# parfum", etc.) — collapse both spellings to the same abbreviation before
# scoring so either form matches regardless of which one the name uses.
CONCENTRATION_ALIASES = {
    r"\beau de parfum\b": "edp",
    r"\beau de toilette\b": "edt",
    r"\beau de cologne\b": "edc",
    r"\bextrait de parfum\b": "extrait",
    r"\bparfum extrait\b": "extrait",
}


def normalize_concentration(text):
    import re
    for pattern, replacement in CONCENTRATION_ALIASES.items():
        text = re.sub(pattern, replacement, text)
    return text


@pandas_udf(DoubleType())
def smart_fuzzy_score(names: pd.Series, search_term_series: pd.Series) -> pd.Series:
    term = normalize_concentration(search_term_series.iloc[0].lower())

    def calculate_score(name):
        if pd.isna(name):
            return 0.0
        name_lower = normalize_concentration(str(name).lower())
        ratio = fuzz.ratio(name_lower, term)
        token_set = fuzz.token_set_ratio(name_lower, term)
        token_sort = fuzz.token_sort_ratio(name_lower, term)
        substring_bonus = 20 if term in name_lower else 0
        return ratio * 0.4 + token_set * 0.4 + token_sort * 0.2 + substring_bonus * 0.1

    return names.apply(calculate_score)

search_results = (
    fragrance_cleaned
    .withColumn("score", smart_fuzzy_score(col("name"), lit(search_term)))
    .filter(col("score") > 70)
    .orderBy(col("score").desc())
    .limit(200)
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

# DBTITLE 1,Compute similarity
# Load every embedding once and compute cosine similarity as a single
# vectorized matrix-vector product (one BLAS call over an in-memory NumPy
# matrix), instead of a per-row UDF + a global Spark sort. At this table
# size (~70K x 384 floats, ~100MB) this comfortably fits in driver memory.
embeddings_pdf = spark.table("fragrance_db.default.fragrance_embeddings") \
    .select("id", "embedding").toPandas()
ids = embeddings_pdf["id"].to_numpy()
matrix = np.stack(embeddings_pdf["embedding"].to_numpy()).astype(np.float32)
norms = np.linalg.norm(matrix, axis=1)

target_idx = np.where(ids == selected_id)[0][0]
similarities = (matrix @ matrix[target_idx]) / (norms * norms[target_idx])

n = top_n + 1  # +1 to drop the selected perfume itself below
top_idx = np.argpartition(-similarities, n - 1)[:n]
top_idx = top_idx[np.argsort(-similarities[top_idx])]

recs_pdf = pd.DataFrame({
    "id": ids[top_idx].astype("int64"),
    "similarity": similarities[top_idx].astype("float64"),
})
recs_pdf = recs_pdf[recs_pdf["id"] != selected_id].head(top_n)

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
