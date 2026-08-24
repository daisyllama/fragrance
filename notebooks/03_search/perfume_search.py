# Databricks notebook source
dbutils.widgets.text("2. perfume_id_input", "")
perfume_id_input = dbutils.widgets.get("2. perfume_id_input").strip()
if perfume_id_input:  # This checks if the string is non-empty
    perfume_id_input = int(perfume_id_input)
else:
    perfume_id_input = None
print(perfume_id_input)

dbutils.widgets.text("9. number of top results", "")
limit_n = int(dbutils.widgets.get("9. number of top results").strip())
print(limit_n)

# COMMAND ----------

from rapidfuzz import fuzz
from pyspark.sql.functions import col, lit, pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

dbutils.widgets.text("1. search_term", "")
search_term = dbutils.widgets.get("1. search_term").strip()  # Fixed: strip() on the result, not the key
print(f"Searching for \"{search_term}\"...")

# Load your data as Spark DataFrame
df = spark.table("fragrance_db.default.fragrance_cleaned")

# Define pandas UDF for scoring
@pandas_udf(DoubleType())
def smart_fuzzy_score(names: pd.Series, search_term_series: pd.Series) -> pd.Series:
    search_term = search_term_series.iloc[0].lower()
    
    def calculate_score(name):
        if pd.isna(name):
            return 0.0
            
        name_lower = str(name).lower()
        
        # Multiple scoring strategies
        ratio = fuzz.ratio(name_lower, search_term)
        token_set = fuzz.token_set_ratio(name_lower, search_term)
        token_sort = fuzz.token_sort_ratio(name_lower, search_term)
        
        # Bonus for substring match
        substring_bonus = 20 if search_term in name_lower else 0
        
        # Weighted combination
        final_score = (
            ratio * 0.4 +
            token_set * 0.4 +
            token_sort * 0.2 +
            substring_bonus * 0.1
        )
        
        return final_score
    
    return names.apply(calculate_score)

# Apply the search
search_results = (df
    .withColumn("score", smart_fuzzy_score(col("name"), lit(search_term)))
    .filter(col("score") > 70)
    .orderBy(col("score").desc())
    .limit(20)
)

# Display results
display(search_results.select("id", "name", "brand", "score"))

selected_search_result = search_results.first().id
print(f"id of the top search result: {selected_search_result}")

# COMMAND ----------

import numpy as np
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Cosine similarity function
def cosine_sim(a, b):
    a, b = np.array(a, dtype=np.float32), np.array(b, dtype=np.float32)
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

cosine_udf = udf(cosine_sim, DoubleType())

# Get the vector of the target perfume
try:
    if perfume_id_input is not None:
        selected_search_result = perfume_id_input
except NameError:
    pass

target_vec = spark.table("fragrance_db.default.fragrance_embeddings") \
    .filter(col("id") == selected_search_result) \
    .select("embedding") \
    .collect()[0][0]

target_name = spark.table("fragrance_db.default.fragrance_cleaned") \
    .filter(col("id") == selected_search_result) \
    .select("name") \
    .collect()[0][0]

brand = spark.table("fragrance_db.default.fragrance_cleaned") \
    .filter(col("id") == selected_search_result) \
    .select("brand") \
    .collect()[0][0]

# Compute similarity
df = spark.table("fragrance_db.default.fragrance_embeddings")
df_result = df.withColumn(
    "similarity", 
    cosine_udf(col("embedding"), lit(target_vec))
).withColumn(
    "rank", 
    row_number().over(Window.orderBy(col("similarity").desc()))
)

limit_n = limit_n + 1 # To get top n results excluding selected perfume
df_top_similarity_results = df_result.orderBy(col("rank").asc()).limit(limit_n)

# COMMAND ----------

display(df_top_similarity_results)

# COMMAND ----------

df_frag = spark.sql(f"""
    SELECT id, name, brand, gender, accords, top_notes, mid_notes, base_notes, url
    FROM fragrance_db.default.fragrance_cleaned
    """
)

df_result_view = df_frag.join(
    df_top_similarity_results.select("rank", "id", "similarity"),
    on="id",
    how="inner"
).select((df_top_similarity_results["rank"]-1).alias("rank"), df_frag["*"], df_top_similarity_results["similarity"]
).orderBy(col("similarity").desc())

# COMMAND ----------

count = df_result_view.count() - 1

print(f"Top {count} similar perfumes to {target_name} by {brand} below:")
display(df_result_view)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from fragrance_db.default.fragrance_cleaned 
# MAGIC where id = 72944