# Databricks notebook source
# MAGIC %sql
# MAGIC select * from fragrance_db.default.fragrance_cleaned
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import ArrayType, FloatType
from sentence_transformers import SentenceTransformer
import pandas as pd
import pyspark.sql.functions as F

# Load a pretrained embedding model globally
model = SentenceTransformer("all-MiniLM-L6-v2")

# Define the Pandas UDF
@pandas_udf(ArrayType(FloatType()))
def get_embeddings_udf(texts: pd.Series) -> pd.Series:
    embeddings = model.encode(texts.tolist(), convert_to_tensor=False)
    return pd.Series(embeddings.tolist())

perfume_cleaned_df = spark.table("fragrance_db.default.fragrance_cleaned").select("id", "perfume_string")

print(f"Row before filter counts: {perfume_cleaned_df.count()}")

# Filter out null / empty string
perfume_cleaned_df = perfume_cleaned_df.filter(
    (F.col("perfume_string").isNotNull()) & (F.col("perfume_string") != "")
)

# Get embeddings
embeddings_df = perfume_cleaned_df.withColumn(
    "embedding",
    get_embeddings_udf(col("perfume_string"))
).withColumn(
    "id",
    F.col("id").cast("long")
)

print(f"Row count after filter: {embeddings_df.count()}")

# COMMAND ----------

embeddings_df.printSchema()


# COMMAND ----------

# DBTITLE 1,Write embeddings table to UC as delta table
# Only append IDs that are not already in the embeddings table
existing_ids = spark.table("fragrance_db.default.fragrance_embeddings").select("id")
new_embeddings_df = embeddings_df.join(existing_ids, on="id", how="left_anti")

print(f"New embeddings count: {new_embeddings_df.count()}")


# COMMAND ----------

if new_embeddings_df.count() > 0:
    new_embeddings_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("fragrance_db.default.fragrance_embeddings")
else:
    print("No new embeddings to write.")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optional: If new_embeddings count is large, then run this.
# MAGIC OPTIMIZE fragrance_db.default.fragrance_embeddings
# MAGIC ZORDER BY (id)
# MAGIC ;