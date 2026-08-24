# Databricks notebook source
# MAGIC %sql
# MAGIC select * from fragrance_db.default.fragrance_cleaned
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

df = spark.sql("""
select a.id
from fragrance_db.default.fragrance_cleaned a
inner join fragrance_db.default.perfume_embeddings b
  on a.id = b.id
where a.perfume_string <> b.perfume_string
and a.perfume_string is not NULL
""")


new_ids = [row["id"] for row in df.collect()]
new_ids_str = ",".join(f"'{str(i)}'" for i in new_ids)

print(new_ids_str)

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


# Which frag needs to generate new embeddings
# 1) newly updated ones where string don't match
# 2) newly added ones with no string
# new_ids_str captures both


perfume_id_to_generate_embeddings_df = spark.sql(f"""
select id, perfume_string
from fragrance_db.default.fragrance_cleaned
where id in ({new_ids_str})
""")


# Get embeddings
embeddings_df = perfume_id_to_generate_embeddings_df.withColumn(
    "embedding",
    get_embeddings_udf(col("perfume_string"))
)
print(f"Generated embeddings for {embeddings_df.count()} record(s).")


# Cast as embedding col to double and id to long
embeddings_df = embeddings_df.withColumn(
    "embedding",
    F.expr("transform(embedding, x -> cast(x as double))") #TODO: change this to float
).withColumn(
    "id",
    F.col("id").cast("long")
)

# Add partition key for parallel writes / query efficiency
embeddings_df = embeddings_df.withColumn(
    "partition_key",
    (F.col("id") % 10).cast("string")
)



# COMMAND ----------

# DBTITLE 1,Write embeddings table to UC as delta table
embeddings_df.write.format("delta").mode("append").saveAsTable("fragrance_db.default.perfume_embeddings")
print(f"Added {embeddings_df.count()} embedding(s) to perfume_embeddings.")

cnt = (
    spark.read.table("fragrance_db.default.perfume_embeddings")
    .filter(F.col("embedding").isNotNull())
    .count()
)


print(f"Total count of fragrances with embeddings: {cnt}")

# COMMAND ----------

# # Optional: If new_embeddings count is large, then run this.
# spark.sql("""
# OPTIMIZE fragrance_db.default.perfume_embeddings
# ZORDER BY (id)
# """)

# COMMAND ----------

# %sql

# OPTIMIZE fragrance_db.default.perfume_embeddings
# ZORDER BY (id)
# ;