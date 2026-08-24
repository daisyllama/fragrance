# Databricks notebook source
# MAGIC %sql
# MAGIC select * from fragrance_db.default.fragrance_cleaned
# MAGIC limit 10
# MAGIC ;

# COMMAND ----------

df = spark.sql("""
select a.id
from fragrance_db.default.fragrance_cleaned a
left join fragrance_db.default.fragrance_embeddings b
  on a.id = b.id
where a.perfume_string is not NULL
and (b.id is NULL or a.perfume_string <> b.perfume_string)
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


# Cast embedding col to float and id to long
embeddings_df = embeddings_df.withColumn(
    "embedding",
    F.expr("transform(embedding, x -> cast(x as float))")
).withColumn(
    "id",
    F.col("id").cast("long")
)

# COMMAND ----------

# DBTITLE 1,Upsert into fragrance_embeddings (covers both new and updated rows)
from delta.tables import DeltaTable

# Explicit column sets, not whenMatchedUpdateAll()/whenNotMatchedInsertAll():
# fragrance_embeddings has columns (e.g. partition_key) this pipeline never
# populates, and the "update/insert all" star-equivalent requires every
# target column to resolve against the source or the MERGE fails with
# DELTA_MERGE_UNRESOLVED_EXPRESSION.
target = DeltaTable.forName(spark, "fragrance_db.default.fragrance_embeddings")

(
    target.alias("t")
    .merge(embeddings_df.alias("s"), "t.id = s.id")
    .whenMatchedUpdate(set={"perfume_string": "s.perfume_string", "embedding": "s.embedding"})
    .whenNotMatchedInsert(
        values={"id": "s.id", "perfume_string": "s.perfume_string", "embedding": "s.embedding"}
    )
    .execute()
)

print(f"Upserted {embeddings_df.count()} embedding(s) into fragrance_embeddings.")

cnt = (
    spark.read.table("fragrance_db.default.fragrance_embeddings")
    .filter(F.col("embedding").isNotNull())
    .count()
)

print(f"Total count of fragrances with embeddings: {cnt}")