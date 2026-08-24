"""
Generate/update embeddings for new or changed fragrances.

Plain Python script (not a Databricks notebook — no cell markers) meant to
be triggered as a spark_python_task via the Databricks Jobs API (see
common/databricks_jobs.py), which is simpler to trigger programmatically
than pointing at a Git-folder notebook path. Same logic as
notebooks/05_generate_embeddings_for_new_frag.py, which stays as the
interactive/cell-by-cell version for troubleshooting in Databricks; this
file is the automation-triggered equivalent.
"""

import pandas as pd
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import ArrayType, FloatType
from sentence_transformers import SentenceTransformer

CATALOG_SCHEMA = "fragrance_db.default"


def main():
    spark = SparkSession.builder.getOrCreate()

    # Which fragrances need embeddings: newly added ones (no row in
    # fragrance_embeddings yet) or updated ones (perfume_string changed).
    df = spark.sql(f"""
        SELECT a.id
        FROM {CATALOG_SCHEMA}.fragrance_cleaned a
        LEFT JOIN {CATALOG_SCHEMA}.fragrance_embeddings b
          ON a.id = b.id
        WHERE a.perfume_string IS NOT NULL
        AND (b.id IS NULL OR a.perfume_string <> b.perfume_string)
    """)
    new_ids = [row["id"] for row in df.collect()]

    if not new_ids:
        print("No new or changed fragrances to embed.")
        return

    new_ids_str = ",".join(f"'{str(i)}'" for i in new_ids)
    print(f"Generating embeddings for {len(new_ids)} fragrance(s): {new_ids_str}")

    model = SentenceTransformer("all-MiniLM-L6-v2")

    @pandas_udf(ArrayType(FloatType()))
    def get_embeddings_udf(texts: pd.Series) -> pd.Series:
        embeddings = model.encode(texts.tolist(), convert_to_tensor=False)
        return pd.Series(embeddings.tolist())

    perfume_id_to_generate_embeddings_df = spark.sql(f"""
        SELECT id, perfume_string
        FROM {CATALOG_SCHEMA}.fragrance_cleaned
        WHERE id IN ({new_ids_str})
    """)

    embeddings_df = perfume_id_to_generate_embeddings_df.withColumn(
        "embedding", get_embeddings_udf(col("perfume_string"))
    )
    print(f"Generated embeddings for {embeddings_df.count()} record(s).")

    embeddings_df = embeddings_df.withColumn(
        "embedding", F.expr("transform(embedding, x -> cast(x as float))")
    ).withColumn("id", F.col("id").cast("long"))

    target = DeltaTable.forName(spark, f"{CATALOG_SCHEMA}.fragrance_embeddings")
    (
        target.alias("t")
        .merge(embeddings_df.alias("s"), "t.id = s.id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Upserted {embeddings_df.count()} embedding(s) into fragrance_embeddings.")

    cnt = (
        spark.read.table(f"{CATALOG_SCHEMA}.fragrance_embeddings")
        .filter(F.col("embedding").isNotNull())
        .count()
    )
    print(f"Total count of fragrances with embeddings: {cnt}")


if __name__ == "__main__":
    main()
