from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd
import os

@pandas_udf(ArrayType(FloatType()))
def get_embeddings_udf(texts: pd.Series) -> pd.Series:
    """
    Generate embeddings for input texts using sentence-transformers.
    Returns embeddings as arrays of floats.
    Model is loaded inside the UDF to avoid serialization issues.
    """
    # Set cache directories to avoid getpwuid issues on serverless
    os.environ['TRANSFORMERS_CACHE'] = '/tmp/transformers_cache'
    os.environ['TORCH_HOME'] = '/tmp/torch_cache'
    os.environ['HF_HOME'] = '/tmp/hf_cache'
    
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = model.encode(texts.tolist(), convert_to_tensor=False)
    return pd.Series(embeddings.tolist())

@dp.materialized_view(
    comment="Fragrance embeddings for vector search, generated using sentence-transformers",
    cluster_by=["partition_key"]
)
def gold_fragrance_embeddings():
    """
    Generates embeddings for fragrances with valid perfume_string.
    Includes partition key for efficient parallel processing and querying.
    Feeds the Vector Search Delta Sync index.
    """
    return (
        spark.read.table("silver_fragrance_cleaned")
        .filter((F.col("perfume_string").isNotNull()) & (F.col("perfume_string") != ""))
        .select(
            F.col("id").cast("bigint").alias("id"),
            F.col("perfume_string")
        )
        .withColumn("embedding", get_embeddings_udf(F.col("perfume_string")))
        .withColumn("embedding", F.expr("transform(embedding, x -> cast(x as float))"))
        .withColumn("partition_key", (F.col("id") % 10).cast("string"))
        .select("id", "perfume_string", "embedding", "partition_key")
    )
