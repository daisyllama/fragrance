# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC This is no longer in use. endpoint name is `rag-vs-endpoint`

# COMMAND ----------

# DBTITLE 1,Step 3: Create Vector Search Endpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import EndpointType

w = WorkspaceClient()

# Create a Standard endpoint (low latency ~20-50ms)
# Note: Storage-Optimized not available in current SDK - Standard works well for 68K vectors
# endpoint_name = "fragrance-vs-endpoint"
endpoint_name = "rag-vs-endpoint"

try:
    endpoint = w.vector_search_endpoints.create_endpoint(
        name=endpoint_name,
        endpoint_type=EndpointType.STANDARD  # Low latency, good for 68K vectors
    )
    print(f"✓ Endpoint '{endpoint_name}' creation started!")
    print("\nWait ~5 minutes for endpoint to become ONLINE before proceeding to next cell.")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"✓ Endpoint '{endpoint_name}' already exists.")
        # Check status in next cell
    else:
        raise e

# COMMAND ----------

# DBTITLE 1,Step 4: Check Endpoint Status
# Check if endpoint is ready
endpoint = w.vector_search_endpoints.get_endpoint(endpoint_name=endpoint_name)
print(f"Endpoint Status: {endpoint.endpoint_status.state}")

if str(endpoint.endpoint_status.state) == "EndpointStatusState.ONLINE":
    print("\n✓ Endpoint is ONLINE and ready!")
else:
    print(f"\n⏳ Endpoint is {endpoint.endpoint_status.state}. Wait a few minutes and re-run this cell.")

# COMMAND ----------

# DBTITLE 1,Step 5: Create Vector Search Index with HYBRID Search
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingVectorColumn,
    VectorIndexType,
    PipelineType
)

# Create Delta Sync index with self-managed embeddings
index_name = "fragrance_db.default.fragrance_vs_index"

try:
    index = w.vector_search_indexes.create_index(
        name=index_name,
        endpoint_name="rag-vs-endpoint",
        primary_key="id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table="fragrance_db.default.fragrance_embeddings",
            # Your embeddings are already computed (all-MiniLM-L6-v2 = 384 dimensions)
            embedding_vector_columns=[
                EmbeddingVectorColumn(
                    name="embedding",
                    embedding_dimension=384  # SentenceTransformer all-MiniLM-L6-v2
                )
            ],
            pipeline_type=PipelineType.TRIGGERED  # Manual sync
        )
    )
    print(f"✓ Index '{index_name}' created successfully!")
    print(f"\nInitial sync will start automatically. Check status in next cell.")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"✓ Index '{index_name}' already exists.")
    else:
        raise e

# COMMAND ----------

# DBTITLE 1,Step 6: Monitor Index Sync Status
import time

index_name = "fragrance_db.default.fragrance_vs_index"

# Check index status
index = w.vector_search_indexes.get_index(index_name=index_name)

print(f"Index: {index.name}")
print(f"Ready: {index.status.ready}")
print(f"Message: {index.status.message}")
if index.status.indexed_row_count:
    print(f"Indexed rows: {index.status.indexed_row_count:,}")

if index.status.ready:
    print("\n✓ Index is ONLINE and ready for queries!")
    print(f"Total rows indexed: {index.status.indexed_row_count:,}")
else:
    print("\n⏳ Index is syncing. This takes ~5-10 minutes for 68K rows. Re-run this cell to check progress.")

# COMMAND ----------

# DBTITLE 1,Step 7: Query with HYBRID Search (Semantic + Keyword)
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
index = vsc.get_index(
    endpoint_name="fragrance-vs-endpoint",
    index_name="fragrance_db.default.fragrance_vs_index"
)

# Example 1: Pure semantic search (your old approach)
print("=" * 50)
print("Example 1: Semantic-only search")
print("=" * 50)
results_semantic = index.similarity_search(
    query_text="woody oud amber perfumes",
    columns=["id", "perfume_string"],
    num_results=5
)
print(results_semantic)

# Example 2: HYBRID search (semantic + keyword matching)
print("\n" + "=" * 50)
print("Example 2: HYBRID search (semantic + keyword BM25)")
print("=" * 50)
results_hybrid = index.similarity_search(
    query_text="woody oud amber perfumes",
    columns=["id", "perfume_string"],
    query_type="HYBRID",  # 🔥 This enables hybrid!
    num_results=5
)
print(results_hybrid)

print("\n💡 HYBRID ensures 'oud' literally appears while finding semantically similar scents!")

# COMMAND ----------

# DBTITLE 1,Step 8: Hybrid Search with Filters (Advanced)
# You can also add SQL-like filters (Storage-Optimized endpoints)
# First, let's check what columns are available in your embedding table
spark.table("fragrance_db.default.fragrance_embeddings").printSchema()

# Example: Hybrid search with partition filter
# Note: perfume_string contains gender/brand info, so you could filter on that
results_filtered = index.similarity_search(
    query_text="fresh citrus aromatic",
    columns=["id", "perfume_string"],
    query_type="HYBRID",
    filters="partition_key = '5'",  # Example: search only partition 5
    num_results=5
)

print("Filtered Results:")
for result in results_filtered.get('result', {}).get('data_array', []):
    print(f"ID: {result[0]}, Similarity: {result[-1]:.4f}")

# COMMAND ----------

# Delete endpoint to stop charges
w.vector_search_endpoints.delete_endpoint(
    endpoint_name="fragrance-vs-endpoint"
)

# COMMAND ----------

# DBTITLE 1,Step 9: Update Your perfume_search Notebook (Next Steps)
# MAGIC %md
# MAGIC ## Next Steps: Migrate Your perfume_search Notebook
# MAGIC
# MAGIC Now that Vector Search is set up, you should update your [perfume_search](#notebook-1121556488264796) notebook to use it:
# MAGIC
# MAGIC ### Before (Current - Slow):
# MAGIC ```python
# MAGIC # Your current approach: manual cosine similarity with Spark UDF
# MAGIC cosine_udf = udf(cosine_sim, DoubleType())
# MAGIC df_embeddings.withColumn("similarity", cosine_udf(col("embedding"), lit(target_vec)))
# MAGIC ```
# MAGIC **Problems:**
# MAGIC * Computes similarity across ALL 68K vectors every query
# MAGIC * No keyword matching
# MAGIC * Window operations without partitioning
# MAGIC
# MAGIC ### After (Vector Search - Fast):
# MAGIC ```python
# MAGIC from databricks.vector_search.client import VectorSearchClient
# MAGIC
# MAGIC vsc = VectorSearchClient()
# MAGIC index = vsc.get_index(
# MAGIC     endpoint_name="fragrance-vs-endpoint",
# MAGIC     index_name="fragrance_db.default.fragrance_vs_index"
# MAGIC )
# MAGIC
# MAGIC # Get target perfume embedding
# MAGIC target_vec = spark.table("fragrance_db.default.fragrance_embeddings") \
# MAGIC     .filter(col("id") == perfume_id_input) \
# MAGIC     .select("embedding") \
# MAGIC     .collect()[0][0]
# MAGIC
# MAGIC # HYBRID search with pre-computed vector
# MAGIC results = index.similarity_search(
# MAGIC     query_vector=target_vec,  # Use the embedding directly
# MAGIC     columns=["id", "perfume_string"],
# MAGIC     query_type="HYBRID",
# MAGIC     num_results=limit_n
# MAGIC )
# MAGIC
# MAGIC # Convert to Spark DataFrame
# MAGIC result_ids = [row[0] for row in results['result']['data_array']]
# MAGIC df_results = spark.createDataFrame(
# MAGIC     [(i, int(row[0]), float(row[-1])) for i, row in enumerate(results['result']['data_array'])],
# MAGIC     ["rank", "id", "similarity"]
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC * ⚡ **10-100x faster** - indexed search instead of full scan
# MAGIC * 🎯 **Hybrid matching** - semantic + keyword (great for notes like "oud", "vanilla")
# MAGIC * 📊 **Scales better** - works efficiently even with 1M+ perfumes
# MAGIC * 🔍 **Add filters** - filter by gender, brand, accords before similarity search
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ **Setup Complete!** Your Vector Search index is ready.
# MAGIC
# MAGIC **What you built:**
# MAGIC 1. Storage-Optimized endpoint (cost-effective for 68K vectors)
# MAGIC 2. Delta Sync index with your pre-computed embeddings (all-MiniLM-L6-v2)
# MAGIC 3. HYBRID search enabled (semantic + keyword BM25)
# MAGIC
# MAGIC **Next actions:**
# MAGIC 1. Update [perfume_search](#notebook-1121556488264796) to use Vector Search
# MAGIC 2. Add filters for gender/brand/accords
# MAGIC 3. Consider re-computing embeddings with a larger model (e.g., `databricks-gte-large-en` for 1024 dims) for better semantic quality

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,External API Access: Python Example
import requests
import json

# Configuration
WORKSPACE_URL = "https://<your-workspace>.cloud.databricks.com"
TOKEN = "<your-databricks-token>"  # Store securely (env var, secrets manager)
INDEX_NAME = "fragrance_db.default.fragrance_vs_index"

def search_fragrances(query_text, num_results=5):
    """
    Search fragrances using Vector Search API from external application.
    
    Args:
        query_text: Natural language query (e.g., "woody oud amber perfumes")
        num_results: Number of results to return
    
    Returns:
        List of matching fragrances with similarity scores
    """
    url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{INDEX_NAME}/query"
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "query_text": query_text,
        "columns": ["id", "perfume_string"],
        "num_results": num_results,
        "query_type": "HYBRID"  # Semantic + keyword search
    }
    
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    
    return response.json()

# Example usage
if __name__ == "__main__":
    results = search_fragrances("fresh citrus aromatic", num_results=3)
    
    print("Search Results:")
    for row in results['result']['data_array']:
        perfume_id = row[0]
        perfume_string = row[1]
        similarity = row[2]
        print(f"\nID: {perfume_id}")
        print(f"Similarity: {similarity:.4f}")
        print(f"Details: {perfume_string}")

# COMMAND ----------

# DBTITLE 1,External API Access: curl Example
# MAGIC %undefined
# MAGIC # Example 1: Text-based search (natural language query)
# MAGIC curl -X POST "https://<your-workspace>.cloud.databricks.com/api/2.0/vector-search/indexes/fragrance_db.default.fragrance_vs_index/query" \
# MAGIC   -H "Authorization: Bearer <your-token>" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{
# MAGIC     "query_text": "woody oud amber perfumes",
# MAGIC     "columns": ["id", "perfume_string"],
# MAGIC     "num_results": 5,
# MAGIC     "query_type": "HYBRID"
# MAGIC   }'
# MAGIC
# MAGIC # Example 2: Vector-based search (perfume similarity using embedding)
# MAGIC # First, get the embedding for perfume ID 12345
# MAGIC curl -X POST "https://<your-workspace>.cloud.databricks.com/api/2.0/vector-search/indexes/fragrance_db.default.fragrance_vs_index/query" \
# MAGIC   -H "Authorization: Bearer <your-token>" \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '{
# MAGIC     "query_vector": [0.123, -0.456, 0.789, ...],  # 384-dim embedding
# MAGIC     "columns": ["id", "perfume_string"],
# MAGIC     "num_results": 5,
# MAGIC     "filters": "id != 12345"  # Exclude the query perfume itself
# MAGIC   }'

# COMMAND ----------

# DBTITLE 1,External API Integration Guide
# MAGIC %md
# MAGIC ## External API Integration: Key Points
# MAGIC
# MAGIC ### Authentication
# MAGIC * **Personal Access Token**: User Settings → Developer → Access Tokens
# MAGIC * **Service Principal** (recommended for production): Better for CI/CD, app integration
# MAGIC * Store tokens securely: environment variables, AWS Secrets Manager, Azure Key Vault, etc.
# MAGIC
# MAGIC ### API Request Types
# MAGIC
# MAGIC **1. Text-based search** (`query_text`):
# MAGIC * Natural language queries: "fresh citrus aromatic perfumes"
# MAGIC * Databricks generates embeddings on-the-fly
# MAGIC * Best for: user search interfaces, chatbots
# MAGIC
# MAGIC **2. Vector-based search** (`query_vector`):
# MAGIC * Pre-computed 384-dimensional embedding array
# MAGIC * Best for: perfume similarity ("find similar to this perfume"), batch recommendations
# MAGIC * You need to generate embeddings client-side using the same model (all-MiniLM-L6-v2)
# MAGIC
# MAGIC **3. Hybrid search** (`query_type: "HYBRID"`):
# MAGIC * Combines semantic similarity + keyword matching (BM25)
# MAGIC * Best for: queries with specific terms like "oud", "vanilla", brand names
# MAGIC
# MAGIC ### Filters
# MAGIC You can add SQL-like filters:
# MAGIC ```json
# MAGIC {
# MAGIC   "query_text": "woody perfumes",
# MAGIC   "filters": "partition_key IN ('1', '2', '3')",
# MAGIC   "num_results": 10
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Rate Limits & Performance
# MAGIC * **Latency**: 20-50ms for STANDARD endpoint (your setup)
# MAGIC * **Rate limits**: Depends on your Databricks workspace tier
# MAGIC * **Concurrent queries**: STANDARD endpoints handle concurrent requests well
# MAGIC
# MAGIC ### Use Cases
# MAGIC 1. **Web app search**: User types query → API call → display results
# MAGIC 2. **Mobile app**: Perfume discovery, recommendations
# MAGIC 3. **E-commerce integration**: "Customers also liked..."
# MAGIC 4. **Chatbot/AI assistant**: Natural language perfume search
# MAGIC 5. **Batch recommendations**: Pre-compute similar perfumes for all products
# MAGIC
# MAGIC ### Security Best Practices
# MAGIC * ✅ Use service principals (not personal tokens) in production
# MAGIC * ✅ Rotate tokens regularly
# MAGIC * ✅ Store tokens in secrets manager (never in code)
# MAGIC * ✅ Use HTTPS only
# MAGIC * ✅ Add IP allowlisting if needed (Databricks workspace settings)
# MAGIC * ✅ Monitor API usage via Databricks audit logs

# COMMAND ----------

# DBTITLE 1,External API: Find Similar Perfumes (Complete Flow)
import requests
import json

# Configuration
WORKSPACE_URL = "https://<your-workspace>.cloud.databricks.com"
TOKEN = "<your-databricks-token>"
INDEX_NAME = "fragrance_db.default.fragrance_vs_index"

def get_perfume_embedding(perfume_id):
    """
    Get the embedding vector for a specific perfume.
    You'd typically fetch this from your embeddings table via SQL API or cache it.
    """
    # Option 1: Query via Databricks SQL API
    sql_url = f"{WORKSPACE_URL}/api/2.0/sql/statements"
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "warehouse_id": "<your-warehouse-id>",  # Get from SQL Warehouses page
        "statement": f"SELECT embedding FROM fragrance_db.default.fragrance_embeddings WHERE id = {perfume_id}"
    }
    
    response = requests.post(sql_url, headers=headers, json=payload)
    result = response.json()
    
    # Extract embedding from result
    embedding = result['result']['data_array'][0][0]
    return embedding

def find_similar_perfumes(perfume_id, num_results=5):
    """
    Find perfumes similar to a given perfume ID.
    This is the most common use case for fragrance recommendations.
    """
    # Step 1: Get the embedding for the target perfume
    embedding = get_perfume_embedding(perfume_id)
    
    # Step 2: Search for similar perfumes using that embedding
    search_url = f"{WORKSPACE_URL}/api/2.0/vector-search/indexes/{INDEX_NAME}/query"
    
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "query_vector": embedding,  # Use the perfume's embedding
        "columns": ["id", "perfume_string"],
        "num_results": num_results + 1,  # +1 because query perfume will be in results
        "filters": f"id != {perfume_id}"  # Exclude the query perfume itself
    }
    
    response = requests.post(search_url, headers=headers, json=payload)
    response.raise_for_status()
    
    return response.json()

# Example usage: "Customers who liked perfume 12345 also liked..."
if __name__ == "__main__":
    perfume_id = 12345
    
    print(f"Finding perfumes similar to ID {perfume_id}...\n")
    results = find_similar_perfumes(perfume_id, num_results=5)
    
    print("Recommended perfumes:")
    for i, row in enumerate(results['result']['data_array'], 1):
        rec_id = row[0]
        perfume_string = row[1]
        similarity = row[2]
        print(f"\n{i}. ID: {rec_id}")
        print(f"   Similarity: {similarity:.4f}")
        print(f"   Details: {perfume_string[:100]}...")  # Truncate for display