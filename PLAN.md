# 🗺️ Fragrance ETL Pipeline - Implementation Roadmap

## 📍 Current State

### What We've Built (Phase 1: Manual Validation)

**Purpose**: Validate transformation logic and data quality before productionization

**Approach**: Interactive notebooks for one-time batch load
- ✅ **Notebook**: `01_data_processing/clean_from_raw`
  - Read raw CSV from Unity Catalog Volume
  - Apply SQL transformations (regex parsing, deduplication, text generation)
  - Output Delta table and CSV export
  - **Result**: 69,948 validated fragrance records

- ✅ **Notebook**: `02_embeddings/generate_embeddings_job`
  - Load cleaned CSV
  - Generate 384-dim embeddings using sentence-transformers
  - Write to embeddings table with partitioning

**Key Learnings**:
- ✓ Transformation logic is correct and produces clean data
- ✓ Embedding generation works at scale (~70K records)
- ✓ Data model supports downstream vector search
- ✓ Schema validated: 17 columns including `perfume_string` for embeddings

---

## 🎯 Short-Term Goal: Productionize (Phase 2)

### Objective
Convert validated notebook logic into **automated, declarative Lakeflow Pipeline**

### Scope
- Replace manual notebook execution with scheduled pipeline
- Maintain same transformation logic (no changes to business rules)
- Enable monitoring, lineage tracking, and error handling
- Support initial full load and manual re-runs

### Architecture: Medallion (Bronze → Silver → Gold)

```
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw Ingestion)                                      │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
│  Input:  /Volumes/fragrance_db/default/data/frag_raw.csv           │
│  Method: Auto Loader (cloudFiles)                                  │
│  Table:  bronze_fragrance_raw                                      │
│  Mode:   Streaming (initial: full load, future: incremental)       │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER (Data Cleaning & Transformation)                     │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
│  Input:  bronze_fragrance_raw                                      │
│  Logic:  Port from clean_from_raw notebook:                        │
│          • Extract brand, name from URL                            │
│          • Parse release_year, perfumers from description          │
│          • Extract top/mid/base notes with regex                   │
│          • Deduplicate by URL (row_number)                         │
│          • Generate perfume_string (accords + notes concat)        │
│  Table:  silver_fragrance_cleaned                                  │
│  Expectations: NOT NULL checks on id, name, url                    │
└─────────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER (Embeddings & ML Features)                             │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
│  Input:  silver_fragrance_cleaned                                  │
│  Logic:  Generate embeddings from perfume_string                   │
│          • Model: all-MiniLM-L6-v2                                 │
│          • Pandas UDF for distributed processing                   │
│          • Add partition_key (id % 10)                             │
│  Table:  gold_fragrance_embeddings                                 │
│  Sync:   Auto-sync to Vector Search index                         │
└─────────────────────────────────────────────────────────────────────┘
```

### Implementation Steps

#### Step 1: Create Bronze Layer (`pipeline/bronze/bronze_fragrance_raw.py`)
```python
import dlt

@dlt.table(
    name="bronze_fragrance_raw",
    comment="Raw fragrance data ingested from Unity Catalog Volume"
)
def bronze_fragrance_raw():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load("/Volumes/fragrance_db/default/data/frag_raw.csv")
    )
```

#### Step 2: Create Silver Layer (`pipeline/silver/silver_fragrance_cleaned.py`)
```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="silver_fragrance_cleaned",
    comment="Cleaned and enriched fragrance data"
)
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_url", "url IS NOT NULL")
def silver_fragrance_cleaned():
    # Port the SQL transformation from clean_from_raw notebook
    df = dlt.read_stream("bronze_fragrance_raw")
    
    # Create temp view and apply SQL logic
    df.createOrReplaceTempView("raw_view")
    
    return spark.sql("""
        WITH ranked AS (
            SELECT
                REGEXP_EXTRACT(url, '([a-zA-Z0-9]+)\\.html, 1) as id,
                -- ... (rest of transformation from notebook)
                row_number() over (partition by url order by rating_count desc) as rn
            FROM raw_view
        )
        SELECT * FROM ranked WHERE rn = 1
    """)
```

#### Step 3: Create Gold Layer (`pipeline/gold/gold_fragrance_embeddings.py`)
```python
import dlt
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import ArrayType, FloatType
from sentence_transformers import SentenceTransformer
import pandas as pd

# Load model globally
model = SentenceTransformer("all-MiniLM-L6-v2")

@pandas_udf(ArrayType(FloatType()))
def get_embeddings_udf(texts: pd.Series) -> pd.Series:
    embeddings = model.encode(texts.tolist(), convert_to_tensor=False)
    return pd.Series(embeddings.tolist())

@dlt.table(
    name="gold_fragrance_embeddings",
    comment="Vector embeddings for semantic search"
)
def gold_fragrance_embeddings():
    return (
        dlt.read_stream("silver_fragrance_cleaned")
            .filter((col("perfume_string").isNotNull()) & (col("perfume_string") != ""))
            .withColumn("embedding", get_embeddings_udf(col("perfume_string")))
            .withColumn("partition_key", (col("id") % 10).cast("string"))
    )
```

#### Step 3b: Understanding Embeddings Table vs Vector Search Index

**Important Distinction**: The Gold layer produces a **Delta table**, not a Vector Search index.

```
┌────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER OUTPUT: fragrance_db.default.gold_fragrance_embeddings │
│  ──────────────────────────────────────────────────────────────────│
│  Type: Delta Lake Table (Data Warehouse)                          │
│  Purpose: Store embeddings for ETL, analytics, and as VS source   │
│  Schema:                                                           │
│    • id (bigint) - Primary key                                     │
│    • perfume_string (string) - Text description                   │
│    • embedding (array<float>) - 384-dim vector                    │
│    • partition_key (string) - For data organization               │
│  Query Method: Standard SQL (spark.table())                       │
│  Latency: Seconds (full table scan for similarity)                │
└────────────────────────────────────────────────────────────────────┘
                              │
                              │ Delta Sync (separate process)
                              │ Triggered manually or on schedule
                              ↓
┌────────────────────────────────────────────────────────────────────┐
│  VECTOR SEARCH INDEX: fragrance_db.default.fragrance_vs_index      │
│  ──────────────────────────────────────────────────────────────────│
│  Type: Databricks Vector Search Index (Search Engine)             │
│  Purpose: Fast semantic + keyword search in production             │
│  Index Structure: HNSW graph for nearest-neighbor lookup          │
│  Source: Syncs from gold_fragrance_embeddings table               │
│  Query Method: VectorSearchClient API                              │
│  Latency: Milliseconds (~20-50ms)                                  │
│  Features: Hybrid search (vector + keyword), filters               │
└────────────────────────────────────────────────────────────────────┘
```

**Key Points**:

1. **Separate Components**: The embeddings table and vector search index are distinct
   - Gold layer **writes** to Delta table
   - Vector search index **reads** from Delta table via sync

2. **When to Use Each**:
   - **Delta Table** (`gold_fragrance_embeddings`): 
     - ETL pipeline output
     - Batch analytics and reporting
     - Data quality checks
     - Manual SQL queries
   
   - **Vector Search Index** (`fragrance_vs_index`):
     - Production search API
     - Real-time user queries
     - Sub-second latency requirements
     - Hybrid search (semantic + keywords)

3. **Sync Strategy**:
   - **Phase 2** (Initial): Manual/triggered sync after pipeline completes
   - **Phase 3** (Future): Continuous sync on Delta table changes

4. **Cost Structure**:
   - Delta table: Storage only (~$23/TB/month)
   - Vector Search index: Storage + compute endpoint (~$0.07/compute-hour + storage)

**Setup Sequence** (Phase 2):
```bash
# 1. Run Lakeflow Pipeline → creates/updates gold_fragrance_embeddings
# 2. Create Vector Search endpoint (one-time)
# 3. Create Vector Search index pointing to gold_fragrance_embeddings
# 4. Trigger initial sync to populate index
# 5. Use index for production queries
```

See `setup/setup_vector_search_hybrid.py` for index creation code.

#### Step 4: Deploy Pipeline
```bash
# Create DLT pipeline via UI or API
databricks pipelines create \
  --name "fragrance_etl_pipeline" \
  --storage "/pipelines/fragrance_etl" \
  --target "fragrance_db.default" \
  --notebooks "pipeline/bronze,pipeline/silver,pipeline/gold" \
  --continuous false  # Triggered mode for now
```

#### Step 5: Schedule & Monitor
- Schedule via Databricks Jobs (daily/weekly)
- Monitor via Pipeline UI (data quality expectations, lineage)
- Set up alerts for pipeline failures

---

## 🚀 Long-Term Goal: Incremental Updates (Phase 3)

### Objective
**Enable ongoing, automated ingestion of new and updated fragrances**

### Requirements

**R1: Incremental Data Detection**
- Detect new fragrance records added to source
- Identify updates to existing fragrances (price, rating, notes)
- Handle deletes (fragrance discontinued)

**R2: Efficient Processing**
- Process only changed records (not full reload)
- Minimize compute cost and latency
- Support near-real-time updates (streaming) or batch (daily)

**R3: Data Quality**
- Maintain referential integrity across tables
- Prevent duplicate embeddings
- Track data lineage and change history

**R4: Vector Search Sync**
- Automatically update vector search index on changes
- Handle embedding regeneration for updated fragrances
- Maintain search consistency

### Proposed Architecture

#### Option A: Change Data Feed (CDC) Approach

**Assumption**: Source provides CDC stream (Kafka, Delta Change Feed, API webhooks)

```
┌───────────────────────────────────────────────────────────────┐
│  CHANGE DATA STREAM                                           │
│  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━  │
│  Source: Fragrantica API / Web scraper / Delta CDF           │
│  Format: JSON events {id, operation, timestamp, payload}      │
│  Operations: INSERT, UPDATE, DELETE                           │
└───────────────────────────────────────────────────────────────┘
                         ↓
┌───────────────────────────────────────────────────────────────┐
│  BRONZE: RAW CHANGE EVENTS                                    │
│  Table: bronze_fragrance_changes                              │
│  Append-only log of all changes                               │
└───────────────────────────────────────────────────────────────┘
                         ↓
┌───────────────────────────────────────────────────────────────┐
│  SILVER: APPLY CHANGES (MERGE)                                │
│  Logic: dlt.apply_changes()                                   │
│  • Track SCD Type 1 (current state)                           │
│  • Sequence by: timestamp                                     │
│  • Keys: id                                                   │
│  Table: silver_fragrance_cleaned (upsert target)              │
└───────────────────────────────────────────────────────────────┘
                         ↓
┌───────────────────────────────────────────────────────────────┐
│  GOLD: INCREMENTAL EMBEDDINGS                                 │
│  Logic: Detect changed perfume_string → regenerate embedding  │
│  • Use _change_type to filter inserts/updates                 │
│  • Skip unchanged records                                     │
│  • MERGE into gold_fragrance_embeddings                       │
└───────────────────────────────────────────────────────────────┘
```

**Code Snippet (Silver Layer with CDC)**:
```python
import dlt

dlt.create_streaming_table("bronze_fragrance_changes")

dlt.apply_changes(
    target="silver_fragrance_cleaned",
    source="bronze_fragrance_changes",
    keys=["id"],
    sequence_by="timestamp",
    stored_as_scd_type=1  # Current state only
)
```

#### Option B: Timestamp-Based Incremental (Batch)

**Assumption**: Source has `last_updated` timestamp column

```
1. Track high-water mark (max timestamp processed)
2. Query source for records WHERE last_updated > high_water_mark
3. Apply transformations to delta records only
4. MERGE into silver table
5. Regenerate embeddings for changed records
6. Update high-water mark
```

**Code Snippet**:
```python
import dlt
from pyspark.sql import functions as F

@dlt.table
def silver_fragrance_cleaned():
    # Read bronze with watermark
    df = dlt.read_stream("bronze_fragrance_raw")
    
    # Apply transformations (same as Phase 2)
    transformed = apply_transformations(df)
    
    # Output as merge-compatible stream
    return transformed

# In pipeline config, set:
# - applyChanges: true
# - sequenceBy: "last_updated"
```

### Implementation Roadmap (Phase 3)

#### Week 1-2: Source Integration
- [ ] Identify change data source (API, scraper, file drops)
- [ ] Design event schema (id, operation, timestamp, payload)
- [ ] Set up ingestion connector (Kafka, API polling, Auto Loader)
- [ ] Validate change event quality

#### Week 3-4: CDC Pipeline
- [ ] Implement `bronze_fragrance_changes` streaming table
- [ ] Add `dlt.apply_changes()` to silver layer
- [ ] Test INSERT, UPDATE, DELETE operations
- [ ] Validate deduplication and merge logic

#### Week 5-6: Incremental Embeddings
- [ ] Add change detection in gold layer (hash `perfume_string`)
- [ ] Implement conditional embedding regeneration
- [ ] Optimize for delta-only processing
- [ ] Benchmark performance (cost, latency)

#### Week 7-8: Vector Search Integration
- [ ] Configure vector search index for incremental updates
- [ ] Test sync behavior on MERGE operations
- [ ] Validate search consistency (no stale results)
- [ ] Monitor sync latency

#### Week 9-10: Testing & Validation
- [ ] End-to-end testing with synthetic change stream
- [ ] Load testing (1K, 10K, 100K updates/day)
- [ ] Data quality validation (no orphaned embeddings)
- [ ] Rollback/recovery procedures

#### Week 11-12: Production Deployment
- [ ] Deploy to production environment
- [ ] Configure monitoring & alerts (SLA: < 5 min latency)
- [ ] Document operational runbooks
- [ ] Train team on maintenance procedures

---

## 📊 Success Metrics

### Phase 2 (Productionization)
- ✓ Pipeline runs successfully without manual intervention
- ✓ 100% of notebook transformation logic ported
- ✓ Data quality expectations pass (no dropped records)
- ✓ Pipeline completes in < 30 minutes (full load)

### Phase 3 (Incremental Updates)
- ✓ Ingests new fragrances within 5 minutes of source update
- ✓ Processes only changed records (< 1% of full dataset per run)
- ✓ Maintains 99.9% data consistency with source
- ✓ Vector search reflects updates within 10 minutes
- ✓ Zero manual intervention for 99% of runs

---

## 🛡️ Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| **Schema drift in source data** | High | Add schema evolution to bronze layer with notifications |
| **Duplicate embeddings after updates** | Medium | Use MERGE with id as key, track embedding_version |
| **Vector search sync lag** | Medium | Monitor sync latency, add manual sync trigger if needed |
| **Costly full embedding regeneration** | High | Implement smart delta detection (hash perfume_string) |
| **Pipeline failure cascades** | High | Add circuit breakers, retry logic, dead-letter queues |

---

## 📚 Next Steps

**Immediate (This Week)**
1. Review and approve this plan
2. Set up Lakeflow Pipeline development environment
3. Port bronze layer notebook logic to pipeline script

**Short-Term (Next 2 Weeks)**
1. Complete Phase 2 implementation (bronze/silver/gold)
2. Deploy to dev environment and run end-to-end test
3. Compare output with manual notebook results

**Medium-Term (Next Quarter)**
1. Research source system for change data availability
2. Design CDC event schema and ingestion pattern
3. Prototype Phase 3 incremental pipeline

---

**Document Owner**: Data Engineering Team  
**Last Updated**: 2025-01-20  
**Status**: Phase 1 Complete, Phase 2 Planned, Phase 3 Roadmap Defined