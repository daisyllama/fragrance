# Fragrance Intelligence Platform

A comprehensive data platform for fragrance analysis, semantic search, and recommendation using vector embeddings and hybrid search capabilities.

## Overview

This project implements an end-to-end pipeline for processing fragrance data and enabling intelligent search through:
- Data ingestion and cleaning from raw fragrance datasets
- Vector embeddings generation for semantic search
- Hybrid search combining vector similarity and keyword matching
- Real-time fragrance discovery and recommendations

## Project Structure

```
sniffers/
├── README.md                    # Project documentation
├── PLAN.md                      # ETL productionization roadmap
├── data/
│   ├── raw/                     # Original source data
│   │   └── frag_raw.csv        # Raw fragrance dataset (31MB)
│   ├── processed/               # Legacy workspace files (archived)
│   │   ├── frag_cleaned_sh.csv # Legacy format (superseded by Unity Catalog)
│   │   └── all_unique_accords_and_notes.json  # Extracted fragrance taxonomy
│   └── archive/
│       └── frag_cleaned_ref.csv # Earlier reference snapshot of cleaned data
├── pipeline/                    # Lakeflow ETL Pipeline (Bronze/Silver/Gold) 
│   ├── bronze/
│   │   └── bronze_fragrance_raw.py          # Auto Loader ingestion from UC Volume
│   ├── silver/
│   │   └── silver_fragrance_cleaned.py      # Data cleaning and quality checks
│   └── gold/
│       └── gold_fragrance_embeddings.py     # Vector embeddings generation
├── notebooks/
│   ├── 01_data_processing/      # Data ingestion and cleaning
│   │   └── clean_from_raw      # Transform raw data into clean format
│   ├── 02_embeddings/           # Vector embeddings generation
│   │   ├── generate_embeddings_job            # Batch embedding generation
│   │   └── generate_embeddings_for_new_frag   # Real-time embedding for new entries
│   ├── 03_search/               # Search and discovery
│   │   └── perfume_search      # Hybrid search implementation
│   └── 04_maintenance/          # Data management
│       └── add_update_fragrance # CRUD operations for fragrance records
├── setup/
│   └── setup_vector_search_hybrid.py  # Vector search index configuration
├── sql/
│   ├── create generate_perfume_string function.dbquery.ipynb  # UDF for text generation
│   ├── create mv_clean_frag.sql.dbquery.ipynb                # Materialized view definition
│   └── cardinality_of_embeddings.dbquery.ipynb               # Data quality checks
└── scraping/                    # Local pre-Databricks stage: produces data/raw/frag_raw.csv
    ├── notebooks/
    │   ├── scraper.ipynb
    │   └── fragrantica_scraper.ipynb
    └── fixtures/                # Sample scraped HTML pages used to test the scrapers
```

## Getting Started

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Vector Search endpoint configured
- Serverless compute or cluster with ML runtime

### Setup Instructions

1. **Initialize Vector Search Infrastructure**
   ```bash
   # Run the setup script to create vector search index
   python setup/setup_vector_search_hybrid.py
   ```

2. **Create Database Objects**
   ```sql
   -- Run SQL setup queries in order:
   -- 1. Create the perfume string generation UDF
   -- 2. Create the materialized view for clean fragrances
   ```

3. **Process Data Pipeline**
   ```
   01_data_processing/clean_from_raw
   → 02_embeddings/generate_embeddings_job
   → 03_search/perfume_search
   ```

## Scraping (Local, Pre-Databricks)

Before data reaches `data/raw/frag_raw.csv`, it's collected locally with `scraping/notebooks/scraper.ipynb` and `scraping/notebooks/fragrantica_scraper.ipynb`. `scraping/fixtures/` holds sample scraped HTML pages used to test the scraper parsing logic against real markup; these fixtures are gitignored since they're large local test data, not source.

A `legacy/` folder (gitignored) may exist locally with superseded notebook/data versions kept for reference — it's not part of the tracked project.

## Data Pipeline

### Lakeflow ETL Pipeline (Medallion Architecture)

The project includes a production-ready **Lakeflow Spark Declarative Pipeline** implementing the medallion architecture:

** Bronze Layer** (`pipeline/bronze/`)
- **Purpose**: Raw data ingestion
- **Table**: `fragrance_db.default.bronze_fragrance_raw`
- **Method**: Auto Loader with streaming from Unity Catalog Volume
- **Features**: Automatic schema inference and type detection

** Silver Layer** (`pipeline/silver/`)
- **Purpose**: Data cleaning and quality validation
- **Table**: `fragrance_db.default.silver_fragrance_cleaned`
- **Process**: Parse notes, validate formats, extract metadata
- **Output**: Clean, structured fragrance data

** Gold Layer** (`pipeline/gold/`)
- **Purpose**: Business-ready analytics and ML features
- **Table**: `fragrance_db.default.gold_fragrance_embeddings`
- **Process**: Generate vector embeddings using sentence-transformers
- **Output**: Embeddings optimized for semantic search

### Notebook-Based Processing

#### Stage 1: Data Ingestion
- **Input**: `/Volumes/fragrance_db/default/data/frag_raw.csv` (31MB raw dataset)
- **Notebook**: `01_data_processing/clean_from_raw`
- **Process**: Clean, normalize, and enrich fragrance metadata with SQL transformations
- **Outputs**:
  - Delta table: `fragrance_db.default.fragrance_cleaned` (69,948 rows, 17 columns)
  - CSV export: `/Volumes/fragrance_db/default/data/frag_cleaned_with_texts.csv` (50MB)
  - Key column: `perfume_string` (concatenated accords and notes for embeddings)

#### Stage 2: Embedding Generation
- **Input**: `/Volumes/fragrance_db/default/data/frag_cleaned_with_texts.csv`
- **Notebook**: `02_embeddings/generate_embeddings_job`
- **Model**: `all-MiniLM-L6-v2` sentence-transformers
- **Process**: Generate 384-dimensional embeddings from `perfume_string` column
- **Output**: Embeddings table `fragrance_db.default.fragrance_embeddings` with partition keys for vector search indexing

### Stage 3: Search & Discovery
- **Method**: Hybrid search (vector similarity + keyword matching)
- **Features**: 
  - Semantic search by scent profile
  - Filter by notes, accords, brand, or season
  - Similarity-based recommendations

## Key Features

- **Semantic Search**: Find fragrances by natural language descriptions
- **Hybrid Retrieval**: Combines vector similarity with traditional filters
- **Real-time Updates**: Add and embed new fragrances on-the-fly
- **Quality Monitoring**: Built-in data validation and embedding cardinality checks

## Monitoring & Maintenance

- **Data Quality**: Run `sql/cardinality_of_embeddings` to verify embedding coverage
- **Add New Data**: Use `notebooks/04_maintenance/add_update_fragrance`
- **Refresh Embeddings**: Execute `notebooks/02_embeddings/generate_embeddings_for_new_frag`

## Technology Stack

- **Data Processing**: Databricks, Delta Lake
- **Vector Search**: Databricks Vector Search
- **Embeddings**: Transformer-based models
- **Storage**: Unity Catalog managed tables
- **Compute**: Databricks Serverless

## Notes

### Data Storage Architecture
- **Production Tables**: All active data stored in Unity Catalog Delta tables
  - Source: `fragrance_db.default.fragrance_cleaned` (69,948 fragrances)
  - Location: `/Volumes/fragrance_db/default/data/`
- **Legacy Files**: Workspace files (`frag_cleaned_sh.csv`) are superseded by UC tables
- **Embeddings**: Vector representations generated from `perfume_string` column
- **Search**: Hybrid approach combining semantic similarity with metadata filters

### Pipeline Validation (Last Run)
```
✓ Input:  frag_raw.csv                    30.73 MB
✓ Output: fragrance_cleaned (Delta)       69,948 rows, 17 columns
✓ Export: frag_cleaned_with_texts.csv     50.29 MB
✓ Used by: generate_embeddings_job        ← downstream consumer
```

---

**Last Updated**: July 2026  
**Status**: Production Ready