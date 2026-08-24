# Databricks notebook source
# MAGIC %md
# MAGIC # Add/Update Fragrance - Manual Entry Tool
# MAGIC
# MAGIC ## Workflow Overview
# MAGIC
# MAGIC This notebook allows you to manually add or update fragrance records across all data layers:
# MAGIC
# MAGIC 1. **Input** (Cells 1-4): Paste raw data from Fragrantica
# MAGIC    - Accords from website → Cell 1
# MAGIC    - URL, description, gender → Widgets (Cell 2-3)
# MAGIC    - Creates `temp_input` view
# MAGIC
# MAGIC 2. **MERGE into frag_raw** (Cell 5): Store raw input
# MAGIC    - Preserves original data before transformation
# MAGIC    - Uses URL as key
# MAGIC
# MAGIC 3. **Build cleaned_input** (Cell 6): Apply the same regex transform
# MAGIC    `clean_from_raw.py` uses (id/name/brand/release_year/notes from url +
# MAGIC    description), reused here as one shared view instead of two copies
# MAGIC
# MAGIC 4. **Check Existing Data** (Cell 7): Compare with database
# MAGIC    - Verify if record exists and what will change
# MAGIC
# MAGIC 5. **MERGE into fragrance_cleaned** (Cell 8): Store transformed data
# MAGIC    - INSERT if new, UPDATE if exists
# MAGIC
# MAGIC 6. **Delete (Testing)** (Cell 9): Remove from all tables
# MAGIC    - ⚠️ Only run this to test INSERT or remove bad data
# MAGIC    - Deletes from frag_raw, fragrance_cleaned, and fragrance_embeddings
# MAGIC
# MAGIC ## Next Steps After Running
# MAGIC
# MAGIC - Run `generate_embeddings_for_new_frag` notebook to create/update the embedding

# COMMAND ----------

import sys
sys.path.append("..")  # repo root, so `common` (shared with 01_clean_from_raw.py) is importable

from common.cleaning import EXTRACTED_FIELDS_SQL

# COMMAND ----------

# copy and paste the accords from website to the space between quotes below:
accords_raw = '''
amber
warm spicy
aromatic
vanilla
woody
balsamic
powdery
fresh spicy
lavender
sweet
'''

# COMMAND ----------

dbutils.widgets.text('url', '', '')
dbutils.widgets.text('description', '', '')
dbutils.widgets.dropdown('gender','',['', 'men', 'women', 'unisex'], '')

# COMMAND ----------

url = dbutils.widgets.get('url')
description = dbutils.widgets.get('description')
gender = dbutils.widgets.get('gender')
accords = str([line.strip() for line in accords_raw.split("\n") if line.strip()])


print(url)
print(accords)
print(description)
print(gender)

# COMMAND ----------

input = [(url, accords, description, gender)]
input_df = spark.createDataFrame(input, ["url", "main_accords", "description", "gender"])
input_df.show(truncate=False)

input_df.createOrReplaceTempView("temp_input")

# COMMAND ----------

# DBTITLE 1,MERGE into frag_raw (raw data layer)
# MAGIC %sql
# MAGIC -- Step 1: Merge into raw data table
# MAGIC -- This preserves the original input before any transformation
# MAGIC
# MAGIC MERGE INTO fragrance_db.default.frag_raw AS target
# MAGIC USING temp_input AS source
# MAGIC ON target.url = source.url
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.main_accords = source.main_accords,
# MAGIC     target.description = source.description,
# MAGIC     target.gender = source.gender
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     url,
# MAGIC     main_accords,
# MAGIC     description,
# MAGIC     gender
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.url,
# MAGIC     source.main_accords,
# MAGIC     source.description,
# MAGIC     source.gender
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,Build cleaned_input (same transform as clean_from_raw.py, one row)
# Same regex-based extraction 01_clean_from_raw.py uses for the bulk table
# (imported from common/cleaning.py — one definition, not two copies),
# applied here to just the one row in temp_input. No dependency on
# extract_* UDFs (their definitions aren't tracked in this repo, only
# generate_perfume_string is).
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cleaned_input AS
WITH a AS (
    SELECT
        {EXTRACTED_FIELDS_SQL},
        gender,
        url,
        description
    FROM temp_input
)
SELECT *, fragrance_db.default.generate_perfume_string(accords, top_notes, mid_notes, base_notes) as perfume_string
FROM a
""")

display(spark.sql("SELECT * FROM cleaned_input"))

# COMMAND ----------

# DBTITLE 1,Check: Compare with existing data
# MAGIC %sql
# MAGIC -- Compare the transformed data with existing data in the database
# MAGIC
# MAGIC SELECT * FROM fragrance_db.default.fragrance_cleaned
# MAGIC WHERE id = (SELECT id FROM cleaned_input);

# COMMAND ----------

# DBTITLE 1,MERGE into fragrance_cleaned (transformed data layer)
# MAGIC %sql
# MAGIC -- Step 2: Merge into cleaned data table
# MAGIC -- This stores the transformed and enriched data
# MAGIC
# MAGIC MERGE INTO fragrance_db.default.fragrance_cleaned AS target
# MAGIC USING cleaned_input AS source
# MAGIC on target.id = source.id
# MAGIC when matched then 
# MAGIC   UPDATE SET
# MAGIC         target.name = source.name,
# MAGIC         target.brand = source.brand,
# MAGIC         target.release_year = source.release_year,
# MAGIC         target.gender = source.gender,
# MAGIC         target.accords = source.accords,
# MAGIC         target.top_notes = source.top_notes,
# MAGIC         target.mid_notes = source.mid_notes,
# MAGIC         target.base_notes = source.base_notes,
# MAGIC         target.url = source.url,
# MAGIC         target.description = source.description,
# MAGIC         target.perfume_string = source.perfume_string
# MAGIC when not matched then
# MAGIC   INSERT (
# MAGIC     id, 
# MAGIC     name, 
# MAGIC     brand, 
# MAGIC     release_year, 
# MAGIC     gender, 
# MAGIC     accords, 
# MAGIC     top_notes, 
# MAGIC     mid_notes, 
# MAGIC     base_notes, 
# MAGIC     url, 
# MAGIC     description, 
# MAGIC     perfume_string
# MAGIC   )
# MAGIC   values (
# MAGIC     source.id, 
# MAGIC     source.name, 
# MAGIC     source.brand, 
# MAGIC     source.release_year, 
# MAGIC     source.gender, 
# MAGIC     source.accords, 
# MAGIC     source.top_notes, 
# MAGIC     source.mid_notes, 
# MAGIC     source.base_notes, 
# MAGIC     source.url, 
# MAGIC     source.description, 
# MAGIC     source.perfume_string
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,Test: Delete id to test insert
# MAGIC %sql
# MAGIC -- Delete from all three tables: frag_raw, fragrance_cleaned, and fragrance_embeddings
# MAGIC -- This is useful for testing or removing incorrect entries
# MAGIC
# MAGIC -- Delete from embeddings table
# MAGIC DELETE FROM fragrance_db.default.fragrance_embeddings
# MAGIC WHERE id = (SELECT id FROM cleaned_input);
# MAGIC
# MAGIC -- Delete from cleaned table
# MAGIC DELETE FROM fragrance_db.default.fragrance_cleaned
# MAGIC WHERE id = (SELECT id FROM cleaned_input);
# MAGIC
# MAGIC -- Delete from raw table
# MAGIC DELETE FROM fragrance_db.default.frag_raw
# MAGIC WHERE url = (SELECT url FROM temp_input);