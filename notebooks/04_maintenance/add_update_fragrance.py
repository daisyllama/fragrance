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
# MAGIC 3. **Preview Transformation** (Cell 6): See how data will be processed
# MAGIC    - Applies UDFs to extract id, name, brand, notes
# MAGIC    - Generates perfume_string for embeddings
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
# MAGIC - Run `generate_embeddings_for_new_frag` notebook to create vector embeddings
# MAGIC - Sync to Vector Search index for production queries

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

# DBTITLE 1,Preview: Transform raw data using UDFs
# MAGIC %sql
# MAGIC -- Preview the transformation before merging
# MAGIC -- This shows how the raw input will be processed
# MAGIC
# MAGIC with a as (
# MAGIC     SELECT
# MAGIC         fragrance_db.default.extract_id(url) AS id,
# MAGIC         fragrance_db.default.extract_name(url) AS name,
# MAGIC         fragrance_db.default.extract_brand(url) AS brand,
# MAGIC         fragrance_db.default.extract_release_year(url) AS release_year,
# MAGIC         gender,
# MAGIC         fragrance_db.default.extract_accord(main_accords) AS accords,
# MAGIC         fragrance_db.default.extract_top_notes(description) AS top_notes,
# MAGIC         fragrance_db.default.extract_mid_notes(description) AS mid_notes,
# MAGIC         fragrance_db.default.extract_base_notes(description) AS base_notes,
# MAGIC         url,
# MAGIC         description
# MAGIC     FROM temp_input
# MAGIC     )
# MAGIC     select *, fragrance_db.default.generate_perfume_string(accords, top_notes, mid_notes, base_notes) as perfume_string
# MAGIC     from a
# MAGIC ;

# COMMAND ----------

# DBTITLE 1,Check: Compare with existing data
# MAGIC %sql
# MAGIC -- Compare the transformed data with existing data in the database
# MAGIC -- Change the id to match your input (extract from url)
# MAGIC
# MAGIC SELECT * FROM fragrance_db.default.fragrance_cleaned
# MAGIC WHERE id = (SELECT fragrance_db.default.extract_id(url) FROM temp_input);

# COMMAND ----------

# DBTITLE 1,MERGE into fragrance_cleaned (transformed data layer)
# MAGIC %sql
# MAGIC -- Step 2: Merge into cleaned data table
# MAGIC -- This stores the transformed and enriched data
# MAGIC
# MAGIC MERGE INTO fragrance_db.default.fragrance_cleaned AS target
# MAGIC USING (
# MAGIC   with a as (
# MAGIC     SELECT
# MAGIC         fragrance_db.default.extract_id(url) AS id,
# MAGIC         fragrance_db.default.extract_name(url) AS name,
# MAGIC         fragrance_db.default.extract_brand(url) AS brand,
# MAGIC         fragrance_db.default.extract_release_year(url) AS release_year,
# MAGIC         gender,
# MAGIC         fragrance_db.default.extract_accord(main_accords) AS accords,
# MAGIC         fragrance_db.default.extract_top_notes(description) AS top_notes,
# MAGIC         fragrance_db.default.extract_mid_notes(description) AS mid_notes,
# MAGIC         fragrance_db.default.extract_base_notes(description) AS base_notes,
# MAGIC         url,
# MAGIC         description
# MAGIC     FROM temp_input
# MAGIC     )
# MAGIC     select *, fragrance_db.default.generate_perfume_string(accords, top_notes, mid_notes, base_notes) as perfume_string
# MAGIC     from a
# MAGIC ) as source
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
# MAGIC WHERE id = (SELECT fragrance_db.default.extract_id(url) AS id FROM temp_input);
# MAGIC
# MAGIC -- Delete from cleaned table
# MAGIC DELETE FROM fragrance_db.default.fragrance_cleaned
# MAGIC WHERE id = (SELECT fragrance_db.default.extract_id(url) AS id FROM temp_input);
# MAGIC
# MAGIC -- Delete from raw table
# MAGIC DELETE FROM fragrance_db.default.frag_raw
# MAGIC WHERE url = (SELECT url FROM temp_input);