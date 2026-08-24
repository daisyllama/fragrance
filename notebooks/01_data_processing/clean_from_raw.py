# Databricks notebook source
file_path = "/Volumes/fragrance_db/default/data/frag_raw.csv"

# Read the CSV file into a DataFrame
frag_raw_df = spark.read.csv(
    str(file_path),
    header=True,
    inferSchema=True
)
frag_raw_df.show(n=5, truncate=False)

print("\n ======================================= \n Data Types:")
print(frag_raw_df.dtypes)

# COMMAND ----------

file_path = "/Volumes/fragrance_db/default/data/frag_raw.csv"

try:
    frag_raw_df = spark.read.csv(str(file_path), header=True, inferSchema=True)
    frag_raw_df.createOrReplaceTempView("frag_raw")

    cleaned_df = spark.sql("""
    WITH ranked AS (
        SELECT
            REGEXP_EXTRACT(url, '([a-zA-Z0-9]+)\\.html$', 1) as id,
            replace(regexp_extract(url, '/perfume/[^/]+/([^/]+)-[0-9]+\\.html', 1), '-', ' ') as name,
            replace(regexp_extract(url, '/perfume/([^/]+)/', 1), '-', ' ') as brand,
            REGEXP_EXTRACT(description, 'was launched in ([0-9]{4})', 1) as release_year,
            replace(replace(replace(perfumers, '[', ''), ']', ''),"'", "") as perfumers,
            CASE
                WHEN name LIKE '%for women and men' THEN 'unisex'
                WHEN name LIKE '%for women' THEN 'women'
                WHEN name LIKE '%for men' THEN 'men'
                ELSE NULL
            END as gender,
            TRY_CAST(REGEXP_REPLACE(rating_count, ',', '') AS INT) as rating_count,
            TRY_CAST(rating AS DECIMAL(10,2)) as rating,
            replace(replace(replace(main_accords, '[', ''), ']', ''),"'", "") as accords,
            lower(replace(regexp_extract(description, '(?i)top note[s]? (is|are) ([^.;]*)', 2), ' and', ',')) as top_notes,
            lower(replace(regexp_extract(description, '(?i)middle note[s]? (is|are) ([^.;]*)', 2), ' and', ',')) as mid_notes,
            lower(replace(regexp_extract(description, '(?i)base note[s]? (is|are) ([^.;]*)', 2), ' and', ',')) as base_notes,
            description,
            url,
            row_number() over (partition by url order by rating_count desc) as rn
        FROM frag_raw
    )
    SELECT 
        *,
        TRIM(
        CONCAT(
            CASE WHEN accords IS NOT NULL AND accords != ''
                THEN ' ' || array_join(
                    transform(split(accords, ','), x -> concat('accords_', replace(trim(x), ' ', '_'))),
                    ' '
                )
                ELSE ''
            END,
            CASE WHEN top_notes IS NOT NULL AND top_notes != ''
                THEN ' ' || array_join(
                    transform(split(top_notes, ','), x -> concat('top_notes_', replace(trim(x), ' ', '_'))),
                    ' '
                )
                ELSE ''
            END,
            CASE WHEN mid_notes IS NOT NULL AND mid_notes != ''
                THEN ' ' || array_join(
                    transform(split(mid_notes, ','), x -> concat('mid_notes_', replace(trim(x), ' ', '_'))),
                    ' '
                )
                ELSE ''
            END,
            CASE WHEN base_notes IS NOT NULL AND base_notes != ''
                THEN ' ' || array_join(
                    transform(split(base_notes, ','), x -> concat('base_notes_', replace(trim(x), ' ', '_'))),
                    ' '
                )
                ELSE ''
            END
        )
    ) AS perfume_string
    FROM ranked
    WHERE rn = 1
    """)

    cleaned_df.show(truncate=False)
    print(f"Number of rows in cleaned_df: {cleaned_df.count()}")

    # print("Schema:")
    # cleaned_df.printSchema()

    print("\n ======================================= \n Data Types:")
    print(cleaned_df.dtypes)

    # Save to Delta table
    cleaned_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable("fragrance_db.default.fragrance_cleaned")
    print("Delta table updated!")

    # Save to csv file
    cleaned_df.toPandas().to_csv("/Volumes/fragrance_db/default/data/frag_cleaned_with_texts.csv", index=False)

except Exception as e:
    print(f"An error occurred: {e}")


# COMMAND ----------

Not required after here.

# COMMAND ----------


# from pyspark.sql.functions import split, trim, to_json, col, regexp_replace

# def to_json_array(df, col_name, new_col_name):
#     return df.withColumn(
#         new_col_name,
#         to_json(
#             split(
#                 trim(regexp_replace(col(col_name), r"[\[\]']", "")),
#                 r",\s*"
#             )
#         )
#     )

# # Apply to all relevant columns
# cleaned_df = to_json_array(cleaned_df, "main_accords", "main_accords_json")
# cleaned_df = to_json_array(cleaned_df, "top_notes", "top_notes_json")
# cleaned_df = to_json_array(cleaned_df, "mid_notes", "mid_notes_json")
# cleaned_df = to_json_array(cleaned_df, "base_notes", "base_notes_json")

# cleaned_df.show(n=10, truncate=False)


# cleaned_df.toPandas().to_csv("frag_cleaned_sh.csv", index=False)
# print("\n======================================= \n\nCleaned data written to frag_cleaned_sh.csv")

# print("\n======================================= \n\nData Types:")
# print(cleaned_df.dtypes)


# COMMAND ----------

# cleaned_df.createOrReplaceTempView("temp_view")
# filtered_df = spark.sql("""
#         SELECT
#             *
#         FROM temp_view
#         WHERE `main_accords_json` LIKE '%this perfume is%'
#                         or `top_notes_json` LIKE '%this perfume is%'
#                         or `mid_notes_json` LIKE '%this perfume is%'
#                         or `base_notes_json` LIKE '%this perfume is%'
#     """)

# filtered_df.show(truncate=False)
# print(f"Number of rows in filtered_df: {filtered_df.count()}")

# COMMAND ----------

# # Get all unique accords and notes from the JSON columns
# from pyspark.sql.functions import explode, from_json, array_distinct
# from pyspark.sql.types import ArrayType, StringType

# # Parse JSON arrays back to Spark arrays and explode
# def get_unique_from_json_col(df, json_col):
#     return (
#         df
#         .withColumn("arr", from_json(col(json_col), ArrayType(StringType())))
#         .select(explode(col("arr")).alias("item"))
#         .distinct()
#         .select("item")
#     )

# # Collect unique values from each column
# main_accords_unique = get_unique_from_json_col(cleaned_df, "main_accords_json")
# top_notes_unique = get_unique_from_json_col(cleaned_df, "top_notes_json")
# mid_notes_unique = get_unique_from_json_col(cleaned_df, "mid_notes_json")
# base_notes_unique = get_unique_from_json_col(cleaned_df, "base_notes_json")

# # Union all and get unique values
# all_unique = (
#     main_accords_unique
#     .union(top_notes_unique)
#     .union(mid_notes_unique)
#     .union(base_notes_unique)
#     .distinct()
#     .orderBy("item")
# )

# all_unique_list = [row.item for row in all_unique.collect()]
# print("\n======================================= \n \nAll Unique Accords and Notes:")
# print(all_unique_list)
# print(f"Number of distinct items: {len(all_unique_list)}")

# COMMAND ----------

# import pandas as pd
# import json

# # Write the unique accords and notes to a JSON file
# with open("all_unique_accords_and_notes.json", "w", encoding="utf-8") as f:
#     json.dump(all_unique_list, f, ensure_ascii=False, indent=2)

# print("Unique accords and notes written to all_unique_accords_and_notes.json")
