# Databricks notebook source
frag_raw_df = spark.table("fragrance_db.default.frag_raw")
frag_raw_df.show(n=5, truncate=False)

print("\n ======================================= \n Data Types:")
print(frag_raw_df.dtypes)

# COMMAND ----------

try:
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
        FROM fragrance_db.default.frag_raw
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
