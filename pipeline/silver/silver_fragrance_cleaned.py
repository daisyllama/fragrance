from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Cleaned fragrance data with parsed fields and data quality checks"
)
@dp.expect_or_fail("valid_id", "id IS NOT NULL")
def silver_fragrance_cleaned():
    """
    Transforms bronze fragrance data by:
    - Parsing URL to extract id, name, brand
    - Parsing description for release year and notes (top, mid, base)
    - Generating perfume_string using UDF
    - Applying data quality expectation on id
    """
    return (
        spark.readStream.table("bronze_fragrance_raw")
        .withColumn("id", F.expr("fragrance_db.default.extract_id(url)"))
        .withColumn("name", F.expr("fragrance_db.default.extract_name(url)"))
        .withColumn("brand", F.expr("fragrance_db.default.extract_brand(url)"))
        .withColumn("release_year", F.expr("fragrance_db.default.extract_release_year(description)"))
        .withColumn("accords", F.expr("fragrance_db.default.extract_accord(main_accords)"))
        .withColumn("top_notes", F.expr("fragrance_db.default.extract_top_notes(description)"))
        .withColumn("mid_notes", F.expr("fragrance_db.default.extract_mid_notes(description)"))
        .withColumn("base_notes", F.expr("fragrance_db.default.extract_base_notes(description)"))
        .withColumn("perfume_string", 
                   F.expr("fragrance_db.default.generate_perfume_string(accords, top_notes, mid_notes, base_notes)"))
        .select(
            "id",
            "name",
            "brand",
            "release_year",
            "gender",
            "accords",
            "top_notes",
            "mid_notes",
            "base_notes",
            "perfume_string",
            "description",
            "url"
        )
    )
