from pyspark import pipelines as dp

@dp.table(
    comment="Raw fragrance data ingested from CSV files in Unity Catalog Volume"
)
def bronze_fragrance_raw():
    """
    Ingests raw fragrance CSV files from /Volumes/fragrance_db/default/data/
    using Auto Loader with automatic schema inference and type detection.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load("/Volumes/fragrance_db/default/data/")
    )
