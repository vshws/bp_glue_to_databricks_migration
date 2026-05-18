"""
Bronze Layer: Counterparties Ingestion
Ingests counterparties data from S3 using Auto Loader
"""

from pyspark import pipelines as dp

@dp.table(
    name="bronze.counterparties",
    comment="Raw counterparties data ingested from S3 using Auto Loader"
)
def counterparties():
    """
    Ingests counterparties CSV files from S3 using Auto Loader.
    Schema is automatically inferred with type detection enabled.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://bp-glue-demo/raw/counterparties/")
    )
