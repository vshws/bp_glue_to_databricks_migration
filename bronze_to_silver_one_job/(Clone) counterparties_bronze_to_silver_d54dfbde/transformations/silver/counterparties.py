"""
Silver Layer: Counterparties Cleaning and Validation
Reads from bronze, applies data quality rules, and produces clean counterparties
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name="silver.counterparties",
    comment="Clean and validated counterparties with data quality checks applied",
    cluster_by=["country", "credit_rating"]
)
def counterparties():
    """
    Cleans and validates counterparties data:
    1. Trims whitespace from names
    2. Standardizes country codes to uppercase
    3. Parses onboarded_date with multiple format support
    4. Filters out records with missing critical fields
    5. Validates credit ratings
    6. Deduplicates by counterparty_id (keeps most recent)
    """
    # Read from bronze
    df_raw = spark.read.table("bronze.counterparties")
    
    # Step 1: Trim & standardize
    df_clean = (
        df_raw
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("country", F.upper(F.col("country")))
    )
    
    # Step 2: Parse onboarded_date with multiple format support
    df_clean = df_clean.withColumn(
        "onboarded_date_parsed",
        F.coalesce(
            F.try_to_date("onboarded_date", "yyyy-MM-dd"),
            F.try_to_date("onboarded_date", "dd/MM/yyyy")
        )
    )
    
    # Step 3: Filter out bad records (missing critical fields or future dates)
    df_clean = df_clean.filter(
        F.col("name").isNotNull() &
        F.col("country").isNotNull() &
        F.col("onboarded_date_parsed").isNotNull() &
        (F.col("onboarded_date_parsed") <= F.current_date())
    )
    
    # Step 4: Validate credit ratings
    valid_ratings = ["AAA", "AA", "A", "BBB", "BB", "B", "CCC"]
    df_clean = df_clean.filter(F.col("credit_rating").isin(valid_ratings))
    
    # Step 5: Deduplicate by counterparty_id (keep most recent onboarded_date)
    window_spec = Window.partitionBy("counterparty_id").orderBy(F.col("onboarded_date_parsed").desc())
    
    df_final = (
        df_clean
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    
    return df_final
