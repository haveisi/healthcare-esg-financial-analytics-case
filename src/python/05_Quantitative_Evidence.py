# Databricks notebook source
from pyspark.sql import functions as F

evidence = spark.table(
    "marshfield_esg.silver.structured_evidence"
)

# COMMAND ----------

numeric_candidates = (
    evidence
    .filter(
        F.col("clean_text").rlike(
            r"\$|\d{1,3}(,\d{3})+|\d+(\.\d+)?%"
        )
    )
)

display(
    numeric_candidates.select(
        "source_id",
        "page_number",
        "evidence_domain",
        "topic",
        F.substring("clean_text", 1, 500).alias("preview")
    )
)

# COMMAND ----------

fin002 = numeric_candidates.filter(
    F.col("source_id") == "FIN002"
)

# COMMAND ----------

display(
    fin002.filter(
        F.lower("clean_text").contains("374,895")
    ).select(
        "page_number",
        "topic",
        "clean_text"
    )
)

# COMMAND ----------

display(
    fin002.filter(
        F.lower("clean_text").rlike(
            "174,830|174.83|taxable bonds"
        )
    ).select(
        "page_number",
        "topic",
        "clean_text"
    )
)

# COMMAND ----------

display(
    fin002.filter(
        F.lower("clean_text").rlike(
            "liquidity|days cash|operating revenue|operating expenses|debt service"
        )
    ).select(
        "page_number",
        "topic",
        "clean_text"
    )
)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType
)

metric_schema = StructType([
    StructField("metric_id", StringType(), False),
    StructField("source_id", StringType(), False),
    StructField("page_number", IntegerType(), False),

    StructField("evidence_domain", StringType(), True),
    StructField("topic", StringType(), True),

    StructField("metric_name", StringType(), False),
    StructField("metric_value", DoubleType(), True),
    StructField("unit", StringType(), True),

    StructField("reporting_year", IntegerType(), True),
    StructField("geography", StringType(), True),

    StructField("evidence_text", StringType(), True),

    StructField("verified", BooleanType(), True),
    StructField("notes", StringType(), True)
])

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            source_id,
            page_number,
            clean_text
        FROM marshfield_esg.silver.document_pages_clean
        WHERE source_id = 'FIN002'
          AND clean_text LIKE '%374,895%'
        ORDER BY page_number
    """)
)

# COMMAND ----------

metric_rows = [
    (
        "FIN002_M001",
        "FIN002",
        1,
        "FINANCIAL",
        "Debt and Financing",
        "Series 2024A revenue bonds",
        374895000.0,
        "USD",
        2024,
        "System-wide",
        "Official Statement reports $374.895 million of Series 2024A revenue bonds.",
        True,
        "Series 2024A tax-exempt bond issuance"
    )
]

# COMMAND ----------

metrics_df = spark.createDataFrame(
    metric_rows,
    schema=metric_schema
)

display(metrics_df)

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            page_number,
            clean_text
        FROM marshfield_esg.silver.document_pages_clean
        WHERE source_id = 'FIN002'
          AND (
              clean_text LIKE '%374895%'
              OR clean_text LIKE '%374.895%'
              OR lower(clean_text) LIKE '%series 2024a%'
          )
        ORDER BY page_number
    """)
)

# COMMAND ----------

metrics_df = spark.createDataFrame(
    metric_rows,
    schema=metric_schema
)

display(metrics_df)

# COMMAND ----------

metrics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.silver.quantitative_evidence")

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM marshfield_esg.silver.quantitative_evidence
    """)
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            page_number,
            clean_text
        FROM marshfield_esg.silver.document_pages_clean
        WHERE source_id = 'FIN002'
          AND (
              clean_text LIKE '%174,830%'
              OR clean_text LIKE '%174.83%'
              OR lower(clean_text) LIKE '%taxable bonds%'
          )
        ORDER BY page_number
    """)
)

# COMMAND ----------

metric_rows.append(
    (
        "FIN002_M002",
        "FIN002",
        354,
        "FINANCIAL",
        "Debt and Financing",
        "Additional taxable bonds",
        174830000.0,
        "USD",
        2024,
        "System-wide",
        "Official Statement describes approximately $174.83 million of taxable financing.",
        True,
        "Concurrent taxable financing"
    )
)

# COMMAND ----------

metrics_df = spark.createDataFrame(
    metric_rows,
    schema=metric_schema
)

display(metrics_df)

# COMMAND ----------

metrics_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.silver.quantitative_evidence")

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            page_number,
            clean_text
        FROM marshfield_esg.silver.document_pages_clean
        WHERE source_id = 'FIN002'
          AND lower(clean_text) RLIKE
              'construction|renovation|capital project|facilities|equipment|sources and uses'
        ORDER BY page_number
    """)
)

# COMMAND ----------



# COMMAND ----------

display(
    spark.sql("""
        SELECT
            page_number,
            clean_text
        FROM marshfield_esg.silver.document_pages_clean
        WHERE source_id = 'MCHS005'
          AND lower(clean_text) RLIKE
              'mental health|behavioral health|substance use|food insecurity|access to care|percent|%'
        ORDER BY page_number
    """)
)

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            page_number,
            clean_text
        FROM marshfield_esg.silver.document_pages_clean
        WHERE source_id = 'MCHS006'
          AND lower(clean_text) RLIKE
              'mental health|behavioral health|substance use|food insecurity|access to care|percent|%'
        ORDER BY page_number
    """)
)