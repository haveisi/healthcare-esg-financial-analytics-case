# Databricks notebook source
from pyspark.sql import functions as F

candidates = spark.table(
    "marshfield_esg.silver.evidence_candidates"
)

# COMMAND ----------

structured = (
    candidates
    .withColumn(
        "topic",
        F.when(
            F.lower("clean_text").rlike(
                "revenue|operating revenue|net patient revenue|investment income"
            ),
            F.lit("Revenue")
        )
        .when(
            F.lower("clean_text").rlike(
                "expense|operating expense|salary|wages|benefits"
            ),
            F.lit("Expenses")
        )
        .when(
            F.lower("clean_text").rlike(
                "debt|bond|debt service|borrowing|interest expense"
            ),
            F.lit("Debt and Financing")
        )
        .when(
            F.lower("clean_text").rlike(
                "liquidity|cash and cash equivalents|days cash"
            ),
            F.lit("Liquidity")
        )
        .when(
            F.lower("clean_text").rlike(
                "capital expenditure|capital project|construction|renovation|facility"
            ),
            F.lit("Capital Investment")
        )
        .when(
            F.lower("clean_text").rlike(
                "behavioral health|mental health"
            ),
            F.lit("Behavioral Health")
        )
        .when(
            F.lower("clean_text").rlike(
                "substance use|opioid|alcohol|drug use"
            ),
            F.lit("Substance Use")
        )
        .when(
            F.lower("clean_text").rlike(
                "health equity|disparit|underserved"
            ),
            F.lit("Health Equity")
        )
        .when(
            F.lower("clean_text").rlike(
                "food insecurity|food access|nutrition"
            ),
            F.lit("Food Insecurity")
        )
        .when(
            F.lower("clean_text").rlike(
                "access to care|healthcare access|provider shortage|transportation"
            ),
            F.lit("Access to Care")
        )
        .when(
            F.lower("clean_text").rlike(
                "board|governance|audit committee|compliance"
            ),
            F.lit("Governance and Oversight")
        )
        .when(
            F.lower("clean_text").rlike(
                "climate|resilience|energy|emissions|water|waste"
            ),
            F.lit("Environmental and Resilience")
        )
        .otherwise(F.lit("Other"))
    )
)

# COMMAND ----------

structured.groupBy(
    "source_id",
    "topic"
).count().orderBy(
    "source_id",
    F.desc("count")
).show(100, truncate=False)

# COMMAND ----------

structured = structured.withColumn(
    "evidence_id",
    F.concat_ws(
        "_",
        F.col("source_id"),
        F.lit("P"),
        F.lpad(F.col("page_number").cast("string"), 4, "0")
    )
)

# COMMAND ----------

structured = structured.withColumn(
    "evidence_id",
    F.concat(
        F.col("source_id"),
        F.lit("_P"),
        F.lpad(F.col("page_number").cast("string"), 4, "0")
    )
)

# COMMAND ----------

display(
    structured.select(
        "evidence_id",
        "source_id",
        "page_number",
        "evidence_domain",
        "topic",
        "word_count",
        F.substring("clean_text", 1, 300).alias("evidence_preview")
    )
)

# COMMAND ----------

structured.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.silver.structured_evidence")

# COMMAND ----------

spark.sql("""
SELECT
    evidence_domain,
    topic,
    COUNT(*) AS records
FROM marshfield_esg.silver.structured_evidence
GROUP BY evidence_domain, topic
ORDER BY evidence_domain, records DESC
""").show(100, truncate=False)

# COMMAND ----------

