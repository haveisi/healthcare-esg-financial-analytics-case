# Databricks notebook source
from pyspark.sql import functions as F

bronze_pages = spark.table("marshfield_esg.bronze.document_pages")

# COMMAND ----------

silver_pages = (
    bronze_pages
    .withColumn(
        "clean_text",
        F.regexp_replace(F.col("raw_text"), r"\s+", " ")
    )
    .withColumn(
        "clean_text",
        F.trim(F.col("clean_text"))
    )
    .withColumn(
        "is_empty_page",
        F.when(F.length(F.col("clean_text")) == 0, True).otherwise(False)
    )
    .withColumn(
        "word_count",
        F.size(F.split(F.col("clean_text"), r"\s+"))
    )
)

# COMMAND ----------

display(
    silver_pages.select(
        "source_id",
        "page_number",
        "character_count",
        "word_count",
        "is_empty_page",
        F.substring("clean_text", 1, 300).alias("preview")
    )
)

# COMMAND ----------

silver_pages.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.silver.document_pages_clean")

# COMMAND ----------

spark.sql("""
SELECT
    source_id,
    COUNT(*) AS pages,
    SUM(CASE WHEN is_empty_page THEN 1 ELSE 0 END) AS empty_pages,
    ROUND(AVG(word_count), 1) AS avg_words_per_page
FROM marshfield_esg.silver.document_pages_clean
GROUP BY source_id
ORDER BY source_id
""").show()

# COMMAND ----------

evidence_candidates = spark.sql("""
SELECT
    source_id,
    file_name,
    page_number,
    clean_text,
    word_count,

    CASE
        WHEN lower(clean_text) RLIKE 'revenue|expense|operating income|net assets|debt|liquidity|capital expenditure|bond|debt service'
            THEN 'FINANCIAL'

        WHEN lower(clean_text) RLIKE 'community health|health equity|behavioral health|substance use|food insecurity|access to care|social determinants'
            THEN 'SOCIAL'

        WHEN lower(clean_text) RLIKE 'governance|board|compliance|audit|privacy|cybersecurity|risk management'
            THEN 'GOVERNANCE'

        WHEN lower(clean_text) RLIKE 'energy|emissions|greenhouse gas|climate|water|waste|renewable|resilience'
            THEN 'ENVIRONMENTAL'

        ELSE 'OTHER'
    END AS evidence_domain

FROM marshfield_esg.silver.document_pages_clean

WHERE word_count >= 20
""")

# COMMAND ----------

display(
    evidence_candidates.select(
        "source_id",
        "page_number",
        "evidence_domain",
        "word_count",
        F.substring("clean_text", 1, 250).alias("preview")
    )
    .orderBy("source_id", "page_number")
)

# COMMAND ----------

evidence_candidates.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.silver.evidence_candidates")

# COMMAND ----------

spark.sql("""
SELECT
    source_id,
    evidence_domain,
    COUNT(*) AS candidate_pages
FROM marshfield_esg.silver.evidence_candidates
GROUP BY source_id, evidence_domain
ORDER BY source_id, candidate_pages DESC
""").show()

# COMMAND ----------

spark.sql("""
SELECT
    source_id,
    COUNT(*) AS total_pages,
    SUM(CASE WHEN is_empty_page THEN 1 ELSE 0 END) AS empty_pages,
    ROUND(AVG(word_count), 1) AS avg_words_per_page,
    MAX(word_count) AS max_words_per_page
FROM marshfield_esg.silver.document_pages_clean
GROUP BY source_id
ORDER BY source_id
""").show()

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            source_id,
            page_number,
            word_count,
            clean_text
        FROM marshfield_esg.silver.document_pages_clean
        WHERE source_id = 'FIN001'
        ORDER BY page_number
        LIMIT 20
    """)
)