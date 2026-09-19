-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC silver_pages = spark.table(
-- MAGIC     "health_resilience.silver.marshfield_report_pages_clean"
-- MAGIC )
-- MAGIC
-- MAGIC print("Silver rows:", silver_pages.count())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC candidate_pages = (
-- MAGIC     silver_pages
-- MAGIC     .filter(
-- MAGIC         F.col("flag_mental_health") |
-- MAGIC         F.col("flag_injury") |
-- MAGIC         F.col("flag_youth_safety") |
-- MAGIC         F.col("flag_atv") |
-- MAGIC         F.col("flag_heat") |
-- MAGIC         F.col("flag_respiratory") |
-- MAGIC         F.col("flag_pesticide") |
-- MAGIC         F.col("flag_ergonomic") |
-- MAGIC         F.col("flag_rural_access") |
-- MAGIC         F.col("flag_prevention")
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("Candidate pages:", candidate_pages.count())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC candidate_pages.groupBy("year").count().orderBy("year").show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC candidate_pages = (
-- MAGIC     candidate_pages
-- MAGIC     .withColumn(
-- MAGIC         "primary_theme",
-- MAGIC         F.when(F.col("flag_mental_health"), "MENTAL_HEALTH")
-- MAGIC         .when(F.col("flag_atv"), "ATV_SAFETY")
-- MAGIC         .when(F.col("flag_heat"), "HEAT")
-- MAGIC         .when(F.col("flag_respiratory"), "RESPIRATORY")
-- MAGIC         .when(F.col("flag_pesticide"), "PESTICIDE")
-- MAGIC         .when(F.col("flag_ergonomic"), "ERGONOMIC")
-- MAGIC         .when(F.col("flag_injury"), "INJURY")
-- MAGIC         .when(F.col("flag_rural_access"), "RURAL_ACCESS")
-- MAGIC         .when(F.col("flag_youth_safety"), "YOUTH_SAFETY")
-- MAGIC         .when(F.col("flag_prevention"), "PREVENTION")
-- MAGIC         .otherwise("OTHER")
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC candidate_pages = (
-- MAGIC     candidate_pages
-- MAGIC     .withColumn("validation_status", F.lit("PENDING"))
-- MAGIC     .withColumn("validation_note", F.lit(None).cast("string"))
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC candidate_pages = (
-- MAGIC     candidate_pages
-- MAGIC     .withColumn(
-- MAGIC         "evidence_id",
-- MAGIC         F.concat_ws(
-- MAGIC             "_",
-- MAGIC             F.col("evidence_page_id"),
-- MAGIC             F.col("primary_theme")
-- MAGIC         )
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC evidence_candidates = (
-- MAGIC     candidate_pages
-- MAGIC     .select(
-- MAGIC         "evidence_id",
-- MAGIC         "source_id",
-- MAGIC         F.col("year").alias("source_year"),
-- MAGIC         F.col("page_number").alias("source_page"),
-- MAGIC         "evidence_page_id",
-- MAGIC         "primary_theme",
-- MAGIC         "health_risk_flag",
-- MAGIC         "social_risk_flag",
-- MAGIC         "resilience_intervention_flag",
-- MAGIC         "esg_social_flag",
-- MAGIC         "page_text_clean",
-- MAGIC         "validation_status",
-- MAGIC         "validation_note"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     evidence_candidates
-- MAGIC     .write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .saveAsTable(
-- MAGIC         "health_resilience.silver.marshfield_evidence_candidates"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

SELECT
    source_year,
    source_page,
    evidence_id,
    primary_theme,
    LEFT(page_text_clean, 1200) AS evidence_text
FROM health_resilience.silver.marshfield_evidence_candidates
WHERE primary_theme = 'MENTAL_HEALTH'
ORDER BY source_year DESC, source_page;

-- COMMAND ----------

SELECT
    source_year,
    source_page,
    page_text_clean
FROM health_resilience.silver.marshfield_evidence_candidates
WHERE evidence_page_id = 'MCRI_NCC_2021_P_017';

-- COMMAND ----------

UPDATE health_resilience.silver.marshfield_evidence_candidates

SET
    validation_status = 'VALID',
    validation_note = 'Direct discussion of Farm Adolescent Mental Health program/project.'

WHERE evidence_page_id = 'MCRI_NCC_2021_P_017'
  AND primary_theme = 'MENTAL_HEALTH';

-- COMMAND ----------

SELECT
    evidence_id,
    primary_theme,
    validation_status,
    validation_note
FROM health_resilience.silver.marshfield_evidence_candidates
WHERE evidence_page_id = 'MCRI_NCC_2021_P_017';

-- COMMAND ----------

SELECT
    evidence_id,
    source_year,
    source_page,
    primary_theme,
    page_text_clean
FROM health_resilience.silver.marshfield_evidence_candidates
WHERE evidence_page_id = 'MCRI_NCC_2021_P_017';