-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Step 3 — Silver Evidence Preparation
-- MAGIC
-- MAGIC ## Goal
-- MAGIC Transform raw Marshfield annual-report pages into clean,
-- MAGIC traceable evidence for population health, risk, resilience,
-- MAGIC ESG/social factors, and intervention analysis.
-- MAGIC
-- MAGIC ## Principle
-- MAGIC Bronze preserves raw source evidence.
-- MAGIC Silver cleans, standardizes, and classifies it.
-- MAGIC No unsupported conclusions are created.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC bronze_pages = spark.table(
-- MAGIC     "health_resilience.bronze.marshfield_pdf_pages"
-- MAGIC )
-- MAGIC
-- MAGIC print("Bronze rows:", bronze_pages.count())
-- MAGIC
-- MAGIC bronze_pages.groupBy("year").count().orderBy("year").show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pages_clean = (
-- MAGIC     bronze_pages
-- MAGIC     .filter(F.col("extraction_status") == "SUCCESS")
-- MAGIC     .filter(F.col("page_text").isNotNull())
-- MAGIC )
-- MAGIC
-- MAGIC print("Successful pages:", pages_clean.count())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pages_clean = (
-- MAGIC     pages_clean
-- MAGIC     .withColumn(
-- MAGIC         "page_text_clean",
-- MAGIC         F.trim(
-- MAGIC             F.regexp_replace(
-- MAGIC                 F.regexp_replace(
-- MAGIC                     F.regexp_replace(
-- MAGIC                         F.col("page_text"),
-- MAGIC                         r"\r\n|\r|\n",
-- MAGIC                         " "
-- MAGIC                     ),
-- MAGIC                     r"\t",
-- MAGIC                     " "
-- MAGIC                 ),
-- MAGIC                 r"\s+",
-- MAGIC                 " "
-- MAGIC             )
-- MAGIC         )
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     pages_clean
-- MAGIC     .select(
-- MAGIC         "year",
-- MAGIC         "page_number",
-- MAGIC         "page_text_clean"
-- MAGIC     )
-- MAGIC     .orderBy("year", "page_number")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pages_clean = (
-- MAGIC     pages_clean
-- MAGIC     .withColumn(
-- MAGIC         "clean_character_count",
-- MAGIC         F.length("page_text_clean")
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "text_quality",
-- MAGIC         F.when(
-- MAGIC             F.col("clean_character_count") >= 500,
-- MAGIC             "GOOD"
-- MAGIC         )
-- MAGIC         .when(
-- MAGIC             F.col("clean_character_count") >= 100,
-- MAGIC             "REVIEW"
-- MAGIC         )
-- MAGIC         .otherwise(
-- MAGIC             "LOW_TEXT"
-- MAGIC         )
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     pages_clean
-- MAGIC     .groupBy("year", "text_quality")
-- MAGIC     .count()
-- MAGIC     .orderBy("year", "text_quality")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC theme_keywords = {
-- MAGIC
-- MAGIC     "MENTAL_HEALTH": [
-- MAGIC         "mental health",
-- MAGIC         "depression",
-- MAGIC         "anxiety",
-- MAGIC         "suicide",
-- MAGIC         "stress"
-- MAGIC     ],
-- MAGIC
-- MAGIC     "INJURY": [
-- MAGIC         "injury",
-- MAGIC         "injuries",
-- MAGIC         "trauma",
-- MAGIC         "accident"
-- MAGIC     ],
-- MAGIC
-- MAGIC     "YOUTH_SAFETY": [
-- MAGIC         "youth",
-- MAGIC         "child",
-- MAGIC         "children",
-- MAGIC         "adolescent"
-- MAGIC     ],
-- MAGIC
-- MAGIC     "ATV": [
-- MAGIC         "atv",
-- MAGIC         "all-terrain vehicle"
-- MAGIC     ],
-- MAGIC
-- MAGIC     "HEAT": [
-- MAGIC         "heat stress",
-- MAGIC         "heat illness",
-- MAGIC         "heat-related",
-- MAGIC         "extreme heat"
-- MAGIC     ],
-- MAGIC
-- MAGIC     "RESPIRATORY": [
-- MAGIC         "respiratory",
-- MAGIC         "respirator",
-- MAGIC         "lung",
-- MAGIC         "air quality"
-- MAGIC     ],
-- MAGIC
-- MAGIC     "PESTICIDE": [
-- MAGIC         "pesticide",
-- MAGIC         "chemical exposure"
-- MAGIC     ],
-- MAGIC
-- MAGIC     "ERGONOMIC": [
-- MAGIC         "ergonomic",
-- MAGIC         "musculoskeletal"
-- MAGIC     ],
-- MAGIC
-- MAGIC     "RURAL_ACCESS": [
-- MAGIC         "rural health",
-- MAGIC         "access to care",
-- MAGIC         "healthcare access",
-- MAGIC         "rural community"
-- MAGIC     ],
-- MAGIC
-- MAGIC     "PREVENTION": [
-- MAGIC         "prevention",
-- MAGIC         "preventive",
-- MAGIC         "intervention",
-- MAGIC         "safety program"
-- MAGIC     ]
-- MAGIC }

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import re
-- MAGIC
-- MAGIC for theme, keywords in theme_keywords.items():
-- MAGIC
-- MAGIC     pattern = "|".join(
-- MAGIC         re.escape(keyword.lower())
-- MAGIC         for keyword in keywords
-- MAGIC     )
-- MAGIC
-- MAGIC     pages_clean = (
-- MAGIC         pages_clean
-- MAGIC         .withColumn(
-- MAGIC             f"flag_{theme.lower()}",
-- MAGIC             F.lower(F.col("page_text_clean")).rlike(pattern)
-- MAGIC         )
-- MAGIC     )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     pages_clean
-- MAGIC     .select(
-- MAGIC         "year",
-- MAGIC         "page_number",
-- MAGIC         "flag_mental_health",
-- MAGIC         "flag_injury",
-- MAGIC         "flag_youth_safety",
-- MAGIC         "flag_atv",
-- MAGIC         "flag_heat",
-- MAGIC         "flag_respiratory",
-- MAGIC         "flag_prevention"
-- MAGIC     )
-- MAGIC     .orderBy("year", "page_number")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pages_clean = (
-- MAGIC     pages_clean
-- MAGIC
-- MAGIC     .withColumn(
-- MAGIC         "health_risk_flag",
-- MAGIC         (
-- MAGIC             F.col("flag_mental_health") |
-- MAGIC             F.col("flag_injury") |
-- MAGIC             F.col("flag_heat") |
-- MAGIC             F.col("flag_respiratory") |
-- MAGIC             F.col("flag_pesticide") |
-- MAGIC             F.col("flag_ergonomic")
-- MAGIC         )
-- MAGIC     )
-- MAGIC
-- MAGIC     .withColumn(
-- MAGIC         "social_risk_flag",
-- MAGIC         (
-- MAGIC             F.col("flag_youth_safety") |
-- MAGIC             F.col("flag_rural_access") |
-- MAGIC             F.col("flag_mental_health")
-- MAGIC         )
-- MAGIC     )
-- MAGIC
-- MAGIC     .withColumn(
-- MAGIC         "resilience_intervention_flag",
-- MAGIC         F.col("flag_prevention")
-- MAGIC     )
-- MAGIC
-- MAGIC     .withColumn(
-- MAGIC         "esg_social_flag",
-- MAGIC         (
-- MAGIC             F.col("flag_youth_safety") |
-- MAGIC             F.col("flag_rural_access") |
-- MAGIC             F.col("flag_mental_health") |
-- MAGIC             F.col("flag_injury") |
-- MAGIC             F.col("flag_prevention")
-- MAGIC         )
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pages_clean = (
-- MAGIC     pages_clean
-- MAGIC     .withColumn(
-- MAGIC         "evidence_page_id",
-- MAGIC         F.concat_ws(
-- MAGIC             "_",
-- MAGIC             F.col("source_id"),
-- MAGIC             F.lit("P"),
-- MAGIC             F.lpad(
-- MAGIC                 F.col("page_number").cast("string"),
-- MAGIC                 3,
-- MAGIC                 "0"
-- MAGIC             )
-- MAGIC         )
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pages_clean = (
-- MAGIC     pages_clean
-- MAGIC     .withColumn(
-- MAGIC         "silver_processed_at",
-- MAGIC         F.current_timestamp()
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     pages_clean
-- MAGIC     .write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .saveAsTable(
-- MAGIC         "health_resilience.silver.marshfield_report_pages_clean"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

SELECT
    year,
    COUNT(*) AS total_pages,

    SUM(
        CASE WHEN text_quality = 'GOOD'
        THEN 1 ELSE 0 END
    ) AS good_pages,

    SUM(
        CASE WHEN text_quality = 'REVIEW'
        THEN 1 ELSE 0 END
    ) AS review_pages,

    SUM(
        CASE WHEN text_quality = 'LOW_TEXT'
        THEN 1 ELSE 0 END
    ) AS low_text_pages

FROM health_resilience.silver.marshfield_report_pages_clean

GROUP BY year
ORDER BY year;

-- COMMAND ----------

SELECT
    year,

    SUM(CASE WHEN flag_mental_health THEN 1 ELSE 0 END)
        AS mental_health_pages,

    SUM(CASE WHEN flag_injury THEN 1 ELSE 0 END)
        AS injury_pages,

    SUM(CASE WHEN flag_youth_safety THEN 1 ELSE 0 END)
        AS youth_safety_pages,

    SUM(CASE WHEN flag_atv THEN 1 ELSE 0 END)
        AS atv_pages,

    SUM(CASE WHEN flag_heat THEN 1 ELSE 0 END)
        AS heat_pages,

    SUM(CASE WHEN flag_respiratory THEN 1 ELSE 0 END)
        AS respiratory_pages,

    SUM(CASE WHEN flag_prevention THEN 1 ELSE 0 END)
        AS prevention_pages

FROM health_resilience.silver.marshfield_report_pages_clean

GROUP BY year
ORDER BY year;

-- COMMAND ----------

SELECT
    year,
    page_number,
    evidence_page_id,
    LEFT(page_text_clean, 1000) AS evidence_text

FROM health_resilience.silver.marshfield_report_pages_clean

WHERE flag_mental_health = TRUE

ORDER BY year DESC, page_number;