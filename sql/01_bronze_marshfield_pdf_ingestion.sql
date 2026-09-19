-- Databricks notebook source
-- MAGIC %python
-- MAGIC # Step 2 — Marshfield Annual Report PDF Ingestion
-- MAGIC
-- MAGIC ## Goal
-- MAGIC Ingest public Marshfield Clinic Research Institute NCCRAHS annual reports into a controlled Bronze layer.
-- MAGIC
-- MAGIC ## Outputs
-- MAGIC 1. health_resilience.bronze.marshfield_pdf_inventory
-- MAGIC 2. health_resilience.bronze.marshfield_pdf_pages
-- MAGIC
-- MAGIC ## Controls
-- MAGIC - Public data only
-- MAGIC - No PHI
-- MAGIC - Preserve source filename and source ID
-- MAGIC - Preserve page number
-- MAGIC - Record extraction status
-- MAGIC - Maintain source-to-evidence traceability

-- COMMAND ----------

-- MAGIC %python
-- MAGIC base_path = "/Volumes/health_resilience/bronze/marshfield"
-- MAGIC
-- MAGIC manifest_path = (
-- MAGIC     "/Volumes/health_resilience/bronze/marshfield/"
-- MAGIC     "marshfield_source_manifest.csv"
-- MAGIC )
-- MAGIC
-- MAGIC print(base_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC manifest_df = (
-- MAGIC     spark.read
-- MAGIC     .option("header", True)
-- MAGIC     .option("inferSchema", True)
-- MAGIC     .csv(manifest_path)
-- MAGIC )
-- MAGIC
-- MAGIC display(
-- MAGIC     manifest_df.select(
-- MAGIC         "source_id",
-- MAGIC         "source_name",
-- MAGIC         "year",
-- MAGIC         "local_filename",
-- MAGIC         "analytics_domain"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pdf_binary_df = (
-- MAGIC     spark.read
-- MAGIC     .format("binaryFile")
-- MAGIC     .option("pathGlobFilter", "*.pdf")
-- MAGIC     .load(base_path)
-- MAGIC )
-- MAGIC
-- MAGIC display(
-- MAGIC     pdf_binary_df.select(
-- MAGIC         "path",
-- MAGIC         "length",
-- MAGIC         "modificationTime"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("PDF count:", pdf_binary_df.count())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import (
-- MAGIC     regexp_extract,
-- MAGIC     col,
-- MAGIC     sha2,
-- MAGIC     current_timestamp
-- MAGIC )
-- MAGIC
-- MAGIC pdf_inventory = (
-- MAGIC     pdf_binary_df
-- MAGIC     .withColumn(
-- MAGIC         "local_filename",
-- MAGIC         regexp_extract(col("path"), r"([^/]+)$", 1)
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "file_sha256",
-- MAGIC         sha2(col("content"), 256)
-- MAGIC     )
-- MAGIC     .withColumn(
-- MAGIC         "ingested_at",
-- MAGIC         current_timestamp()
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC manifest_pdf = (
-- MAGIC     manifest_df
-- MAGIC     .filter(col("source_id").startswith("MCRI_NCC_"))
-- MAGIC     .select(
-- MAGIC         "source_id",
-- MAGIC         "source_name",
-- MAGIC         "year",
-- MAGIC         "local_filename",
-- MAGIC         "organization",
-- MAGIC         "analytics_domain",
-- MAGIC         "contains_phi",
-- MAGIC         "public_data"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pdf_inventory_joined = (
-- MAGIC     pdf_inventory.alias("p")
-- MAGIC     .join(
-- MAGIC         manifest_pdf.alias("m"),
-- MAGIC         on="local_filename",
-- MAGIC         how="left"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(
-- MAGIC     pdf_inventory_joined.select(
-- MAGIC         "source_id",
-- MAGIC         "year",
-- MAGIC         "local_filename",
-- MAGIC         "length",
-- MAGIC         "file_sha256",
-- MAGIC         "contains_phi",
-- MAGIC         "public_data"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC unmatched = (
-- MAGIC     pdf_inventory_joined
-- MAGIC     .filter(col("source_id").isNull())
-- MAGIC )
-- MAGIC
-- MAGIC display(unmatched)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(
-- MAGIC     "Unmatched PDFs:",
-- MAGIC     unmatched.count()
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pdf_inventory_final = (
-- MAGIC     pdf_inventory_joined
-- MAGIC     .select(
-- MAGIC         "source_id",
-- MAGIC         "organization",
-- MAGIC         "source_name",
-- MAGIC         "year",
-- MAGIC         "local_filename",
-- MAGIC         "path",
-- MAGIC         "length",
-- MAGIC         "modificationTime",
-- MAGIC         "file_sha256",
-- MAGIC         "analytics_domain",
-- MAGIC         "public_data",
-- MAGIC         "contains_phi",
-- MAGIC         "ingested_at"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC     pdf_inventory_final
-- MAGIC     .write
-- MAGIC     .format("delta")
-- MAGIC     .mode("overwrite")
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .saveAsTable(
-- MAGIC         "health_resilience.bronze.marshfield_pdf_inventory"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

SELECT *
FROM health_resilience.bronze.marshfield_pdf_inventory
ORDER BY year;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %pip install pypdf
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC base_path = "/Volumes/health_resilience/bronze/marshfield"
-- MAGIC
-- MAGIC manifest_path = (
-- MAGIC     "/Volumes/health_resilience/bronze/marshfield/"
-- MAGIC     "marshfield_source_manifest.csv"
-- MAGIC )
-- MAGIC
-- MAGIC manifest_df = (
-- MAGIC     spark.read
-- MAGIC     .option("header", True)
-- MAGIC     .option("inferSchema", True)
-- MAGIC     .csv(manifest_path)
-- MAGIC )
-- MAGIC
-- MAGIC print("Base path:", base_path)
-- MAGIC print("Manifest rows:", manifest_df.count())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pypdf import PdfReader
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC pdf_records = []
-- MAGIC
-- MAGIC pdf_names = [
-- MAGIC     "mcri_nccrahs_2021_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2022_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2023_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2024_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2025_annual_report.pdf",
-- MAGIC ]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for filename in pdf_names:
-- MAGIC     filepath = f"{base_path}/{filename}"
-- MAGIC     with open(filepath, "rb") as f:
-- MAGIC         header = f.read(8)
-- MAGIC     print(filename, header)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pypdf import PdfReader
-- MAGIC from pyspark.sql.functions import col
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC pdf_records = []
-- MAGIC
-- MAGIC pdf_names = [
-- MAGIC     "mcri_nccrahs_2021_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2022_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2023_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2024_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2025_annual_report.pdf",
-- MAGIC ]
-- MAGIC
-- MAGIC for filename in pdf_names:
-- MAGIC
-- MAGIC     filepath = f"{base_path}/{filename}"
-- MAGIC
-- MAGIC     manifest_row = (
-- MAGIC         manifest_df
-- MAGIC         .filter(col("local_filename") == filename)
-- MAGIC         .select("source_id", "year")
-- MAGIC         .collect()
-- MAGIC     )
-- MAGIC
-- MAGIC     if not manifest_row:
-- MAGIC         print(f"Manifest match missing: {filename}")
-- MAGIC         continue
-- MAGIC
-- MAGIC     source_id = manifest_row[0]["source_id"]
-- MAGIC     year = manifest_row[0]["year"]
-- MAGIC
-- MAGIC     try:
-- MAGIC         reader = PdfReader(filepath)
-- MAGIC
-- MAGIC         print(
-- MAGIC             f"Processing {filename} | pages={len(reader.pages)}"
-- MAGIC         )
-- MAGIC
-- MAGIC         for page_index, page in enumerate(reader.pages):
-- MAGIC
-- MAGIC             try:
-- MAGIC                 text = page.extract_text() or ""
-- MAGIC
-- MAGIC                 pdf_records.append({
-- MAGIC                     "source_id": source_id,
-- MAGIC                     "year": year,
-- MAGIC                     "local_filename": filename,
-- MAGIC                     "page_number": page_index + 1,
-- MAGIC                     "page_text": text,
-- MAGIC                     "character_count": len(text),
-- MAGIC                     "extraction_status": "SUCCESS",
-- MAGIC                     "extraction_error": None
-- MAGIC                 })
-- MAGIC
-- MAGIC             except Exception as page_error:
-- MAGIC
-- MAGIC                 pdf_records.append({
-- MAGIC                     "source_id": source_id,
-- MAGIC                     "year": year,
-- MAGIC                     "local_filename": filename,
-- MAGIC                     "page_number": page_index + 1,
-- MAGIC                     "page_text": None,
-- MAGIC                     "character_count": 0,
-- MAGIC                     "extraction_status": "PAGE_ERROR",
-- MAGIC                     "extraction_error": str(page_error)
-- MAGIC                 })
-- MAGIC
-- MAGIC     except Exception as file_error:
-- MAGIC
-- MAGIC         print(f"FILE ERROR: {filename} -> {file_error}")
-- MAGIC
-- MAGIC         pdf_records.append({
-- MAGIC             "source_id": source_id,
-- MAGIC             "year": year,
-- MAGIC             "local_filename": filename,
-- MAGIC             "page_number": None,
-- MAGIC             "page_text": None,
-- MAGIC             "character_count": 0,
-- MAGIC             "extraction_status": "FILE_ERROR",
-- MAGIC             "extraction_error": str(file_error)
-- MAGIC         })
-- MAGIC
-- MAGIC print("Extracted records:", len(pdf_records))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pages_pd = pd.DataFrame(pdf_records)
-- MAGIC
-- MAGIC display(pages_pd)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC summary = (
-- MAGIC     pages_pd
-- MAGIC     .groupby(["year", "extraction_status"])
-- MAGIC     .agg(
-- MAGIC         pages=("page_number", "count"),
-- MAGIC         characters=("character_count", "sum")
-- MAGIC     )
-- MAGIC     .reset_index()
-- MAGIC )
-- MAGIC
-- MAGIC display(summary)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for year in sorted(pages_pd["year"].dropna().unique()):
-- MAGIC
-- MAGIC     sample = (
-- MAGIC         pages_pd[
-- MAGIC             (pages_pd["year"] == year) &
-- MAGIC             (pages_pd["page_number"] == 1)
-- MAGIC         ]
-- MAGIC         ["page_text"]
-- MAGIC         .iloc[0]
-- MAGIC     )
-- MAGIC
-- MAGIC     print("\n" + "=" * 80)
-- MAGIC     print("YEAR:", year)
-- MAGIC     print("=" * 80)
-- MAGIC     print(sample[:1000])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pypdf import PdfReader
-- MAGIC from pyspark.sql.functions import col
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC bad_year_files = [
-- MAGIC     "mcri_nccrahs_2022_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2023_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2024_annual_report.pdf",
-- MAGIC ]
-- MAGIC
-- MAGIC replacement_records = []
-- MAGIC
-- MAGIC for filename in bad_year_files:
-- MAGIC
-- MAGIC     filepath = f"{base_path}/{filename}"
-- MAGIC
-- MAGIC     manifest_row = (
-- MAGIC         manifest_df
-- MAGIC         .filter(col("local_filename") == filename)
-- MAGIC         .select("source_id", "year")
-- MAGIC         .collect()
-- MAGIC     )
-- MAGIC
-- MAGIC     source_id = manifest_row[0]["source_id"]
-- MAGIC     year = manifest_row[0]["year"]
-- MAGIC
-- MAGIC     reader = PdfReader(filepath)
-- MAGIC
-- MAGIC     print(filename, "pages =", len(reader.pages))
-- MAGIC
-- MAGIC     for page_index, page in enumerate(reader.pages):
-- MAGIC
-- MAGIC         text = page.extract_text() or ""
-- MAGIC
-- MAGIC         replacement_records.append({
-- MAGIC             "source_id": source_id,
-- MAGIC             "year": year,
-- MAGIC             "local_filename": filename,
-- MAGIC             "page_number": page_index + 1,
-- MAGIC             "page_text": text,
-- MAGIC             "character_count": len(text),
-- MAGIC             "extraction_status": "SUCCESS",
-- MAGIC             "extraction_error": None
-- MAGIC         })

-- COMMAND ----------

-- MAGIC %python
-- MAGIC replacement_pd = pd.DataFrame(replacement_records)
-- MAGIC replacement_spark = spark.createDataFrame(replacement_pd)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("""
-- MAGIC DELETE FROM health_resilience.bronze.marshfield_pdf_pages
-- MAGIC WHERE year IN (2022, 2023, 2024)
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import current_timestamp
-- MAGIC
-- MAGIC replacement_spark = (
-- MAGIC     replacement_spark
-- MAGIC     .withColumn("extracted_at", current_timestamp())
-- MAGIC )
-- MAGIC
-- MAGIC (
-- MAGIC     replacement_spark
-- MAGIC     .write
-- MAGIC     .format("delta")
-- MAGIC     .mode("append")
-- MAGIC     .saveAsTable(
-- MAGIC         "health_resilience.bronze.marshfield_pdf_pages"
-- MAGIC     )
-- MAGIC )

-- COMMAND ----------

SELECT
    year,
    COUNT(*) AS pages,
    SUM(character_count) AS characters,
    SUM(
        CASE
            WHEN extraction_status <> 'SUCCESS'
            THEN 1 ELSE 0
        END
    ) AS failures
FROM health_resilience.bronze.marshfield_pdf_pages
GROUP BY year
ORDER BY year;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for f in dbutils.fs.ls("/Volumes/health_resilience/bronze/marshfield"):
-- MAGIC     if "2022" in f.name or "2023" in f.name or "2024" in f.name:
-- MAGIC         print(f.name, f.size)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pypdf import PdfReader
-- MAGIC
-- MAGIC files_to_check = [
-- MAGIC     "mcri_nccrahs_2022_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2023_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2024_annual_report.pdf",
-- MAGIC ]
-- MAGIC
-- MAGIC for filename in files_to_check:
-- MAGIC     path = f"/Volumes/health_resilience/bronze/marshfield/{filename}"
-- MAGIC     reader = PdfReader(path)
-- MAGIC     print(filename, "pages =", len(reader.pages))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, current_timestamp
-- MAGIC from pypdf import PdfReader
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC replacement_records = []
-- MAGIC
-- MAGIC for filename in files_to_check:
-- MAGIC
-- MAGIC     filepath = f"/Volumes/health_resilience/bronze/marshfield/{filename}"
-- MAGIC
-- MAGIC     manifest_row = (
-- MAGIC         manifest_df
-- MAGIC         .filter(col("local_filename") == filename)
-- MAGIC         .select("source_id", "year")
-- MAGIC         .collect()
-- MAGIC     )
-- MAGIC
-- MAGIC     source_id = manifest_row[0]["source_id"]
-- MAGIC     year = manifest_row[0]["year"]
-- MAGIC
-- MAGIC     reader = PdfReader(filepath)
-- MAGIC
-- MAGIC     for page_index, page in enumerate(reader.pages):
-- MAGIC
-- MAGIC         text = page.extract_text() or ""
-- MAGIC
-- MAGIC         replacement_records.append({
-- MAGIC             "source_id": source_id,
-- MAGIC             "year": year,
-- MAGIC             "local_filename": filename,
-- MAGIC             "page_number": page_index + 1,
-- MAGIC             "page_text": text,
-- MAGIC             "character_count": len(text),
-- MAGIC             "extraction_status": "SUCCESS",
-- MAGIC             "extraction_error": None
-- MAGIC         })
-- MAGIC
-- MAGIC replacement_pd = pd.DataFrame(replacement_records)
-- MAGIC
-- MAGIC replacement_spark = (
-- MAGIC     spark.createDataFrame(replacement_pd)
-- MAGIC     .withColumn("extracted_at", current_timestamp())
-- MAGIC )

-- COMMAND ----------

DELETE FROM health_resilience.bronze.marshfield_pdf_pages
WHERE year IN (2022, 2023, 2024);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pypdf import PdfReader
-- MAGIC from pyspark.sql.functions import col, current_timestamp
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC base_path = "/Volumes/health_resilience/bronze/marshfield"
-- MAGIC
-- MAGIC manifest_path = (
-- MAGIC     "/Volumes/health_resilience/bronze/marshfield/"
-- MAGIC     "marshfield_source_manifest.csv"
-- MAGIC )
-- MAGIC
-- MAGIC manifest_df = (
-- MAGIC     spark.read
-- MAGIC     .option("header", True)
-- MAGIC     .option("inferSchema", True)
-- MAGIC     .csv(manifest_path)
-- MAGIC )
-- MAGIC
-- MAGIC files_to_check = [
-- MAGIC     "mcri_nccrahs_2022_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2023_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2024_annual_report.pdf",
-- MAGIC ]
-- MAGIC
-- MAGIC replacement_records = []
-- MAGIC
-- MAGIC for filename in files_to_check:
-- MAGIC
-- MAGIC     filepath = f"{base_path}/{filename}"
-- MAGIC
-- MAGIC     manifest_row = (
-- MAGIC         manifest_df
-- MAGIC         .filter(col("local_filename") == filename)
-- MAGIC         .select("source_id", "year")
-- MAGIC         .collect()
-- MAGIC     )
-- MAGIC
-- MAGIC     source_id = manifest_row[0]["source_id"]
-- MAGIC     year = manifest_row[0]["year"]
-- MAGIC
-- MAGIC     reader = PdfReader(filepath)
-- MAGIC
-- MAGIC     print(filename, "pages =", len(reader.pages))
-- MAGIC
-- MAGIC     for page_index, page in enumerate(reader.pages):
-- MAGIC
-- MAGIC         text = page.extract_text() or ""
-- MAGIC
-- MAGIC         replacement_records.append({
-- MAGIC             "source_id": source_id,
-- MAGIC             "year": year,
-- MAGIC             "local_filename": filename,
-- MAGIC             "page_number": page_index + 1,
-- MAGIC             "page_text": text,
-- MAGIC             "character_count": len(text),
-- MAGIC             "extraction_status": "SUCCESS",
-- MAGIC             "extraction_error": None
-- MAGIC         })
-- MAGIC
-- MAGIC replacement_pd = pd.DataFrame(replacement_records)
-- MAGIC
-- MAGIC replacement_spark = (
-- MAGIC     spark.createDataFrame(replacement_pd)
-- MAGIC     .withColumn("extracted_at", current_timestamp())
-- MAGIC )
-- MAGIC
-- MAGIC replacement_spark.groupBy("year").count().orderBy("year").show()

-- COMMAND ----------

DELETE FROM health_resilience.bronze.marshfield_pdf_pages
WHERE year IN (2022, 2023, 2024);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pypdf import PdfReader
-- MAGIC from pyspark.sql.functions import col, current_timestamp
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC base_path = "/Volumes/health_resilience/bronze/marshfield"
-- MAGIC
-- MAGIC manifest_path = (
-- MAGIC     "/Volumes/health_resilience/bronze/marshfield/"
-- MAGIC     "marshfield_source_manifest.csv"
-- MAGIC )
-- MAGIC
-- MAGIC manifest_df = (
-- MAGIC     spark.read
-- MAGIC     .option("header", True)
-- MAGIC     .option("inferSchema", True)
-- MAGIC     .csv(manifest_path)
-- MAGIC )
-- MAGIC
-- MAGIC files_to_check = [
-- MAGIC     "mcri_nccrahs_2022_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2023_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2024_annual_report.pdf",
-- MAGIC ]
-- MAGIC
-- MAGIC replacement_records = []
-- MAGIC
-- MAGIC for filename in files_to_check:
-- MAGIC     filepath = f"{base_path}/{filename}"
-- MAGIC
-- MAGIC     manifest_row = (
-- MAGIC         manifest_df
-- MAGIC         .filter(col("local_filename") == filename)
-- MAGIC         .select("source_id", "year")
-- MAGIC         .collect()
-- MAGIC     )
-- MAGIC
-- MAGIC     source_id = manifest_row[0]["source_id"]
-- MAGIC     year = manifest_row[0]["year"]
-- MAGIC
-- MAGIC     reader = PdfReader(filepath)
-- MAGIC
-- MAGIC     print(filename, "pages =", len(reader.pages))
-- MAGIC
-- MAGIC     for page_index, page in enumerate(reader.pages):
-- MAGIC         text = page.extract_text() or ""
-- MAGIC
-- MAGIC         replacement_records.append({
-- MAGIC             "source_id": source_id,
-- MAGIC             "year": year,
-- MAGIC             "local_filename": filename,
-- MAGIC             "page_number": page_index + 1,
-- MAGIC             "page_text": text,
-- MAGIC             "character_count": len(text),
-- MAGIC             "extraction_status": "SUCCESS",
-- MAGIC             "extraction_error": None
-- MAGIC         })
-- MAGIC
-- MAGIC replacement_pd = pd.DataFrame(replacement_records)
-- MAGIC
-- MAGIC replacement_spark = (
-- MAGIC     spark.createDataFrame(replacement_pd)
-- MAGIC     .withColumn("extracted_at", current_timestamp())
-- MAGIC )
-- MAGIC
-- MAGIC print("Replacement rows:", replacement_spark.count())
-- MAGIC
-- MAGIC replacement_spark.groupBy("year").count().orderBy("year").show()
-- MAGIC
-- MAGIC (
-- MAGIC     replacement_spark
-- MAGIC     .write
-- MAGIC     .format("delta")
-- MAGIC     .mode("append")
-- MAGIC     .saveAsTable("health_resilience.bronze.marshfield_pdf_pages")
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pypdf import PdfReader
-- MAGIC
-- MAGIC base_path = "/Volumes/health_resilience/bronze/marshfield"
-- MAGIC
-- MAGIC files_to_check = [
-- MAGIC     "mcri_nccrahs_2022_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2023_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2024_annual_report.pdf",
-- MAGIC ]
-- MAGIC
-- MAGIC for filename in files_to_check:
-- MAGIC     path = f"{base_path}/{filename}"
-- MAGIC     reader = PdfReader(path)
-- MAGIC
-- MAGIC     print(
-- MAGIC         filename,
-- MAGIC         "| pages =", len(reader.pages),
-- MAGIC         "| first page chars =", len(reader.pages[0].extract_text() or "")
-- MAGIC     )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for f in dbutils.fs.ls(base_path):
-- MAGIC     if "2022" in f.name or "2023" in f.name or "2024" in f.name:
-- MAGIC         print(f.name, round(f.size / 1024, 1), "KB")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pypdf import PdfReader
-- MAGIC
-- MAGIC base_path = "/Volumes/health_resilience/bronze/marshfield"
-- MAGIC
-- MAGIC files_to_check = [
-- MAGIC     "mcri_nccrahs_2022_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2023_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2024_annual_report.pdf",
-- MAGIC ]
-- MAGIC
-- MAGIC for filename in files_to_check:
-- MAGIC     path = f"{base_path}/{filename}"
-- MAGIC     reader = PdfReader(path)
-- MAGIC
-- MAGIC     print(
-- MAGIC         filename,
-- MAGIC         "| pages =", len(reader.pages),
-- MAGIC         "| first page chars =", len(reader.pages[0].extract_text() or "")
-- MAGIC     )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for f in dbutils.fs.ls(base_path):
-- MAGIC     if "2022" in f.name or "2023" in f.name or "2024" in f.name:
-- MAGIC         print(f.name, round(f.size / 1024, 1), "KB")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pypdf import PdfReader
-- MAGIC
-- MAGIC base_path = "/Volumes/health_resilience/bronze/marshfield"
-- MAGIC
-- MAGIC files_to_check = [
-- MAGIC     "mcri_nccrahs_2022_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2023_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2024_annual_report.pdf",
-- MAGIC ]
-- MAGIC
-- MAGIC for filename in files_to_check:
-- MAGIC     path = f"{base_path}/{filename}"
-- MAGIC     reader = PdfReader(path)
-- MAGIC
-- MAGIC     print(
-- MAGIC         filename,
-- MAGIC         "| pages =", len(reader.pages),
-- MAGIC         "| first page chars =", len(reader.pages[0].extract_text() or "")
-- MAGIC     )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for f in dbutils.fs.ls(base_path):
-- MAGIC     if "2022" in f.name or "2023" in f.name or "2024" in f.name:
-- MAGIC         print(f.name, round(f.size / 1024, 1), "KB")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC base_path = "/Volumes/health_resilience/bronze/marshfield"
-- MAGIC
-- MAGIC for f in dbutils.fs.ls(base_path):
-- MAGIC     print(f.name, round(f.size / 1024, 1), "KB")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pypdf import PdfReader
-- MAGIC
-- MAGIC files_to_check = [
-- MAGIC     "mcri_nccrahs_2022_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2023_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2024_annual_report.pdf",
-- MAGIC ]
-- MAGIC
-- MAGIC for filename in files_to_check:
-- MAGIC     path = f"{base_path}/{filename}"
-- MAGIC     reader = PdfReader(path)
-- MAGIC
-- MAGIC     print(
-- MAGIC         filename,
-- MAGIC         "| pages =", len(reader.pages),
-- MAGIC         "| first page chars =", len(reader.pages[0].extract_text() or "")
-- MAGIC     )

-- COMMAND ----------

DELETE FROM health_resilience.bronze.marshfield_pdf_pages
WHERE year IN (2022, 2023, 2024);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pypdf import PdfReader
-- MAGIC from pyspark.sql.functions import col, current_timestamp
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC base_path = "/Volumes/health_resilience/bronze/marshfield"
-- MAGIC
-- MAGIC manifest_path = (
-- MAGIC     "/Volumes/health_resilience/bronze/marshfield/"
-- MAGIC     "marshfield_source_manifest.csv"
-- MAGIC )
-- MAGIC
-- MAGIC manifest_df = (
-- MAGIC     spark.read
-- MAGIC     .option("header", True)
-- MAGIC     .option("inferSchema", True)
-- MAGIC     .csv(manifest_path)
-- MAGIC )
-- MAGIC
-- MAGIC files_to_replace = [
-- MAGIC     "mcri_nccrahs_2022_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2023_annual_report.pdf",
-- MAGIC     "mcri_nccrahs_2024_annual_report.pdf",
-- MAGIC ]
-- MAGIC
-- MAGIC replacement_records = []
-- MAGIC
-- MAGIC for filename in files_to_replace:
-- MAGIC     filepath = f"{base_path}/{filename}"
-- MAGIC
-- MAGIC     manifest_row = (
-- MAGIC         manifest_df
-- MAGIC         .filter(col("local_filename") == filename)
-- MAGIC         .select("source_id", "year")
-- MAGIC         .collect()
-- MAGIC     )
-- MAGIC
-- MAGIC     source_id = manifest_row[0]["source_id"]
-- MAGIC     year = manifest_row[0]["year"]
-- MAGIC
-- MAGIC     reader = PdfReader(filepath)
-- MAGIC
-- MAGIC     for page_index, page in enumerate(reader.pages):
-- MAGIC         try:
-- MAGIC             text = page.extract_text() or ""
-- MAGIC
-- MAGIC             replacement_records.append({
-- MAGIC                 "source_id": source_id,
-- MAGIC                 "year": year,
-- MAGIC                 "local_filename": filename,
-- MAGIC                 "page_number": page_index + 1,
-- MAGIC                 "page_text": text,
-- MAGIC                 "character_count": len(text),
-- MAGIC                 "extraction_status": "SUCCESS",
-- MAGIC                 "extraction_error": None
-- MAGIC             })
-- MAGIC
-- MAGIC         except Exception as e:
-- MAGIC             replacement_records.append({
-- MAGIC                 "source_id": source_id,
-- MAGIC                 "year": year,
-- MAGIC                 "local_filename": filename,
-- MAGIC                 "page_number": page_index + 1,
-- MAGIC                 "page_text": None,
-- MAGIC                 "character_count": 0,
-- MAGIC                 "extraction_status": "PAGE_ERROR",
-- MAGIC                 "extraction_error": str(e)
-- MAGIC             })
-- MAGIC
-- MAGIC replacement_pd = pd.DataFrame(replacement_records)
-- MAGIC
-- MAGIC replacement_spark = (
-- MAGIC     spark.createDataFrame(replacement_pd)
-- MAGIC     .withColumn("extracted_at", current_timestamp())
-- MAGIC )
-- MAGIC
-- MAGIC replacement_spark.groupBy("year").count().orderBy("year").show()
-- MAGIC
-- MAGIC (
-- MAGIC     replacement_spark
-- MAGIC     .write
-- MAGIC     .format("delta")
-- MAGIC     .mode("append")
-- MAGIC     .saveAsTable("health_resilience.bronze.marshfield_pdf_pages")
-- MAGIC )

-- COMMAND ----------

SELECT
    year,
    COUNT(*) AS pages,
    SUM(character_count) AS characters,
    SUM(
        CASE
            WHEN extraction_status <> 'SUCCESS'
            THEN 1 ELSE 0
        END
    ) AS failures
FROM health_resilience.bronze.marshfield_pdf_pages
GROUP BY year
ORDER BY year;