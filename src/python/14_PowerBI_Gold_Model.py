# Databricks notebook source
executive_esg = spark.sql("""
SELECT
    m.issue_id,
    m.issue_name,
    i.esg_pillar,

    ROUND(m.impact_score,2) AS impact_score,
    ROUND(m.financial_score,2) AS financial_score,
    ROUND(m.evidence_strength,2) AS evidence_strength,
    ROUND(m.priority_score,2) AS priority_score,

    m.time_horizon,
    m.financial_channel,
    m.evidence_source,
    m.evidence_page

FROM marshfield_esg.silver.materiality_iro m

LEFT JOIN marshfield_esg.silver.issue_universe i
    ON m.issue_id = i.issue_id
""")

# COMMAND ----------

executive_esg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.gold.executive_esg_summary")

# COMMAND ----------

gold_kpi = spark.sql("""
SELECT
    k.kpi_id,
    k.issue_id,
    k.issue_name,
    i.esg_pillar,

    k.kpi_name,
    k.kpi_definition,
    k.unit,

    k.baseline_value,
    k.baseline_year,
    k.target_value,
    k.target_year,

    k.business_owner,
    k.frequency,

    k.management_kpi,
    k.disclosure_metric,

    k.financial_link,
    k.decision_use,
    k.evidence_status,
    k.source_id

FROM marshfield_esg.silver.kpi_catalog k

LEFT JOIN marshfield_esg.silver.issue_universe i
    ON k.issue_id = i.issue_id
""")

# COMMAND ----------

gold_kpi.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.gold.esg_kpi_dashboard")

# COMMAND ----------

gold_bridge = spark.sql("""
SELECT
    b.bridge_id,
    b.issue_id,
    k.issue_name,
    k.kpi_id,
    k.kpi_name,

    b.financial_channel,
    b.financial_statement_area,
    b.impact_mechanism,
    b.direction,

    b.quantifiable_now,
    b.current_value,
    b.unit,

    b.model_required,
    b.decision_use,
    b.confidence

FROM marshfield_esg.silver.esg_financial_bridge b

LEFT JOIN marshfield_esg.silver.kpi_catalog k
    ON b.kpi_id = k.kpi_id
""")

# COMMAND ----------

gold_bridge.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.gold.esg_financial_bridge")

# COMMAND ----------

display(
    spark.sql("""
        SHOW TABLES IN marshfield_esg.silver
    """)
)

# COMMAND ----------

gold_project_finance = spark.sql("""
SELECT
    scenario_id,
    intervention_id,
    project_name,
    scenario_name,
    capex,
    annual_operating_savings,
    annual_opex_increase,
    annual_net_benefit,
    useful_life_years,
    discount_rate,
    residual_value,
    simple_payback_years,
    npv,
    pv_operating_benefits,
    benefit_cost_ratio,
    financial_case,
    data_status,
    notes
FROM marshfield_esg.silver.project_financial_scenarios
""")

# COMMAND ----------

gold_project_finance.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.gold.project_financial_analysis")

# COMMAND ----------

display(
    spark.sql("""
        SHOW TABLES IN marshfield_esg.gold
    """)
)

# COMMAND ----------

display(
    spark.table(
        "marshfield_esg.gold.project_financial_analysis"
    )
)

# COMMAND ----------

display(
    spark.sql("""
        SHOW TABLES IN marshfield_esg.silver
    """)
)

# COMMAND ----------

gold_project_finance = spark.sql("""
SELECT
    scenario_id,
    intervention_id,
    project_name,
    scenario_name,
    capex,
    annual_operating_savings,
    annual_opex_increase,
    annual_net_benefit,
    useful_life_years,
    discount_rate,
    residual_value,
    simple_payback_years,
    npv,
    pv_operating_benefits,
    benefit_cost_ratio,
    financial_case,
    data_status,
    notes
FROM marshfield_esg.silver.project_financial_scenarios
""")

# COMMAND ----------

gold_project_finance.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.gold.project_financial_analysis")

# COMMAND ----------

display(
    spark.sql("""
        SHOW TABLES IN marshfield_esg.gold
    """)
)

# COMMAND ----------

display(
    spark.table(
        "marshfield_esg.gold.project_financial_analysis"
    )
)

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS scenario_count,
    ROUND(SUM(capex), 2) AS total_capex,
    ROUND(SUM(npv), 2) AS total_npv,
    ROUND(AVG(simple_payback_years), 2) AS avg_payback
FROM marshfield_esg.gold.project_financial_analysis
""").show()

# COMMAND ----------

executive_esg = spark.sql("""
SELECT
    m.issue_id,
    m.issue_name,
    i.esg_pillar,

    m.impact_description,
    m.risk_description,
    m.opportunity_description,

    m.stakeholders,
    m.time_horizon,
    m.financial_channel,

    m.evidence_source,
    m.evidence_page,

    ROUND(m.impact_score, 2) AS impact_score,
    ROUND(m.financial_score, 2) AS financial_score,
    ROUND(m.evidence_strength, 2) AS evidence_strength,
    ROUND(m.priority_score, 2) AS priority_score

FROM marshfield_esg.silver.materiality_iro m

LEFT JOIN marshfield_esg.silver.issue_universe i
    ON m.issue_id = i.issue_id
""")

# COMMAND ----------

display(executive_esg)

# COMMAND ----------

executive_esg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "marshfield_esg.gold.executive_esg_summary"
    )

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            issue_id,
            issue_name,
            esg_pillar,
            impact_score,
            financial_score,
            evidence_strength,
            priority_score,
            financial_channel
        FROM marshfield_esg.gold.executive_esg_summary
        ORDER BY priority_score DESC
    """)
)

# COMMAND ----------

spark.sql("""
SELECT
    COUNT(*) AS total_material_issues,

    SUM(
        CASE WHEN esg_pillar IS NULL
        THEN 1 ELSE 0 END
    ) AS missing_esg_pillar,

    SUM(
        CASE WHEN priority_score IS NULL
        THEN 1 ELSE 0 END
    ) AS missing_priority_score,

    SUM(
        CASE WHEN evidence_source IS NULL
        THEN 1 ELSE 0 END
    ) AS missing_evidence_source

FROM marshfield_esg.gold.executive_esg_summary
""").show()

# COMMAND ----------

display(
    spark.sql("""
        SHOW TABLES IN marshfield_esg.gold
    """)
)

# COMMAND ----------

data_quality = spark.sql("""
SELECT
    m.source_id,
    m.source_name,
    m.source_type,
    m.reliability_tier,
    m.downloaded,
    m.local_filename,

    CASE
        WHEN f.source_id IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS file_present,

    CASE
        WHEN m.downloaded = TRUE
             AND f.source_id IS NOT NULL
             AND m.local_filename = f.file_name
            THEN 'PASS'

        WHEN m.downloaded = TRUE
             AND f.source_id IS NULL
            THEN 'MISSING_FILE'

        WHEN m.downloaded = TRUE
             AND f.source_id IS NOT NULL
             AND m.local_filename <> f.file_name
            THEN 'FILENAME_MISMATCH'

        WHEN m.downloaded = FALSE
            THEN 'NOT_DOWNLOADED'

        ELSE 'REVIEW'
    END AS lineage_status

FROM marshfield_esg.bronze.source_manifest m

LEFT JOIN marshfield_esg.bronze.file_inventory f
    ON m.source_id = f.source_id
""")

# COMMAND ----------

data_quality.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.gold.data_quality_summary")

# COMMAND ----------

display(
    spark.sql("""
        SELECT
            lineage_status,
            COUNT(*) AS sources
        FROM marshfield_esg.gold.data_quality_summary
        GROUP BY lineage_status
        ORDER BY lineage_status
    """)
)

# COMMAND ----------

environmental_gap = spark.sql("""
SELECT
    issue_id,
    metric_name,
    unit,
    data_status
FROM marshfield_esg.silver.environmental_data_requirements
""")

# COMMAND ----------

environmental_gap.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.gold.environmental_data_gap")

# COMMAND ----------

display(
    spark.table(
        "marshfield_esg.gold.environmental_data_gap"
    )
)

# COMMAND ----------

display(
    spark.sql("""
        SHOW TABLES IN marshfield_esg.gold
    """)
)

# COMMAND ----------

tables = [
    "executive_esg_summary",
    "esg_kpi_dashboard",
    "esg_financial_bridge",
    "project_financial_analysis",
    "data_quality_summary"
]

for table in tables:
    df = spark.table(f"marshfield_esg.gold.{table}")

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"/Volumes/marshfield_esg/bronze/source_files/powerbi_exports/{table}")
    )

# COMMAND ----------

for table in tables:
    df = spark.table(f"marshfield_esg.gold.{table}")

    (
        df.write
        .mode("overwrite")
        .parquet(
            f"/Volumes/marshfield_esg/bronze/source_files/powerbi_exports/{table}"
        )
    )

# COMMAND ----------

dbutils.fs.mkdirs(
    "/Volumes/marshfield_esg/bronze/source_files/powerbi_exports/"
)

# COMMAND ----------

tables = [
    "executive_esg_summary",
    "esg_kpi_dashboard",
    "esg_financial_bridge",
    "project_financial_analysis",
    "data_quality_summary",
    "environmental_data_gap"
]

for table in tables:
    df = spark.table(f"marshfield_esg.gold.{table}")

    (
        df.coalesce(1)
          .write
          .mode("overwrite")
          .option("header", "true")
          .csv(
              f"/Volumes/marshfield_esg/bronze/source_files/powerbi_exports/{table}"
          )
    )

# COMMAND ----------

spark.sql("""
SELECT
    issue_id,
    issue_name,
    impact_score,
    financial_score,
    evidence_strength,
    priority_score
FROM marshfield_esg.silver.materiality_iro
ORDER BY issue_id
""").show(truncate=False)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

iro_schema = StructType([
    StructField("issue_id", StringType(), False),
    StructField("issue_name", StringType(), False),
    StructField("impact_description", StringType(), True),
    StructField("risk_description", StringType(), True),
    StructField("opportunity_description", StringType(), True),
    StructField("stakeholders", StringType(), True),
    StructField("time_horizon", StringType(), True),
    StructField("financial_channel", StringType(), True),
    StructField("evidence_source", StringType(), True),
    StructField("evidence_page", IntegerType(), True),
    StructField("impact_score", DoubleType(), True),
    StructField("financial_score", DoubleType(), True),
    StructField("evidence_strength", DoubleType(), True),
    StructField("priority_score", DoubleType(), True)
])

# COMMAND ----------

iro_rows = [

    (
        "S01",
        "Access to Care",
        "Limited healthcare access can reduce timely use of preventive, primary, and specialty services.",
        "Provider shortages, transportation barriers, affordability, and service availability can increase unmet need.",
        "Telehealth, community partnerships, and targeted service delivery can improve access and system efficiency.",
        "Patients, rural communities, providers, community organizations",
        "Short/Medium",
        "Revenue, utilization, staffing cost, patient affordability, community benefit",
        "MCHS005",
        1,   # replace with actual page
        5.0,
        4.0,
        5.0,
        None
    ),

    (
        "S02",
        "Behavioral Health",
        "Behavioral and mental health needs directly affect patient and community well-being.",
        "Insufficient capacity can increase emergency utilization, delayed treatment, and workforce pressure.",
        "Integrated behavioral-health services and community partnerships can improve outcomes and reduce avoidable utilization.",
        "Patients, families, providers, communities",
        "Short/Medium",
        "Operating cost, utilization, staffing, community investment",
        "MCHS005",
        1,   # replace with actual page
        5.0,
        4.0,
        5.0,
        None
    ),

    (
        "S03",
        "Substance Use",
        "Substance-use disorders create significant health and community impacts.",
        "High substance-use burden can increase acute-care demand and treatment cost.",
        "Prevention, treatment access, and coordinated community programs can reduce adverse outcomes.",
        "Patients, families, healthcare providers, community partners",
        "Short/Medium",
        "Utilization, treatment cost, grant funding, community benefit",
        "MCHS005",
        1,   # replace with actual page
        4.0,
        3.0,
        5.0,
        None
    ),

    (
        "S04",
        "Health Equity",
        "Differences in access, socioeconomic conditions, geography, and other barriers may create unequal health outcomes.",
        "Persistent disparities can increase unmet need and weaken community-health performance.",
        "Targeted interventions can improve equitable access, community trust, and population-health outcomes.",
        "Patients, underserved populations, communities, providers",
        "Medium/Long",
        "Community benefit, utilization, uncompensated care, program investment",
        "MCHS006",
        1,   # replace with actual page
        5.0,
        3.0,
        5.0,
        None
    ),

    (
        "G03",
        "Financial Resilience and Capital Structure",
        "Large financing requirements affect the system's ability to maintain and modernize healthcare infrastructure.",
        "Debt service, liquidity pressure, or constrained capital availability could reduce financial flexibility.",
        "Capital planning can integrate resilience, energy efficiency, lifecycle cost, and operational-performance considerations.",
        "Leadership, patients, employees, lenders, communities",
        "Short/Medium",
        "Debt service, CAPEX, liquidity, operating margin",
        "FIN002",
        1,   # replace with actual page
        4.0,
        5.0,
        5.0,
        None
    )
]

# COMMAND ----------

from pyspark.sql import functions as F

iro_df = spark.createDataFrame(
    iro_rows,
    schema=iro_schema
)

iro_df = iro_df.withColumn(
    "priority_score",
    0.4 * F.col("impact_score") +
    0.4 * F.col("financial_score") +
    0.2 * F.col("evidence_strength")
)

display(iro_df)

# COMMAND ----------

iro_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.silver.materiality_iro")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS material_issues
FROM marshfield_esg.silver.materiality_iro
""").show()

# COMMAND ----------

executive_esg = spark.sql("""
SELECT
    m.issue_id,
    m.issue_name,
    i.esg_pillar,

    m.impact_description,
    m.risk_description,
    m.opportunity_description,
    m.stakeholders,
    m.time_horizon,
    m.financial_channel,

    m.evidence_source,
    m.evidence_page,

    ROUND(m.impact_score, 2) AS impact_score,
    ROUND(m.financial_score, 2) AS financial_score,
    ROUND(m.evidence_strength, 2) AS evidence_strength,
    ROUND(m.priority_score, 2) AS priority_score

FROM marshfield_esg.silver.materiality_iro m

LEFT JOIN marshfield_esg.silver.issue_universe i
    ON m.issue_id = i.issue_id
""")

# COMMAND ----------

display(executive_esg)

# COMMAND ----------

executive_esg.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.gold.executive_esg_summary")

# COMMAND ----------

spark.sql("""
SELECT
    issue_id,
    issue_name,
    esg_pillar,
    impact_score,
    financial_score,
    evidence_strength,
    priority_score
FROM marshfield_esg.gold.executive_esg_summary
ORDER BY priority_score DESC
""").show(truncate=False)

# COMMAND ----------

(
    spark.table("marshfield_esg.gold.executive_esg_summary")
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(
        "/Volumes/marshfield_esg/bronze/source_files/powerbi_exports/executive_esg_summary"
    )
)