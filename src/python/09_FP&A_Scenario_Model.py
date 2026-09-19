# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
    IntegerType, BooleanType
)

intervention_schema = StructType([
    StructField("intervention_id", StringType(), False),
    StructField("issue_id", StringType(), False),
    StructField("intervention_name", StringType(), False),
    StructField("intervention_type", StringType(), True),

    StructField("capex", DoubleType(), True),
    StructField("annual_opex_change", DoubleType(), True),
    StructField("annual_financial_benefit", DoubleType(), True),

    StructField("implementation_years", IntegerType(), True),
    StructField("useful_life_years", IntegerType(), True),

    StructField("discount_rate", DoubleType(), True),

    StructField("financial_data_status", StringType(), True),
    StructField("esg_benefit", StringType(), True),
    StructField("financial_mechanism", StringType(), True),

    StructField("evidence_source", StringType(), True),
    StructField("assumption_flag", BooleanType(), True),

    StructField("notes", StringType(), True)
])

# COMMAND ----------

intervention_rows = [

    (
        "INT001",
        "S01",
        "Expand rural access / telehealth capacity",
        "Access",
        None,
        None,
        None,
        2,
        7,
        0.07,
        "MODEL_REQUIRED",
        "Improved healthcare access",
        "Potential utilization improvement and avoided delayed-care cost",
        "MCHS005",
        True,
        "Requires internal utilization, staffing, reimbursement, and program-cost data."
    ),

    (
        "INT002",
        "S02",
        "Integrated behavioral-health capacity expansion",
        "Behavioral Health",
        None,
        None,
        None,
        2,
        7,
        0.07,
        "MODEL_REQUIRED",
        "Improved behavioral-health access and outcomes",
        "Potential reduction in avoidable ED utilization and care escalation",
        "MCHS005",
        True,
        "Requires encounter-level utilization and cost data."
    ),

    (
        "INT003",
        "S03",
        "Substance-use prevention and treatment partnership",
        "Community Health",
        None,
        None,
        None,
        2,
        5,
        0.07,
        "MODEL_REQUIRED",
        "Reduced substance-use burden",
        "Potential avoided treatment and emergency-care costs",
        "MCHS005",
        True,
        "Requires program cost and utilization assumptions."
    ),

    (
        "INT004",
        "S04",
        "Targeted health-equity access program",
        "Population Health",
        None,
        None,
        None,
        3,
        7,
        0.07,
        "MODEL_REQUIRED",
        "Reduced disparities and improved access",
        "Potential effect on uncompensated care and utilization",
        "MCHS006",
        True,
        "Requires population, payer, and utilization data."
    )
]

# COMMAND ----------

intervention_df = spark.createDataFrame(
    intervention_rows,
    schema=intervention_schema
)

display(intervention_df)

# COMMAND ----------

reported_financial_rows = [
    (
        "RF001",
        "FIN002",
        "Series 2024A revenue bonds",
        374895000.0,
        "USD",
        2024,
        "REPORTED"
    ),

    (
        "RF002",
        "FIN002",
        "Additional taxable financing",
        174830000.0,
        "USD",
        2024,
        "REPORTED"
    )
]

# COMMAND ----------

reported_financial_schema = StructType([
    StructField("financial_fact_id", StringType(), False),
    StructField("source_id", StringType(), False),
    StructField("metric_name", StringType(), False),
    StructField("metric_value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("reporting_year", IntegerType(), True),
    StructField("data_type", StringType(), True)
])

# COMMAND ----------

reported_financial_df = spark.createDataFrame(
    reported_financial_rows,
    schema=reported_financial_schema
)

reported_financial_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "marshfield_esg.silver.reported_financial_facts"
    )

# COMMAND ----------

spark.sql("""
SELECT
    SUM(metric_value) AS combined_reported_financing
FROM marshfield_esg.silver.reported_financial_facts
WHERE reporting_year = 2024
""").show()

# COMMAND ----------

assumption_rows = [
    ("A001", "Discount rate", 0.07, "decimal", "MODELED", "Base-case analytical assumption"),
    ("A002", "Analysis horizon", 10.0, "years", "MODELED", "Initial project comparison horizon"),
    ("A003", "Inflation rate", 0.025, "decimal", "MODELED", "Scenario assumption only"),
]

# COMMAND ----------

assumption_schema = StructType([
    StructField("assumption_id", StringType(), False),
    StructField("assumption_name", StringType(), False),
    StructField("assumption_value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("assumption_type", StringType(), True),
    StructField("notes", StringType(), True)
])

# COMMAND ----------

assumption_df = spark.createDataFrame(
    assumption_rows,
    schema=assumption_schema
)

assumption_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "marshfield_esg.silver.financial_assumptions"
    )

# COMMAND ----------

gold_finance = spark.sql("""
SELECT
    SUM(metric_value) AS combined_financing,
    'USD' AS unit,
    2024 AS reporting_year,
    'CALCULATED_FROM_REPORTED_VALUES' AS value_status
FROM marshfield_esg.silver.reported_financial_facts
""")

# COMMAND ----------

gold_finance.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "marshfield_esg.gold.financial_baseline"
    )

# COMMAND ----------

display(
    spark.sql("""
        SELECT *
        FROM marshfield_esg.gold.financial_baseline
    """)
)