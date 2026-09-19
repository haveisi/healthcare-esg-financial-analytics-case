# Databricks notebook source
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType
)

bridge_schema = StructType([
    StructField("bridge_id", StringType(), False),
    StructField("issue_id", StringType(), False),
    StructField("kpi_id", StringType(), False),

    StructField("financial_channel", StringType(), False),
    StructField("financial_statement_area", StringType(), True),

    StructField("impact_mechanism", StringType(), True),
    StructField("direction", StringType(), True),

    StructField("quantifiable_now", BooleanType(), True),
    StructField("current_value", DoubleType(), True),
    StructField("unit", StringType(), True),

    StructField("model_required", StringType(), True),
    StructField("decision_use", StringType(), True),

    StructField("confidence", StringType(), True),
    StructField("notes", StringType(), True)
])

# COMMAND ----------

bridge_rows = [

    (
        "FB001",
        "S01",
        "S01_KPI01",
        "Revenue / Utilization",
        "Income Statement",
        "Poor access may reduce preventive and scheduled service utilization while increasing delayed or acute care use.",
        "Mixed",
        False,
        None,
        None,
        "Utilization and service-line model",
        "Assess whether access investments change utilization patterns and service demand.",
        "MEDIUM",
        "Needs utilization and payer data."
    ),

    (
        "FB002",
        "S02",
        "S02_KPI01",
        "Operating Cost",
        "Income Statement",
        "Behavioral-health capacity constraints may increase emergency utilization, staffing burden, and avoidable high-cost care.",
        "Negative",
        False,
        None,
        None,
        "Avoided-cost model",
        "Estimate financial benefit of behavioral-health capacity expansion or integrated care.",
        "MEDIUM",
        "Requires utilization, staffing, and encounter-cost data."
    ),

    (
        "FB003",
        "S03",
        "S03_KPI01",
        "Community Benefit / Treatment Cost",
        "Income Statement / Community Benefit",
        "Substance-use burden can increase treatment cost and community-health investment requirements.",
        "Negative",
        False,
        None,
        None,
        "Program-cost and avoided-utilization model",
        "Evaluate prevention and treatment interventions against program cost and avoided utilization.",
        "MEDIUM",
        "Requires program and utilization data."
    ),

    (
        "FB004",
        "S04",
        "S04_KPI01",
        "Uncompensated Care / Community Benefit",
        "Income Statement",
        "Health disparities may affect service access, uncompensated care, utilization, and targeted community investment.",
        "Negative",
        False,
        None,
        None,
        "Population-health financial model",
        "Prioritize equity interventions based on population impact and financial consequences.",
        "LOW",
        "Needs payer, access, utilization, and community-benefit data."
    ),

    (
        "FB005",
        "G03",
        "G03_KPI01",
        "Debt / Capital Availability",
        "Balance Sheet / Cash Flow",
        "Large bond financing affects debt service, liquidity, and capacity for additional capital investment.",
        "Negative",
        True,
        374895000.0,
        "USD",
        "Debt-service and capital-allocation model",
        "Assess available capital and trade-offs among infrastructure, resilience, and sustainability investments.",
        "HIGH",
        "Reported Series 2024A financing amount."
    ),

    (
        "FB006",
        "G03",
        "G03_KPI02",
        "Debt / Cost of Capital",
        "Balance Sheet / Cash Flow",
        "Additional taxable financing affects leverage, interest expense, and overall financing cost.",
        "Negative",
        True,
        174830000.0,
        "USD",
        "Capital-structure model",
        "Evaluate financing mix and implications for future capital projects.",
        "HIGH",
        "Reported concurrent taxable financing."
    )
]

# COMMAND ----------

bridge_rows = [

    (
        "FB001",
        "S01",
        "S01_KPI01",
        "Revenue / Utilization",
        "Income Statement",
        "Poor access may reduce preventive and scheduled service utilization while increasing delayed or acute care use.",
        "Mixed",
        False,
        None,
        None,
        "Utilization and service-line model",
        "Assess whether access investments change utilization patterns and service demand.",
        "MEDIUM",
        "Needs utilization and payer data."
    ),

    (
        "FB002",
        "S02",
        "S02_KPI01",
        "Operating Cost",
        "Income Statement",
        "Behavioral-health capacity constraints may increase emergency utilization, staffing burden, and avoidable high-cost care.",
        "Negative",
        False,
        None,
        None,
        "Avoided-cost model",
        "Estimate financial benefit of behavioral-health capacity expansion or integrated care.",
        "MEDIUM",
        "Requires utilization, staffing, and encounter-cost data."
    ),

    (
        "FB003",
        "S03",
        "S03_KPI01",
        "Community Benefit / Treatment Cost",
        "Income Statement / Community Benefit",
        "Substance-use burden can increase treatment cost and community-health investment requirements.",
        "Negative",
        False,
        None,
        None,
        "Program-cost and avoided-utilization model",
        "Evaluate prevention and treatment interventions against program cost and avoided utilization.",
        "MEDIUM",
        "Requires program and utilization data."
    ),

    (
        "FB004",
        "S04",
        "S04_KPI01",
        "Uncompensated Care / Community Benefit",
        "Income Statement",
        "Health disparities may affect service access, uncompensated care, utilization, and targeted community investment.",
        "Negative",
        False,
        None,
        None,
        "Population-health financial model",
        "Prioritize equity interventions based on population impact and financial consequences.",
        "LOW",
        "Needs payer, access, utilization, and community-benefit data."
    ),

    (
        "FB005",
        "G03",
        "G03_KPI01",
        "Debt / Capital Availability",
        "Balance Sheet / Cash Flow",
        "Large bond financing affects debt service, liquidity, and capacity for additional capital investment.",
        "Negative",
        True,
        374895000.0,
        "USD",
        "Debt-service and capital-allocation model",
        "Assess available capital and trade-offs among infrastructure, resilience, and sustainability investments.",
        "HIGH",
        "Reported Series 2024A financing amount."
    ),

    (
        "FB006",
        "G03",
        "G03_KPI02",
        "Debt / Cost of Capital",
        "Balance Sheet / Cash Flow",
        "Additional taxable financing affects leverage, interest expense, and overall financing cost.",
        "Negative",
        True,
        174830000.0,
        "USD",
        "Capital-structure model",
        "Evaluate financing mix and implications for future capital projects.",
        "HIGH",
        "Reported concurrent taxable financing."
    )
]

# COMMAND ----------

bridge_rows = [

    (
        "FB001",
        "S01",
        "S01_KPI01",
        "Revenue / Utilization",
        "Income Statement",
        "Poor access may reduce preventive and scheduled service utilization while increasing delayed or acute care use.",
        "Mixed",
        False,
        None,
        None,
        "Utilization and service-line model",
        "Assess whether access investments change utilization patterns and service demand.",
        "MEDIUM",
        "Needs utilization and payer data."
    ),

    (
        "FB002",
        "S02",
        "S02_KPI01",
        "Operating Cost",
        "Income Statement",
        "Behavioral-health capacity constraints may increase emergency utilization, staffing burden, and avoidable high-cost care.",
        "Negative",
        False,
        None,
        None,
        "Avoided-cost model",
        "Estimate financial benefit of behavioral-health capacity expansion or integrated care.",
        "MEDIUM",
        "Requires utilization, staffing, and encounter-cost data."
    ),

    (
        "FB003",
        "S03",
        "S03_KPI01",
        "Community Benefit / Treatment Cost",
        "Income Statement / Community Benefit",
        "Substance-use burden can increase treatment cost and community-health investment requirements.",
        "Negative",
        False,
        None,
        None,
        "Program-cost and avoided-utilization model",
        "Evaluate prevention and treatment interventions against program cost and avoided utilization.",
        "MEDIUM",
        "Requires program and utilization data."
    ),

    (
        "FB004",
        "S04",
        "S04_KPI01",
        "Uncompensated Care / Community Benefit",
        "Income Statement",
        "Health disparities may affect service access, uncompensated care, utilization, and targeted community investment.",
        "Negative",
        False,
        None,
        None,
        "Population-health financial model",
        "Prioritize equity interventions based on population impact and financial consequences.",
        "LOW",
        "Needs payer, access, utilization, and community-benefit data."
    ),

    (
        "FB005",
        "G03",
        "G03_KPI01",
        "Debt / Capital Availability",
        "Balance Sheet / Cash Flow",
        "Large bond financing affects debt service, liquidity, and capacity for additional capital investment.",
        "Negative",
        True,
        374895000.0,
        "USD",
        "Debt-service and capital-allocation model",
        "Assess available capital and trade-offs among infrastructure, resilience, and sustainability investments.",
        "HIGH",
        "Reported Series 2024A financing amount."
    ),

    (
        "FB006",
        "G03",
        "G03_KPI02",
        "Debt / Cost of Capital",
        "Balance Sheet / Cash Flow",
        "Additional taxable financing affects leverage, interest expense, and overall financing cost.",
        "Negative",
        True,
        174830000.0,
        "USD",
        "Capital-structure model",
        "Evaluate financing mix and implications for future capital projects.",
        "HIGH",
        "Reported concurrent taxable financing."
    )
]

# COMMAND ----------

bridge_df = spark.createDataFrame(
    bridge_rows,
    schema=bridge_schema
)

display(bridge_df)

# COMMAND ----------

bridge_df = spark.createDataFrame(
    bridge_rows,
    schema=bridge_schema
)

display(bridge_df)

# COMMAND ----------

bridge_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("marshfield_esg.silver.esg_financial_bridge")