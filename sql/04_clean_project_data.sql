-- Example cleaning logic for project-level analytical data

SELECT
    TRIM(project_id) AS project_id,
    TRIM(project_name) AS project_name,
    TRIM(category) AS category,
    CAST(capex AS DECIMAL(18,2)) AS capex,
    CAST(annual_savings AS DECIMAL(18,2)) AS annual_savings,
    CAST(annual_operating_cost AS DECIMAL(18,2)) AS annual_operating_cost,
    CAST(annual_net_benefit AS DECIMAL(18,2)) AS annual_net_benefit,
    CAST(discount_rate AS DECIMAL(10,4)) AS discount_rate,
    CAST(project_life_years AS INT) AS project_life_years,
    TRIM(financial_scenario) AS financial_scenario,
    TRIM(financial_channel) AS financial_channel,
    TRIM(data_status) AS data_status
FROM project_data;
