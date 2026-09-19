-- Example project and scenario summary

SELECT
    financial_scenario,
    COUNT(DISTINCT project_id) AS project_count,
    SUM(capex) AS total_capex,
    SUM(annual_net_benefit) AS total_annual_net_benefit
FROM project_financial
GROUP BY financial_scenario
ORDER BY financial_scenario;
