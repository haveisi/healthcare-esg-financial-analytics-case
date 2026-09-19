# Key Power BI Measures

These are example measures. Replace table/column names with those used in the final model.

## Total CAPEX

```DAX
Total CAPEX =
SUM(Project_Financial[capex])
```

## Annual Net Benefit

```DAX
Annual Net Benefit =
SUM(Project_Financial[annual_net_benefit])
```

## Selected Scenario

```DAX
Selected Scenario =
SELECTEDVALUE(
    Project_Financial[financial_scenario],
    "All Scenarios"
)
```

## Project Count

```DAX
Project Count =
DISTINCTCOUNT(Project_Financial[project_id])
```

## Average NPV

```DAX
Average NPV =
AVERAGE(Project_Financial[npv])
```

## Average IRR

```DAX
Average IRR =
AVERAGE(Project_Financial[irr])
```

## CAPEX per Annual Net Benefit

```DAX
CAPEX per Annual Net Benefit =
DIVIDE(
    [Total CAPEX],
    [Annual Net Benefit]
)
```

## Modeling Note

Keep static project attributes separate from scenario-sensitive measures. A slicer should change only the measures that are intended to respond to scenario selection.
