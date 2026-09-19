# Healthcare Sustainability & Financial Analytics Case

Connecting sustainability and community-impact information with operational and financial decision-making.

> **Independent portfolio case study.** This project uses publicly available information associated with Marshfield Clinic as an analytical example. It was not commissioned, sponsored, reviewed, or endorsed by Marshfield Clinic or Sanford Health. Financial scenarios are illustrative unless explicitly identified as public-source values.

## Project at a Glance

**Domain:** Healthcare sustainability / ESG analytics  
**Tools:** Power BI, DAX, Python, SQL, Databricks-style medallion architecture  
**Focus:** Sustainability → Operations → Finance  
**Outputs:** Executive dashboard, project analysis, financial scenario model, QA controls  
**Data:** Public information + clearly labeled modeled assumptions

## Why I Built This

Sustainability reporting often tells us what happened. It does not always answer the next management question: **what does this mean operationally and financially?**

I built this case to explore how healthcare sustainability and community-impact information could be connected to project economics, capital decisions, resilience, and executive reporting.

The goal was not to build another ESG scorecard. I wanted to test whether sustainability information could be structured in the same decision framework used by operations and finance.

## Business Questions

1. Which sustainability and community-impact metrics are most useful for management decisions?
2. How can sustainability initiatives be translated into CAPEX, operating savings, and annual net benefit?
3. How do project rankings change under different financial assumptions?
4. Can sustainability and financial performance be viewed within the same Power BI workflow?
5. What information would still be required before the modeled scenarios could support real capital decisions?

## Analytical Workflow

```text
Public Sources
      ↓
Raw / Bronze Data
      ↓
Cleaning + Standardization
Python / SQL
      ↓
Validation / QA
      ↓
Analytical / Gold Tables
      ↓
Financial Scenario Model
      ↓
Power BI
      ↓
Executive + Operational Decision Views
```

I separated source collection, cleaning, validation, analytical calculations, and visualization rather than performing every transformation inside Power BI. This made assumptions easier to trace and calculations easier to test independently.

## What I Built

### 1. Data Preparation

I organized public information into structured project, operational, sustainability, community-impact, and financial tables. I standardized field names, units, categories, and project identifiers before loading the analytical model.

### 2. Validation

I added checks for missing identifiers, invalid financial values, inconsistent categories, duplicate rows, and incomplete assumptions. Validation logic was kept separate from visualization so problematic records could be identified before reporting.

### 3. Financial Modeling

I modeled project-level CAPEX, annual operating impact, annual net benefit, and scenario-dependent financial outcomes. Where reliable real-world values were unavailable, assumptions were explicitly labeled as modeled rather than observed.

### 4. Power BI Decision Layer

The report is organized around four views:

- **Executive Overview** — high-level sustainability, operational, and financial indicators
- **Sustainability & Operations** — operational drivers behind headline metrics
- **Project Analysis** — initiative-level comparison and prioritization
- **Financial Scenario Analysis** — CAPEX, annual net benefit, NPV, IRR, and scenario sensitivity

## Dashboard Screenshots

Add final screenshots to the `screenshots/` folder using these names:

- `01_executive_overview.png`
- `02_sustainability_operations.png`
- `03_project_analysis.png`
- `04_financial_scenarios.png`

Example Markdown after adding an image:

```markdown
![Executive Overview](screenshots/01_executive_overview.png)
```

## Challenges and Decisions

### Scenario Filtering

One issue during development was that CAPEX did not initially respond correctly to the scenario slicer. The problem was not formatting; it was the relationship and filter context between the scenario logic and project-level calculations.

I corrected the model so scenario-sensitive measures respond to the selected scenario while static project attributes remain unchanged where appropriate.

### Descriptive Fields vs. Calculation Logic

I also found that descriptive fields such as `financial_channel` were not always appropriate as calculation drivers. I separated descriptive dimensions from financial logic rather than forcing one field to control multiple behaviors.

### Data Availability

The most important analytical constraint was distinguishing what could be supported by public information from what had to remain modeled. That distinction is documented throughout the repository.

## Key Power BI Measures

See [`powerbi/measures.md`](powerbi/measures.md) for example DAX measures used in the model.

## Assumptions

See [`docs/assumptions.md`](docs/assumptions.md).

## Data Dictionary

See [`docs/data_dictionary.md`](docs/data_dictionary.md).

## Source Traceability

See [`data/source_manifest.csv`](data/source_manifest.csv).

## Limitations

This is not an internal Marshfield Clinic analysis. I did not have access to facility utility bills, internal capital plans, confidential operating data, procurement records, engineering estimates, or private financial records.

The financial scenarios therefore demonstrate analytical architecture and decision logic rather than estimates of Marshfield Clinic's actual project economics.

## What I Learned

The main lesson from this case was that sustainability metrics become more useful when they are tied to the same decision structure used for capital and operational planning.

The difficult part was not visualization. The harder work was deciding which information was reliable, separating observed values from modeled assumptions, designing relationships that behaved correctly under filtering, and ensuring that the financial interpretation did not overstate what the underlying data could support.

## Skills Demonstrated

- Healthcare sustainability / ESG analytics
- Power BI dashboard development
- DAX
- Data modeling
- Python / pandas
- SQL
- Data validation and QA
- Financial scenario analysis
- CAPEX / OPEX analysis
- NPV / IRR
- Sustainability-to-finance translation
- Executive reporting
- Documentation and auditability

## Repository Structure

```text
healthcare-sustainability-analytics-marshfield/
│
├── README.md
├── LICENSE
├── .gitignore
│
├── docs/
│   ├── methodology.md
│   ├── assumptions.md
│   ├── data_dictionary.md
│   └── limitations.md
│
├── data/
│   ├── README.md
│   ├── sample_data.csv
│   └── source_manifest.csv
│
├── src/
│   ├── data_cleaning.py
│   ├── validation.py
│   └── financial_analysis.py
│
├── sql/
│   ├── clean_project_data.sql
│   └── financial_summary.sql
│
├── powerbi/
│   ├── README.md
│   └── measures.md
│
├── screenshots/
│   └── README.md
│
└── assets/
    └── README.md
```

## Portfolio Positioning

**Healthcare Sustainability & Financial Analytics — Independent Case Study**

Developed an end-to-end healthcare sustainability analytics prototype using public Marshfield Clinic information, integrating operational, community-impact, and financial scenario data through Python, SQL, and Power BI; modeled CAPEX, annual net benefit, NPV, IRR, and scenario-sensitive decision metrics with documented QA controls and assumptions.
