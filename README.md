# Healthcare ESG, Resilience & Financial Decision Analytics

**Independent Marshfield Clinic public-evidence case study**

This project explores a practical question: how can community-health, ESG, resilience, and financial information be brought together in a way that supports management decisions?

I used publicly available Marshfield Clinic information to build an end-to-end analytics prototype in Databricks, Python, SQL, and Power BI. The project moves from source evidence and data validation to materiality assessment, ESG-to-financial pathways, and scenario-based financial analysis.

> This is an independent portfolio case study. It was not commissioned, sponsored, reviewed, or endorsed by Marshfield Clinic or Sanford Health. Financial scenarios are illustrative unless explicitly identified as public-source values.

---

## Project at a Glance

**Domain:** Healthcare sustainability, ESG, community impact, and resilience  
**Tools:** Databricks, Python, SQL, Power BI, DAX  
**Architecture:** Bronze → Silver → Gold  
**Focus:** Community Health / ESG → Operational Impact → Financial Decision Support  
**Outputs:** Materiality analysis, ESG-financial bridge, scenario model, data-quality controls, executive dashboard

---

## Why I Built This

Sustainability reporting can be good at describing performance, but management usually needs another layer of information:

**What does this issue mean operationally, financially, and in terms of priorities?**

I built this case to test whether public community-health, financial, and organizational evidence could be transformed into a traceable decision model.

The goal was not to create another ESG scorecard. I wanted to connect evidence to decisions.

That meant answering questions such as:

- Which issues are most material from both impact and financial perspectives?
- What financial channels might those issues affect?
- Which assumptions can be quantified now, and which still require internal data?
- How do project economics change under different scenarios?
- Can all of this be presented in a way that is useful to finance, operations, sustainability, and leadership?

---

## Solution Architecture

The project separates raw evidence, structured analysis, decision modeling, and reporting so each step can be traced and tested independently.

![Databricks Medallion Architecture](assets/databricks_medallion_architecture.png)

The Databricks workflow follows a Bronze–Silver–Gold structure:

- **Bronze:** source files, document pages, and source metadata
- **Silver:** cleaned and structured evidence, KPI definitions, facility information, and financial assumptions
- **Gold:** executive ESG summaries, financial pathways, data-quality outputs, resilience priorities, and project-level financial analysis

The Gold layer feeds the Power BI semantic model and decision-support views.

![Power BI Semantic Model](assets/powerbi_semantic_model.png)

---

## What I Built

### 1. Evidence and Source Traceability

I started with public-source documents, including community-health assessments, financial filings, and financing documents.

Each source was tracked through a source register and linked to the analytical model so that important findings could be traced back to the original evidence.

### 2. Data Preparation and Validation

I used Python and SQL to clean and structure the evidence, standardize fields and categories, and prepare decision-ready tables.

I also added validation checks for:

- missing identifiers
- invalid or incomplete values
- inconsistent categories
- duplicate records
- incomplete assumptions
- source and lineage status

I kept validation separate from visualization so data problems could be identified before they reached the dashboard.

### 3. Materiality and ESG Analysis

I translated public evidence into an analytical materiality framework covering issues such as:

- access to care
- behavioral health
- health equity
- substance use
- financial resilience and capital structure

The prioritization scores are analytical outputs developed for this case and are not Marshfield-published ratings.

### 4. ESG-to-Financial Translation

One of the main objectives was to move beyond reporting and identify how ESG and community-health issues could affect financial performance.

The model links issues to potential financial channels such as:

- operating cost
- revenue and utilization
- uncompensated care
- community benefit
- debt and cost of capital
- capital availability

Some linkages can be quantified using public information. Others are explicitly marked as requiring internal data.

### 5. Financial Scenario Analysis

I built illustrative scenarios around:

- CAPEX
- annual net benefit
- NPV
- payback
- benefit-cost ratio

These scenarios are sensitivity-based and are intended to demonstrate decision logic, not to represent Marshfield Clinic's actual project economics.

---

## Power BI Decision Support

### Executive ESG & Decision Support

![Executive ESG & Decision Support](screenshots/Executive%20ESG%20%26%20Decision%20Support.png)

This page provides a management-level view of material issues, evidence strength, and decision context.

### Materiality, Impacts, Risks & Opportunities

![Materiality, Impacts, Risks & Opportunities](screenshots/Materiality%2C%20Impacts%2C%20Risks%20%26%20Opportunities.png)

This view connects community and organizational issues with impact, financial significance, stakeholders, and available evidence.

### ESG-to-Financial Impact

![ESG Financial Impact](screenshots/ESG%20Financial%20Impact.png)

This page shows how ESG and community-health issues may translate into operating and financial effects.

### Financial Scenario Analysis

![Financial Scenario Analysis](screenshots/Financial%20Scenario%20Analysis.png)

This page connects modeled project assumptions to CAPEX, annual benefit, NPV, payback, and benefit-cost metrics.

---

## Challenges and Decisions

### Separating evidence from assumptions

The biggest analytical challenge was distinguishing between what could be supported by public evidence and what had to remain modeled.

I made that distinction explicit throughout the project rather than filling gaps with unsupported estimates.

### Scenario filtering in Power BI

One issue during development was that CAPEX did not initially respond correctly to the scenario slicer.

The problem was not the visual. It was the relationship and filter context between the scenario logic and the project-level calculations.

I corrected the model so scenario-sensitive measures respond to user selections while static project attributes remain unchanged where appropriate.

### Descriptive fields vs. calculation logic

I also found that descriptive fields such as `financial_channel` were useful for interpretation but not always appropriate as direct calculation drivers.

I separated descriptive dimensions from financial logic instead of forcing one field to control multiple behaviors.

---

## Data Gaps

Public evidence was sufficient to support community-health, financing, and selected governance analysis.

However, several important environmental metrics require internal operational data, including:

- energy consumption
- energy cost
- renewable electricity
- Scope 1 emissions
- Scope 2 emissions
- water consumption
- waste generation

I treated these as data gaps rather than estimating values without evidence.

---

## What I Learned

The main lesson from this project was that sustainability metrics become more useful when they are tied to the same decision structure used for capital and operational planning.

The hardest part was not building the dashboard. It was deciding which information was reliable, separating observed values from modeled assumptions, designing the data relationships correctly, and making sure the financial interpretation did not go beyond what the evidence could support.

---

## Skills Demonstrated

- Healthcare sustainability and ESG analytics
- Databricks medallion architecture
- Python / pandas
- SQL
- Power BI
- DAX
- Data modeling
- Evidence lineage and source traceability
- Data validation and QA
- Materiality analysis
- Financial scenario modeling
- CAPEX / OPEX analysis
- NPV / IRR
- Sustainability-to-finance translation
- Executive reporting
- Audit-ready documentation

---

## Limitations

This is not an internal Marshfield Clinic analysis.

I did not have access to confidential operating data, facility utility bills, internal capital plans, procurement records, engineering estimates, or private financial information.

The project should therefore be viewed as a demonstration of analytical architecture and decision-support methods rather than an estimate of Marshfield Clinic's actual project economics.
