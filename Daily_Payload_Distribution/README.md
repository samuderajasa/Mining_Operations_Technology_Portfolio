# Daily Payload Distribution

## Objective
The objective of this project is to visualize daily payload distribution by categorizing payload performance into underload, overload, in-range, lower-range, and upper-range categories. The report presents payload distribution using tables and histogram charts, along with the corresponding actual payload values (tons).

This visualization supports monitoring of loading consistency, payload compliance, and opportunities to improve hauling efficiency and operational performance.

## Table of Content
- [Dataset](#dataset)
- [Technologies](#technologies)
- [Data Pipeline](#data-pipeline)
- [Microsoft SQL Server](#microsoft-sql-server)
- [Power BI](#power-bi)

## Dataset
The dataset used in this project is derived from proprietary Fleet Management System (FMS) data and is subject to company confidentiality restrictions. As such, the data is not shared publicly in this repository.

## Technologies
The following technologies were used to build this project:
- Language: SQL
- Database & Storage: Microsoft SQL Server
- ETL & Data Processing: Microsoft SQL Server (Views, Joins, Aggregations)
- Reporting & Visualization: Power BI (Dataflows, Reports, Service)

## Data Pipeline

```mermaid
graph LR;
    SQLServer[SQL Server] --> SQLView[SQL View];
    SQLView --> PowerBIDataflow[Power BI Dataflow];
    PowerBIDataflow --> PowerBIReport[Power BI Report];
    PowerBIReport --> PowerBIService[Power BI Service];
```

## Microsoft SQL Server
Microsoft SQL Server is used as the primary data source and transformation layer for this project. Raw operational data from the Fleet Management System (FMS) is stored across multiple tables within the database.

To support time usage analysis, a SQL view was developed to:

- Join multiple operational tables (haul cycle trans, equip, status trans)
- Aggregate data at an hourly granularity
- Standardize fields and calculations for consistent reporting

<img alt="sql-view" src="">

The [SQL view]() serves as a single source of truth for downstream analytics and reporting, ensuring data accuracy, performance, and reusability when consumed by Power BI Dataflows and reports.

## Power BI
Power BI was used for data modeling, visualization, and business insights delivery.

Key activities include:
- Importing and modeling data from SQL Server
- Creating relationships and a star schema data model
- Developing calculated measures using DAX
- Designing interactive dashboards with filters and slicers

<img alt="pbi-report" src="">

The final [dashboard]() provides an overview of key metrics and enables users to explore the data interactively to support data-driven decision making.

