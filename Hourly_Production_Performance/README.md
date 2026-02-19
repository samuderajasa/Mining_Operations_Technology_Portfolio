# Hourly Production Performance

## Objective
The objective of this project is to develop a Power BI report that visualizes hourly excavator overburden production performance by presenting key operational metrics, including total production, productivity, working time, loading time, hanging time, spotting time, queueing time, and truck fleet speed.

This report serves as a monitoring tool to evaluate the production performance of loading units and hauling units, enabling timely operational control and performance analysis.

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

To support hourly production analysis, a SQL view was developed to:
- Join multiple operational tables (haul cycle, equipment status)
- Aggregate data at an hourly granularity
- Standardize fields and calculations for consistent reporting

<img alt="sql-view" src="https://github.com/samuderajasa/Mining_Operations_Technology_Portfolio/blob/master/Hourly_Production_Performance/tmp_fa3e1604-08b9-4a0c-947b-d09d09889d61.png">

The [SQL view](https://github.com/samuderajasa/Mining_Operations_Technology_Portfolio/blob/master/Hourly_Production_Performance/tmp_fa3e1604-08b9-4a0c-947b-d09d09889d61.png) serves as a single source of truth for downstream analytics and reporting, ensuring data accuracy, performance, and reusability when consumed by Power BI Dataflows and reports.

## Power BI
Power BI was used for data modeling, visualization, and business insights delivery.

Key activities include:
- Importing and modeling data from SQL Server
- Creating relationships and a star schema data model
- Developing calculated measures using DAX
- Designing interactive dashboards with filters and slicers
- Visualizing key performance indicators (KPIs) and trends

<img alt="pbi-report" src="https://github.com/samuderajasa/Mining_Operations_Technology_Portfolio/blob/master/Hourly_Production_Performance/tmp_acc62f6b-d26d-4a63-9145-9efcb622d3d1.png">

The final [dashboard](https://github.com/samuderajasa/Mining_Operations_Technology_Portfolio/blob/master/Hourly_Production_Performance/hourly_production_performance.pbix) provides an overview of key metrics and enables users to explore the data interactively to support data-driven decision making.
