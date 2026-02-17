# Hourly Production Performance

## Objective
The objective of this project is to develop a Power BI report that visualizes hourly excavator overburden production performance by presenting key operational metrics, including total production, productivity, working time, loading time, hanging time, spotting time, queueing time, and truck fleet speed.

This report serves as a monitoring tool to evaluate the production performance of loading units and hauling units, enabling timely operational control and performance analysis.

## Table of Content
[Dataset](Dataset)
[Technologies](Technologies)
Data Pipeline
SQL
Power BI

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

## SQL Server
