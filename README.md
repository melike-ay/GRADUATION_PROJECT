Lumen Electronics Sales Data Warehouse Project

Overview

This project is a realistic data warehouse and business intelligence pipeline built from scratch using a self-generated dataset inspired by a Kaggle dataset.
The goal of the project is to simulate a real-world data engineering and analytics workflow, from data generation to final business reporting.

Project Description

I generated two large-scale CSV datasets (over 1,000,000 total rows and 53 columns) using Python:
  Offline Sales Dataset: U.S.-based retail transactions.
  Online Sales Dataset: Global e-commerce sales.
These datasets serve as the source data for the data warehouse.

The project follows a multi-layer ETL pipeline built on PostgreSQL, including:

Staging Layer: Temporary storage for raw data (all columns as VARCHAR(1000) to avoid type mismatch).
Cleansing Layer: Data validation, transformation, and standardization.
3NF Normalized Layer: Data structured in third normal form to ensure consistency and reduce redundancy.
Dimensional (DM) Layer: Denormalized tables prepared for analytical queries and reporting.

After the ETL process, I will perform data analytics and create Power BI dashboards to simulate a client-oriented business report, demonstrating real-world data visualization and decision-making support.

Tech Stack:
PostgreSQL – Database and ETL storage
DBeaver – SQL scripting and data management
Python – Data generation, preprocessing, and automation
Power BI – Visualization and business intelligence reporting

Documentation and Progress
The project includes a documentation file called Business_Template_Lumen that I update regularly, describing each step of the Design, ETL process and architectural decisions.
At the end of the project, a final business report will be added, containing Power BI dashboards and insights derived from the data warehouse.

Repository Structure (Planned)
📁 data/                                # Source CSV files (offline + online)
📁 sql_scripts/                         # SQL scripts for staging, cleansing, and ETL layers
📁 documentation/                       # Ongoing documentation and process notes
📁 reports/                             # Power BI dashboards and final client report
📁 python_scripts/                      # Scripts used for data generation and transformation
📄 README.md                            # Project overview (this file)
📄 Graduation_Project_Proposal.pdf      # Project proposal
