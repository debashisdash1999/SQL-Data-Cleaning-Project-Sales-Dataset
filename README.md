# SQL Data Cleaning Project – Sales Dataset

## 📌 Project Overview

This project demonstrates a complete data cleaning and preparation workflow using SQL Server. A raw sales dataset in CSV format was ingested, cleaned, standardized, and transformed into a structured dataset suitable for analysis and reporting.

The objective was to simulate a real-world data engineering scenario where incoming data is messy, inconsistent, and not directly usable for business insights.

---

## ⚠️ Challenges Faced During Data Ingestion

While attempting to load the CSV file using BULK INSERT, several issues were encountered:

* Data type mismatches between CSV values and table schema
* Errors due to invalid numeric values and inconsistent formats
* Problems with multiline text fields (especially customer address)
* Unexpected end-of-file errors caused by broken row structures
* Inconsistent delimiters and quoted text leading to parsing failures

These issues made BULK INSERT unreliable for this dataset because it expects well-structured and clean input data.

---

## ✅ Why Import Wizard Was Used Instead

To overcome these challenges, the SQL Server Import Flat File Wizard was used.

Reasons for choosing the wizard:

* Handles messy CSV structures more gracefully
* Automatically detects column boundaries and data patterns
* Supports quoted text and multiline fields without breaking rows
* Allows manual control over column datatypes before import
* Prevents data loss by avoiding strict schema enforcement during ingestion

This approach ensured that all raw data was successfully loaded without errors, forming a reliable foundation for further cleaning.

---

## 🏗️ Data Processing Approach

The project follows a structured data cleaning pipeline:

### 1. Raw Data Ingestion

* Loaded CSV data into a SQL Server table
* Used flexible datatypes to prevent load failures
* Preserved all raw values for accurate processing

### 2. Data Profiling

* Checked total row counts
* Identified missing values
* Detected duplicate records
* Analyzed inconsistencies across columns

### 3. Data Cleaning

* Removed duplicate records based on transaction identifiers
* Handled null and missing values
* Corrected negative and invalid numeric entries
* Standardized categorical values such as payment methods
* Fixed inconsistent and invalid email formats
* Addressed missing or incorrect total amounts

### 4. Data Transformation

* Converted string-based columns into appropriate datatypes
* Standardized date formats into proper date fields
* Ensured numerical consistency across metrics

### 5. Data Validation

* Verified absence of duplicates
* Checked for null values in critical fields
* Validated calculated fields such as total amount
* Ensured consistency across categorical values

### 6. Final Clean Dataset

* Created a clean, structured table ready for analytics
* Applied constraints such as primary keys
* Ensured data integrity and usability

---

## 🔍 Data Quality Checks Implemented

* Duplicate record detection
* Null value analysis across columns
* Validation of numeric fields (no negative or invalid values)
* Date validation (no invalid or future dates)
* Email format validation
* Consistency checks for categorical fields
* Verification of calculated metrics

---

## 🎯 Key Learnings

* Real-world data is often messy and requires flexible ingestion strategies
* Strict loading methods like BULK INSERT can fail with unclean data
* Separating raw and clean layers is critical in data engineering
* Data cleaning is as important as data ingestion
* Validation is essential to ensure data reliability

---

## 🚀 Outcome

* Successfully built an end-to-end SQL data cleaning pipeline
* Converted raw, inconsistent data into a structured analytical dataset
* Implemented real-world data engineering practices
* Created a reusable workflow for similar datasets

---

## 📊 Use Cases

The cleaned dataset can now be used for:

* Sales performance analysis
* Customer behavior insights
* Revenue trend analysis
* Business intelligence dashboards
* Reporting and decision-making



---

