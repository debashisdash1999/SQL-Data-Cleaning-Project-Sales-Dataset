# SQL Data Cleaning Project – Sales Dataset

## Project Overview

This project demonstrates a complete data cleaning and preparation workflow using SQL Server (SSMS). A raw sales dataset in CSV format (1,004 rows, 13 columns) was ingested, profiled, cleaned, standardized, and transformed into a structured table ready for analysis and reporting.

The objective was to simulate a real-world data engineering scenario where incoming data is messy, inconsistent, and not directly usable for business insights.

---

## Challenges Faced During Data Ingestion

While attempting to load the CSV file using `BULK INSERT`, several issues were encountered:

- Data type mismatches between CSV values and the table schema
- Errors caused by invalid numeric values and inconsistent formats
- Problems with multiline text fields, particularly `customer_address`
- Unexpected end-of-file errors caused by broken row structures
- Inconsistent delimiters and quoted text leading to parsing failures

These issues made `BULK INSERT` unreliable for this dataset because it expects well-structured, clean input data before any cleaning has taken place.

---

## Why the Import Wizard Was Used Instead

To overcome these ingestion challenges, the **SQL Server Import Flat File Wizard** was used.

Reasons for choosing the wizard:

- Handles messy CSV structures more gracefully than `BULK INSERT`
- Automatically detects column boundaries and data patterns
- Supports quoted text and multiline fields without breaking rows
- Allows manual control over column datatypes before import
- Imports everything as `NVARCHAR` by default, which prevents schema-level failures during ingestion

This approach ensured that all raw data was successfully loaded without errors, forming a reliable foundation for the cleaning steps that followed.

---

## Data Issues Found in the Raw Dataset

Before writing a single query, the dataset was profiled to understand what needed fixing. The following issues were identified:

| Issue | Details |
|---|---|
| Duplicate rows | transaction_id 1001, 1004, 1030 appeared as exact duplicates |
| Negative quantities | Many rows had qty like -1, -3, -5 — invalid for a sales record |
| Negative total_amount | Derived from negative quantities, also invalid |
| Inconsistent payment methods | 'CC', 'creditcard', 'credit', 'Credit Card' all refer to the same method |
| Invalid email addresses | Values like `brownbenjamin`, `tammydaniels` — no @ or domain |
| Missing category | Blank category on multiple rows |
| Missing payment_method | Blank on several rows |
| Missing delivery_status | Blank on several rows |
| Missing total_amount | Populated where price × quantity was available |
| Missing price or quantity | Some rows had no price or no quantity |
| Invalid dates | `2024-02-30` appears multiple times — Feb 30 does not exist |
| Two date formats | Most rows use `dd/MM/yyyy`; some use `yyyy-MM-dd` |
| Zero quantity rows | Rows with qty = 0, likely cancelled orders — flagged for review |
| total_amount inconsistency | Some rows where stored total ≠ price × quantity |

---

## Data Processing Pipeline

### Step 0 — Data Profiling (Before Touching Anything)

Four audit queries were run first to establish a "before" baseline:

- **Row & uniqueness count** — total rows vs distinct transaction IDs to immediately confirm whether duplicates exist
- **Column-level null & blank count** — a single query showing how many missing values exist per column across the entire table
- **Payment method distribution** — all distinct values present, exposing inconsistent spellings before writing the standardization logic
- **Category distribution** — all distinct categories, exposing blank values

Profiling before cleaning is important because it tells you exactly what you're fixing and gives you a baseline to validate against once cleaning is done.

---

### Step 1 — Trim Whitespace from Text Columns

Applied `LTRIM(RTRIM())` to `customer_name`, `email`, `category`, `payment_method`, `delivery_status`, and `customer_address`.

Leading and trailing spaces introduced during CSV export are invisible but cause GROUP BY, joins, and comparisons to silently fail. Trimming is always the first step before any comparison or standardization logic.

---

### Step 2 — Remove Duplicate Rows

Used `ROW_NUMBER()` with `PARTITION BY transaction_id, customer_id, purchase_date, product_id` inside a CTE, then deleted all rows where `rn > 1`.

Partitioning on multiple columns (not just `transaction_id`) ensures only true exact duplicates are removed — not different transactions that happen to share an ID, which would indicate a different kind of data integrity problem.

SQL Server supports deleting directly from a CTE that uses `ROW_NUMBER()`, making this a clean one-step operation. A follow-up SELECT confirms 0 duplicates remain.

---

### Step 3 — Audit Invalid Numeric Data

Two read-only `SELECT` queries using `TRY_CAST` to identify rows where `quantity` or `price` cannot be converted to a valid number.

`TRY_CAST` returns `NULL` instead of throwing an error when conversion fails, making it safe to use as a filter. This step doesn't change any data — it gives you a list of rows to investigate before the actual fixes run.

---

### Step 4 — Handle Negative Quantities, Prices, and Total Amounts

Set `quantity`, `price`, and `total_amount` to `NULL` wherever they were negative.

A sales transaction cannot have a negative quantity or price — these are data entry errors. Nulling them (rather than deleting the row) preserves the rest of the record, which may still have valid customer, date, and product data. The `total_amount` is also nulled because it was derived from the bad quantity and will be recalculated in Step 9.

The WHERE clause uses `ISNULL(TRY_CAST(quantity AS INT), -1) < 0` — the `ISNULL` wrapping maps any non-numeric conversion failure to -1, which ensures non-numeric junk values also get caught and nulled rather than silently passing through.

---

### Step 5 — Fill NULL / Blank Categorical Values

Replaced blank or NULL values in `category`, `payment_method`, and `delivery_status` with `'Unknown'`.

Deleting rows with missing categoricals would remove otherwise valid transaction data. Using `'Unknown'` keeps the row in the dataset while making the gap explicit — downstream analysts can filter or handle these as needed. Both `IS NULL` and `= ''` are checked because both can exist depending on how the CSV was imported.

---

### Step 6 — Standardize Payment Method Values

Used a `CASE` statement with `LOWER()` to map all variants to a consistent label:

| Raw value(s) | Standardized to |
|---|---|
| `CC`, `creditcard`, `credit card`, `credit` | `Credit Card` |
| `debitcard`, `debit card` | `Debit Card` |
| `bank transfer` | `Bank Transfer` |
| `paypal` | `PayPal` |

Without this, a GROUP BY on `payment_method` would return 5-6 separate rows for Credit Card variants alone, making any revenue or transaction breakdown by payment type completely wrong. The `ELSE` clause preserves any values not in the list exactly as-is. A post-update `SELECT DISTINCT` confirms only clean values remain.

---

### Step 7 — Validate and Nullify Invalid Emails

Set `email` to `NULL` for any value that fails the pattern `'%_@_%._%'` or contains a space.

The pattern checks that there is at least one character before `@`, at least one between `@` and `.`, and at least one after the final `.` — catching obvious invalids like `brownbenjamin` or `tammydaniels`. Email is not critical for transaction analysis (the `customer_id` still links the record), so nulling is the right call over deleting.

---

### Step 8 — Date Validation and Cleanup

The dataset has two date formats: `dd/MM/yyyy` (majority of rows) and `yyyy-MM-dd` (a smaller number of rows). `TRY_CONVERT` with style 103 handles the first format and style 120 handles the second.

A SELECT query flags rows where neither format can be parsed — these are genuinely invalid dates like `2024-02-30` (February 30 does not exist). These rows are then updated to `NULL` so they don't cause cast errors when the clean table is created in Step 11. Invalid dates cannot be inferred from context, so they are left NULL rather than guessed.

---

### Step 9 — Recalculate Missing Total Amounts

For rows where `total_amount` is NULL or blank but `price` and `quantity` are both valid positive numbers, `total_amount` is recalculated as `price × quantity`.

Only rows where both inputs are confirmed valid and positive are filled in — if either is missing, `total_amount` stays NULL. This is intentional: it's better to honestly represent missing data than to calculate from incomplete inputs.

---

### Step 10 — Total Amount Consistency Audit

A SELECT query flags rows where the stored `total_amount` doesn't match `price × quantity`, using a tolerance of `0.01` to account for floating-point rounding.

These discrepancies may reflect discounts applied at the point of sale, data entry errors, or system-generated values. They are flagged for manual review rather than automatically overwritten, since we can't know which value is correct without business context.

---

### Step 11 — Create Clean Table with Correct Data Types

All cleaned data is written to a new table `dbo.sales_clean` using `SELECT INTO`, with every column cast to its correct data type using `TRY_CAST` and `TRY_CONVERT`.

A safety `DROP TABLE IF EXISTS` runs first so the script can be re-executed without errors. The original `sales` table is left untouched as an audit trail. Dates are handled with `COALESCE(TRY_CONVERT(DATE, ..., 103), TRY_CONVERT(DATE, ..., 120))` to correctly handle both formats in a single expression.

Any value that still can't be cast after all the cleaning becomes `NULL` — this is safe and expected. Better a NULL than a runtime failure.

---

### Step 12 — Add Primary Key Constraint

`ALTER TABLE dbo.sales_clean ADD CONSTRAINT PK_sales_clean PRIMARY KEY (transaction_id)`

This is the final structural guarantee. If Step 2 successfully deduplicated all rows, this constraint applies cleanly. If it fails, it means duplicates still exist and Step 2 needs to be revisited.

---

### Steps 13–14 — Final Validation Checks

Five targeted checks run against `dbo.sales_clean`, each of which should return 0 rows if cleaning was successful:

- Duplicate `transaction_id` values
- NULL in `customer_id` or `purchase_date` (critical columns)
- Negative values in `quantity`, `price`, or `total_amount`
- `total_amount` inconsistency vs `price × quantity` (with 0.01 tolerance)
- Invalid email format

A final single-row summary query reports null counts across every column in the clean table — this is the documented "after" state to compare against the Step 0 baseline.

---

### Step 15 — Distribution Checks (Post-Clean)

GROUP BY counts on `category`, `payment_method`, and `delivery_status` confirm:

- Only valid category names appear, plus `'Unknown'` where category was blank
- Exactly the 5 expected payment method values: `Credit Card`, `Debit Card`, `Bank Transfer`, `PayPal`, `Unknown`
- All delivery statuses are clean

Any unexpected value appearing here means a standardization step missed a variant.

---

## Key Learnings

- Real-world data is messy and requires a flexible ingestion strategy — strict methods like `BULK INSERT` fail before cleaning can even begin
- Profiling before cleaning is not optional — you need to know what you're fixing
- Separating raw and clean tables is essential for traceability and re-runnability
- `TRY_CAST` and `TRY_CONVERT` are safer than `CAST` and `CONVERT` when data quality is unknown
- Nulling bad values is often better than deleting rows — most records have partial valid data
- Validation queries at the end are as important as the cleaning queries themselves

---

## Outcome

- Built a complete, re-runnable SQL data cleaning pipeline in SSMS
- Converted raw, inconsistent CSV data into a structured analytical table
- Applied real-world data engineering practices: profiling, cleaning, validation, and separation of raw and clean layers
- Produced a clean dataset ready for Power BI dashboards, sales analysis, and revenue reporting

---

## Cleaned Dataset — Available Use Cases

- Sales performance analysis by category, payment method, and delivery status
- Customer behavior and transaction pattern insights
- Revenue trend analysis over time
- Input for Power BI dashboards and BI reporting
- Foundation for further transformation into a dimensional model (Star Schema)
