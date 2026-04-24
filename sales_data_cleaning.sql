/* =============================================================================
   PROJECT  : Sales Data Cleaning — SQL Server (SSMS)
   DATASET  : sales (imported via SQL Server Import Wizard)
   PURPOSE  : Clean and standardize raw sales data for analytical use.
              The wizard imports every column as NVARCHAR by default, so all
              type casting is deferred to the final step once the data is clean.
   RUN ORDER: Execute each step in sequence — later steps depend on earlier ones.
   ============================================================================= */


/* =============================================================================
   STEP 0A — BASIC ROW & UNIQUENESS PROFILING
   -----------------------------------------------------------------------------
   Before touching any data, understand what you're working with.
   This gives you a baseline to compare against after cleaning.
   - total_rows       : how many records are in the raw table
   - unique_transactions : how many distinct transaction_ids exist
   If these two numbers differ, you already know duplicates are present.
   ============================================================================= */

SELECT
    COUNT(*)                    AS total_rows,
    COUNT(DISTINCT transaction_id) AS unique_transactions
FROM sales;


/* =============================================================================
   STEP 0B — COLUMN-LEVEL NULL & BLANK PROFILING
   -----------------------------------------------------------------------------
   This tells you exactly which columns have missing or empty values and how bad
   the problem is. Run this before cleaning so you have a documented "before" 
   state. Useful for reporting data quality metrics in a project context.
   ============================================================================= */

SELECT
    SUM(CASE WHEN customer_name   IS NULL OR LTRIM(RTRIM(customer_name))   = '' THEN 1 ELSE 0 END) AS missing_customer_name,
    SUM(CASE WHEN email           IS NULL OR LTRIM(RTRIM(email))           = '' THEN 1 ELSE 0 END) AS missing_email,
    SUM(CASE WHEN category        IS NULL OR LTRIM(RTRIM(category))        = '' THEN 1 ELSE 0 END) AS missing_category,
    SUM(CASE WHEN payment_method  IS NULL OR LTRIM(RTRIM(payment_method))  = '' THEN 1 ELSE 0 END) AS missing_payment_method,
    SUM(CASE WHEN delivery_status IS NULL OR LTRIM(RTRIM(delivery_status)) = '' THEN 1 ELSE 0 END) AS missing_delivery_status,
    SUM(CASE WHEN price           IS NULL OR LTRIM(RTRIM(price))           = '' THEN 1 ELSE 0 END) AS missing_price,
    SUM(CASE WHEN quantity        IS NULL OR LTRIM(RTRIM(quantity))        = '' THEN 1 ELSE 0 END) AS missing_quantity,
    SUM(CASE WHEN total_amount    IS NULL OR LTRIM(RTRIM(total_amount))    = '' THEN 1 ELSE 0 END) AS missing_total_amount,
    SUM(CASE WHEN customer_address IS NULL OR LTRIM(RTRIM(customer_address))= '' THEN 1 ELSE 0 END) AS missing_customer_address
FROM sales;


/* =============================================================================
   STEP 0C — PAYMENT METHOD DISTRIBUTION AUDIT
   -----------------------------------------------------------------------------
   The raw data has many inconsistent spellings for the same payment method:
   'CC', 'creditcard', 'credit', 'Credit Card' all refer to the same thing.
   This query shows every distinct value that exists so you can identify all
   variants before writing the standardization logic in Step 6.
   ============================================================================= */

SELECT
    payment_method,
    COUNT(*) AS occurrences
FROM sales
GROUP BY payment_method
ORDER BY occurrences DESC;


/* =============================================================================
   STEP 0D — CATEGORY DISTRIBUTION AUDIT
   -----------------------------------------------------------------------------
   Similarly, check what distinct category values exist. Blank categories will
   appear here as NULL or empty string — confirms the scope of Step 5.
   ============================================================================= */

SELECT
    category,
    COUNT(*) AS occurrences
FROM sales
GROUP BY category
ORDER BY occurrences DESC;


/* =============================================================================
   STEP 1 — TRIM WHITESPACE FROM TEXT COLUMNS
   -----------------------------------------------------------------------------
   When data is imported from CSV or Excel, leading/trailing spaces often sneak
   in — especially in columns like email or category. These hidden spaces cause
   lookups, joins, and GROUP BY to fail silently.
   LTRIM removes leading spaces, RTRIM removes trailing ones.
   Always trim before any comparison or standardization logic.
   ============================================================================= */

UPDATE sales
SET
    customer_name    = LTRIM(RTRIM(customer_name)),
    email            = LTRIM(RTRIM(email)),
    category         = LTRIM(RTRIM(category)),
    payment_method   = LTRIM(RTRIM(payment_method)),
    delivery_status  = LTRIM(RTRIM(delivery_status)),
    customer_address = LTRIM(RTRIM(customer_address));


/* =============================================================================
   STEP 2 — REMOVE DUPLICATE ROWS
   -----------------------------------------------------------------------------
   The dataset has exact duplicate rows (e.g., transaction_id 1001, 1004, 1030
   appear more than once with identical data across all columns).
   
   ROW_NUMBER() assigns a sequential number to each row within each group of
   the same transaction_id. The first occurrence gets rn = 1, duplicates get
   rn > 1. We then delete anything where rn > 1.

   SQL Server supports deleting directly from a CTE that uses ROW_NUMBER(),
   which makes this a clean one-step operation.

   PARTITION BY transaction_id, customer_id, purchase_date, product_id:
   Using multiple columns in the partition ensures we only delete true
   duplicates — not different transactions that happen to share an ID (which
   would indicate a data integrity problem worth investigating separately).
   ============================================================================= */

WITH duplicates_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY transaction_id, customer_id, purchase_date, product_id
               ORDER BY transaction_id
           ) AS rn
    FROM sales
)
DELETE FROM duplicates_cte
WHERE rn > 1;

-- Verify: this should return 0 rows after the delete above
SELECT transaction_id, COUNT(*) AS occurrences
FROM sales
GROUP BY transaction_id
HAVING COUNT(*) > 1;


/* =============================================================================
   STEP 3 — AUDIT INVALID NUMERIC DATA (READ-ONLY CHECK)
   -----------------------------------------------------------------------------
   TRY_CAST attempts to convert the value to the target type.
   If it fails (e.g., the value is 'abc' or empty), it returns NULL instead
   of throwing an error.
   
   This is a pure audit query — it doesn't change any data.
   Review the results before proceeding. These rows may need manual review
   or will be handled by the NULL-handling steps below.
   ============================================================================= */

-- Rows where quantity cannot be cast to a valid integer
SELECT *
FROM sales
WHERE TRY_CAST(quantity AS INT) IS NULL
  AND quantity IS NOT NULL
  AND LTRIM(RTRIM(quantity)) <> '';

-- Rows where price cannot be cast to a valid decimal
SELECT *
FROM sales
WHERE TRY_CAST(price AS FLOAT) IS NULL
  AND price IS NOT NULL
  AND LTRIM(RTRIM(price)) <> '';


/* =============================================================================
   STEP 4 — HANDLE NEGATIVE QUANTITY AND PRICE VALUES
   -----------------------------------------------------------------------------
   The dataset contains many rows with negative quantity (e.g., -1, -3, -5).
   A sales transaction cannot have a negative quantity or price — these are
   data entry errors or system artifacts, not returns (which would be a
   separate transaction type).

   We set them to NULL rather than deleting the row, because:
   - The rest of the row (customer, date, product) is still valid
   - NULL signals "unknown / bad value" which is honest and recoverable
   - total_amount can be recalculated in Step 9 if price and qty are valid

   ISNULL(TRY_CAST(...), -1):
   - TRY_CAST returns NULL if conversion fails (non-numeric text)
   - ISNULL maps that NULL to -1, which is < 0, so those rows also get nulled
   - This prevents bad non-numeric values from slipping through the WHERE clause
   ============================================================================= */

-- Null out negative quantities
UPDATE sales
SET quantity = NULL
WHERE ISNULL(TRY_CAST(quantity AS INT), -1) < 0;

-- Null out negative prices
UPDATE sales
SET price = NULL
WHERE ISNULL(TRY_CAST(price AS FLOAT), -1) < 0;

-- Also null out total_amount where it is negative (derived from bad qty/price)
-- These will be recalculated in Step 9
UPDATE sales
SET total_amount = NULL
WHERE ISNULL(TRY_CAST(total_amount AS FLOAT), -1) < 0;


/* =============================================================================
   STEP 5A — HANDLE ZERO QUANTITY (BOUNDARY CASE AUDIT)
   -----------------------------------------------------------------------------
   Rows with quantity = 0 are ambiguous — they could be cancelled orders that
   weren't removed, or data entry errors. They won't cause numeric errors but
   they will distort aggregations like average order size and revenue totals.
   
   This is an audit query. Review the results and decide:
   - If these are clearly invalid → UPDATE quantity = NULL
   - If they represent cancelled/empty orders → leave as-is or flag them
   ============================================================================= */

SELECT *
FROM sales
WHERE TRY_CAST(quantity AS INT) = 0;

-- Optional: null out zero quantities if you decide they are invalid
-- UPDATE sales SET quantity = NULL WHERE TRY_CAST(quantity AS INT) = 0;


/* =============================================================================
   STEP 5B — REPLACE NULL / BLANK CATEGORIES WITH 'Unknown'
   -----------------------------------------------------------------------------
   Many rows have a missing category. Rather than deleting these rows (which
   would lose valid transaction data), we replace the blank with 'Unknown'.
   This keeps the row in the dataset while clearly flagging it for downstream
   analysts to handle or filter as needed.

   We check both IS NULL (true NULL from the DB) and = '' (empty string from
   CSV import) because both can exist depending on how the file was imported.
   ============================================================================= */

UPDATE sales
SET category = 'Unknown'
WHERE category IS NULL OR LTRIM(RTRIM(category)) = '';


/* =============================================================================
   STEP 5C — REPLACE NULL / BLANK PAYMENT METHODS WITH 'Unknown'
   -----------------------------------------------------------------------------
   Same logic as category. Missing payment method doesn't invalidate the
   transaction — we preserve the row and label the unknown clearly.
   ============================================================================= */

UPDATE sales
SET payment_method = 'Unknown'
WHERE payment_method IS NULL OR LTRIM(RTRIM(payment_method)) = '';


/* =============================================================================
   STEP 5D — REPLACE NULL / BLANK DELIVERY STATUS WITH 'Unknown'
   -----------------------------------------------------------------------------
   Delivery status is useful for operational reporting. Rows with no status
   are labeled 'Unknown' so they don't get silently excluded from GROUP BY
   aggregations that include delivery_status.
   ============================================================================= */

UPDATE sales
SET delivery_status = 'Unknown'
WHERE delivery_status IS NULL OR LTRIM(RTRIM(delivery_status)) = '';


/* =============================================================================
   STEP 6 — STANDARDIZE PAYMENT METHOD VALUES
   -----------------------------------------------------------------------------
   The raw data has multiple spellings for the same payment method:
   - 'CC', 'creditcard', 'credit card', 'credit' all mean 'Credit Card'
   - 'debitcard', 'debit card' mean 'Debit Card'
   
   Without standardization, a GROUP BY on payment_method would return 5-6
   rows for Credit Card variants instead of one, completely distorting reports.

   LOWER() is applied before comparison so the matching is case-insensitive.
   The ELSE clause preserves any values not in the list (e.g., 'PayPal',
   'Bank Transfer') exactly as they are — we only fix what's broken.

   This step must run AFTER Step 5C so 'Unknown' values aren't overwritten.
   ============================================================================= */

UPDATE sales
SET payment_method =
    CASE
        WHEN LOWER(LTRIM(RTRIM(payment_method))) IN (
            'cc', 'creditcard', 'credit card', 'credit'
        )                                          THEN 'Credit Card'

        WHEN LOWER(LTRIM(RTRIM(payment_method))) IN (
            'debitcard', 'debit card'
        )                                          THEN 'Debit Card'

        WHEN LOWER(LTRIM(RTRIM(payment_method))) = 'bank transfer' THEN 'Bank Transfer'

        WHEN LOWER(LTRIM(RTRIM(payment_method))) = 'paypal' THEN 'PayPal'

        ELSE payment_method
    END;

-- Verify: confirm only clean, consistent values remain
SELECT DISTINCT payment_method FROM sales ORDER BY payment_method;


/* =============================================================================
   STEP 7 — VALIDATE AND NULLIFY INVALID EMAIL ADDRESSES
   -----------------------------------------------------------------------------
   The raw data has emails like 'brownbenjamin', 'tammydaniels', 'carolinejones'
   — these are clearly invalid (no @ symbol, no domain).

   The pattern '%_@_%._%' checks that:
   - There is at least one character before '@'
   - There is at least one character between '@' and '.'
   - There is at least one character after the last '.'
   - There are no spaces in the value

   We NULL these out rather than deleting the row. Email is not critical for
   transaction analysis — the customer_id still links to the customer.

   Note: This pattern won't catch every edge case (e.g., double @ signs), but
   it handles all the invalid cases present in this dataset.
   ============================================================================= */

UPDATE sales
SET email = NULL
WHERE email IS NOT NULL
  AND LTRIM(RTRIM(email)) <> ''
  AND (
        email NOT LIKE '%_@_%._%'
        OR email LIKE '% %'
      );

-- Audit: confirm what was nulled — run this BEFORE the update to preview
SELECT email FROM sales WHERE email NOT LIKE '%_@_%._%' OR email LIKE '% %';


/* =============================================================================
   STEP 8 — VALIDATE DATE VALUES (AUDIT STEP)
   -----------------------------------------------------------------------------
   The raw data has dates in two formats:
   - dd/MM/yyyy  (e.g., '24/01/2024') — the majority of rows
   - yyyy-MM-dd  (e.g., '2024-02-30') — a small number of rows, likely system
                                         generated, and some are logically
                                         impossible (Feb 30 does not exist)

   TRY_CONVERT with style 103 handles the dd/MM/yyyy format.
   Style 120 handles the yyyy-MM-dd ISO format.

   If BOTH return NULL for a given row, the date is genuinely invalid (like
   '2024-02-30') and that row is flagged for review.

   We don't UPDATE dates here because invalid dates should go to a manual
   review queue — we can't infer the correct date from context.
   ============================================================================= */

-- Rows where neither date format can be parsed — these need manual review
SELECT
    transaction_id,
    customer_name,
    purchase_date
FROM sales
WHERE TRY_CONVERT(DATE, purchase_date, 103) IS NULL   -- not dd/MM/yyyy
  AND TRY_CONVERT(DATE, purchase_date, 120) IS NULL   -- not yyyy-MM-dd
  AND purchase_date IS NOT NULL
  AND LTRIM(RTRIM(purchase_date)) <> '';

-- Optional: NULL out confirmed invalid dates so they don't cause cast errors in Step 11
UPDATE sales
SET purchase_date = NULL
WHERE TRY_CONVERT(DATE, purchase_date, 103) IS NULL
  AND TRY_CONVERT(DATE, purchase_date, 120) IS NULL
  AND purchase_date IS NOT NULL
  AND LTRIM(RTRIM(purchase_date)) <> '';


/* =============================================================================
   STEP 9 — RECALCULATE MISSING TOTAL_AMOUNT
   -----------------------------------------------------------------------------
   Many rows have a missing or blank total_amount even though price and quantity
   are valid. We can safely derive the correct value as price * quantity.

   Conditions:
   - total_amount is NULL or blank (missing)
   - price is a valid positive number (not NULL, not non-numeric)
   - quantity is a valid positive integer (not NULL, not non-numeric)

   We only fill in what we can calculate with confidence. If either price or
   quantity is missing, we leave total_amount as NULL — better to be honest
   about missing data than to guess.
   ============================================================================= */

UPDATE sales
SET total_amount = CAST(
                      TRY_CAST(price AS FLOAT) * TRY_CAST(quantity AS INT)
                   AS VARCHAR(50))
WHERE (total_amount IS NULL OR LTRIM(RTRIM(total_amount)) = '')
  AND TRY_CAST(price    AS FLOAT) IS NOT NULL
  AND TRY_CAST(quantity AS INT)   IS NOT NULL
  AND TRY_CAST(price    AS FLOAT) > 0
  AND TRY_CAST(quantity AS INT)   > 0;


/* =============================================================================
   STEP 10 — AUDIT TOTAL_AMOUNT CONSISTENCY
   -----------------------------------------------------------------------------
   Even where total_amount is populated, it may not match price * quantity.
   This can happen due to discounts, rounding, or data entry errors.

   This is an audit query — it flags rows where total_amount doesn't match
   the calculated value. Review these manually.

   A tolerance of 0.01 is used in the WHERE clause to account for rounding
   differences in floating-point arithmetic.
   ============================================================================= */

SELECT
    transaction_id,
    customer_name,
    price,
    quantity,
    total_amount                                             AS stored_total,

    TRY_CAST(price AS FLOAT) * TRY_CAST(quantity AS INT)    AS calculated_total,

    ABS(TRY_CAST(total_amount AS FLOAT) - (TRY_CAST(price AS FLOAT) * TRY_CAST(quantity AS INT))) AS discrepancy

FROM sales
WHERE TRY_CAST(total_amount AS FLOAT) IS NOT NULL
  AND TRY_CAST(price        AS FLOAT) IS NOT NULL
  AND TRY_CAST(quantity     AS INT)   IS NOT NULL
  AND ABS(TRY_CAST(total_amount AS FLOAT) - (TRY_CAST(price AS FLOAT) * TRY_CAST(quantity AS INT))) > 0.01
ORDER BY discrepancy DESC;


/* =============================================================================
   STEP 11 — CREATE CLEAN TABLE WITH PROPER DATA TYPES
   -----------------------------------------------------------------------------
   Up to this point, all columns in [sales] are still NVARCHAR (as imported
   by the wizard). Now that the data is clean, we cast everything to its
   correct data type and write the result into a new table: [sales_clean].

   Why a new table instead of altering the original?
   - Preserves the raw data as an audit trail
   - Lets you re-run cleaning steps without starting from scratch
   - Clean and raw are cleanly separated for traceability

   TRY_CAST / TRY_CONVERT:
   - Used instead of CAST / CONVERT to prevent runtime errors
   - Any value that still can't be cast after cleaning becomes NULL
   - This is safe and expected — better NULL than a failed query

   Date handling:
   - Most dates are dd/MM/yyyy (style 103)
   - The ISO format dates (style 120) are handled as a fallback using COALESCE
   ============================================================================= */

-- Safety drop: if you re-run this script, drop the old clean table first
-- to avoid "object already exists" errors
IF OBJECT_ID('dbo.sales_clean', 'U') IS NOT NULL
    DROP TABLE dbo.sales_clean;

SELECT
    TRY_CAST(transaction_id    AS INT)          AS transaction_id,
    TRY_CAST(customer_id       AS INT)          AS customer_id,
    customer_name,
    email,
    -- Try dd/MM/yyyy first; fall back to yyyy-MM-dd for any remaining valid ISO dates
    COALESCE(
        TRY_CONVERT(DATE, purchase_date, 103),
        TRY_CONVERT(DATE, purchase_date, 120)
    )                                           AS purchase_date,
    TRY_CAST(product_id        AS INT)          AS product_id,
    category,
    TRY_CAST(price             AS DECIMAL(10,2)) AS price,
    TRY_CAST(quantity          AS INT)           AS quantity,
    TRY_CAST(total_amount      AS DECIMAL(12,2)) AS total_amount,
    payment_method,
    delivery_status,
    customer_address
INTO dbo.sales_clean
FROM sales;


/* =============================================================================
-- STEP 12A: Find out which rows have a NULL transaction_id in the clean table
-- These are rows where the original transaction_id couldn't be cast to INT */
SELECT *
FROM dbo.sales_clean
WHERE transaction_id IS NULL;

-- Mark the column as NOT NULL at the schema level
ALTER TABLE dbo.sales_clean
ALTER COLUMN transaction_id INT NOT NULL;

-- Now the PK will apply cleanly
ALTER TABLE dbo.sales_clean
ADD CONSTRAINT PK_sales_clean PRIMARY KEY (transaction_id);


/* =============================================================================
   STEP 13A — FINAL CHECK: DUPLICATE TRANSACTION IDs
   -----------------------------------------------------------------------------
   Should return 0 rows. If any rows come back, the primary key above would
   have already failed — but this is a useful standalone check.
   ============================================================================= */

SELECT transaction_id, COUNT(*) AS occurrences
FROM dbo.sales_clean
GROUP BY transaction_id
HAVING COUNT(*) > 1;


/* =============================================================================
   STEP 13B — FINAL CHECK: NULLS IN CRITICAL COLUMNS
   -----------------------------------------------------------------------------
   customer_id and purchase_date are the minimum required for any meaningful
   transaction analysis. Flag any rows where these are NULL so analysts know
   to treat these records with caution.
   ============================================================================= */

SELECT *
FROM dbo.sales_clean
WHERE customer_id   IS NULL
   OR purchase_date IS NULL;


/* =============================================================================
   STEP 13C — FINAL CHECK: NEGATIVE VALUES IN NUMERIC COLUMNS
   -----------------------------------------------------------------------------
   After Step 4, there should be no negative quantity, price, or total_amount
   in the clean table. This confirms the cleaning worked.
   Should return 0 rows.
   ============================================================================= */

SELECT *
FROM dbo.sales_clean
WHERE quantity     < 0
   OR price        < 0
   OR total_amount < 0;


/* =============================================================================
   STEP 13D — FINAL CHECK: TOTAL_AMOUNT CONSISTENCY
   -----------------------------------------------------------------------------
   Flags any remaining rows where total_amount doesn't match price * quantity.
   A tolerance of 0.01 is used to account for rounding.
   Rows that appear here likely have discounts or require manual review.
   ============================================================================= */

SELECT
    transaction_id,
    price,
    quantity,
    total_amount,
    ROUND(price * quantity, 2)                         AS expected_total,
    ABS(total_amount - ROUND(price * quantity, 2))     AS discrepancy
FROM dbo.sales_clean
WHERE total_amount IS NOT NULL
  AND price        IS NOT NULL
  AND quantity     IS NOT NULL
  AND ABS(total_amount - ROUND(price * quantity, 2)) > 0.01
ORDER BY discrepancy DESC;


/* =============================================================================
   STEP 13E — FINAL CHECK: INVALID EMAIL FORMAT
   -----------------------------------------------------------------------------
   Confirms that no invalid emails slipped through Step 7.
   Should return 0 rows.
   ============================================================================= */

SELECT transaction_id, email
FROM dbo.sales_clean
WHERE email IS NOT NULL
  AND (
        email NOT LIKE '%_@_%._%'
        OR email LIKE '% %'
      );


/* =============================================================================
   STEP 14 — FINAL DATA QUALITY SUMMARY REPORT
   -----------------------------------------------------------------------------
   This gives you a single-row summary of the cleaned dataset — useful for
   documentation, project reporting, or presenting the "after" state compared
   to the Step 0 "before" baseline.
   ============================================================================= */

SELECT
    COUNT(*)                                                         AS total_rows,
    COUNT(DISTINCT transaction_id)                                   AS unique_transactions,
    SUM(CASE WHEN transaction_id  IS NULL THEN 1 ELSE 0 END)        AS null_transaction_ids,
    SUM(CASE WHEN customer_id     IS NULL THEN 1 ELSE 0 END)        AS null_customer_ids,
    SUM(CASE WHEN customer_name   IS NULL THEN 1 ELSE 0 END)        AS null_customer_names,
    SUM(CASE WHEN email           IS NULL THEN 1 ELSE 0 END)        AS null_or_invalid_emails,
    SUM(CASE WHEN purchase_date   IS NULL THEN 1 ELSE 0 END)        AS null_purchase_dates,
    SUM(CASE WHEN price           IS NULL THEN 1 ELSE 0 END)        AS null_prices,
    SUM(CASE WHEN quantity        IS NULL THEN 1 ELSE 0 END)        AS null_quantities,
    SUM(CASE WHEN total_amount    IS NULL THEN 1 ELSE 0 END)        AS null_total_amounts,
    SUM(CASE WHEN category        = 'Unknown' THEN 1 ELSE 0 END)    AS unknown_categories,
    SUM(CASE WHEN payment_method  = 'Unknown' THEN 1 ELSE 0 END)    AS unknown_payment_methods,
    SUM(CASE WHEN delivery_status = 'Unknown' THEN 1 ELSE 0 END)    AS unknown_delivery_status
FROM dbo.sales_clean;


/* =============================================================================
   STEP 15 — CATEGORY AND PAYMENT METHOD DISTRIBUTION (POST-CLEAN)
   -----------------------------------------------------------------------------
   Compare these counts against the Step 0 audit queries.
   - Categories should now show only clean, known values + 'Unknown'
   - Payment methods should now show exactly 5 values:
     Credit Card, Debit Card, Bank Transfer, PayPal, Unknown
   Any extra values appearing here means Step 5 or Step 6 missed a variant.
   ============================================================================= */

-- Category distribution
SELECT category, COUNT(*) AS row_count
FROM dbo.sales_clean
GROUP BY category
ORDER BY row_count DESC;

-- Payment method distribution
SELECT payment_method, COUNT(*) AS row_count
FROM dbo.sales_clean
GROUP BY payment_method
ORDER BY row_count DESC;

-- Delivery status distribution
SELECT delivery_status, COUNT(*) AS row_count
FROM dbo.sales_clean
GROUP BY delivery_status
ORDER BY row_count DESC;
