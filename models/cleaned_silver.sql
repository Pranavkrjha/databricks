WITH silver_retail AS (
    SELECT * FROM {{ ref('bronze_retail') }}  -- Reference the bronze table in dbt
),
silver_data AS (
  SELECT
    Invoice, -- Rename the Invoice column to Invoice_Num for clarity
    StockCode,
    Description,
    COALESCE(Quantity, 0) AS Quantity, -- Handle missing quantity values
    CASE WHEN Price <= 0 THEN NULL ELSE Price END AS Price, -- Handle invalid prices
    TO_TIMESTAMP(InvoiceDate, 'dd-MM-yyyy HH:mm') AS InvoiceDate,
    CAST(Customer_ID AS INT) AS Customer_ID,
    TRIM(Country) AS Country
  FROM silver_retail
  WHERE Invoice IS NOT NULL
)

SELECT * FROM silver_data;