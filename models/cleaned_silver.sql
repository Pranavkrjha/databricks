-- Materialize the result as a table
{{ config(materialized='table') }}

-- Data ingestion from the bronze layer
WITH source_data AS (
  SELECT * FROM {{ ref('bronze_retail') }}
),

cleaned_data AS (
  SELECT
    Invoice_Num,
    StockCode,
    Description,
    CAST(Quantity AS INT) AS Quantity,
    TO_TIMESTAMP(InvoiceDate, 'dd-MM-yyyy HH:mm') AS InvoiceDate,
    CAST(Price AS DECIMAL(10, 2)) AS Price,
    CAST(Customer_ID AS INT) AS Customer_ID,
    TRIM(Country) AS Country
  FROM source_data
  WHERE Invoice_Num IS NOT NULL
    AND Quantity > 0
    AND Price > 0
),

enriched_data AS (
  SELECT
    *,
    CAST(Quantity * Price AS DECIMAL(10, 2)) AS TotalAmount,
    DATE(InvoiceDate) AS InvoiceDay,
    YEAR(InvoiceDate) AS InvoiceYear,
    MONTH(InvoiceDate) AS InvoiceMonth
  FROM cleaned_data
)

SELECT * FROM enriched_data;