WITH silver_data AS (
  SELECT * FROM {{ ref('cleaned_silver') }} --data ingestion from silver table
)

SELECT
  InvoiceDay,
  COUNT(DISTINCT Invoice_Num) AS TotalNumberOfInvoices, 
  COUNT(DISTINCT Customer_ID) AS TotalNumberOfCustomers,
  SUM(Quantity) AS TotalQuantity,
  SUM(TotalAmount) AS Total_Sales_Revenue,
  AVG(TotalAmount) AS AverageValueOfOrder,
  COUNT(DISTINCT StockCode) AS NumberOfDifferentItems
FROM silver_data
GROUP BY InvoiceDay
ORDER BY InvoiceDay;

{{ config(materialized='table') }}

