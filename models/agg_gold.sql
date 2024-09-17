WITH cleaned_data AS (
    SELECT * FROM {{ ref('cleaned_silver') }}
)
    SELECT
        Invoice,
        StockCode,
        Description,
        SUM(Quantity) AS TotalQuantity,
        SUM(Price * Quantity) AS TotalRevenue,
        MAX(InvoiceDate) AS LatestInvoiceDate,
        MIN(InvoiceDate) AS EarliestInvoiceDate,
        COUNT(*) AS TotalOrders,
        AVG(Quantity) AS AverageOrderQuantity,
        AVG(Price * Quantity) AS AverageOrderValue
    FROM cleaned_data
    GROUP BY Invoice, StockCode, Description

