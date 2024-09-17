SELECT
    Invoice AS Invoice_Num, -- Rename the Invoice column to Invoice_Num for clarity
    StockCode,
    Description,
    Quantity,
    InvoiceDate,
    Price,
    Customer_ID,
    Country
FROM newworkspace.default.online_retail;