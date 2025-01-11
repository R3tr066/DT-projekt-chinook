-- GRAF 1
SELECT 
    d.dayOfWeekAsString AS "Day of Week",
    SUM(i.dim_total) AS "Total Sales"
FROM dimDate d
JOIN dimInvoice i 
  ON d.date = i.invoiceDate
GROUP BY d.dayOfWeekAsString
ORDER BY 
    CASE d.dayOfWeekAsString
        WHEN 'Pondelok' THEN 1
        WHEN 'Utorok' THEN 2
        WHEN 'Streda' THEN 3
        WHEN 'Štvrtok' THEN 4
        WHEN 'Piatok' THEN 5
        WHEN 'Sobota' THEN 6
        WHEN 'Nedeľa' THEN 7
    END;

-- GRAF 2
SELECT 
    CONCAT(c.dim_firstName, ' ', c.dim_lastName) AS "Customer Name",
    SUM(i.dim_total) AS "Total Sales"
FROM dimCustomer c
JOIN dimInvoice i 
  ON c.dim_customerID = i.dim_customerID
GROUP BY "Customer Name"
ORDER BY "Total Sales" DESC
LIMIT 5;

-- GRAF 3
SELECT 
    t.dim_genre AS "Genre",
    SUM(il.dim_unitPrice * il.dim_quantity) AS "Total Sales"
FROM dimTrack t
JOIN InvoiceLineFact il 
  ON t.dim_trackID = il.trackID
GROUP BY t.dim_genre
ORDER BY "Total Sales" DESC;

-- GRAF 4
SELECT 
    CONCAT(e.dim_firstName, ' ', e.dim_lastName) AS "Employee Name",
    COUNT(DISTINCT c.dim_customerID) AS "Customer Count"
FROM dimEmployee e
JOIN dimCustomer c 
  ON e.dim_employeeID = c.dim_supportRepID
GROUP BY "Employee Name"
ORDER BY "Customer Count" DESC;

-- GRAF 5
SELECT 
    d.year AS "Year",
    d.month AS "Month",
    SUM(i.dim_total) AS "Total Sales"
FROM dimDate d
JOIN dimInvoice i 
  ON d.date = i.invoiceDate
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
