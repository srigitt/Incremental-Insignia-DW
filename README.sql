--SQL Scripts for Table Creation
-- Use the database
USE insignia_database;
-- Lineage Table
CREATE TABLE Lineage (
 Lineage_Id BIGINT IDENTITY(1,1) PRIMARY KEY,
 Source_System VARCHAR(100),
 Load_Stat_Datetime DATETIME,
 Load_EndDatetime DATETIME,
 Rows_at_Source INT,
 Rows_at_destination_Fact INT,
 Load_Status BIT
);
-- Date Dimension
CREATE TABLE DimDate (
 DateKey INT PRIMARY KEY,
 Date DATE,
 Day_Number INT,
 Month_Name VARCHAR(20),
 Short_Month CHAR(3),
 Calendar_Month_Number INT,
 Calendar_Year INT,
 Fiscal_Month_Number INT,
 Fiscal_Year INT,
 Week_Number INT
);
-- Customer Dimension
CREATE TABLE DimCustomer (
 CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
 CustomerId INT,
 CustomerName VARCHAR(100),
 CustomerGender VARCHAR(10),
 CustomerDOB DATE,
 CustomerAddress VARCHAR(200),
 CustomerEmail VARCHAR(100),
 CustomerContactNumber VARCHAR(15),
 Lineage_Id BIGINT
);
-- Employee Dimension
CREATE TABLE DimEmployee (
 EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
 EmployeeId INT,
 EmployeeName VARCHAR(100),
 EmployeeDepartment VARCHAR(100),
 Lineage_Id BIGINT
);
-- Geography Dimension
CREATE TABLE DimGeography (
 GeographyKey INT IDENTITY(1,1) PRIMARY KEY,
 City_ID INT,
 City VARCHAR(100),
 State_Province VARCHAR(100),
 Country VARCHAR(100),
 Continent VARCHAR(100),
 Sales_Territory VARCHAR(100),
 Region VARCHAR(100),
 Subregion VARCHAR(100),
 Latest_Recorded_Population INT,
 Lineage_Id BIGINT
);
-- Product Dimension
CREATE TABLE DimProduct (
 ProductKey INT IDENTITY(1,1) PRIMARY KEY,
 Description VARCHAR(200),
 Lineage_Id BIGINT
);
-- Sales Fact Table
CREATE TABLE FactSales (
 SalesKey INT IDENTITY(1,1) PRIMARY KEY,
 InvoiceId INT,
 ProductKey INT,
 CustomerKey INT,
 EmployeeKey INT,
 GeographyKey INT,
 DateKey INT,
 Quantity INT,
 Unit_Price FLOAT,
 Tax_Rate FLOAT,
 Total_Excluding_Tax FLOAT,
 Tax_Amount FLOAT,
 Profit FLOAT,
 Total_Including_Tax FLOAT,
 Lineage_Id BIGINT
);




--ETL Scripts
--Step 1: Create a copy of the Insignia_staging table
USE insignia_database;
-- Create the staging copy table
IF OBJECT_ID ('Insignia_staging_copy', 'U') IS NOT NULL
 DROP TABLE Insignia_staging_copy;
SELECT * INTO Insignia_staging_copy
FROM Insignia_staging;
Step 2: Load data from Insignia_staging_copy into Dimensions
Load Date Dimension
-- Assuming the staging table has a Date column (example: SalesDate)
-- Generate the Date Dimension data
INSERT INTO DimDate (DateKey, Date, Day_Number, Month_Name, Short_Month,
Calendar_Month_Number, Calendar_Year, Fiscal_Month_Number, Fiscal_Year,
Week_Number)
SELECT
 CONVERT (INT, FORMAT (SalesDate, 'yyyyMMdd')) AS DateKey,
 SalesDate AS Date,
 DAY(SalesDate) AS Day_Number,
 DATENAME (MONTH, SalesDate) AS Month_Name,
 LEFT(DATENAME(MONTH, SalesDate), 3) AS Short_Month,
 MONTH(SalesDate) AS Calendar_Month_Number,
 YEAR(SalesDate) AS Calendar_Year,
 -- Adjust fiscal year calculations based on July start
 CASE WHEN MONTH(SalesDate) >= 7 THEN MONTH(SalesDate) - 6 ELSE
MONTH(SalesDate) + 6 END AS Fiscal_Month_Number,
 CASE WHEN MONTH(SalesDate) >= 7 THEN YEAR(SalesDate) ELSE
YEAR(SalesDate) - 1 END AS Fiscal_Year,
 DATEPART(WEEK, SalesDate) AS Week_Number
FROM Insignia_staging_copy
GROUP BY SalesDate;
--Load Customer Dimension (SCD Type 2)
-- Insert new customers or changed customers into DimCustomer
INSERT INTO DimCustomer (CustomerId, CustomerName, CustomerGender,
CustomerDOB, CustomerAddress, CustomerEmail, CustomerContactNumber,
Lineage_Id)
SELECT
 CustomerId, CustomerName, CustomerGender, CustomerDOB, CustomerAddress,
CustomerEmail, CustomerContactNumber, @Lineage_Id
FROM Insignia_staging_copy sc
LEFT JOIN DimCustomer dc ON sc.CustomerId = dc.CustomerId
WHERE dc.CustomerId IS NULL
 OR (sc.CustomerName != dc.CustomerName OR sc.CustomerGender !=
dc.CustomerGender OR sc.CustomerDOB != dc.CustomerDOB OR
 sc.CustomerAddress != dc.CustomerAddress OR sc.CustomerEmail !=
dc.CustomerEmail OR sc.CustomerContactNumber != dc.CustomerContactNumber);
--Load Employee Dimension (SCD Type 2)
-- Insert new or changed employees into DimEmployee
INSERT INTO DimEmployee (EmployeeId, EmployeeName, EmployeeDepartment,
Lineage_Id)
SELECT
 employee_Id, employee_name, Employee_department, @Lineage_Id
FROM Insignia_staging_copy sc
LEFT JOIN DimEmployee de ON sc.employee_Id = de.EmployeeId
WHERE de.EmployeeId IS NULL
 OR (sc.employee_name != de.EmployeeName OR sc.Employee_department !=
de.EmployeeDepartment);
--Load Geography Dimension (SCD Type 3)
USE insignia_database;
-- Insert new geography records
INSERT INTO DimGeography (City_ID, City, State_Province, Country,
Continent, Sales_Territory, Region, Subregion, Latest_Recorded_Population,
Previous_Population, Lineage_Id)
SELECT
 sc.City_ID,
 sc.City,
 sc.State_Province,
 sc.Country,
 sc.Continent,
 sc.Sales_Territory,
 sc.Region,
 sc.Subregion,
 sc.Latest_Recorded_Population,
 NULL AS Previous_Population,
 NEWID()
FROM Insignia_staging_copy sc
LEFT JOIN DimGeography dg ON sc.City_ID = dg.City_ID
WHERE dg.City_ID IS NULL;
-- Update existing geography records with new population data
UPDATE dg
SET
 dg.Previous_Population = dg.Latest_Recorded_Population,
 dg.Latest_Recorded_Population = sc.Latest_Recorded_Population,
 dg.Lineage_Id = NEWID()
FROM DimGeography dg
JOIN Insignia_staging_copy sc ON dg.City_ID = sc.City_ID
WHERE dg.Latest_Recorded_Population != sc.Latest_Recorded_Population;
Product Dimension (SCD Type 1)
USE insignia_database;
-- Insert new products
INSERT INTO DimProduct (ProductKey, Description, Category, Price,
Lineage_Id)
SELECT
 sc.ProductKey,
 sc.Description,
 sc.Category,
 sc.Price,
 NEWID ()
FROM Insignia_staging_copy sc
LEFT JOIN DimProduct dp ON sc.Description = dp.Description
WHERE dp.Description IS NULL;
-- Update existing products
UPDATE dp
SET
 dp.Description = sc.Description,
 dp.Category = sc.Category,
 dp.Price = sc.Price,
 dp.Lineage_Id = NEWID()
FROM DimProduct dp
JOIN Insignia_staging_copy sc ON dp.Description = sc.Description;
--Step 3: Load Data into Fact Table
--Let's start with loading the Fact table. We assume that the data in the
Insignia_staging_copy is ready to be processed into the Fact table.
Fact Table Script
-- Load data into FactSales
INSERT INTO FactSales (InvoiceId, ProductKey, CustomerKey, EmployeeKey,
GeographyKey, DateKey, Quantity, Unit_Price, Tax_Rate, Total_Excluding_Tax,
Tax_Amount, Profit, Total_Including_Tax, Lineage_Id)
SELECT
 sc.InvoiceId,
 dp.ProductKey,
 dc.CustomerKey,
 de.EmployeeKey,
 dg.GeographyKey,
 dd.DateKey,
 sc.Quantity,
 sc.Unit_Price,
 sc.Tax_Rate,
 sc.Total_Excluding_Tax,
 sc.Tax_Amount,
 sc.Profit,
 sc.Total_Including_Tax,
 @Lineage_Id
FROM Insignia_staging_copy sc
JOIN DimProduct dp ON sc.Description = dp.Description
JOIN DimCustomer dc ON sc.CustomerId = dc.CustomerId
JOIN DimEmployee de ON sc.employee_Id = de.EmployeeId
JOIN DimGeography dg ON sc.City_ID = dg.City_ID
JOIN DimDate dd ON CONVERT(INT, FORMAT(sc.SalesDate, 'yyyyMMdd')) =
dd.DateKey;
--Step 4: Insert Incremental Data
-- Insert incremental data into staging copy table
INSERT INTO Insignia_staging_copy
SELECT * FROM Insignia_incremental;
--Step 5: Truncate Insignia_staging_copy Before Incremental Load
-- Truncate staging copy table before incremental load
TRUNCATE TABLE Insignia_staging_copy;
--Step 6: Load Incremental Data into Dimension and Fact Tables
The incremental data loading into dimensions and fact tables would follow the
same pattern as the initial load, using the same insert scripts provided in
previous steps.
--Step 7: None of the dimension and fact tables must be truncated.
Repeat the steps for loading dimensions and fact table with the new data in
Insignia_staging_copy.
--Step 8: Reconciliation Module (Bonus)
USE insignia_database;
-- Reconciliation to check number of rows processed after a full ETL run
SELECT
 'DimEmployee' AS TableName, COUNT(*) AS RowCount
FROM DimEmployee
UNION ALL
SELECT
 'DimCustomer' AS TableName, COUNT(*) AS RowCount
FROM DimCustomer
UNION ALL
SELECT
 'DimProduct' AS TableName, COUNT(*) AS RowCount
FROM DimProduct
UNION ALL
SELECT
 'DimGeography' AS TableName, COUNT(*) AS RowCount
FROM DimGeography
UNION ALL
SELECT
 'FactSales' AS TableName, COUNT(*) AS RowCount
FROM FactSales;
