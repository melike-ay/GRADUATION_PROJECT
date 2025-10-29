-- STAGING LAYER:
-- - All columns in foreign tables and src tables are varchar(1000) to ensure no data is lost due to data type mismatch issues.
-- - This script creates staging schemas: sa_offline_sales, sa_online_sales.



-- 1. Enable necessary extensions
CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 2. Create the foreign server if it does not exist
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_foreign_server WHERE srvname = 'csv_server'
    ) THEN
        CREATE SERVER csv_server FOREIGN DATA WRAPPER file_fdw;
    END IF;
END$$;

-- 3. Create schemas (staging and downstream layer schemas)
CREATE SCHEMA IF NOT EXISTS sa_offline_sales;
CREATE SCHEMA IF NOT EXISTS sa_online_sales;


-- 4. Creation of foreign tables (external sources) in staging
--    All columns are VARCHAR(1000).

--------------------------
-- OFFLINE SALES: ext_offline_sales
--------------------------
CREATE FOREIGN TABLE IF NOT EXISTS sa_offline_sales.ext_offline_sales (
    Invoice_ID            varchar(1000),
    Product_Code          varchar(1000),
    Product_Name          varchar(1000),
    Sales_Quantity        varchar(1000),
    Unit                  varchar(1000),
    Price                 varchar(1000),
    Currency_Unit         varchar(1000),
    Product_Cost          varchar(1000),
    Discount              varchar(1000),
    Company_Name          varchar(1000),
    Country               varchar(1000),
    City                  varchar(1000),
    Store_Name            varchar(1000),
    Sales_Representative  varchar(1000),
    Sale_Date             varchar(1000),
    Delivery_Date         varchar(1000),
    Payment_Method        varchar(1000),
    Customer_ID           varchar(1000),
    Total_Sale_Amount     varchar(1000),
    Warehouse_Name        varchar(1000),
    Sales_Channel         varchar(1000),
    Address_ID            varchar(1000),
    Order_Status          varchar(1000)
)
SERVER csv_server
OPTIONS (
    filename '/var/lib/postgresql/data/csv/offline_sales_dataset.csv',
    format 'csv',
    header 'true',
    delimiter ','
);

--------------------------
-- ONLINE SALES: ext_online_sales
--------------------------
CREATE FOREIGN TABLE IF NOT EXISTS sa_online_sales.ext_online_sales (
    Order_ID                 varchar(1000),
    Product_Code             varchar(1000),
    Product_Name             varchar(1000),
    Sales_Quantity           varchar(1000),
    Unit                     varchar(1000),
    Price_Local              varchar(1000),
    Currency_Unit            varchar(1000),
    Price_USD                varchar(1000),
    Product_Cost_Local       varchar(1000),
    Product_Cost_USD         varchar(1000),
    Discount                 varchar(1000),
    Website                  varchar(1000),
    Country                  varchar(1000),
    City                     varchar(1000),
    Customer_ID              varchar(1000),
    Payment_Method           varchar(1000),
    Sale_Date                varchar(1000),
    Delivery_Date            varchar(1000),
    Total_Sale_Amount_Local  varchar(1000),
    Total_Sale_Amount_USD    varchar(1000),
    Sales_Channel            varchar(1000),
    Warehouse_Name           varchar(1000),
    Order_Status             varchar(1000),
    Platform_Fee             varchar(1000),
    Delivery_Status          varchar(1000),
    Address_ID               varchar(1000),
    Seller_ID                varchar(1000),
    Seller_Name              varchar(1000),
    Review_Score             varchar(1000),
    Customer_Feedback        varchar(1000)
)
SERVER csv_server
OPTIONS (
    filename '/var/lib/postgresql/data/csv/online_sales_dataset.csv',
    format 'csv',
    header 'true',
    delimiter ','
);

-- 5. Creation of src tables in staging (regular PostgreSQL tables)
--    These will be populated from the ext_ foreign tables.

--------------------------
-- OFFLINE SALES: src_offline_sales
--------------------------
CREATE TABLE IF NOT EXISTS sa_offline_sales.src_offline_sales (
    Invoice_ID            varchar(1000),
    Product_Code          varchar(1000),
    Product_Name          varchar(1000),
    Sales_Quantity        varchar(1000),
    Unit                  varchar(1000),
    Price                 varchar(1000),
    Currency_Unit         varchar(1000),
    Product_Cost          varchar(1000),
    Discount              varchar(1000),
    Company_Name          varchar(1000),
    Country               varchar(1000),
    City                  varchar(1000),
    Store_Name            varchar(1000),
    Sales_Representative  varchar(1000),
    Sale_Date             varchar(1000),
    Delivery_Date         varchar(1000),
    Payment_Method        varchar(1000),
    Customer_ID           varchar(1000),
    Total_Sale_Amount     varchar(1000),
    Warehouse_Name        varchar(1000),
    Sales_Channel         varchar(1000),
    Address_ID            varchar(1000),
    Order_Status          varchar(1000)
);

--------------------------
-- ONLINE SALES: src_online_sales
--------------------------
CREATE TABLE IF NOT EXISTS sa_online_sales.src_online_sales (
    Order_ID                 varchar(1000),
    Product_Code             varchar(1000),
    Product_Name             varchar(1000),
    Sales_Quantity           varchar(1000),
    Unit                     varchar(1000),
    Price_Local              varchar(1000),
    Currency_Unit            varchar(1000),
    Price_USD                varchar(1000),
    Product_Cost_Local       varchar(1000),
    Product_Cost_USD         varchar(1000),
    Discount                 varchar(1000),
    Website                  varchar(1000),
    Country                  varchar(1000),
    City                     varchar(1000),
    Customer_ID              varchar(1000),
    Payment_Method           varchar(1000),
    Sale_Date                varchar(1000),
    Delivery_Date            varchar(1000),
    Total_Sale_Amount_Local  varchar(1000),
    Total_Sale_Amount_USD    varchar(1000),
    Sales_Channel            varchar(1000),
    Warehouse_Name           varchar(1000),
    Order_Status             varchar(1000),
    Platform_Fee             varchar(1000),
    Delivery_Status          varchar(1000),
    Address_ID               varchar(1000),
    Seller_ID                varchar(1000),
    Seller_Name              varchar(1000),
    Review_Score             varchar(1000),
    Customer_Feedback        varchar(1000)
);

-- 6.

-- Populate source table from foreign table (offline)
INSERT INTO sa_offline_sales.src_offline_sales (
    Invoice_ID, Product_Code, Product_Name, Sales_Quantity, Unit, Price,
    Currency_Unit, Product_Cost, Discount, Company_Name, Country,
    City, Store_Name, Sales_Representative, Sale_Date, Delivery_Date,
    Payment_Method, Customer_ID, Total_Sale_Amount, Warehouse_Name,
    Sales_Channel, Address_ID, Order_Status
)
SELECT
    e.Invoice_ID, e.Product_Code, e.Product_Name, e.Sales_Quantity, e.Unit, e.Price,
    e.Currency_Unit, e.Product_Cost, e.Discount, e.Company_Name, e.Country,
    e.City, e.Store_Name, e.Sales_Representative, e.Sale_Date, e.Delivery_Date,
    e.Payment_Method, e.Customer_ID, e.Total_Sale_Amount, e.Warehouse_Name,
    e.Sales_Channel, e.Address_ID, e.Order_Status
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY Invoice_ID ORDER BY (SELECT NULL)) AS rn
    FROM sa_offline_sales.ext_offline_sales
) AS e
WHERE e.rn = 1
AND NOT EXISTS (
    SELECT 1
    FROM sa_offline_sales.src_offline_sales s
    WHERE s.Invoice_ID = e.Invoice_ID
);



-- Populate source table from foreign table (online)
INSERT INTO sa_online_sales.src_online_sales (
    Order_ID, Product_Code, Product_Name, Sales_Quantity, Unit, Price_Local,
    Currency_Unit, Price_USD, Product_Cost_Local, Product_Cost_USD, Discount,
    Website, Country, City, Customer_ID, Payment_Method, Sale_Date,
    Delivery_Date, Total_Sale_Amount_Local, Total_Sale_Amount_USD,
    Sales_Channel, Warehouse_Name, Order_Status, Platform_Fee, Delivery_Status,
    Address_ID, Seller_ID, Seller_Name, Review_Score, Customer_Feedback
)
SELECT
    e.Order_ID, e.Product_Code, e.Product_Name, e.Sales_Quantity, e.Unit, e.Price_Local,
    e.Currency_Unit, e.Price_USD, e.Product_Cost_Local, e.Product_Cost_USD, e.Discount,
    e.Website, e.Country, e.City, e.Customer_ID, e.Payment_Method, e.Sale_Date,
    e.Delivery_Date, e.Total_Sale_Amount_Local, e.Total_Sale_Amount_USD,
    e.Sales_Channel, e.Warehouse_Name, e.Order_Status, e.Platform_Fee, e.Delivery_Status,
    e.Address_ID, e.Seller_ID, e.Seller_Name, e.Review_Score, e.Customer_Feedback
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY Order_ID ORDER BY (SELECT NULL)) AS rn
    FROM sa_online_sales.ext_online_sales
) AS e
WHERE e.rn = 1
AND NOT EXISTS (
    SELECT 1
    FROM sa_online_sales.src_online_sales s
    WHERE s.Order_ID = e.Order_ID
);




-- 7. Quick verification queries to run after insertions.

-- Count records in each src table:
-- SELECT 'offline src count' as what, COUNT(*) FROM sa_offline_sales.src_offline_sales;
-- SELECT 'online src count'  as what, COUNT(*) FROM sa_online_sales.src_online_sales;

-- Sample peek:
-- SELECT * FROM sa_offline_sales.src_offline_sales LIMIT 5;
-- SELECT * FROM sa_online_sales.src_online_sales LIMIT 5;

