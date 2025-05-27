-- Databricks notebook source
create database sales_scd;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # clean the path, if its already existing
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/sales_scd.db/orders", True)

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_scd.Orders (
    OrderID INT,
    OrderDate DATE,
    CustomerID INT,
    CustomerName VARCHAR(100),
    CustomerEmail VARCHAR(100),
    ProductID INT,
    ProductName VARCHAR(100),
    ProductCategory VARCHAR(50),
    RegionID INT,
    RegionName VARCHAR(50),
    Country VARCHAR(50),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalAmount DECIMAL(10,2)
);

-- COMMAND ----------

INSERT INTO sales_scd.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
VALUES 
(1, '2024-02-01', 101, 'Alice Johnson', 'alice@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
(2, '2024-02-02', 102, 'Bob Smith', 'bob@example.com', 202, 'Smartphone', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00),
(3, '2024-02-03', 103, 'Charlie Brown', 'charlie@example.com', 203, 'Tablet', 'Electronics', 303, 'Asia', 'India', 3, 300.00, 900.00),
(4, '2024-02-04', 101, 'Alice Johnson', 'alice@example.com', 204, 'Headphones', 'Accessories', 301, 'North America', 'USA', 1, 150.00, 150.00),
(5, '2024-02-05', 104, 'David Lee', 'david@example.com', 205, 'Gaming Console', 'Electronics', 302, 'Europe', 'France', 1, 400.00, 400.00),
(6, '2024-02-06', 102, 'Bob Smith', 'bob@example.com', 206, 'Smartwatch', 'Electronics', 303, 'Asia', 'China', 2, 200.00, 400.00),
(7, '2024-02-07', 105, 'Eve Adams', 'eve@example.com', 201, 'Laptop', 'Electronics', 301, 'North America', 'Canada', 1, 800.00, 800.00),
(8, '2024-02-08', 106, 'Frank Miller', 'frank@example.com', 207, 'Monitor', 'Accessories', 302, 'Europe', 'Italy', 2, 250.00, 500.00),
(9, '2024-02-09', 107, 'Grace White', 'grace@example.com', 208, 'Keyboard', 'Accessories', 303, 'Asia', 'Japan', 3, 100.00, 300.00),
(10, '2024-02-10', 104, 'David Lee', 'david@example.com', 209, 'Mouse', 'Accessories', 301, 'North America', 'USA', 1, 50.00, 50.00);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SCD TYPE-1

-- COMMAND ----------

select * from sales_scd.orders

-- COMMAND ----------

create or replace view sales_scd.view_DimProducts
as
select distinct(ProductID) as ProductID, ProductName, ProductCategory 
from sales_scd.orders

-- COMMAND ----------

CREATE or REPLACE TABLE sales_scd.DimProducts
(
  ProductID INT,
  ProductName  STRING,
  ProductCategory STRING
)

-- COMMAND ----------

INSERT INTO sales_scd.dimproducts
SELECT ProductID, ProductName, ProductCategory FROM sales_scd.view_DimProducts

-- COMMAND ----------

INSERT INTO sales_scd.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
VALUES 
(1, '2024-02-11', 101, 'Alice Johnson', 'alice@example.com', 201, 'GamingLaptop', 'Electronics', 301, 'North America', 'USA', 2, 800.00, 1600.00),
(2, '2024-02-12', 102, 'Bob Smith', 'bob@example.com', 230, 'Airpods', 'Electronics', 302, 'Europe', 'Germany', 1, 500.00, 500.00)

-- COMMAND ----------

create or replace view sales_scd.view_DimProducts
as
select distinct(ProductID) as ProductID, ProductName, ProductCategory 
from sales_scd.orders
where OrderDate > '2024-02-10'

-- COMMAND ----------

select * from sales_scd.view_DimProducts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # MERGE SCD-TYPE1

-- COMMAND ----------

MERGE INTO sales_scd.dimproducts as TRGT
using sales_scd.view_dimproducts as SRC
on TRGT.ProductID = SRC.ProductID
when matched then update set *
when not matched then insert *

-- COMMAND ----------

select * from sales_scd.dimproducts

-- COMMAND ----------

