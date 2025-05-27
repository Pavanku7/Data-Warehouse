-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Incremental Data Loading

-- COMMAND ----------

create database sales_new;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # clean the path, if its already existing
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/sales_new.db/orders", True)

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_new.Orders (
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

INSERT INTO sales_new.Orders (OrderID, OrderDate, CustomerID, CustomerName, CustomerEmail, ProductID, ProductName, ProductCategory, RegionID, RegionName, Country, Quantity, UnitPrice, TotalAmount) 
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

select * from sales_new.orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DATA WAREHOUSING

-- COMMAND ----------

CREATE DATABASE orderDWH

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # clean the path, if its already existing
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/orderdwh.db/stg_sales", True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Staging Layer

-- COMMAND ----------

-- Initial load of data into data warehouse

create table orderDWH.stg_sales
as
select * from sales_new.orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Transformation

-- COMMAND ----------

create view orderDWH.trans_sales
as
select * from orderDWH.stg_sales where Quantity is not null

-- COMMAND ----------

select * from orderdwh.trans_sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Core Layer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DimCustomers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # clean the path, if its already existing
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/orderdwh.db/dimcustomers", True)

-- COMMAND ----------

-- Create dimensional table customer

CREATE OR REPLACE table orderDWH.DimCustomers
(
  CustomerID int,
  DimCustomerKey int,
  CustomerName string,
  CustomerEmail string
)

-- COMMAND ----------

CREATE or REPLACE VIEW orderDWH.view_DimCustomers
As
select T.*,row_number() over(order by CustomerID) as DimCustomerKey FROM
(
select
  distinct CustomerID as CustomerID,
  -- 
  CustomerName, CustomerEmail
from 
  orderdwh.trans_sales
) as T

-- COMMAND ----------

select * from orderDWH.view_dimcustomers

-- COMMAND ----------

insert into orderdwh.DimCustomers(CustomerID, DimCustomerKey, CustomerName, CustomerEmail)
select CustomerID, DimCustomerKey, CustomerName, CustomerEmail from orderdwh.view_DimCustomers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimProduct

-- COMMAND ----------

CREATE TABLE orderDWH.DimProducts
(
  ProductID INT,
  ProductName STRING,
  ProductCategory STRING,
  DimProductsKey INT
)

-- COMMAND ----------

CREATE or REPLACE VIEW orderDWH.view_DimProducts
As
select T.*,row_number() over(order by t.ProductID) as DimProductsKey FROM
(
select
  distinct(ProductID) as ProductID,
  ProductName,
  ProductCategory
from 
  orderdwh.trans_sales
) as T

-- COMMAND ----------

-- select * from orderDWH.view_dimproducts
-- select * from orderDWH.dimproducts

-- COMMAND ----------

insert into orderDWH.dimproducts
select * from orderdwh.view_DimProducts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimRegions

-- COMMAND ----------

CREATE TABLE orderDWH.DimRegion
(
  RegionID INT,
  RegionName STRING,
  Country STRING,
  DimRegionKey INT
)

-- COMMAND ----------

CREATE or REPLACE VIEW orderDWH.view_DimRegion 
As
select T.*,row_number() over(order by t.RegionID) as DimRegionKey FROM
(
select
  distinct(RegionID) as RegionID,
  RegionName,
  Country
from 
  orderdwh.trans_sales
) as T

-- COMMAND ----------

insert into orderDWH.DimRegion
select * from orderdwh.view_DimRegion

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DimDate

-- COMMAND ----------

CREATE or REPLACE TABLE orderDWH.DimDate
(
  OrderDate DATE,
  DimDateKey INT
)

-- COMMAND ----------

CREATE or REPLACE VIEW orderDWH.view_DimDate 
As
select T.*,row_number() over(order by t.OrderDate) as DimDateKey FROM
(
select
  distinct(OrderDate) as OrderDate
from 
  orderdwh.trans_sales
) as T

-- COMMAND ----------

insert into orderDWH.DimDate
select * from orderdwh.view_DimDate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### FACT TABLE

-- COMMAND ----------

CREATE TABLE orderDWH.FactSales
(
  OrderID INT,
  Quantity DECIMAL,
  UnitPrice DECIMAL,
  TotalAmount INT,
  DimProductsKey INT,
  DimCustomerKey INT,
  DimRegionKey INT,
  DimDateKey INT
)

-- COMMAND ----------

select
 F.OrderID,
 F.Quantity,
 F.UnitPrice,
 F.TotalAmount,
 DC.DimCustomerKey
from
  orderdwh.trans_sales F 
left join
  orderdwh.DimCustomers DC
  on F.CustomerID = DC.CustomerID

-- COMMAND ----------

select
 F.OrderID,
 F.Quantity,
 F.UnitPrice,
 F.TotalAmount,
 DC.DimCustomerKey,
 DP.DimProductsKey,
 DD.DimDateKey,
 DR.DimRegionKey
from
  orderdwh.trans_sales F 
left join
  orderdwh.DimCustomers DC
  on F.CustomerID = DC.CustomerID
left join
  orderdwh.dimproducts DP
  on F.ProductID = DP.ProductID
left join
  orderdwh.dimregion DR 
  on F.Country = DR.Country
left join
  orderdwh.dimdate DD
  on F.OrderDate = DD.OrderDate

-- COMMAND ----------

-- We need to pick the Country column while performing the join, bcoz of more granular level of data available in table
select * from orderdwh.dimregion


-- COMMAND ----------

