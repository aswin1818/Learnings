CREATE TABLE Sales (
    SalesID INT PRIMARY KEY,
    ProductID INT,
    SaleDate DATE,
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalPrice DECIMAL(10,2),
    -- Add other relevant columns like customer ID, store ID, etc.
    FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
);



-- Create the Product dimension table with SCD 2
CREATE TABLE Product (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Price DECIMAL(10,2),
    ValidFrom DATETIME,
    ValidTo DATETIME,
    CurrentFlag CHAR(1)
);


CREATE TABLE Store (
    StoreID INT PRIMARY KEY,
    StoreName VARCHAR(100),
    Location VARCHAR(100),
    Size INT,
    -- Other relevant attributes
);


CREATE TABLE Customer (
    CustomerID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Address VARCHAR(100),
    segment VARCHAR(50)
    -- Other relevant attributes
);



CREATE TABLE Employee (
    EmployeeID INT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Role VARCHAR(50),
    Department VARCHAR(50),
    -- Other relevant attributes
);
