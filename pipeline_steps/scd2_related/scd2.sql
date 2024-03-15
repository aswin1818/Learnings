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

-- Insert initial record
INSERT INTO Product (ProductID, ProductName, Category, Price, ValidFrom, ValidTo, CurrentFlag)
VALUES (1, 'ABC123', 'Electronics', 10.00, '2024-01-01', '9999-12-31', 'Y');

-- Update price
UPDATE Product
SET ValidTo = '2024-02-01', CurrentFlag = 'N'
WHERE ProductID = 1;

-- Insert new version of the record with updated price
INSERT INTO Product (ProductID, ProductName, Category, Price, ValidFrom, ValidTo, CurrentFlag)
VALUES (1, 'ABC123', 'Electronics', 15.00, '2024-02-01', '9999-12-31', 'Y');
