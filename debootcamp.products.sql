-- Table: debootcamp.products

DROP TABLE debootcamp.products;

CREATE TABLE IF NOT EXISTS debootcamp.products
(
    InvoiceNo VARCHAR(10),
    StockCode VARCHAR(20),
    Description VARCHAR(1000),
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice NUMERIC(8, 3),
    CustomerID INT,
    Country VARCHAR(255)
);