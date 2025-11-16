CREATE TABLE IF NOT EXISTS ecommerce (
  InvoiceNo VARCHAR(20),
  StockCode VARCHAR(20),
  Description VARCHAR(255),
  Quantity INT,
  InvoiceDate TIMESTAMP,
  UnitPrice NUMERIC(10,2),
  CustomerID INT,
  Country VARCHAR(100)
);

-- Load CSV
COPY ecommerce(InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
FROM '/docker-entrypoint-initdb.d/ecommerce_kaggle_sample.csv'
DELIMITER ','
CSV HEADER;
