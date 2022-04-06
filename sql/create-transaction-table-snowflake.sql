CREATE OR REPLACE TABLE transaction (
  ID NUMBER,
  Invoice VARCHAR(55),
  StockCode VARCHAR(55),
  Description VARCHAR(255),
  Quantity NUMBER(9,3),
  InvoiceDate TIMESTAMP_NTZ(9),
  Price NUMBER(10,2),
  CustomerID NUMBER(9,1),
  Country VARCHAR(255)
);