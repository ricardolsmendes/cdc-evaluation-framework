CREATE TABLE IF NOT EXISTS transactions (
  transaction_id NUMBER,
  invoice VARCHAR(55),
  stock_code VARCHAR(55),
  description VARCHAR(255),
  quantity NUMBER(9,3),
  invoice_date TIMESTAMP_NTZ(9),
  price NUMBER(9,3),
  customer_id NUMBER(9,1),
  country VARCHAR(255)
);
