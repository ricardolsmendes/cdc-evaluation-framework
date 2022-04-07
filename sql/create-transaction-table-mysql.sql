CREATE TABLE IF NOT EXISTS transactions (
  transaction_id BIGINT NOT NULL AUTO_INCREMENT,
  invoice VARCHAR(55) NOT NULL,
  stock_code VARCHAR(55) NOT NULL,
  description VARCHAR(255),
  quantity DECIMAL(9,3) NOT NULL,
  invoice_date DATETIME NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  customer_id DECIMAL(9,1),
  country VARCHAR(255),
  PRIMARY KEY (transaction_id)
);
