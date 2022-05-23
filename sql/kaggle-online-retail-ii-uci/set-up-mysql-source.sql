-- Copyright 2022 Ricardo Mendes
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- 1. Create the source database
CREATE DATABASE cdc_evaluation;
USE cdc_evaluation;

-- 2. Create the source table
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

-- 3. Create a user for replication matters
CREATE USER `cdc-replication-agent`@`%`
  IDENTIFIED WITH mysql_native_password BY `<password>`;

-- 4. Grant the user only the minimal required privileges
GRANT REPLICATION SLAVE, REPLICATION CLIENT
  ON *.*
  TO `cdc-replication-agent`@`%`;

GRANT SELECT
  ON cdc_evaluation.transactions
  TO `cdc-replication-agent`@`%`;

-- 5. Load data into the source table
LOAD DATA LOCAL INFILE '/tmp/online_retail_II.csv'
  INTO TABLE transactions
  FIELDS TERMINATED BY ','
  OPTIONALLY ENCLOSED BY '"'
  IGNORE 1 ROWS
  (invoice, stock_code, description, quantity, invoice_date, price,
   @customer_id, country)
  SET customer_id = NULLIF(@customer_id, '');
