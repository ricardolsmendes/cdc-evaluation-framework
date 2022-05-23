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

-- 1. Create the destination database
CREATE DATABASE evaluation;
USE evaluation;

-- 2. Create the destination schema
CREATE SCHEMA cdc;
USE evaluation.cdc;

-- 3. Create the destination table
CREATE TABLE IF NOT EXISTS transactions (
  transaction_id NUMBER,
  invoice VARCHAR(55),
  stock_code VARCHAR(55),
  description VARCHAR(255),
  quantity NUMBER(9,3),
  invoice_date TIMESTAMP_NTZ(9),
  price NUMBER(10,2),
  customer_id NUMBER(9,1),
  country VARCHAR(255)
);

-- 4. Create a role and a user for replication matters
CREATE ROLE dataeditor;

CREATE USER cdcreplicationagent
  PASSWORD = '<password>';

GRANT ROLE dataeditor
  TO USER cdcreplicationagent;

-- 5. Grant the role only the minimal required privileges
GRANT DELETE, INSERT, SELECT, UPDATE
  ON TABLE evaluation.cdc.transactions
  TO ROLE dataeditor;
