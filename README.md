# cdc-evaluation-framework

A provider-agnostic framework to evaluate ordinary Change Data Capture (CDC)
features, comprising:

1. A high-level evaluation plan.
2. SQL statements to set up source and destination DB/DW environments.
3. SQL statements to fully ingest CSV files into the source database tables.
4. Python scripts to manage chunks of data in the source database tables and enable 
   users to evaluate CDC tools' capabilities.

The framework is intended to allow users to evaluate replication/streaming
capabilities using the tools of their choice.

[![license](https://img.shields.io/github/license/ricardolsmendes/cdc-evaluation-framework.svg)](https://github.com/ricardolsmendes/cdc-evaluation-framework/blob/main/LICENSE)
[![issues](https://img.shields.io/github/issues/ricardolsmendes/cdc-evaluation-framework.svg)](https://github.com/ricardolsmendes/cdc-evaluation-framework/issues)

<!--
  DO NOT UPDATE THE TABLE OF CONTENTS MANUALLY
  run `npx markdown-toc -i README.md`.

  Please stick to 80-character line wraps as much as you can.
-->

## Table of Contents

<!-- toc -->

- [1. Change Data Capture Evaluation Plan](#1-change-data-capture-evaluation-plan)
- [2. Environment setup](#2-environment-setup)
  * [2.1. Python](#21-python)
    + [2.1.1. Install Python 3.8+](#211-install-python-38)
    + [2.1.2. Create a folder](#212-create-a-folder)
    + [2.1.3. Create and activate an isolated execution environment](#213-create-and-activate-an-isolated-execution-environment)
    + [2.1.4. Install the package](#214-install-the-package)
- [3. Usage instructions](#3-usage-instructions)
  * [3.1. Online Retail II UCI dataset (available on Kaggle)](#31-online-retail-ii-uci-dataset-available-on-kaggle)
    + [3.1.1. Download the dataset](#311-download-the-dataset)
    + [3.1.2. Set up the source and destination DB/DW](#312-set-up-the-source-and-destination-dbdw)
    + [3.1.3. Load all transaction data into a MySQL source table](#313-load-all-transaction-data-into-a-mysql-source-table)
    + [3.1.4. Insert random transactions into the source table](#314-insert-random-transactions-into-the-source-table)
    + [3.1.5. Delete random transactions from the source table](#315-delete-random-transactions-from-the-source-table)
- [4. How to contribute](#4-how-to-contribute)
  * [4.1. Report issues](#41-report-issues)
  * [4.2. Contribute code](#42-contribute-code)

<!-- tocstop -->

---

## 1. Change Data Capture Evaluation Plan

The high-level CDC evaluation plan comprises 5 major steps:

1. Load big chunks of data into the source database before starting any
   replication/streaming jobs.
2. Start the replication/streaming jobs using the CDC tool of your choice.
3. _[Optional]_ Wait for the CDC tool to stream all existing data (aka take an
   initial snapshot).
4. Insert/update/delete records into/from the source tables using the Python
   scripts provided by the present framework.
5. Monitor the CDC tool to make sure it is properly streaming all data changes.

## 2. Environment setup

### 2.1. Python

#### 2.1.1. Install Python 3.8+

Please refer to [python.org/downloads](https://www.python.org/downloads/) for
further details.

#### 2.1.2. Create a folder

This is recommended so all related stuff will reside at the same place, making
it easier to follow the next instructions.

```shell
mkdir ./cdc-evaluation-framework
cd ./cdc-evaluation-framework
```

_All paths starting with `./` in the next steps are relative to the
`cdc-evaluation-framework` folder._

#### 2.1.3. Create and activate an isolated execution environment

This step is optional, but strongly recommended.

```shell
python3 -m venv env
source ./env/bin/activate
```

#### 2.1.4. Install the package

COMING SOON!

## 3. Usage instructions

### 3.1. Online Retail II UCI dataset (available on Kaggle)

> The Online Retail II data set contains all the transactions occurring for a
> UK-based and registered, non-store online retail between 01/12/2009 and
> 09/12/2011. The company mainly sells unique all-occasion gift-ware. Many
> customers of the company are wholesalers.

Summary:

| Industry | Rows      | Columns | Verified at |
|----------|-----------|--------:|------------:|
| Retail   | 1,067,371 |       8 |  2022-05-23 |

#### 3.1.1. Download the dataset

Download the `archive.zip` file from
[kaggle.com/datasets/mashlyn/online-retail-ii-uci](https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci)
into `cdc-evaluation-framework`, unzip and move it into a temporary folder:

```shell
unzip archive.zip
mv online_retail_II.csv /tmp/
```

#### 3.1.2. Set up the source and destination DB/DW

Please refer to the
[sql/kaggle-online-retail-ii-uci](./sql/kaggle-online-retail-ii-uci)
folder for help. 

#### 3.1.3. Load all transaction data into a MySQL source table

Use the below instructions to accomplish the first step of the [CDC Evaluation
Plan](#1-change-data-capture-evaluation-plan).

1. Set the server's `local_infile` system variable to `1`:

   ```sql
   set global local_infile = 1;
   ```

2. Provide the `--local-infile=1` flag when connecting the client:

   ```shell
   mysql -u <USER> -p --local-infile=1 <DATABASE>
   ```

3. Run the below command using the client:
   ```sql
   LOAD DATA LOCAL INFILE '/tmp/online_retail_II.csv'
     INTO TABLE transactions
     FIELDS TERMINATED BY ','
     OPTIONALLY ENCLOSED BY '"'
     IGNORE 1 ROWS
     (invoice, stock_code, description, quantity, invoice_date, price,
      @customer_id, country)
     SET customer_id = NULLIF(@customer_id, '');
   ```

#### 3.1.4. Insert random transactions into the source table

You can use the below command to automate the fourth step of the [CDC
Evaluation Plan](#1-change-data-capture-evaluation-plan).

```shell
cdc-eval kaggle-online-retail-uci \
  --data-file datasets/kaggle-online-retail-ii-uci.csv \
  --invoices <THE-NUMBER-OF-TRANSACTIONS> \
  --db-conn <SQLALCHEMY-CONNECTION-STRING>
```

#### 3.1.5. Delete random transactions from the source table

You can use the below command to automate the fourth step of the [CDC
Evaluation Plan](#1-change-data-capture-evaluation-plan).

```shell
cdc-eval kaggle-online-retail-uci \
  --data-file datasets/kaggle-online-retail-ii-uci.csv \
  --invoices <THE-NUMBER-OF-TRANSACTIONS> \
  --db-conn <SQLALCHEMY-CONNECTION-STRING> \
  --operation-mode delete
```

## 4. How to contribute

Please make sure to take a moment and read the [Code of
Conduct](https://github.com/ricardolsmendes/cdc-evaluation-framework/blob/main/.github/CODE_OF_CONDUCT.md).

### 4.1. Report issues

Please report bugs and suggest features via the [GitHub
Issues](https://github.com/ricardolsmendes/cdc-evaluation-framework/issues).

Before opening an issue, search the tracker for possible duplicates. If you find a duplicate, please
add a comment saying that you encountered the problem as well.

### 4.2. Contribute code

Please make sure to read the [Contributing
Guide](https://github.com/ricardolsmendes/cdc-evaluation-framework/blob/main/.github/CONTRIBUTING.md)
before making a pull request.
