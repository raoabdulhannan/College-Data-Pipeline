# College Scorecard Database

## Description
This repository contains scripts and resources for creating a database pipeline for College Scorecard data, including SQL schema, data loading scripts, and data processing functions. The dataset is sourced from the U.S. Department of Education’s College Scorecard and the Integrated Postsecondary Education Data System (IPEDS). 

The primary components are:
- `schema.sql`: Creates tables in the PostgreSQL database for the College Scorecard data.
- `load-ipeds.py`: Loads IPEDS data into the database, including institutional and crosswalk data.
- `load-scorecard.py`: Loads annual College Scorecard data, handling financial and other performance data.

## Setup
After pulling the repository, create a `credentials.py` file in the root directory with the following structure:

```python
DB_NAME = "YOUR_DATABASE_NAME"
DB_USER = "YOUR_USERNAME"
DB_PASSWORD = "YOUR_PASSWORD"
```

## Instructions

### schema.sql
Defines the SQL schema for the project, creating tables to store institutions, annual college scorecard data, and financial data. Ensure this script is run first to initialize the database with the necessary tables and constraints.

### load-ipeds.py
Loads IPEDS data from a specified CSV file into the PostgreSQL database. The script processes missing values, formats certain fields, and inserts data into the `Institutions` and `Crosswalks` tables.

**Command to run:**
```bash
python load-ipeds.py <file_path>
```

**Example:**
```bash
python load-ipeds.py hd2022.csv
```

### load-scorecard.py
Loads College Scorecard data into the database, including financial details and institutional performance metrics. The script handles data cleaning and formats currency fields appropriately before insertion.

**Command to run:**
```bash
python load-scorecard.py <file_path>
```

**Example:**
```bash
python load-scorecard.py MERGED2021_2022_P.csv
```

## Data Processing Details
- **process_value**: A helper function used in each script to handle empty or invalid values, converting them to `NULL` or formatting them as required by the database schema.
- **Data Validation**: If any constraints are violated during data insertion, the scripts rollback and report the specific row, enabling easy troubleshooting.

## Notes
- The database host is configured as `pinniped.postgres.database.azure.com`.
- All scripts are formatted following PEP 8 and include docstrings for readability and maintainability.
