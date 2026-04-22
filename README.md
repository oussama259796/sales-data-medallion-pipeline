# Medallion Data Pipeline for Messy CSV Data

A production-style ETL pipeline that turns raw, messy CSV files into clean, structured, analytics-ready PostgreSQL tables.

This project is a practical example of the kind of data automation work I build for businesses that still rely on manual spreadsheet cleanup.

## The Business Problem

Many businesses do not actually have a data problem.

They have a **messy CSV problem**:

- inconsistent column names
- duplicate records
- invalid rows
- missing values
- manual Excel cleanup every week
- data that is not ready for dashboards or reporting

This pipeline solves that by turning raw input files into a repeatable, reliable workflow.

## What This Pipeline Does

### Bronze Layer
- Reads multiple raw CSV files
- Normalizes columns into a consistent structure
- Adds metadata such as source file and load timestamp
- Loads raw data into PostgreSQL

### Silver Layer
- Cleans and standardizes the data
- Converts numeric fields safely
- fixes inconsistent text fields
- detects invalid rows
- separates rejected rows for review
- removes duplicates while keeping the latest valid record

### Gold Layer
Builds analytics-ready tables for reporting, including:

- sales by city
- sales by product
- sales by customer
- sales by status

## Real Results

This pipeline was tested on a real dataset and produced the following results:

- **250,000 rows** processed
- **5 CSV files** ingested
- **64,992 invalid rows** detected and separated
- **132,290 clean rows** loaded into the final clean table
- full pipeline completed in **about 16 seconds**

## Why This Matters

For a business, this means:

- no more manual spreadsheet cleanup
- no more repeated copy-paste work
- cleaner reporting inputs
- faster dashboard preparation
- a repeatable process that can run again anytime

In other words: less Excel work, more reliable data.

## Architecture

```text
Raw CSV Files
    ↓
Bronze Layer
(raw ingestion in PostgreSQL)
    ↓
Silver Layer
(cleaning, validation, deduplication)
    ↓
Gold Layer
(aggregated business tables for analytics)
```

## Project Structure

```text
project/
├── migrations/
│   ├── V1__create_schemas.sql
│   └── V2__V2__create_bronze_tables.sql
│   ├── V3__V3__create_silver_tables.sql
│   └── V3.1__V3.1__create_quarantine_tables.sql
│   └── V4__create_gold_tables.sql
├── run_pipeline.py
├── utils/
│   ├── db.py
│   └── logger.py
├── pipelines/
│   ├── ingest/
│   │   └── csv_to_bronze.py
│   └── transform/
│       ├── bronze_to_silver.py
│       └── silver_to_gold.py
└── data/
    └── raw/
```
![alt text](image-1.png)

## Tech Stack

- Python
- Pandas
- PostgreSQL
- SQLAlchemy
- Psycopg2

## How to Run

### 1. Clone the repository

```bash
git clone https://github.com/oussama259796/medallion-data-pipeline-postgresql.git
cd medallion-data-pipeline-postgresql
```

### 2. Create and activate a virtual environment

**Windows**
```bash
python -m venv .venv
.venv\Scripts\activate
```

**Linux / macOS**
```bash
python -m venv .venv
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure your PostgreSQL connection

Update your config or `.env` file with your database credentials.

### 5. Run the pipeline

```bash
python run_pipeline.py
```

## Example Output

```text
Pipeline started
Running: Bronze → CSV to PostgreSQL
Done: Bronze → CSV to PostgreSQL
Running: Silver → Clean & Validate
Done: Silver → Clean & Validate
Running: Gold → Aggregate
Done: Gold → Aggregate
Pipeline completed successfully
```

## What I Learned

While learning data engineering, I was often attracted to larger and more complex systems.

But this project reminded me of something more important:

**real-world data engineering starts with strong fundamentals** — reliable ingestion, clean validation logic, careful transformation, and repeatable execution.

The project is simple in structure, but very close to the real problems businesses face every day.

## Ideal Use Cases

This kind of pipeline is useful for businesses that work with:

- Excel or CSV exports from internal systems
- repeated weekly or daily reports
- sales or operations spreadsheets
- data that needs cleaning before dashboards
- manual reporting workflows that waste time

## Future Improvements

- add unit tests
- add Docker support
- add orchestration with Airflow
- add automated data quality checks
- connect Gold outputs to BI dashboards

## Contact

**Oussama**  
Freelance Data Engineer

I help businesses automate messy CSV, Excel, and API data into clean PostgreSQL databases and reporting-ready tables.

- GitHub: https://github.com/oussama259796
- LinkedIn: https://www.linkedin.com/in/oussema-benkhaoua-8006582b5/

If you have a messy file and want to automate the cleanup process, feel free to reach out.