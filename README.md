# Incubyte Data Engineer Technical Assessment

## Project Name

SkyPoints Airline Loyalty Data Engineering Pipeline

## Objective

Build an ETL pipeline to process member data from
multiple countries and a semi-structured JSON feed.

The pipeline performs:

1. Raw data ingestion
2. Data validation
3. Data standardization
4. Age calculation
5. Stale member calculation
6. Latest record selection
7. Country-wise loading
8. JSON redemption flattening
9. Reconciliation and testing

## Technologies

- Python
- Pandas
- SQLite
- SQL
- Pytest
- VS Code

## Input Files

- IND(in).csv
- USA(in).csv
- AUS(Sheet1).csv
- members_flat.txt
- redemptions.json

## Architecture

Source Files
    |
    v
Raw Landing
    |
    v
Validation
    |
    v
Staging
    |
    v
Transformations
    |
    v
Latest Member Snapshot
    |
    v
Country-Specific Tables

JSON Feed
    |
    v
Raw JSON
    |
    v
Flattened Redemption Transactions

## Business Rules

### Age

Age is calculated using Date of Birth and
the supplied processing date.

If DOB is missing, Age is NULL.

### Stale Member

If Last Flight Date is more than 90 days
before the processing date:

Stale_Member = 1

Otherwise:

Stale_Member = 0

If Flight Date is missing, the demo uses 0.

### Latest Record Wins

Records are ranked using:

ROW_NUMBER() OVER (
    PARTITION BY source_namespace, member_id
    ORDER BY feed_date DESC, raw_id DESC
)

Only the latest record is kept.

### Country Separation

Members are inserted into tables based on
their latest country.

Example:

- IND -> table_IND
- USA -> table_USA
- AUS -> table_AUS
- CAN -> table_CAN
- PHIL -> table_PHIL

### JSON Processing

Each transaction in the redemptions array
is converted into a separate database row.

## Data Quality Checks

- Missing Member ID
- Missing Member Name
- Missing Enrollment Date
- Invalid Date Formats
- Future Dates
- DOB Later Than Enrollment
- Duplicate Member Keys
- Raw-to-Staging Reconciliation

## Important Assumptions

The supplied country CSVs use local IDs.

A globally unique member ID is required
to reliably detect international movement.

USA dates are assumed to follow MMDDYYYY.

Dates without leading zeros are normalized.

The Australia record containing 2021-13-13
is rejected.

The sample flat-file Post Code column is
not present and is treated as optional.

For demonstration, files without embedded
feed dates receive a fixed sample feed date.

## Run Project

Install:

python -m pip install -r requirements.txt

Execute:

python -m src.pipeline --as-of 2026-11-19

Test:

python -m pytest -q

## Database

The pipeline creates:

assessment.db

## Expected Sample Output

Raw Members: 14
Staging Members: 13
Rejected Members: 1
Current Members: 13
Redemption Transactions: 2

## Production Architecture

For billions of records per day, use:

Cloud Storage / S3
    |
    v
Spark / Apache Beam
    |
    v
Bronze Layer
    |
    v
Silver Layer
    |
    v
Snowflake / BigQuery
    |
    v
Country Tables

Production considerations include:

- Distributed processing
- Incremental ingestion
- Partitioning
- CDC and MERGE
- Idempotency
- Data quality monitoring
- Dead-letter handling
- Retry and recovery
- Global identity management
- Historical audit trails

The SQLite implementation is a local
functional demonstration, not a
billion-record production deployment.

## AI Usage

AI assistance was used to accelerate
project scaffolding and test development.

Source mappings, validation assumptions,
and results must be reviewed and verified.