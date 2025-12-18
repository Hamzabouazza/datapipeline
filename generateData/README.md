# 🏦 Banking Data Pipeline: PySpark + DBT

A complete, beginner-friendly banking data pipeline that generates realistic transaction data with fraud detection, credit risk assessment, and customer analytics.

---

## 📖 Table of Contents

1. [What is This Project?](#what-is-this-project)
2. [Understanding the Technologies](#understanding-the-technologies)
3. [How PySpark Generates Data](#how-pyspark-generates-data)
4. [What DBT Does For You](#what-dbt-does-for-you)
5. [The Star Schema Pattern](#the-star-schema-pattern)
6. [Quick Start Guide](#quick-start-guide)
7. [Project Structure](#project-structure)
8. [Sample Queries](#sample-queries)
9. [Troubleshooting](#troubleshooting)

---

## 🎯 What is This Project?

This project simulates a **complete banking system** with realistic data. Think of it as building a mini-bank's data warehouse from scratch!

### What You Get:
- **Fake but realistic banking data** - Customers, accounts, transactions, loans, cards
- **A data warehouse** - Organized data ready for analysis
- **Analytics reports** - Fraud detection, customer segmentation, risk assessment

### Real-World Use Cases:
- 🔍 **Fraud Detection** - Find suspicious transactions
- 👥 **Customer Segmentation** - Group customers by behavior
- 💳 **Credit Risk** - Assess loan default probability
- 📊 **Business Intelligence** - Branch performance, transaction trends

---

## 🧠 Understanding the Technologies

### What is PySpark?

**PySpark** is Python's interface to Apache Spark - a powerful tool for processing large amounts of data.

```
Think of it like this:
┌─────────────────────────────────────────────────────────┐
│  Python (easy to write) + Spark (fast processing)      │
│                        =                                │
│              PySpark (best of both!)                    │
└─────────────────────────────────────────────────────────┘
```

**Why use PySpark?**
- ⚡ **Speed**: Can process millions of rows in seconds
- 📈 **Scalability**: Same code works on your laptop or a cluster of 1000 servers
- 🔧 **Features**: Built-in functions for data manipulation, SQL support

### What is DBT (Data Build Tool)?

**DBT** transforms raw data into analysis-ready tables using SQL. It's like having a smart assistant that organizes your messy data.

```
Without DBT:                          With DBT:
┌──────────────┐                     ┌──────────────┐
│  Raw Data    │                     │  Raw Data    │
│  (messy)     │                     │  (messy)     │
└──────┬───────┘                     └──────┬───────┘
       │                                    │
       │ Write complex                      │ Write simple
       │ Python scripts                     │ SQL files
       │                                    │
       ▼                                    ▼
┌──────────────┐                     ┌──────────────┐
│  Clean Data  │                     │  Clean Data  │ ← DBT handles:
│  (manual     │                     │  (automatic  │   • Dependencies
│   work)      │                     │   magic!)    │   • Testing
└──────────────┘                     └──────────────┘   • Documentation
```

**What DBT gives you:**
1. **Organization** - Separates raw, staging, and final data
2. **Dependencies** - Knows which tables need to be built first
3. **Testing** - Validates your data automatically
4. **Documentation** - Auto-generates data documentation
5. **Lineage** - Shows where each piece of data comes from

### What is PostgreSQL?

**PostgreSQL** is where all your data lives - a powerful, free database.

```
┌─────────────────────────────────────────────────────────┐
│                    PostgreSQL Database                  │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │raw_customers│  │raw_accounts │  │raw_transact │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
│         │               │                │              │
│         └───────────────┼────────────────┘              │
│                         ▼                               │
│                    DBT transforms                       │
│                         │                               │
│         ┌───────────────┼───────────────┐              │
│         ▼               ▼               ▼              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │dim_customers│  │fct_transact │  │analytics    │     │
│  └─────────────┘  └─────────────┘  └─────────────┘     │
└─────────────────────────────────────────────────────────┘
```

---

## ⚙️ How PySpark Generates Data

### The Magic Behind Synthetic Data

We use a library called **dbldatagen** (Databricks Labs Data Generator) to create realistic fake data.

### Step-by-Step Process:

#### 1. Define the Data Structure

```python
# In banking_generator.py, we define what data looks like:
customer_spec = (
    dg.DataGenerator(
        spark,                    # Use Spark for processing
        name="customers",         # Name this data generator
        rows=5000,               # Create 5000 customers
        partitions=8,            # Split work across 8 workers
    )
    .withIdOutput()              # Add an ID column
    .withColumn(
        "customer_id",           # Column name
        StringType(),            # Data type (text)
        format="CUST%08d",       # Format: CUST00000001, CUST00000002...
        baseColumn="id"          # Base it on the ID
    )
    .withColumn(
        "first_name",
        StringType(),
        template=r"\\w",         # Generate random word (name)
        random=True
    )
    .withColumn(
        "credit_score",
        IntegerType(),           # Whole number
        minValue=300,            # Minimum credit score
        maxValue=850,            # Maximum credit score
        random=True              # Random value in range
    )
)
```

#### 2. Build and Save the Data

```python
# Generate the DataFrame (table)
df = customer_spec.build()

# Write to PostgreSQL
df.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/device_db") \
    .option("dbtable", "raw_customers") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .mode("overwrite") \
    .save()
```

### Data Generation Techniques Used:

| Technique | Example | Use Case |
|-----------|---------|----------|
| **Format strings** | `"CUST%08d"` → CUST00000001 | IDs, codes |
| **Templates** | `r"\\w"` → "lorem" | Names, text |
| **Value lists** | `values=["Active", "Closed"]` | Status fields |
| **Weighted random** | `weights=[85, 10, 5]` | Realistic distributions |
| **Ranges** | `minValue=300, maxValue=850` | Numeric limits |
| **Date ranges** | `begin="2020-01-01", end="2024-12-31"` | Time periods |

### Realistic Data Patterns:

```python
# Most accounts are Active (85%), few are Closed (5%)
.withColumn(
    "account_status",
    StringType(),
    values=["Active", "Dormant", "Closed", "Frozen"],
    weights=[85, 8, 5, 2],  # 85% Active, 8% Dormant, etc.
    random=True
)

# Fraud is rare (~3%) but more common for large amounts
.withColumn(
    "fraud_flag",
    BooleanType(),
    values=[False, True],
    weights=[97, 3],  # Only 3% are fraud
    random=True
)
```

### The Data Flow:

```
┌─────────────────────────────────────────────────────────────────┐
│                    PySpark Data Generation                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. DEFINE SCHEMA                                               │
│     ┌─────────────────────────────────────────┐                │
│     │ DataGenerator(rows=5000)                 │                │
│     │   .withColumn("name", template="\\w")    │                │
│     │   .withColumn("score", min=300, max=850) │                │
│     └─────────────────────────────────────────┘                │
│                         │                                       │
│                         ▼                                       │
│  2. GENERATE DATA (in parallel using Spark)                    │
│     ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐           │
│     │Worker 1 │ │Worker 2 │ │Worker 3 │ │Worker 4 │           │
│     │ 1250    │ │ 1250    │ │ 1250    │ │ 1250    │           │
│     │ rows    │ │ rows    │ │ rows    │ │ rows    │           │
│     └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘           │
│          │           │           │           │                 │
│          └───────────┴─────┬─────┴───────────┘                 │
│                            ▼                                    │
│  3. COMBINE & SAVE                                             │
│     ┌─────────────────────────────────────────┐                │
│     │         5000 rows combined               │                │
│     │              │                           │                │
│     │              ▼                           │                │
│     │       PostgreSQL Table                   │                │
│     │       (raw_customers)                    │                │
│     └─────────────────────────────────────────┘                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 What DBT Does For You

### DBT's Three-Layer Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DBT TRANSFORMATION LAYERS                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  LAYER 1: STAGING (stg_)                                       │
│  ─────────────────────                                          │
│  • Clean raw data                                               │
│  • Rename columns to standard names                             │
│  • Add calculated fields                                        │
│  • Fix data types                                               │
│                                                                 │
│  Example: stg_customers.sql                                     │
│  ┌─────────────────────────────────────────────────────┐       │
│  │ SELECT                                               │       │
│  │   id as customer_pk,        -- Rename for clarity   │       │
│  │   INITCAP(first_name),      -- Capitalize names     │       │
│  │   EXTRACT(YEAR FROM AGE(date_of_birth)) as age      │       │
│  │ FROM raw_customers          -- Calculate age        │       │
│  └─────────────────────────────────────────────────────┘       │
│                         │                                       │
│                         ▼                                       │
│  LAYER 2: DIMENSIONS & FACTS (dim_, fct_)                      │
│  ────────────────────────────────────────                       │
│  • Build the star schema                                        │
│  • Join related data                                            │
│  • Add business logic                                           │
│                                                                 │
│  Example: dim_customers.sql                                     │
│  ┌─────────────────────────────────────────────────────┐       │
│  │ SELECT                                               │       │
│  │   c.*,                                               │       │
│  │   COUNT(a.account_id) as total_accounts,            │       │
│  │   SUM(t.amount) as lifetime_value,                  │       │
│  │   CASE WHEN last_txn > 30 THEN 'Inactive'           │       │
│  │        ELSE 'Active' END as status                  │       │
│  │ FROM stg_customers c                                 │       │
│  │ JOIN stg_accounts a ...                              │       │
│  └─────────────────────────────────────────────────────┘       │
│                         │                                       │
│                         ▼                                       │
│  LAYER 3: ANALYTICS                                            │
│  ─────────────────────                                          │
│  • Business intelligence views                                  │
│  • Ready-to-use reports                                         │
│  • KPIs and metrics                                             │
│                                                                 │
│  Example: fraud_detection_report.sql                           │
│  ┌─────────────────────────────────────────────────────┐       │
│  │ SELECT                                               │       │
│  │   customer_id,                                       │       │
│  │   COUNT(fraud_events) as fraud_count,               │       │
│  │   SUM(fraud_amount) as total_fraud_amount,          │       │
│  │   CASE WHEN fraud_count > 3 THEN 'High Risk'        │       │
│  │        ELSE 'Normal' END as risk_level              │       │
│  │ FROM fct_fraud_events                                │       │
│  │ GROUP BY customer_id                                 │       │
│  └─────────────────────────────────────────────────────┘       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### DBT Commands Explained

| Command | What It Does | When to Use |
|---------|-------------|-------------|
| `dbt run` | Builds all models (tables/views) | Run your transformations |
| `dbt test` | Validates data quality | Check for errors |
| `dbt docs generate` | Creates documentation | Before sharing |
| `dbt docs serve` | Opens docs in browser | To explore lineage |
| `dbt debug` | Tests database connection | When things don't work |

### DBT Model Types

```sql
-- VIEW (default): Runs every time you query
{{ config(materialized='view') }}
SELECT * FROM raw_customers

-- TABLE: Stores data, faster queries
{{ config(materialized='table') }}
SELECT * FROM raw_customers

-- INCREMENTAL: Only processes new data (efficient!)
{{ config(materialized='incremental', unique_key='id') }}
SELECT * FROM raw_transactions
{% if is_incremental() %}
WHERE timestamp > (SELECT MAX(timestamp) FROM {{ this }})
{% endif %}
```

### DBT's Ref Function

The magic `{{ ref() }}` function handles dependencies automatically:

```sql
-- DBT knows to build dim_customers BEFORE this model
SELECT * 
FROM {{ ref('dim_customers') }}  -- Not 'dim_customers' directly!
JOIN {{ ref('fct_transactions') }}
```

```
DBT builds in correct order:
1. stg_customers (no dependencies)
2. stg_accounts (no dependencies)  
3. dim_customers (depends on stg_customers)
4. fct_transactions (depends on stg_*)
5. fraud_detection_report (depends on fct_*)
```

---

## ⭐ The Star Schema Pattern

### What is a Star Schema?

A **star schema** organizes data with:
- **Fact tables** (center) - Events, transactions, measurements
- **Dimension tables** (points of star) - Descriptive attributes

```
                    ┌─────────────┐
                    │dim_customers│
                    │  - name     │
                    │  - segment  │
                    │  - score    │
                    └──────┬──────┘
                           │
    ┌─────────────┐       │        ┌─────────────┐
    │ dim_date    │       │        │dim_accounts │
    │ - year      ├───────┼────────┤ - type      │
    │ - month     │       │        │ - balance   │
    │ - day       │       │        │ - status    │
    └─────────────┘       │        └─────────────┘
                          │
                 ┌────────┴────────┐
                 │ fct_transactions │  ← FACT TABLE (center)
                 │ - amount         │
                 │ - timestamp      │
                 │ - customer_key   │  ← Links to dimensions
                 │ - account_key    │
                 │ - date_key       │
                 └─────────────────┘
                          │
    ┌─────────────┐       │        ┌─────────────┐
    │ dim_cards   │       │        │dim_branches │
    │ - type      ├───────┴────────┤ - city      │
    │ - network   │                │ - state     │
    └─────────────┘                └─────────────┘
```

### Why Use Star Schema?

| Benefit | Explanation |
|---------|-------------|
| **Fast queries** | Simple JOINs, optimized for analytics |
| **Easy to understand** | Business users can navigate easily |
| **Flexible analysis** | Slice data by any dimension |
| **Scalable** | Add new dimensions without changing facts |

### Our Star Schema Tables

**Dimension Tables (describe things):**
| Table | Description | Key Attributes |
|-------|-------------|----------------|
| `dim_customers` | Who are our customers? | name, segment, credit_score |
| `dim_accounts` | What accounts exist? | type, balance, status |
| `dim_branches` | Where are our branches? | city, state, performance |
| `dim_cards` | What cards are issued? | type, network, utilization |
| `dim_loans` | What loans are active? | type, amount, status |
| `dim_date` | When did things happen? | year, month, weekday |

**Fact Tables (measure things):**
| Table | Description | Key Metrics |
|-------|-------------|-------------|
| `fct_transactions` | Every transaction | amount, status, fraud_flag |
| `fct_fraud_events` | Suspected fraud | risk_level, indicator |
| `fct_daily_account_balance` | Daily balances | inflow, outflow |
| `fct_customer_daily_summary` | Daily activity | transaction_count |

---

## 🚀 Quick Start Guide

### Prerequisites

```bash
# 1. Install Python 3.8+ and Java 11+
python --version  # Should be 3.8+
java -version     # Should be 11+

# 2. Start PostgreSQL (using Docker)
docker-compose up -d

# 3. Activate virtual environment
source env/bin/activate  # Mac/Linux
# or: env\Scripts\activate  # Windows

# 4. Install dependencies
pip install -r requirements.txt
```

### Run the Pipeline

```bash
cd generateData

# Option 1: Full pipeline (recommended first time)
python main.py --job full

# Option 2: Step by step
python main.py --job generate    # Create fake data
python main.py --job transform   # Run DBT models
python main.py --job docs        # Generate documentation
```

### Expected Output

```
============================================================
🏦 BANKING DATA PIPELINE
============================================================

🏦 STEP 1: Generating Banking Data with PySpark
   ✅ Written 50 rows to raw_branches
   ✅ Written 5000 rows to raw_customers
   ✅ Written 8000 rows to raw_accounts
   ✅ Written 6000 rows to raw_cards
   ✅ Written 2000 rows to raw_loans
   ✅ Written 50000 rows to raw_transactions

🔄 STEP 2: Running DBT Transformations
   ✅ 26 of 26 models completed successfully

✅ PIPELINE COMPLETED SUCCESSFULLY
   Total time: ~15 seconds
```

---

## 📁 Project Structure

```
datapipeline/
│
├── 📄 requirements.txt          # Python dependencies
├── 📄 .gitignore               # Files to ignore in git
│
├── 📁 generateData/
│   │
│   ├── 📄 main.py              # 🚀 Main entry point
│   │                            # Run with: python main.py --job full
│   │
│   ├── 📄 .env                  # Environment variables (DB credentials)
│   │
│   ├── 📁 config/
│   │   └── 📄 config.py         # Loads settings from .env
│   │
│   ├── 📁 core/
│   │   └── 📄 DataCore.py       # Spark session manager
│   │
│   ├── 📁 job/
│   │   └── 📄 banking_generator.py  # 🏭 Data generation logic
│   │                                 # Creates all 6 raw tables
│   │
│   └── 📁 dbt_transform/        # 🔄 DBT project
│       │
│       ├── 📄 dbt_project.yml   # DBT configuration
│       │
│       └── 📁 models/
│           │
│           ├── 📁 staging/      # Layer 1: Clean raw data
│           │   ├── sources.yml  # Define raw table sources
│           │   ├── stg_customers.sql
│           │   ├── stg_accounts.sql
│           │   ├── stg_transactions.sql
│           │   ├── stg_cards.sql
│           │   ├── stg_loans.sql
│           │   └── stg_branches.sql
│           │
│           └── 📁 marts/        # Layer 2 & 3: Business models
│               │
│               ├── 📁 dimensions/   # Star schema dimensions
│               │   ├── dim_customers.sql
│               │   ├── dim_accounts.sql
│               │   ├── dim_branches.sql
│               │   ├── dim_cards.sql
│               │   ├── dim_loans.sql
│               │   └── dim_date.sql
│               │
│               ├── 📁 facts/        # Star schema facts
│               │   ├── fct_transactions.sql
│               │   ├── fct_fraud_events.sql
│               │   ├── fct_daily_account_balance.sql
│               │   ├── fct_customer_daily_summary.sql
│               │   ├── fct_loan_payments.sql
│               │   └── fct_card_transactions.sql
│               │
│               └── 📁 analytics/    # Business intelligence
│                   ├── fraud_detection_report.sql
│                   ├── customer_segmentation.sql
│                   ├── credit_risk_assessment.sql
│                   ├── transaction_trends.sql
│                   ├── account_health_dashboard.sql
│                   ├── loan_portfolio_analysis.sql
│                   ├── branch_performance.sql
│                   └── network_performance_analysis.sql
```

---

## 🔍 Sample Queries

### Connect to PostgreSQL

```bash
# Using psql
docker exec -it my-postgres-container psql -U postgres -d device_db

# Or use any SQL client (DBeaver, pgAdmin, etc.)
# Host: localhost, Port: 5432, Database: device_db
```

### 1. Find Fraud Patterns

```sql
-- Top customers with suspicious activity
SELECT 
    full_name,
    total_fraud_attempts,
    total_fraud_amount,
    fraud_risk_level,
    recommended_action
FROM fraud_detection_report
WHERE fraud_risk_level = 'High Risk'
ORDER BY total_fraud_amount DESC
LIMIT 10;
```

### 2. Customer Segmentation

```sql
-- How are customers distributed?
SELECT 
    customer_lifecycle_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(last_30d_volume)::numeric, 2) as avg_monthly_volume
FROM customer_segmentation
GROUP BY customer_lifecycle_segment
ORDER BY customer_count DESC;
```

### 3. Credit Risk Overview

```sql
-- Customers who might default
SELECT 
    full_name,
    credit_score,
    total_loans,
    debt_to_income_ratio,
    overall_risk_level,
    credit_recommendation
FROM credit_risk_assessment
WHERE overall_risk_level IN ('High Risk', 'Medium-High Risk')
ORDER BY debt_to_income_ratio DESC;
```

### 4. Transaction Trends

```sql
-- Daily transaction patterns
SELECT 
    day_name,
    SUM(total_transactions) as total_txns,
    ROUND(AVG(total_volume)::numeric, 2) as avg_volume,
    ROUND(AVG(fraud_rate_percent)::numeric, 2) as avg_fraud_rate
FROM transaction_trends
GROUP BY day_name
ORDER BY total_txns DESC;
```

### 5. Branch Performance

```sql
-- Best performing branches
SELECT 
    branch_name,
    city,
    total_customers,
    total_deposits,
    performance_tier
FROM branch_performance
ORDER BY total_deposits DESC
LIMIT 10;
```

---

## 🐛 Troubleshooting

### Common Issues

| Error | Cause | Solution |
|-------|-------|----------|
| `PyArrow not installed` | Missing dependency | `pip install pyarrow>=17.0.0` |
| `Database does not exist` | DB not created | `docker exec my-postgres-container psql -U postgres -c "CREATE DATABASE device_db;"` |
| `ROUND function error` | PostgreSQL type issue | Cast to numeric: `ROUND(value::numeric, 2)` |
| `Connection refused` | PostgreSQL not running | `docker-compose up -d` |
| `JDBC driver not found` | JAR path wrong | Check `JAR_PATH` in `.env` |

### Debug Commands

```bash
# Check Docker is running
docker ps

# Check DBT connection
cd dbt_transform
dbt debug

# See what tables exist
docker exec my-postgres-container psql -U postgres -d device_db -c "\dt"

# Check data counts
docker exec my-postgres-container psql -U postgres -d device_db -c "
SELECT 
    'raw_customers' as table_name, COUNT(*) FROM raw_customers
UNION ALL
SELECT 'raw_transactions', COUNT(*) FROM raw_transactions;
"
```

---

## 📚 Learning Resources

### For Beginners

- 📖 [DBT Fundamentals Course](https://courses.getdbt.com/courses/fundamentals) - Free, excellent
- 📖 [PySpark Tutorial](https://spark.apache.org/docs/latest/api/python/getting_started/index.html)
- 📖 [Star Schema Basics](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)

### Documentation

- [DBT Docs](https://docs.getdbt.com)
- [PySpark API](https://spark.apache.org/docs/latest/api/python/)
- [PostgreSQL Manual](https://www.postgresql.org/docs/)

---

## 📊 Data Summary

| Layer | Table Count | Description |
|-------|-------------|-------------|
| **Raw** | 6 tables | PySpark-generated data |
| **Staging** | 6 views | Cleaned and standardized |
| **Dimensions** | 6 tables | Star schema descriptors |
| **Facts** | 6 tables | Transaction and event data |
| **Analytics** | 8 views | Business intelligence |
| **Total** | **32 models** | Complete data warehouse |

---

## 🎓 Key Concepts Glossary

| Term | Definition |
|------|------------|
| **ETL** | Extract, Transform, Load - moving and processing data |
| **Data Warehouse** | Central repository for analysis-ready data |
| **Star Schema** | Fact tables surrounded by dimension tables |
| **Dimension** | Descriptive attributes (who, what, where, when) |
| **Fact** | Measurable events (transactions, amounts) |
| **Staging** | Intermediate layer for data cleaning |
| **Incremental** | Processing only new/changed data |
| **Materialization** | How DBT stores results (view, table, incremental) |

---

**Built with ❤️ for learning modern data engineering**

*Questions? Check the troubleshooting section or open an issue!*
