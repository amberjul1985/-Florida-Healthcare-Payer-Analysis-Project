# Database Documentation

## Overview

This document explains the PostgreSQL database structure used in the Hospital Pricing Analysis project. The database stores hospital pricing transparency data from Florida in a normalized, queryable format.

**Database Name:** `healthcare_pricing`  
**Schema Name:** `pricing`  
**PostgreSQL Version:** 12+

---

## Database Architecture

### Three-Table Structure

```
┌─────────────────────────────────────────────────────────────┐
│ pricing.charges (Main Raw Data Table)                       │
│ - All hospital pricing data from CMS files                  │
│ - Wide format converted to tall format                      │
│ - Includes all 6 CPT code slots                             │
│ - ~500K-1M rows depending on data volume                    │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  │ Extract radiology CPT codes (70010-79999)
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│ pricing.charges_radiology (Filtered Analysis Table)         │
│ - Contains only rows with radiology CPT codes               │
│ - CPT range: 70010-79999                                    │
│ - Primary table for analysis                                │
│ - ~56K rows (depends on data)                               │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ pricing.ingestion_log (ETL Tracking Table)                  │
│ - Tracks which source files have been processed             │
│ - Prevents duplicate data loading                           │
│ - Stores file hash and row counts                           │
│ - Enables idempotent ETL operations                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Table Details

### 1. pricing.charges (Main Table)

**Purpose:** Store raw hospital pricing data in tall format

**Size:** 
- Expected rows: 500K - 1M (depends on hospitals and payers in Florida)
- Columns: 40+

**Key Columns:**

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `CCN` | TEXT | CMS Certification Number | "060001" |
| `unique_id` | TEXT | Hospital identifier | "FL_HOSPITAL_001" |
| `hospital_name` | TEXT | Hospital name | "Tampa General Hospital" |
| `hospital_address` | TEXT | Full address | "1 Tampa General Circle, Tampa, FL 33606" |
| `hospital_location` | TEXT | City | "Tampa" |
| `payer_name` | TEXT | Insurance company | "AETNA" |
| `plan_name` | TEXT | Specific plan | "Aetna PPO Gold" |
| `code \| 1` | TEXT | First CPT code | "71260" |
| `code \| 1 \| type` | TEXT | Type of code 1 | "CPT" |
| `code \| 2-6` | TEXT | CPT codes 2-6 | (same pattern) |
| `estimated_amount` | TEXT | Billed charge | "5000.00" |
| `negotiated_percentage` | TEXT | % of estimated amount | "0.45" |
| `negotiated_dollar` | TEXT | Negotiated price | "2250.00" |
| `gross` | TEXT | Gross charge | "5000.00" |
| `discounted_cash` | TEXT | Cash discount price | "3000.00" |
| `natural_key` | TEXT | Unique row identifier | (auto-generated) |

**Key Constraint:**
- `natural_key` is UNIQUE to prevent duplicate rows
- Generated from: hospital + CPT code + payer combination

**Primary Use:**
- Raw data store from ETL pipeline
- Source for radiology table extraction
- Backup reference for data validation

---

### 2. pricing.charges_radiology (Filtered Table)

**Purpose:** Extract and focus analysis on radiology procedures only

**Creation Method:**
- Filtered from `pricing.charges` where CPT codes fall in range 70010-79999
- Searches all 6 CPT code slots for valid radiology codes
- Includes only rows with CPT type = "CPT"

**Size:**
- Expected rows: 50K - 100K
- Same columns as charges table (inherited)

**Why This Table?**
- Radiology procedures are the focus of analysis
- Reduces noise from non-radiology data
- Improves query performance
- Makes SQL queries simpler and faster

**CPT Code Range Explained:**
```
70010-79999 = Radiology CPT codes
  70010-71998 = Diagnostic imaging (X-ray, CT, MRI, ultrasound)
  76000-76999 = Diagnostic ultrasound
  77000-77999 = Radiation oncology (treatment planning, delivery)
  78000-79999 = Nuclear medicine
```

**Example Query:**
```sql
-- Get average price by payer for radiology procedures
SELECT 
  "payer_name",
  COUNT(*) AS row_count,
  ROUND(AVG("negotiated_dollar"::numeric), 2) AS avg_price,
  ROUND(STDDEV("negotiated_dollar"::numeric), 2) AS std_price
FROM pricing.charges_radiology
GROUP BY "payer_name"
ORDER BY avg_price DESC;
```

---

### 3. pricing.ingestion_log (Tracking Table)

**Purpose:** Track ETL operations and prevent duplicate processing

**Structure:**

| Column | Type | Description |
|--------|------|-------------|
| `source_file` | TEXT (PK) | Original CSV filename |
| `file_size` | BIGINT | File size in bytes |
| `file_mtime` | TIMESTAMPTZ | File modification time |
| `file_hash` | TEXT | SHA256 hash of file |
| `inserted_rows` | BIGINT | Rows inserted from file |
| `processed_at` | TIMESTAMPTZ | When file was processed |

**How It Works:**
1. Before processing a file, system checks if it's in log
2. If file hash matches, skip processing (already loaded)
3. If new or changed, process and log the file
4. Enables safe, repeatable ETL operations

**Example Entry:**
```
source_file: florida_hospitals_2024_oct.csv
file_size: 52428800
file_mtime: 2024-10-15 14:30:00
file_hash: a3f5d8e1c2b9f4a6c8d2e1f3a5b7c9d1
inserted_rows: 45230
processed_at: 2024-10-15 15:45:23
```

---

## Indexes & Query Performance

### Indexes Created

**1. Unique Index on natural_key**
```sql
CREATE UNIQUE INDEX ux_charges_natural_key 
  ON pricing.charges ("natural_key");
```
- **Purpose:** Prevent duplicate rows
- **Performance:** Fast lookups by natural_key
- **Lookup time:** O(log n)

**2. CPT Code Indexes (Radiology Table)**
```sql
CREATE INDEX ix_charges_radiology_cpt{1-6}
  ON pricing.charges_radiology ((substring("code | N" FROM '([0-9]{5})')));
```
- **Purpose:** Fast CPT code extraction and filtering
- **Use:** When searching for specific CPT codes
- **Count:** 6 indexes (one per CPT slot)

**3. Hospital Name Index**
```sql
CREATE INDEX ix_charges_radiology_hospital_name
  ON pricing.charges_radiology ("hospital_name");
```
- **Purpose:** Fast hospital lookups
- **Use:** Queries filtering by hospital

**4. Payer Name Index**
```sql
CREATE INDEX ix_charges_radiology_payer_name
  ON pricing.charges_radiology ("payer_name");
```
- **Purpose:** Fast payer lookups
- **Use:** Queries filtering by insurance company

**5. Location Index**
```sql
CREATE INDEX ix_charges_radiology_hospital_location
  ON pricing.charges_radiology ("hospital_location");
```
- **Purpose:** Fast geographic queries
- **Use:** City-level and regional analysis

### Query Performance Tips

✅ **Fast Queries (with indexes):**
```sql
-- Filter by CPT code, payer, hospital, city
SELECT * FROM pricing.charges_radiology
WHERE "payer_name" = 'Medicare'
  AND "hospital_location" = 'Tampa';
```

⚠️ **Slower Queries (without indexes):**
```sql
-- String matching without indexes
SELECT * FROM pricing.charges_radiology
WHERE "hospital_name" LIKE '%Hospital%';
```

---

## Data Flow in Project

### Step 1: Data Acquisition
```
hospitalpricingfiles.org (Florida data)
           ↓
   Download CSV files
   (wide and tall formats)
```

### Step 2: Validate Format
```
Run: count_tall_vs_wide.py
Purpose: Count tall vs wide format files
Output: Summary of file types
```

### Step 3: Load into Database
```
Run: 2_wide_to_tall_to_pg.py
Purpose: ETL pipeline
Steps:
  1. Read CSV files
  2. Convert wide → tall format
  3. Generate natural_key
  4. Load into pricing.charges
  5. Log in ingestion_log
Output: pricing.charges table populated
```

### Step 4: Extract Radiology Data
```
Database: Run database_setup.sql
Purpose: Create filtered radiology table
Output: pricing.charges_radiology created
```

### Step 5: Process and Clean
```
Run: 3_refined_data.py
Purpose: CPT code preparation, payer classification
Output: CSV files for analysis (1_filtered_cpt_codes, 3_filtered_cpt_codes_with_payer_group)

Run: 4_final_data_cleaning.py
Purpose: Final cleaning, imputation, feature engineering
Output: 4_cleaned_prices.csv (analysis-ready)
```

### Step 6: Analysis & Visualization
```
Run: 5_analysis_and_visual.py
Run: 6_geospatial_visual.py
Output: Statistical tables, charts, maps
```

---

## Database Maintenance

### Connection String

```
postgresql://username:password@localhost:5432/healthcare_pricing
```

### Common Admin Tasks

**Check database size:**
```sql
SELECT 
  pg_size_pretty(pg_database_size('healthcare_pricing')) AS database_size;
```

**Check table sizes:**
```sql
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'pricing'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

**Check row counts:**
```sql
SELECT 
  'charges' AS table_name,
  COUNT(*) AS row_count
FROM pricing.charges
UNION ALL
SELECT 
  'charges_radiology',
  COUNT(*)
FROM pricing.charges_radiology
UNION ALL
SELECT 
  'ingestion_log',
  COUNT(*)
FROM pricing.ingestion_log;
```

**Rebuild indexes (maintenance):**
```sql
REINDEX SCHEMA pricing;
```

---

## Troubleshooting

### Issue: "Natural Key Violation" Error
**Problem:** Duplicate row attempted during data load
**Solution:** This is expected behavior - duplicate prevention working correctly
**Action:** Check ingestion_log to see if file was already processed

### Issue: No Data in charges_radiology Table
**Problem:** Radiology table is empty after setup
**Solution:** Must first populate pricing.charges with data
**Action:** Run 2_wide_to_tall_to_pg.py first

### Issue: Slow Queries
**Problem:** Queries taking too long
**Solution:** Verify indexes exist and statistics are current
**Action:** Run `REINDEX SCHEMA pricing; ANALYZE;`

### Issue: Connection Timeout
**Problem:** Cannot connect to database
**Solution:** Verify PostgreSQL is running and credentials correct
**Action:** Test connection with `psql -U username -d healthcare_pricing`

---

## Security Notes

⚠️ **Important:**
- Never commit `.env` file with credentials to GitHub
- Use strong passwords for PostgreSQL users
- Restrict database access to authorized team members only
- Consider encryption for sensitive connection strings

---

## References

- PostgreSQL Documentation: https://www.postgresql.org/docs/
- CMS Hospital Pricing Files: https://hospitalpricingfiles.org/
- CPT Code References: https://www.aapc.com/cpt-codes/

---

**Last Updated:** November 2025  
**Database Version:** 1.0  
**Status:** Production Ready

