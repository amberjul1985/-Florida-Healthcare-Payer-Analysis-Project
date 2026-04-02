-- =============================================================================
-- HOSPITAL PRICING ANALYSIS - PostgreSQL DATABASE SETUP
-- =============================================================================
--
-- Project: Hospital Location and Payer Type Impact on Radiation Oncology Costs
-- Purpose: Create PostgreSQL database schema for healthcare pricing data
-- Version: 1.0
-- Created: November 2025
--
-- This script creates:
--   1. Schema for organizing database objects
--   2. Main charges table for hospital pricing data
--   3. Ingestion log table for tracking ETL operations
--   4. Radiology-specific extracted table for focused analysis
--   5. Indexes for query performance optimization
--
-- Database Requirements: PostgreSQL 12+
-- Usage: psql -U postgres -d healthcare_pricing -f database_setup.sql
--
-- =============================================================================

-- =============================================================================
-- SECTION 1: CREATE SCHEMA
-- =============================================================================
-- The "pricing" schema organizes all database objects related to hospital pricing

CREATE SCHEMA IF NOT EXISTS pricing;

COMMENT ON SCHEMA pricing IS 
  'Schema for hospital pricing transparency data and related analytics';

-- =============================================================================
-- SECTION 2: CREATE MAIN CHARGES TABLE
-- =============================================================================
-- This table stores raw hospital pricing data in tall format
-- Each row represents one hospital-CPT-payer combination
--
-- Key fields:
--   - hospital_name: Name of healthcare facility
--   - hospital_address: Full address of facility
--   - payer_name: Insurance payer organization
--   - plan_name: Specific insurance plan
--   - negotiated_dollar: Price agreed between hospital and payer
--   - code | N: CPT codes (up to 6 different procedures per record)
--   - code | N | type: Type of code (CPT, ICD, etc.)
--   - natural_key: Unique identifier for deduplication

CREATE TABLE IF NOT EXISTS pricing.charges (
  "CCN" TEXT,
  "unique_id" TEXT,
  "hospital_name" TEXT,
  "last_updated_on" TEXT,
  "version" TEXT,
  "hospital_location" TEXT,
  "hospital_address" TEXT,
  "license_number | FL" TEXT,
  "To the best of its knowledge and belief, the hospital has included all applicable standard charge information in accordance with the requirements of 45 CFR 180.50, and the information encoded is true, accurate, and complete as of the date indicated" TEXT,
  "payer_name" TEXT,
  "plan_name" TEXT,
  "estimated_amount" TEXT,
  "negotiated_algorithm" TEXT,
  "negotiated_percentage" TEXT,
  "additional_payer_notes" TEXT,
  "negotiated_dollar" TEXT,
  "methodology" TEXT,
  "source_file" TEXT,
  "description" TEXT,
  "code | 1" TEXT,
  "code | 1 | type" TEXT,
  "code | 2" TEXT,
  "code | 2 | type" TEXT,
  "code | 3" TEXT,
  "code | 3 | type" TEXT,
  "code | 4" TEXT,
  "code | 4 | type" TEXT,
  "code | 5" TEXT,
  "code | 5 | type" TEXT,
  "code | 6" TEXT,
  "code | 6 | type" TEXT,
  "billing_class" TEXT,
  "setting" TEXT,
  "drug_unit_of_measurement" TEXT,
  "drug_type_of_measurement" TEXT,
  "modifiers" TEXT,
  "gross" TEXT,
  "discounted_cash" TEXT,
  "min" TEXT,
  "max" TEXT,
  "additional_generic_notes" TEXT,
  "natural_key" TEXT
);

COMMENT ON TABLE pricing.charges IS 
  'Raw hospital pricing data in tall format from CMS transparency files';

-- =============================================================================
-- SECTION 3: CREATE UNIQUE INDEX ON CHARGES TABLE
-- =============================================================================
-- Prevents duplicate rows based on natural_key (hospital + CPT + payer combination)
-- Improves query performance on lookups by natural_key

CREATE UNIQUE INDEX IF NOT EXISTS ux_charges_natural_key 
  ON pricing.charges ("natural_key");

COMMENT ON INDEX pricing.ux_charges_natural_key IS 
  'Unique index on natural_key to ensure no duplicate hospital-CPT-payer combinations';

-- =============================================================================
-- SECTION 4: CREATE INGESTION LOG TABLE
-- =============================================================================
-- Tracks which files have been processed and prevents re-processing
-- Stores file fingerprints (hash) to detect changes
-- Useful for idempotent ETL operations

CREATE TABLE IF NOT EXISTS pricing.ingestion_log (
  source_file TEXT PRIMARY KEY,
  file_size BIGINT,
  file_mtime TIMESTAMPTZ,
  file_hash TEXT,
  inserted_rows BIGINT,
  processed_at TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE pricing.ingestion_log IS 
  'ETL ingestion log: tracks processed files and their checksums';
COMMENT ON COLUMN pricing.ingestion_log.source_file IS 
  'Original source CSV filename';
COMMENT ON COLUMN pricing.ingestion_log.file_hash IS 
  'SHA256 hash of file for change detection';
COMMENT ON COLUMN pricing.ingestion_log.inserted_rows IS 
  'Number of rows inserted from this file';

-- =============================================================================
-- SECTION 5: CREATE RADIOLOGY-SPECIFIC EXTRACTED TABLE
-- =============================================================================
-- Filtered table containing only rows with radiology CPT codes
-- CPT code ranges for radiology: 70010-79999
-- This is the primary table used for the radiation oncology analysis
--
-- Purpose: Focus analysis on radiology procedures only
-- Method: Searches all 6 CPT code columns for valid radiology codes

CREATE TABLE IF NOT EXISTS pricing.charges_radiology AS
SELECT c.*
FROM pricing.charges c
WHERE EXISTS (
  SELECT 1
  FROM (
    VALUES
      (c."code | 1", c."code | 1 | type"),
      (c."code | 2", c."code | 2 | type"),
      (c."code | 3", c."code | 3 | type"),
      (c."code | 4", c."code | 4 | type"),
      (c."code | 5", c."code | 5 | type"),
      (c."code | 6", c."code | 6 | type")
  ) AS v(code_val, code_type)
  -- Keep only CPT rows
  WHERE UPPER(COALESCE(code_type,'')) = 'CPT'
    -- Extract first 5-digit token from code (handles modifiers like 71260-26)
    AND substring(code_val FROM '([0-9]{5})') IS NOT NULL
    -- Filter to radiology range: 70010-79999
    AND (substring(code_val FROM '([0-9]{5})'))::int BETWEEN 70010 AND 79999
);

COMMENT ON TABLE pricing.charges_radiology IS 
  'Extracted table containing only rows with radiology CPT codes (70010-79999)';

-- =============================================================================
-- SECTION 6: CREATE INDEXES ON RADIOLOGY TABLE
-- =============================================================================
-- These indexes improve query performance on the radiology table
-- Each index is on the extracted CPT code from each of the 6 code columns

CREATE INDEX IF NOT EXISTS ix_charges_radiology_cpt1
  ON pricing.charges_radiology ((substring("code | 1" FROM '([0-9]{5})')));

CREATE INDEX IF NOT EXISTS ix_charges_radiology_cpt2
  ON pricing.charges_radiology ((substring("code | 2" FROM '([0-9]{5})')));

CREATE INDEX IF NOT EXISTS ix_charges_radiology_cpt3
  ON pricing.charges_radiology ((substring("code | 3" FROM '([0-9]{5})')));

CREATE INDEX IF NOT EXISTS ix_charges_radiology_cpt4
  ON pricing.charges_radiology ((substring("code | 4" FROM '([0-9]{5})')));

CREATE INDEX IF NOT EXISTS ix_charges_radiology_cpt5
  ON pricing.charges_radiology ((substring("code | 5" FROM '([0-9]{5})')));

CREATE INDEX IF NOT EXISTS ix_charges_radiology_cpt6
  ON pricing.charges_radiology ((substring("code | 6" FROM '([0-9]{5})')));

-- Additional indexes for common query patterns
CREATE INDEX IF NOT EXISTS ix_charges_radiology_hospital_name
  ON pricing.charges_radiology ("hospital_name");

CREATE INDEX IF NOT EXISTS ix_charges_radiology_payer_name
  ON pricing.charges_radiology ("payer_name");

CREATE INDEX IF NOT EXISTS ix_charges_radiology_hospital_location
  ON pricing.charges_radiology ("hospital_location");

COMMENT ON INDEX pricing.ix_charges_radiology_cpt1 IS 
  'Index on CPT code extracted from code slot 1 for faster queries';

-- =============================================================================
-- SECTION 7: USEFUL QUERY TEMPLATES FOR VALIDATION
-- =============================================================================
-- Uncomment and run these queries to verify database setup

-- Check total row counts
-- SELECT COUNT(*) AS total_rows FROM pricing.charges;
-- SELECT COUNT(*) AS radiology_rows FROM pricing.charges_radiology;

-- Count unique hospitals
-- SELECT COUNT(DISTINCT "hospital_name") AS unique_hospitals FROM pricing.charges;

-- Count unique payers
-- SELECT COUNT(DISTINCT "payer_name") AS unique_payers FROM pricing.charges;

-- Count unique CPT codes in radiology table
-- WITH cpt_raw AS (
--   SELECT NULLIF(TRIM("code | 1"), '') AS code FROM pricing.charges_radiology
--   WHERE UPPER("code | 1 | type") = 'CPT'
--   UNION ALL
--   SELECT NULLIF(TRIM("code | 2"), '') FROM pricing.charges_radiology
--   WHERE UPPER("code | 2 | type") = 'CPT'
--   UNION ALL
--   SELECT NULLIF(TRIM("code | 3"), '') FROM pricing.charges_radiology
--   WHERE UPPER("code | 3 | type") = 'CPT'
--   UNION ALL
--   SELECT NULLIF(TRIM("code | 4"), '') FROM pricing.charges_radiology
--   WHERE UPPER("code | 4 | type") = 'CPT'
--   UNION ALL
--   SELECT NULLIF(TRIM("code | 5"), '') FROM pricing.charges_radiology
--   WHERE UPPER("code | 5 | type") = 'CPT'
--   UNION ALL
--   SELECT NULLIF(TRIM("code | 6"), '') FROM pricing.charges_radiology
--   WHERE UPPER("code | 6 | type") = 'CPT'
-- ),
-- cpt_norm AS (
--   SELECT UPPER(
--     REGEXP_REPLACE(
--       REGEXP_REPLACE(code, '\s+', '', 'g'),
--       '[-].*$', ''
--     )
--   ) AS code_norm
--   FROM cpt_raw
-- ),
-- cpt_valid AS (
--   SELECT code_norm
--   FROM cpt_norm
--   WHERE code_norm IS NOT NULL
--     AND code_norm <> ''
--     AND (code_norm ~ '^[0-9]{5}$' OR code_norm ~ '^[0-9]{4}[A-Z]$')
-- )
-- SELECT COUNT(DISTINCT code_norm) AS unique_cpt_codes FROM cpt_valid;

-- Verify radiology data extraction worked
-- SELECT 
--   COUNT(*) AS total_radiology_rows,
--   COUNT(DISTINCT "hospital_name") AS hospitals_with_radiology,
--   COUNT(DISTINCT "payer_name") AS payers_with_radiology
-- FROM pricing.charges_radiology;

-- =============================================================================
-- SECTION 8: DATABASE SUMMARY
-- =============================================================================
--
-- Tables created:
--   1. pricing.charges
--      - Main table with all hospital pricing data
--      - Loaded from hospital transparency CSV files
--      - Contains raw, unfiltered data
--
--   2. pricing.ingestion_log
--      - Tracks which source files have been processed
--      - Prevents duplicate processing
--      - Enables idempotent ETL operations
--
--   3. pricing.charges_radiology
--      - Filtered table with radiology CPT codes only
--      - CPT range: 70010-79999 (radiology procedures)
--      - Primary table used for analysis
--
-- Indexes created:
--   - ux_charges_natural_key: Prevents duplicate rows
--   - ix_charges_radiology_cpt{1-6}: CPT code extraction from each slot
--   - ix_charges_radiology_hospital_name: Hospital lookups
--   - ix_charges_radiology_payer_name: Payer lookups
--   - ix_charges_radiology_hospital_location: Geographic queries
--
-- Next steps:
--   1. Run 2_wide_to_tall_to_pg.py to load data into pricing.charges
--   2. Run query templates above to verify data loaded correctly
--   3. Run 3_refined_data.py to process CPT codes and payer classification
--   4. Run 4_final_data_cleaning.py for analysis-ready dataset
--
-- =============================================================================
-- END OF DATABASE SETUP SCRIPT
-- =============================================================================
