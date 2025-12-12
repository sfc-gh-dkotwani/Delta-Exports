-- ============================================================================
-- Script: 02_create_stream_for_cdc.sql
-- Purpose: Create Snowflake Stream for Change Data Capture (CDC)
-- ============================================================================
USE ROLE SYSADMIN;
USE DATABASE FINANCE_DB;
USE SCHEMA BALANCE_SHEET;

-- ============================================================================
-- SNOWFLAKE STREAMS: Key Feature for Delta Detection
-- ============================================================================
-- Streams track DML changes (INSERT, UPDATE, DELETE) on a table
-- They provide a "change log" since the last consumption
-- Perfect for identifying delta data for exports (supports multiple per day)

-- Create a stream on the main table to capture all changes
CREATE OR REPLACE STREAM BALANCE_SHEET_CHANGES_STREAM
ON TABLE ACTUAL_AA_BALANCE_SHEET
;

-- Stream metadata columns automatically added:
-- METADATA$ACTION: 'INSERT' or 'DELETE'
-- METADATA$ISUPDATE: TRUE if the row is part of an UPDATE
-- METADATA$ROW_ID: Unique identifier for the row

COMMENT ON STREAM BALANCE_SHEET_CHANGES_STREAM IS 
'CDC stream capturing all changes to ACTUAL_AA_BALANCE_SHEET for delta exports';

-- ============================================================================
-- Create Delta History Table to track exported deltas
-- ============================================================================
CREATE OR REPLACE TABLE DELTA_EXPORT_HISTORY (
    EXPORT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    EXPORT_DATE DATE NOT NULL,
    EXPORT_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EXPORT_BATCH_ID VARCHAR(20) NOT NULL,  -- Unique batch ID (YYYYMMDD_HHMMSS format)
    RECORDS_INSERTED NUMBER DEFAULT 0,
    RECORDS_UPDATED NUMBER DEFAULT 0,
    RECORDS_DELETED NUMBER DEFAULT 0,
    TOTAL_DELTA_RECORDS NUMBER DEFAULT 0,
    PARQUET_FILE_PATH VARCHAR(500),
    PARQUET_FILE_SIZE_BYTES NUMBER,
    EXPORT_STATUS VARCHAR(20) DEFAULT 'PENDING',  -- PENDING, COMPLETED, FAILED, NO_CHANGES
    ERROR_MESSAGE VARCHAR(2000),
    EXPORTED_BY VARCHAR(100) DEFAULT CURRENT_USER()
);

COMMENT ON TABLE DELTA_EXPORT_HISTORY IS 
'Tracks delta exports with record counts and file locations - supports multiple exports per day';

-- ============================================================================
-- Create Staging Table for Delta Data (Supports Multiple Exports Per Day)
-- ============================================================================
CREATE OR REPLACE TABLE DAILY_DELTA_STAGING (
    -- All columns from main table
    RECORD_ID NUMBER,
    PORTFOLIO_TICKER VARCHAR(50),
    REPORTING_FREQUENCY VARCHAR(20),
    REPORTING_CURRENCY VARCHAR(10),
    VALUE_DATE DATE,
    PRELIMINARY BOOLEAN,
    SOURCE VARCHAR(100),
    ACCT_BASIS VARCHAR(20),
    BALANCE_SHEET_TYPE VARCHAR(50),
    
    -- Balance Sheet columns
    CASH_AND_EQUIVALENTS NUMBER(20, 4),
    SHORT_TERM_INVESTMENTS NUMBER(20, 4),
    ACCOUNTS_RECEIVABLE NUMBER(20, 4),
    INVENTORY NUMBER(20, 4),
    PREPAID_EXPENSES NUMBER(20, 4),
    OTHER_CURRENT_ASSETS NUMBER(20, 4),
    TOTAL_CURRENT_ASSETS NUMBER(20, 4),
    LONG_TERM_INVESTMENTS NUMBER(20, 4),
    PROPERTY_PLANT_EQUIPMENT NUMBER(20, 4),
    ACCUMULATED_DEPRECIATION NUMBER(20, 4),
    INTANGIBLE_ASSETS NUMBER(20, 4),
    GOODWILL NUMBER(20, 4),
    OTHER_LONG_TERM_ASSETS NUMBER(20, 4),
    TOTAL_NON_CURRENT_ASSETS NUMBER(20, 4),
    TOTAL_ASSETS NUMBER(20, 4),
    ACCOUNTS_PAYABLE NUMBER(20, 4),
    SHORT_TERM_DEBT NUMBER(20, 4),
    ACCRUED_LIABILITIES NUMBER(20, 4),
    DEFERRED_REVENUE NUMBER(20, 4),
    CURRENT_PORTION_LONG_TERM_DEBT NUMBER(20, 4),
    OTHER_CURRENT_LIABILITIES NUMBER(20, 4),
    TOTAL_CURRENT_LIABILITIES NUMBER(20, 4),
    LONG_TERM_DEBT NUMBER(20, 4),
    DEFERRED_TAX_LIABILITIES NUMBER(20, 4),
    PENSION_LIABILITIES NUMBER(20, 4),
    OTHER_LONG_TERM_LIABILITIES NUMBER(20, 4),
    TOTAL_NON_CURRENT_LIABILITIES NUMBER(20, 4),
    TOTAL_LIABILITIES NUMBER(20, 4),
    COMMON_STOCK NUMBER(20, 4),
    PREFERRED_STOCK NUMBER(20, 4),
    ADDITIONAL_PAID_IN_CAPITAL NUMBER(20, 4),
    RETAINED_EARNINGS NUMBER(20, 4),
    TREASURY_STOCK NUMBER(20, 4),
    ACCUMULATED_OTHER_COMPREHENSIVE_INCOME NUMBER(20, 4),
    MINORITY_INTEREST NUMBER(20, 4),
    TOTAL_SHAREHOLDERS_EQUITY NUMBER(20, 4),
    TOTAL_LIABILITIES_AND_EQUITY NUMBER(20, 4),
    
    -- Metadata
    CREATED_TIMESTAMP TIMESTAMP_NTZ,
    UPDATED_TIMESTAMP TIMESTAMP_NTZ,
    CREATED_BY VARCHAR(100),
    DATA_HASH VARCHAR(64),
    
    -- CDC Metadata from Stream
    CHANGE_TYPE VARCHAR(10),                          -- INSERT, UPDATE, DELETE
    CHANGE_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EXPORT_BATCH_ID VARCHAR(20)                       -- Unique batch ID for each export run
);

COMMENT ON TABLE DAILY_DELTA_STAGING IS 
'Staging table for delta records before export to Parquet - supports multiple exports per day via EXPORT_BATCH_ID';

-- ============================================================================
-- Create Sequence for Export Batch Tracking (Optional - for guaranteed uniqueness)
-- ============================================================================
CREATE OR REPLACE SEQUENCE EXPORT_BATCH_SEQ START = 1 INCREMENT = 1;

-- Verify stream creation
SHOW STREAMS LIKE 'BALANCE_SHEET_CHANGES_STREAM';
