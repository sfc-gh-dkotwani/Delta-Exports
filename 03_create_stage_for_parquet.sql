-- ============================================================================
-- Script: 03_create_stage_for_parquet.sql
-- Purpose: Create Snowflake Stage and File Format for Parquet Export
-- ============================================================================
USE ROLE SYSADMIN;
USE DATABASE FINANCE_DB;
USE SCHEMA BALANCE_SHEET;

-- ============================================================================
-- Create Parquet File Format
-- ============================================================================
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY  -- Efficient compression for Parquet
    --BINARY_AS_TEXT = FALSE
    ;

COMMENT ON FILE FORMAT PARQUET_FORMAT IS 
'Parquet file format with Snappy compression for delta exports';

-- ============================================================================
-- Create Internal Stage for Delta Exports
-- ============================================================================
-- Internal stage stores files within Snowflake
CREATE OR REPLACE STAGE DELTA_PARQUET_STAGE
    FILE_FORMAT = PARQUET_FORMAT
    DIRECTORY = (ENABLE = TRUE)  -- Enable directory table for file listing
    COMMENT = 'Internal stage for daily delta Parquet exports';

-- ============================================================================
-- Alternative: External Stage (for S3/Azure/GCS)
-- Uncomment and configure for cloud storage integration
-- ============================================================================

/*
-- Azure Blob Example
CREATE OR REPLACE STORAGE INTEGRATION AZURE_DELTA_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'AZURE'
    ENABLED = TRUE
    AZURE_TENANT_ID = 'your-tenant-id'
    STORAGE_ALLOWED_LOCATIONS = ('azure://your-account.blob.core.windows.net/delta-exports/');

CREATE OR REPLACE STAGE EXTERNAL_DELTA_STAGE
    STORAGE_INTEGRATION = AZURE_DELTA_INTEGRATION
    URL = 'azure://your-account.blob.core.windows.net/delta-exports/'
    FILE_FORMAT = PARQUET_FORMAT
    DIRECTORY = (ENABLE = TRUE);
*/

-- Verify stage creation
SHOW STAGES LIKE 'DELTA_PARQUET_STAGE%';
SHOW FILE FORMATS LIKE 'PARQUET_FORMAT';

