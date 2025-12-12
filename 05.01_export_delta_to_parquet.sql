-- ============================================================================
-- Script: 05_export_delta_to_parquet.sql
-- Purpose: Export Delta Data to Parquet Files using Snowflake Stream
--          Supports multiple exports per day with datetime-based file naming
-- ============================================================================
USE ROLE SYSADMIN;
USE DATABASE FINANCE_DB;
USE SCHEMA BALANCE_SHEET;

-- ============================================================================
-- Stored Procedure: Capture Delta and Export to Parquet
-- Uses datetime (YYYYMMDD_HHMMSS) for unique file naming per export
-- ============================================================================
CREATE OR REPLACE PROCEDURE EXPORT_DELTA_TO_PARQUET()
    RETURNS VARIANT
    LANGUAGE SQL
    EXECUTE AS CALLER
AS
$$
DECLARE
    v_export_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_export_date DATE DEFAULT CURRENT_DATE();
    v_batch_id VARCHAR;
    v_file_path VARCHAR;
    v_stage_path VARCHAR;
    v_inserted NUMBER DEFAULT 0;
    v_updated NUMBER DEFAULT 0;
    v_deleted NUMBER DEFAULT 0;
    v_total NUMBER DEFAULT 0;
    v_has_data BOOLEAN;
    v_result VARIANT;
    v_copy_sql VARCHAR;
BEGIN
    -- Generate unique batch ID using datetime (YYYYMMDD_HHMMSS format)
    v_batch_id := TO_VARCHAR(v_export_timestamp, 'YYYYMMDD_HH24MISS');
    
    -- Check if stream has data (must assign to variable first)
    SELECT SYSTEM$STREAM_HAS_DATA('BALANCE_SHEET_CHANGES_STREAM') INTO v_has_data;
    
    IF (v_has_data) THEN
        
        -- Step 1: Capture delta data from stream into staging table with batch ID
        INSERT INTO DAILY_DELTA_STAGING (
            RECORD_ID, PORTFOLIO_TICKER, REPORTING_FREQUENCY, REPORTING_CURRENCY,
            VALUE_DATE, PRELIMINARY, SOURCE, ACCT_BASIS, BALANCE_SHEET_TYPE,
            CASH_AND_EQUIVALENTS, SHORT_TERM_INVESTMENTS, ACCOUNTS_RECEIVABLE,
            INVENTORY, PREPAID_EXPENSES, OTHER_CURRENT_ASSETS, TOTAL_CURRENT_ASSETS,
            LONG_TERM_INVESTMENTS, PROPERTY_PLANT_EQUIPMENT, ACCUMULATED_DEPRECIATION,
            INTANGIBLE_ASSETS, GOODWILL, OTHER_LONG_TERM_ASSETS, TOTAL_NON_CURRENT_ASSETS,
            TOTAL_ASSETS, ACCOUNTS_PAYABLE, SHORT_TERM_DEBT, ACCRUED_LIABILITIES,
            DEFERRED_REVENUE, CURRENT_PORTION_LONG_TERM_DEBT, OTHER_CURRENT_LIABILITIES,
            TOTAL_CURRENT_LIABILITIES, LONG_TERM_DEBT, DEFERRED_TAX_LIABILITIES,
            PENSION_LIABILITIES, OTHER_LONG_TERM_LIABILITIES, TOTAL_NON_CURRENT_LIABILITIES,
            TOTAL_LIABILITIES, COMMON_STOCK, PREFERRED_STOCK, ADDITIONAL_PAID_IN_CAPITAL,
            RETAINED_EARNINGS, TREASURY_STOCK, ACCUMULATED_OTHER_COMPREHENSIVE_INCOME,
            MINORITY_INTEREST, TOTAL_SHAREHOLDERS_EQUITY, TOTAL_LIABILITIES_AND_EQUITY,
            CREATED_TIMESTAMP, UPDATED_TIMESTAMP, CREATED_BY, DATA_HASH,
            CHANGE_TYPE, CHANGE_TIMESTAMP, EXPORT_BATCH_ID
        )
        SELECT 
            RECORD_ID, PORTFOLIO_TICKER, REPORTING_FREQUENCY, REPORTING_CURRENCY,
            VALUE_DATE, PRELIMINARY, SOURCE, ACCT_BASIS, BALANCE_SHEET_TYPE,
            CASH_AND_EQUIVALENTS, SHORT_TERM_INVESTMENTS, ACCOUNTS_RECEIVABLE,
            INVENTORY, PREPAID_EXPENSES, OTHER_CURRENT_ASSETS, TOTAL_CURRENT_ASSETS,
            LONG_TERM_INVESTMENTS, PROPERTY_PLANT_EQUIPMENT, ACCUMULATED_DEPRECIATION,
            INTANGIBLE_ASSETS, GOODWILL, OTHER_LONG_TERM_ASSETS, TOTAL_NON_CURRENT_ASSETS,
            TOTAL_ASSETS, ACCOUNTS_PAYABLE, SHORT_TERM_DEBT, ACCRUED_LIABILITIES,
            DEFERRED_REVENUE, CURRENT_PORTION_LONG_TERM_DEBT, OTHER_CURRENT_LIABILITIES,
            TOTAL_CURRENT_LIABILITIES, LONG_TERM_DEBT, DEFERRED_TAX_LIABILITIES,
            PENSION_LIABILITIES, OTHER_LONG_TERM_LIABILITIES, TOTAL_NON_CURRENT_LIABILITIES,
            TOTAL_LIABILITIES, COMMON_STOCK, PREFERRED_STOCK, ADDITIONAL_PAID_IN_CAPITAL,
            RETAINED_EARNINGS, TREASURY_STOCK, ACCUMULATED_OTHER_COMPREHENSIVE_INCOME,
            MINORITY_INTEREST, TOTAL_SHAREHOLDERS_EQUITY, TOTAL_LIABILITIES_AND_EQUITY,
            CREATED_TIMESTAMP, UPDATED_TIMESTAMP, CREATED_BY, DATA_HASH,
            CASE 
                WHEN METADATA$ACTION = 'INSERT' AND NOT METADATA$ISUPDATE THEN 'INSERT'
                WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE THEN 'UPDATE'
                WHEN METADATA$ACTION = 'DELETE' AND NOT METADATA$ISUPDATE THEN 'DELETE'
                ELSE 'UPDATE'
            END,
            CURRENT_TIMESTAMP(),
            :v_batch_id  -- Use the batch ID for this export
        FROM BALANCE_SHEET_CHANGES_STREAM
        WHERE METADATA$ACTION = 'INSERT' OR 
              (METADATA$ACTION = 'DELETE' AND NOT METADATA$ISUPDATE);

        -- Get counts by change type for THIS batch only
        SELECT COUNT(*) INTO v_inserted 
        FROM DAILY_DELTA_STAGING 
        WHERE EXPORT_BATCH_ID = :v_batch_id AND CHANGE_TYPE = 'INSERT';
        
        SELECT COUNT(*) INTO v_updated 
        FROM DAILY_DELTA_STAGING 
        WHERE EXPORT_BATCH_ID = :v_batch_id AND CHANGE_TYPE = 'UPDATE';
        
        SELECT COUNT(*) INTO v_deleted 
        FROM DAILY_DELTA_STAGING 
        WHERE EXPORT_BATCH_ID = :v_batch_id AND CHANGE_TYPE = 'DELETE';
        
        SELECT COUNT(*) INTO v_total 
        FROM DAILY_DELTA_STAGING 
        WHERE EXPORT_BATCH_ID = :v_batch_id;

        -- Step 2: Generate Parquet file path with datetime (YYYYMMDD_HHMMSS)
        v_file_path := 'balance_sheet_delta_' || v_batch_id || '.parquet';
        v_stage_path := '@DELTA_PARQUET_STAGE/' || v_file_path;

        -- Step 3: Export to Parquet using EXECUTE IMMEDIATE for dynamic path
        v_copy_sql := '
            COPY INTO ' || v_stage_path || '
            FROM (
                SELECT 
                    RECORD_ID, PORTFOLIO_TICKER, REPORTING_FREQUENCY, REPORTING_CURRENCY,
                    VALUE_DATE, PRELIMINARY, SOURCE, ACCT_BASIS, BALANCE_SHEET_TYPE,
                    CASH_AND_EQUIVALENTS, SHORT_TERM_INVESTMENTS, ACCOUNTS_RECEIVABLE,
                    INVENTORY, PREPAID_EXPENSES, OTHER_CURRENT_ASSETS, TOTAL_CURRENT_ASSETS,
                    LONG_TERM_INVESTMENTS, PROPERTY_PLANT_EQUIPMENT, ACCUMULATED_DEPRECIATION,
                    INTANGIBLE_ASSETS, GOODWILL, OTHER_LONG_TERM_ASSETS, TOTAL_NON_CURRENT_ASSETS,
                    TOTAL_ASSETS, ACCOUNTS_PAYABLE, SHORT_TERM_DEBT, ACCRUED_LIABILITIES,
                    DEFERRED_REVENUE, CURRENT_PORTION_LONG_TERM_DEBT, OTHER_CURRENT_LIABILITIES,
                    TOTAL_CURRENT_LIABILITIES, LONG_TERM_DEBT, DEFERRED_TAX_LIABILITIES,
                    PENSION_LIABILITIES, OTHER_LONG_TERM_LIABILITIES, TOTAL_NON_CURRENT_LIABILITIES,
                    TOTAL_LIABILITIES, COMMON_STOCK, PREFERRED_STOCK, ADDITIONAL_PAID_IN_CAPITAL,
                    RETAINED_EARNINGS, TREASURY_STOCK, ACCUMULATED_OTHER_COMPREHENSIVE_INCOME,
                    MINORITY_INTEREST, TOTAL_SHAREHOLDERS_EQUITY, TOTAL_LIABILITIES_AND_EQUITY,
                    CHANGE_TYPE, CHANGE_TIMESTAMP, EXPORT_BATCH_ID
                FROM DAILY_DELTA_STAGING
                WHERE EXPORT_BATCH_ID = ''' || v_batch_id || '''
            )
            FILE_FORMAT = (TYPE = PARQUET)
            HEADER = TRUE
            OVERWRITE = FALSE
            SINGLE = TRUE';
        
        EXECUTE IMMEDIATE v_copy_sql;

        -- Step 4: Log export in history table
        INSERT INTO DELTA_EXPORT_HISTORY (
            EXPORT_DATE,
            EXPORT_TIMESTAMP,
            EXPORT_BATCH_ID,
            RECORDS_INSERTED,
            RECORDS_UPDATED,
            RECORDS_DELETED,
            TOTAL_DELTA_RECORDS,
            PARQUET_FILE_PATH,
            EXPORT_STATUS
        ) VALUES (
            :v_export_date,
            :v_export_timestamp,
            :v_batch_id,
            :v_inserted,
            :v_updated,
            :v_deleted,
            :v_total,
            :v_stage_path,
            'COMPLETED'
        );

        -- Step 5: Clean up staging table for THIS BATCH only
        DELETE FROM DAILY_DELTA_STAGING WHERE EXPORT_BATCH_ID = :v_batch_id;

        v_result := OBJECT_CONSTRUCT(
            'status', 'SUCCESS',
            'export_timestamp', v_export_timestamp,
            'export_batch_id', v_batch_id,
            'records_inserted', v_inserted,
            'records_updated', v_updated,
            'records_deleted', v_deleted,
            'total_records', v_total,
            'file_path', v_stage_path
        );
    ELSE
        -- No changes detected
        INSERT INTO DELTA_EXPORT_HISTORY (
            EXPORT_DATE,
            EXPORT_TIMESTAMP,
            EXPORT_BATCH_ID,
            RECORDS_INSERTED,
            RECORDS_UPDATED,
            RECORDS_DELETED,
            TOTAL_DELTA_RECORDS,
            EXPORT_STATUS
        ) VALUES (
            :v_export_date,
            :v_export_timestamp,
            :v_batch_id,
            0, 0, 0, 0,
            'NO_CHANGES'
        );

        v_result := OBJECT_CONSTRUCT(
            'status', 'NO_CHANGES',
            'export_timestamp', v_export_timestamp,
            'export_batch_id', v_batch_id,
            'message', 'No delta data detected for export'
        );
    END IF;

    RETURN v_result;
END;
$$;

COMMENT ON PROCEDURE EXPORT_DELTA_TO_PARQUET() IS 
'Captures delta data from stream and exports to Parquet file - supports multiple exports per day with datetime-based naming';