# Delta-Exports

# Delta Data Identification Solution for Balance Sheet

## Overview

This solution provides a comprehensive Snowflake-based implementation for:
1. Creating and managing a financial balance sheet table
2. Daily data insertion with MERGE operations
3. Identifying delta (changed) data using Snowflake Streams
4. Exporting delta data to Parquet format for consumer sharing
5. Automated scheduling using Snowflake Tasks (Not added yet)

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DAILY DATA PIPELINE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐     │
│   │  Source Systems  │───▶│  Staging Table   │───▶│ MERGE Operation  │     │
│   │  (ERP, Trading)  │    │  (Incoming Data) │    │ (Insert/Update)  │     │
│   └──────────────────┘    └──────────────────┘    └────────┬─────────┘     │
│                                                             │               │
│                                                             ▼               │
│                               ┌─────────────────────────────────────┐       │
│                               │   ACTUAL_AA_BALANCE_SHEET           │       │
│                               │   (Main Balance Sheet Table)        │       │
│                               └─────────────────┬───────────────────┘       │
│                                                 │                           │
│                              ┌──────────────────┴───────────────────┐      │
│                              │                                       │      │
│                              ▼                                       │      │
│   ┌───────────────────────────────────────┐                         │      │
│   │      SNOWFLAKE STREAM (CDC)           │◀────────────────────────┘      │
│   │  Captures INSERT, UPDATE, DELETE      │                                │
│   └─────────────────┬─────────────────────┘                                │
│                     │                                                       │
│                     ▼                                                       │
│   ┌───────────────────────────────────────┐                                │
│   │      DAILY_DELTA_STAGING              │                                │
│   │  (Delta Records with Change Type)     │                                │
│   └─────────────────┬─────────────────────┘                                │
│                     │                                                       │
│                     ▼                                                       │
│   ┌───────────────────────────────────────┐    ┌─────────────────────┐     │
│   │      COPY INTO Parquet                │───▶│  @DELTA_PARQUET_STAGE│     │
│   │  (Export to Stage)                    │    │  (Parquet Files)    │     │
│   └───────────────────────────────────────┘    └─────────┬───────────┘     │
│                                                                             │
└──────────────────────────────────────────────────────────┼──────────────────┘
              
```

---

## Business Grouping Key

The following columns form the unique business key for identifying records:

| Column | Description |
|--------|-------------|
| `PORTFOLIO_TICKER` | Unique identifier for the portfolio |
| `REPORTING_FREQUENCY` | DAILY, WEEKLY, MONTHLY, QUARTERLY, ANNUAL |
| `REPORTING_CURRENCY` | ISO 4217 currency code (USD, EUR, GBP, etc.) |
| `VALUE_DATE` | Effective date of balance sheet values |
| `PRELIMINARY` | TRUE if preliminary data, FALSE if final |
| `SOURCE` | Source system providing the data |
| `ACCT_BASIS` | Accounting basis (GAAP, IFRS, STAT) |
| `BALANCE_SHEET_TYPE` | CONSOLIDATED, STANDALONE, etc. |

---
