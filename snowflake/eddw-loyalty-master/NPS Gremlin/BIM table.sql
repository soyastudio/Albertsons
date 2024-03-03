
USE DATABASE EDM_CONFIRMED_PRD;
USE SCHEMA DW_C_TRANSACTION;

CREATE OR REPLACE TABLE Txn_NPS_Survey_Faulty_Data
(
TXN_ID NUMBER(13,0) COMMENT 'Transaction Id', 
STORE_ID NUMBER(38,0) COMMENT 'Store number in which transaction takes place', 
REGISTER_NBR NUMBER(38,0) COMMENT 'Register number of the store', 
TXN_DTE DATE COMMENT 'Transaction date', 
TXN_TM TIMESTAMP_TZ(9) COMMENT 'Transaction date/time'
);
