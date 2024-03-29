--liquidbase formatted sql
--changeset SYSTEM:CUSTOMER_IDS runOnChange: true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE <<TGT_EDM_DB_NAME>>;
USE SCHEMA <<TAB_DEPLOY_SCHEMA>>;

create or replace TABLE <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CUSTOMER_IDS_Incremental (
	TRANSACTION_ID NUMBER(38,0),
	RETAILER_ID VARCHAR(50),
	RETAIL_STORE_ID VARCHAR(20),
	TRANSACTION_DATE DATE,
	TRANSACTION_TIME TIMESTAMP_TZ(9),
	LANE_ID NUMBER(38,0),
	CUSTOMER_ID_TYPE_CODE NUMBER(38,0),
	CUSTOMER_ID VARCHAR(16777216),
	LOYALTY_PROGRAM_CARD_NBR VARCHAR(16777216),
	DW_CREATE_TS TIMESTAMP_LTZ(9)
);
