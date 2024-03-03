--liquidbase formatted sql
--changeset SYSTEM:CUSTOMER_IDS runOnChange: true splitStatements:false OBJECT_TYPE:VIEW

USE DATABASE <<TGT_EDM_DB_NAME>>;
USE SCHEMA <<VW_DEPLOY_SCHEMA>>;

CREATE OR REPLACE SECURE VIEW <<TGT_EDM_DB_NAME>>.<<VW_DEPLOY_SCHEMA>>.CUSTOMER_IDS_Incremental COPY GRANTS as
SELECT
	TRANSACTION_ID,
	RETAILER_ID,
	RETAIL_STORE_ID,
	TRANSACTION_DATE,
	TRANSACTION_TIME,
	LANE_ID,
	CUSTOMER_ID_TYPE_CODE,
	CUSTOMER_ID,
	LOYALTY_PROGRAM_CARD_NBR,
	DW_CREATE_TS  as LOAD_TIME
from <<TGT_EDM_DB_NAME>>.<<TAB_DEPLOY_SCHEMA>>.CUSTOMER_IDS_Incremental;