--liquibase formatted sql
--changeset SYSTEM:table_Partner_Grocery_Order_DETAIL_EXCEPTIONS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_STAGE;

create or replace TABLE PARTNER_GROCERY_ORDER_HEADER_EXCEPTIONS (
	ORDER_ID VARCHAR(50),
	PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID NUMBER(38,0),
	TRANSACTION_DT DATE,
	STORE_TRANSACTION_TS TIMESTAMP_LTZ(9),
	NET_AMT NUMBER(16,2),
	PARTNER_ID VARCHAR(20),
	PARTNER_NM VARCHAR(100),
	FACILITY_INTEGRATION_ID NUMBER(38,0),
	STORE_ID VARCHAR(20),
	LOADDATE TIMESTAMP_NTZ(9),
	SNAP_IND BOOLEAN,
	FILENAME VARCHAR(16777216),
	EXCEPTION_REASON VARCHAR(16777216),
	DW_CREATE_TS TIMESTAMP_LTZ(9),
	ALCOHOLIC_IND BOOLEAN
);