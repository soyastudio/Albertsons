--liquibase formatted sql
--changeset SYSTEM:table_Partner_Grocery_Order_DETAIL_EXCEPTIONS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_STAGE;

create or replace TABLE PARTNER_GROCERY_ORDER_DETAIL_EXCEPTIONS (
	ORDER_ID VARCHAR(50),
	PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID NUMBER(38,0),
	ITEM_NBR NUMBER(14,0),
	UPC_ID NUMBER(14,0),
	UPC_ID_TXT VARCHAR(14),
	ITEM_DSC VARCHAR(500),
	ITEM_QTY NUMBER(16,5),
	ITEM_TAX_AMT NUMBER(16,2),
	RECEIPT_NBR VARCHAR(50),
	UNIT_PRICE_AMT NUMBER(16,4),
	REVENUE_AMT NUMBER(16,2),
	ALCOHOLIC_IND BOOLEAN,
	STORE_TRANSACTION_TS VARCHAR(16777216),
	LOYALTY_PHONE_NBR VARCHAR(12),
	USER_ID VARCHAR(16777216),
	SNAP_IND BOOLEAN,
	EBT_SNAP_AMT NUMBER(9,2),
	EBT_CASH_AMT NUMBER(9,2),
	UNADJUSTED_ITEM_TAX_RT NUMBER(9,4),
	UNADJUSTED_ITEM_TAX_AMT NUMBER(9,4),
	FILENAME VARCHAR(16777216),
	DML_TYPE VARCHAR(16777216),
	SAMEDAY_CHG_IND VARCHAR(16777216),
	EXCEPTION_REASON VARCHAR(16777216),
	DW_CREATE_TS TIMESTAMP_LTZ(9)
);
