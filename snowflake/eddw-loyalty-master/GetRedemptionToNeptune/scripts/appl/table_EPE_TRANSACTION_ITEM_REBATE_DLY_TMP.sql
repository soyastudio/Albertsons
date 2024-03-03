 
--liquibase formatted sql
--changeset SYSTEM:RETAIL_CUSTOMER_HH_LOYALTY_PROGRAM_REPROCESS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_NAME>>;
use schema DW_STAGE;
	
 create or replace transient TABLE EPE_TRANSACTION_ITEM_REBATE_DLY_TMP (
	UPC ARRAY,
	TRANSACTION_INTEGRATION_ID NUMBER(38,0)
);
 
