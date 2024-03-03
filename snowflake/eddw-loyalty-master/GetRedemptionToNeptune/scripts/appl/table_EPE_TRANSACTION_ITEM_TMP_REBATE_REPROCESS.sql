
--liquibase formatted sql
--changeset SYSTEM:EPE_TRANSACTION_ITEM_TMP_REBATE_REPROCESS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database <<EDM_DB_NAME>>;
use schema DW_STAGE;


create or replace TABLE EPE_TRANSACTION_ITEM_TMP_REBATE_REPROCESS (
	UPC ARRAY,
	TRANSACTION_INTEGRATION_ID NUMBER(38,0)
);
