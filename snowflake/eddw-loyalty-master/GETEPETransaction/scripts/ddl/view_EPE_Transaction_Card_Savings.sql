--liquibase formatted sql
--changeset SYSTEM:EPE_Transaction_Card_Savings runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view EPE_TRANSACTION_CARD_SAVINGS(
	TRANSACTION_INTEGRATION_ID COMMENT 'Generated Integration Identifier of each transaction',
	SAVINGS_CATEGORY_ID COMMENT 'Club card savings category ID.',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	SAVINGS_CATEGORY_NM COMMENT 'Club card savings category Name. W.r.t that offer savings category is mapped',
	SAVINGS_AMT COMMENT 'Sum of Club card savings. W.r.t that offer savings category is mapped',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for EPE_Transaction_Card_Savings'
 as
select
Transaction_Integration_Id,
Savings_Category_Id, 
DW_First_Effective_Dt,
DW_Last_Effective_Dt,
Savings_Category_Nm,
Savings_Amt,
DW_CREATE_TS ,
DW_LAST_UPDATE_TS ,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND    
from  <<EDM_DB_NAME>>.DW_C_RETAILSALE.EPE_Transaction_Card_Savings;
