--liquibase formatted sql
--changeset SYSTEM:view runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view REWARD_TRANSACTION_AUDIT_LOG(
	HOUSEHOLD_ID COMMENT 'Unique identifier of the Household. The ''aggregateId'' in CHMS service is the HHID unlike the other services where the ''aggegatedid'' we map to the CustomerId',
	TRANSACTION_ID COMMENT 'This contains Transaction Id for the transaction',
	LOYALTY_PROGRAM_CD COMMENT 'This Contains Loyalty Program Code',
	TRANSACTION_TYPE_CD COMMENT 'This contains transaction type code of a transaction',
	UPDATE_TS COMMENT 'Contains latest updated timestamp for the transaction',
	CREATE_TS COMMENT 'Conatins the created timestamp of the transaction',
	BEFORE_SNAPSHOT COMMENT 'Snapshot of Reward buckets before the transaction',
	AFTER_SNAPSHOT COMMENT 'Snapshot of Reward buckets after the transaction',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
        STATUS_CD,
        REWARD_DOLLAR_END_TS
) COMMENT='VIEW for Reward_Transaction_Audit_Log'
 as 
SELECT
 Household_Id          
 ,Transaction_Id        
 ,Loyalty_Program_Cd    
 ,Transaction_Type_Cd   
 ,Update_Ts               
 ,Create_Ts             
 ,Before_Snapshot       
 ,After_Snapshot        
 ,Dw_Create_Ts          
 ,Dw_Last_Update_Ts     
 ,Dw_Logical_Delete_Ind 
 ,Dw_Source_Create_Nm   
 ,Dw_Source_Update_Nm   
 ,Dw_Current_Version_Ind
 ,Dw_First_Effective_Dt 
 ,Dw_Last_Effective_Dt
 ,STATUS_CD
 ,REWARD_DOLLAR_END_TS
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Reward_Transaction_Audit_Log;

create or replace view REWARD_TRANSACTION(
	HOUSEHOLD_ID COMMENT 'HOUSEHOLD_ID',
	TRANSACTION_ID COMMENT 'TRANSACTION_ID',
	LOYALTY_PROGRAM_CD COMMENT 'LOYALTY_PROGRAM_CD',
	TRANSACTION_TYPE_CD COMMENT 'TRANSACTION_TYPE_CD',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_FIRST_EFFECTIVE_DT',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_LAST_EFFECTIVE_DT',
	TRANSACTION_DETAIL_TXT COMMENT 'TRANSACTION_DETAIL_TXT',
	TRANSACTION_TS COMMENT 'TRANSACTION_TS',
	REFERENCE_NBR COMMENT 'REFERENCE_NBR',
	LOYALTY_PROGRAM_DSC COMMENT 'LOYALTY_PROGRAM_DSC',
	TRANSACTION_TYPE_DSC COMMENT 'TRANSACTION_TYPE_DSC',
	TRANSACTION_TYPE_SHORT_DSC COMMENT 'TRANSACTION_TYPE_SHORT_DSC',
	REWARD_DOLLAR_START_TS COMMENT 'REWARD_DOLLAR_START_TS',
	REWARD_DOLLAR_END_TS COMMENT 'REWARD_DOLLAR_END_TS',
	REWARD_DOLLAR_POINTS_QTY COMMENT 'REWARD_DOLLAR_POINTS_QTY',
	REWARD_ORIGIN_CD COMMENT 'REWARD_ORIGIN_CD',
	REWARD_ORIGIN_DSC COMMENT 'REWARD_ORIGIN_DSC',
	REWARD_ORIGIN_TS COMMENT 'REWARD_ORIGIN_TS',
	STATUS_CD COMMENT 'STATUS_CD',
	STATUS_DSC COMMENT 'STATUS_DSC',
	STATUS_EFFECTIVE_TS COMMENT 'STATUS_EFFECTIVE_TS',
	STATUS_TYPE_CD COMMENT 'STATUS_TYPE_CD',
	CUSTOMER_TIER_CD COMMENT 'CUSTOMER_TIER_CD',
	CUSTOMER_TIER_DSC COMMENT 'CUSTOMER_TIER_DSC',
	CUSTOMER_TIER_SHORT_DSC COMMENT 'CUSTOMER_TIER_SHORT_DSC',
	CREATE_TS COMMENT 'CREATE_TS',
	CREATE_USER_ID COMMENT 'CREATE_USER_ID',
	UPDATE_TS COMMENT 'UPDATE_TS',
	UPDATE_USER_ID COMMENT 'UPDATE_USER_ID',
	ALT_TRANSACTION_ID COMMENT 'Alt_Transaction_Id',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='VIEW for REWARD_TRANSACTION'
 as
SELECT 
	HOUSEHOLD_ID, 
	TRANSACTION_ID,
	LOYALTY_PROGRAM_CD,
	TRANSACTION_TYPE_CD,
	DW_FIRST_EFFECTIVE_DT,
	DW_LAST_EFFECTIVE_DT,
	TRANSACTION_DETAIL_TXT,
	TRANSACTION_TS,
	REFERENCE_NBR,
	LOYALTY_PROGRAM_DSC,
	TRANSACTION_TYPE_DSC,
	TRANSACTION_TYPE_SHORT_DSC,
	REWARD_DOLLAR_START_TS ,
	REWARD_DOLLAR_END_TS ,
	REWARD_DOLLAR_POINTS_QTY ,
	REWARD_ORIGIN_CD ,
	REWARD_ORIGIN_DSC ,
	REWARD_ORIGIN_TS ,
	STATUS_CD,
	STATUS_DSC ,
	STATUS_EFFECTIVE_TS ,
	STATUS_TYPE_CD ,
	CUSTOMER_TIER_CD ,
	CUSTOMER_TIER_DSC ,
	CUSTOMER_TIER_SHORT_DSC ,
	CREATE_TS ,
	CREATE_USER_ID ,
	UPDATE_TS ,
	UPDATE_USER_ID ,
	Alt_Transaction_Id ,
	DW_CREATE_TS ,
	DW_LAST_UPDATE_TS ,
	DW_LOGICAL_DELETE_IND ,
	DW_SOURCE_CREATE_NM ,
	DW_SOURCE_UPDATE_NM ,
	DW_CURRENT_VERSION_IND 	 
FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.REWARD_TRANSACTION;
