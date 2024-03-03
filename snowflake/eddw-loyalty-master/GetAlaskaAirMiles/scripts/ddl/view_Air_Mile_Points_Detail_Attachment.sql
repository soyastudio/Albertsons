--liquibase formatted sql
--changeset SYSTEM:Air_Mile_Points_Detail_Attachment runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view AIR_MILE_POINTS_DETAIL_ATTACHMENT(
	BATCH_ID COMMENT 'BatchId also known as External BatchId. This represents identifier to the batch file containing the summary of AirMiles records.',
	TRANSACTION_ID COMMENT 'Transaction Id for AirMiles.',
	TRANSACTION_TS COMMENT 'This is the timestamp when the transaction was updated.',
	HOUSEHOLD_ID COMMENT 'Unique identifier of the Household. The aggregateId in CHMS service is the HHID unlike the other services where the aggegatedid we map to the CustomerId',
	FILE_NM COMMENT 'Files assocoiated with AirMiles information.',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	LINK_URL_TXT COMMENT 'Link urls assocoiated with AirMiles information.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for Air_Mile_Points_Detail_Attachment'
 as
select                      
 Batch_Id             
,Transaction_Id       
,Transaction_Ts       
,Household_Id         
,File_Nm              
,DW_First_Effective_Dt
,DW_Last_Effective_Dt 
,Link_URL_Txt         
,DW_CREATE_TS         
,DW_LAST_UPDATE_TS    
,DW_LOGICAL_DELETE_IND
,DW_SOURCE_CREATE_NM  
,DW_SOURCE_UPDATE_NM  
,DW_CURRENT_VERSION_IND 
from  <<EDM_DB_NAME>>.DW_C_LOYALTY.Air_Mile_Points_Detail_Attachment;
