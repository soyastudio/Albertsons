--liquibase formatted sql
--changeset SYSTEM:Air_Mile_Points_Summary runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view AIR_MILE_POINTS_SUMMARY(
	BATCH_ID COMMENT 'BatchId also known as External BatchId. This element represents the batch of AirMiles records that will be processed at summary and detail level.',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is 12/31/9999.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	BATCH_START_DATE_TXT COMMENT 'Start date of the batch file.',
	BATCH_END_DATE_TXT COMMENT 'End date of the batch file.',
	TOTAL_AIR_MILE_POINTS_QTY COMMENT 'Total number of AirMiles in the batch file. Note that AirMiles are calculated from  customers rewards/points they have earned.',
	RECORD_CNT COMMENT 'Total number of records in the AirMiles batch file.',
	TOTAL_REJECTED_AIR_MILE_POINTS_QTY COMMENT 'Total number of rejected AirMiles.',
	REJECTED_RECORD_CNT COMMENT 'Total number of records rejected in the AirMiles batch file.',
	CREATE_TS COMMENT 'Date and time when the record was created in the source system.',
	CREATE_USER_ID COMMENT 'User Id of the record created in the source system record',
	UPDATE_TS COMMENT 'Last updated timestamp of the source sytem record.',
	UPDATE_USER_ID COMMENT 'User Id of the last updated in the source system record',
	SOURCE_TYPE_CD COMMENT 'Indicates if the AirMilePoints are SUMMARY or DETAIL payload.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d.'
) COMMENT='VIEW for Air_Mile_Points_Summary'
 as
select
Batch_Id,
DW_First_Effective_Dt ,
DW_Last_Effective_Dt,
Batch_Start_Date_Txt,
Batch_End_Date_Txt,
Total_Air_Mile_Points_Qty,
Record_Cnt,
Total_Rejected_Air_Mile_Points_Qty,
Rejected_Record_Cnt,
Create_Ts,
Create_User_Id,
Update_Ts,
Update_User_Id ,
Source_Type_Cd ,
DW_CREATE_TS ,
DW_LAST_UPDATE_TS,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND
from  <<EDM_DB_NAME>>.DW_C_LOYALTY.Air_Mile_Points_Summary;
