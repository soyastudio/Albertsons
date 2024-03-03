--liquibase formatted sql
--changeset SYSTEM:Household_Offer_Allocation runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view HOUSEHOLD_OFFER_ALLOCATION(
	OMS_OFFER_ID COMMENT 'offer identified in j4u system',
	HOUSEHOLD_ID COMMENT 'Unique Identifier of Household',
	REGION_ID COMMENT 'loyalty Division Identifier',
	ALLOCATION_START_TS COMMENT 'effective allocation start timestamp to view the offer in gallery',
	ALLOCATION_END_TS COMMENT 'effective allocation end timestamp to view the offer in gallery',
	DW_FIRST_EFFECTIVE_DT COMMENT ' The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT ' for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	ALLOCATION_CNT COMMENT ' Number of offers given or removed',
	EVENT_NM COMMENT ' Type of Allocation Event, Allocation or Deallocation',
	EVENT_TS COMMENT 'Event allocation or deallocation timestamp',
	EVENT_SOURCE_NM COMMENT 'Column describes the source of Allocation',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for Household_Offer_Allocation '
 as
select
Oms_Offer_Id,
Household_Id,
Region_Id,
Allocation_Start_Ts,
Allocation_End_Ts,
Dw_First_Effective_Dt,
Dw_Last_Effective_Dt,
Allocation_Cnt,
Event_Nm,
Event_Ts,
Event_Source_Nm ,
Dw_Create_Ts,
Dw_Last_Update_Ts,
Dw_Logical_Delete_Ind,
Dw_Source_Create_Nm,
Dw_Source_Update_Nm,
Dw_Current_Version_Ind 

from  <<EDM_DB_NAME>>.DW_C_LOYALTY.Household_Offer_Allocation;
