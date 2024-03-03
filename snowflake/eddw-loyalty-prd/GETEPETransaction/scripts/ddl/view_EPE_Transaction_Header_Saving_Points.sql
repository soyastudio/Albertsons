--liquibase formatted sql
--changeset SYSTEM:EPE_Transaction_Header_Saving_Points runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view EPE_TRANSACTION_HEADER_SAVING_POINTS(
	TRANSACTION_INTEGRATION_ID COMMENT 'Generated Integration Identifier of each transaction',
	OFFER_ID COMMENT 'This is the Offer ID applied to the savings item',
	POINTS_PROGRAM_NM COMMENT 'Program Name for the club card points being earned or burned',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	POINTS_BURNED_NBR COMMENT 'Total club card points burned for the transaction',
	POINTS_EARNED_NBR COMMENT 'Total club card points earned for the transaction',
	SCORECARD_TXT COMMENT 'Scorecard Name for the points being earned.',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for EPE_Transaction_Header_Saving_Points'
 as
select
Transaction_Integration_Id,
Offer_Id ,
Points_Program_Nm,
DW_First_Effective_Dt,
DW_Last_Effective_Dt,
Points_Burned_Nbr,
Points_Earned_Nbr,
Scorecard_Txt,
DW_CREATE_TS ,
DW_LAST_UPDATE_TS ,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM  ,
DW_SOURCE_UPDATE_NM  ,
DW_CURRENT_VERSION_IND     
from  <<EDM_DB_NAME>>.DW_C_RETAILSALE.EPE_Transaction_Header_Saving_Points;
