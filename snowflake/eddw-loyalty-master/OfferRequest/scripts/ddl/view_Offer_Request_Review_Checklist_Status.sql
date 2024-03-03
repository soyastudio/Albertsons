--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Review_Checklist_Status runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_REVIEW_CHECKLIST_STATUS(
	OFFER_REQUEST_ID COMMENT 'Unique identifer for each offer request created in source system',
	REVIEW_CHECKLIST_TYPE_CD COMMENT 'Type of the review checklist for the offer',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	REVIEW_CHECKLIST_IND COMMENT 'Indicator if the offer has a review checklist',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='View For Offer_Request_Review_Checklist_Status'
 as
Select
Offer_Request_Id      ,
Review_Checklist_Type_Cd   ,
DW_First_Effective_Dt ,
DW_Last_Effective_Dt  ,
Review_Checklist_Ind  ,
DW_CREATE_TS          ,
DW_LAST_UPDATE_TS     ,
DW_LOGICAL_DELETE_IND ,
DW_SOURCE_CREATE_NM   ,
DW_SOURCE_UPDATE_NM   ,
DW_CURRENT_VERSION_IND  
From EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Review_Checklist_Status;