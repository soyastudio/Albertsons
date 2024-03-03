--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Requirement_Type runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_REQUIREMENT_TYPE(
	OFFER_REQUEST_ID COMMENT 'Offer_Request_Id',
	REQUIREMENT_TYPE_CD COMMENT 'Requirement_Type_Cd',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	REQUIRED_QTY COMMENT 'Required_Qty',
	REQUIRED_IND COMMENT 'Required_Ind',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='View For Offer_Request_Requirement_Type'
 as 
Select
Offer_Request_Id      ,
Requirement_Type_Cd   ,
DW_First_Effective_Dt ,
DW_Last_Effective_Dt  ,
Required_Qty          ,
Required_Ind          ,
DW_CREATE_TS          ,
DW_LAST_UPDATE_TS     ,
DW_LOGICAL_DELETE_IND ,
DW_SOURCE_CREATE_NM   ,
DW_SOURCE_UPDATE_NM   ,
DW_CURRENT_VERSION_IND  

From EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Requirement_Type;