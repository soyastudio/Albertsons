--liquibase formatted sql
--changeset SYSTEM:Offer_Request_Promotion_Period_Type runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema EDM_VIEWS_PRD.DW_VIEWS;

create or replace view OFFER_REQUEST_PROMOTION_PERIOD_TYPE(
	OFFER_REQUEST_ID COMMENT 'Offer_Request_Id',
	PROMOTION_PERIOD_ID COMMENT 'Promotion_Period_Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	PROMOTION_PERIOD_NM COMMENT 'Promotion_Period_Nm',
	PROMOTION_WEEK_ID COMMENT 'Promotion_Week_Id',
	PROMOTION_START_DT COMMENT 'Promotion_Start_Dt',
	PROMOTION_END_DT COMMENT 'Promotion_End_Dt',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='View For Offer_Request_Promotion_Period_Type'
 as 
Select 
Offer_Request_Id	,
Promotion_Period_Id	,
DW_First_Effective_Dt	,
DW_Last_Effective_Dt	,
Promotion_Period_Nm	,
Promotion_Week_Id	,
Promotion_Start_Dt	,
Promotion_End_Dt	,
DW_CREATE_TS	,
DW_LAST_UPDATE_TS	,
DW_LOGICAL_DELETE_IND	,
DW_SOURCE_CREATE_NM	,
DW_SOURCE_UPDATE_NM	,
DW_CURRENT_VERSION_IND	
  From EDM_CONFIRMED_PRD.DW_C_PURCHASING.Offer_Request_Promotion_Period_Type;