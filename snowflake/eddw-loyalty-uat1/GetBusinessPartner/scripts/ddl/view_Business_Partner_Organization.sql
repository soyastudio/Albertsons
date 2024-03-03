--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Organization runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view BUSINESS_PARTNER_ORGANIZATION(
	BUSINESS_PARTNER_INTEGRATION_ID COMMENT 'Business Partner Integration Id',
	PARTNER_NM COMMENT 'Partner Nm',
	ORGANIZATION_TYPE_CD COMMENT 'Organization Type Cd',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW First Effective Date',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW Last Effective Date',
	ORGANIZATION_VALUE_TXT COMMENT 'Organization Value Txt',
	DW_CREATE_TS COMMENT 'DW CREATE TS',
	DW_LAST_UPDATE_TS COMMENT 'DW LAST UPDATE TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW LOGICAL DELETE INDICATOR',
	DW_SOURCE_CREATE_NM COMMENT 'DW SOURCE CREATE NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW SOURCE UPDATE NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW CURRENT VERSION INDICATOR'
) COMMENT='VIEW for Business_Partner_Organization'
 as
select
Business_Partner_Integration_Id    ,
Partner_Nm   ,
Organization_Type_Cd  ,
DW_First_Effective_Dt    ,
DW_Last_Effective_Dt    ,
Organization_Value_Txt  ,
DW_CREATE_TS    ,
DW_LAST_UPDATE_TS    ,
DW_LOGICAL_DELETE_IND    ,
DW_SOURCE_CREATE_NM    ,
DW_SOURCE_UPDATE_NM    ,
DW_CURRENT_VERSION_IND   
from <<EDM_DB_NAME>>.DW_C_LOYALTY.Business_Partner_Organization;
