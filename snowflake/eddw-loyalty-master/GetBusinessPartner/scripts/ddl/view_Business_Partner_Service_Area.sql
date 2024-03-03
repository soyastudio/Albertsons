--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Service_Area runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view BUSINESS_PARTNER_SERVICE_AREA(
	PARTNER_NM COMMENT 'Partner_Nm      ',
	SERVICE_AREA_TYPE_CD COMMENT 'Service_Area_Type_Cd      ',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt      ',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt      ',
	SERVICE_AREA_DSC COMMENT 'Service_Area_Dsc      ',
	SERVICE_AREA_SHORT_DSC COMMENT 'Service_Area_Short_Dsc      ',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS      ',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS      ',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND      ',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM      ',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM      ',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND      '
) COMMENT='View For Business_Partner_Service_Area'
 as
SELECT 

Partner_Nm               ,
Service_Area_Type_Cd       ,
DW_First_Effective_Dt     ,
DW_Last_Effective_Dt      ,
Service_Area_Dsc        ,
Service_Area_Short_Dsc    ,
DW_CREATE_TS            ,
DW_LAST_UPDATE_TS        ,
DW_LOGICAL_DELETE_IND     ,
DW_SOURCE_CREATE_NM     ,
DW_SOURCE_UPDATE_NM      ,
DW_CURRENT_VERSION_IND   

FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.Business_Partner_Service_Area;
