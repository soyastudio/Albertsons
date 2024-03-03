--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Qualification_Points_Group runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_OFFER_QUALIFICATION_POINTS_GROUP(
	OMS_OFFER_ID COMMENT 'OMS Offer',
	POINTS_GROUP_ID COMMENT 'Points group id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'First effective Date',
	DW_LAST_EFFECTIVE_DT COMMENT 'Last effective Date',
	POINTS_GROUP_NM COMMENT 'Points group name',
	POINTS_GROUP_VALUE_AMT COMMENT 'Points group amt',
	DW_CREATE_TS COMMENT 'DW Current Timestamp',
	DW_LAST_UPDATE_TS COMMENT 'DW Last update Timestamp',
	DW_SOURCE_CREATE_NM COMMENT 'DW source Create Name',
	DW_LOGICAL_DELETE_IND COMMENT 'DW logical delete Ind',
	DW_CURRENT_VERSION_IND COMMENT 'DW Current version Indicator',
	DW_SOURCE_UPDATE_NM COMMENT 'DW Source update Name'
) COMMENT='VIEW for OMS_Offer_Qualification_Points_Group'
 as SELECT
 OMS_Offer_Id           ,
 Points_Group_Id        ,
 DW_First_Effective_Dt  ,
 DW_Last_Effective_Dt  ,
 Points_Group_Nm         ,
 Points_Group_Value_Amt   ,
 DW_CREATE_TS            ,
 DW_LAST_UPDATE_TS       ,
 DW_SOURCE_CREATE_NM     ,
 DW_LOGICAL_DELETE_IND    ,
 DW_CURRENT_VERSION_IND    ,
 DW_SOURCE_UPDATE_NM     
FROM <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_Offer_Qualification_Points_Group;
