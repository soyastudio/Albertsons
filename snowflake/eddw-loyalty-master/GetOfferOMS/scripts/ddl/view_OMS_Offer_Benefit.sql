--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Benefit runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_OFFER_BENEFIT(
	OMS_OFFER_ID COMMENT 'OMS Offer',
	DW_FIRST_EFFECTIVE_DT COMMENT 'First effective date',
	DW_LAST_EFFECTIVE_DT COMMENT 'Last effective date',
	BENEFIT_VALUE_TYPE_CD COMMENT 'Benefit value typ cd',
	BENEFIT_VALUE_TYPE_DSC COMMENT 'Benefit value desc',
	BENEFIT_VALUE_AMT COMMENT 'Benefit amt',
	CUSTOMER_GROUP_ID COMMENT 'Customer group id',
	CUSTOMER_GROUP_NM COMMENT 'Customer group nm',
	DW_CREATE_TS COMMENT 'DW Current Timestamp',
	DW_LAST_UPDATE_TS COMMENT 'DW Last update Timestamp',
	DW_SOURCE_CREATE_NM COMMENT 'DW source Create Name',
	DW_LOGICAL_DELETE_IND COMMENT 'DW logical delete Ind',
	DW_CURRENT_VERSION_IND COMMENT 'DW Current version Indicator',
	DW_SOURCE_UPDATE_NM COMMENT 'DW Source update Name'
) COMMENT='VIEW for OMS_Offer_Benefit'
 as 
SELECT
 OMS_Offer_Id           ,
 DW_First_Effective_Dt  ,
 DW_Last_Effective_Dt  ,
 Benefit_Value_Type_Cd    ,
 Benefit_Value_Type_Dsc    ,
 Benefit_Value_Amt   ,
 Customer_Group_Id,
 Customer_Group_Nm       ,
 DW_CREATE_TS            ,
 DW_LAST_UPDATE_TS       ,
 DW_SOURCE_CREATE_NM   ,
 DW_LOGICAL_DELETE_IND    ,
 DW_CURRENT_VERSION_IND    ,
 DW_SOURCE_UPDATE_NM    
FROM <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_Offer_Benefit;
