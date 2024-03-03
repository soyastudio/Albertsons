--liquibase formatted sql
--changeset SYSTEM:OMS_Offer_Qualification_Attribute_Value runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_OFFER_QUALIFICATION_ATTRIBUTE_VALUE(
	OMS_OFFER_ID COMMENT 'OMS Offer',
	QUALIFICATION_ATTRIBUTE_TYPE_TXT COMMENT 'Attribute type',
	QUALIFICATION_ATTRIBUTE_SEQUENCE_ID COMMENT 'Sequence',
	DW_FIRST_EFFECTIVE_DT COMMENT 'First effective Date',
	DW_LAST_EFFECTIVE_DT COMMENT 'Last effective Date',
	QUALIFICATION_ATTRIBUTE_VALUE_TXT COMMENT 'Value text',
	DW_CREATE_TS COMMENT 'DW Current Timestamp',
	DW_LAST_UPDATE_TS COMMENT 'DW Last update Timestamp',
	DW_SOURCE_CREATE_NM COMMENT 'DW source Create Name',
	DW_LOGICAL_DELETE_IND COMMENT 'DW logical delete Ind',
	DW_CURRENT_VERSION_IND COMMENT 'DW Current version Indicator',
	DW_SOURCE_UPDATE_NM COMMENT 'DW Source update Name'
) COMMENT='VIEW for OMS_Offer_Qualification_Attribute_Value'
 as SELECT
 OMS_Offer_Id           ,
 Qualification_Attribute_Type_Txt    ,
 Qualification_Attribute_Sequence_Id   ,
 DW_First_Effective_Dt  ,
 DW_Last_Effective_Dt  ,
 Qualification_Attribute_Value_Txt    ,
 DW_CREATE_TS            ,
 DW_LAST_UPDATE_TS       ,
 DW_SOURCE_CREATE_NM     ,
 DW_LOGICAL_DELETE_IND    ,
 DW_CURRENT_VERSION_IND    ,
 DW_SOURCE_UPDATE_NM     
FROM <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_Offer_Qualification_Attribute_Value;
