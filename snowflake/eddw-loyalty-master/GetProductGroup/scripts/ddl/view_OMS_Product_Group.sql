--liquibase formatted sql
--changeset SYSTEM:OMS_Product_Group runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_PRODUCT_GROUP(
	PRODUCT_GROUP_ID COMMENT 'Product Group Primary Key.',
	DW_FIRST_EFFECTIVE_TS COMMENT 'The date the record was inserted. For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_TS COMMENT 'for the current record this is 12/31/9999. for updated records based on the primary key of the table, this is the new current records',
	PRODUCT_GROUP_NM COMMENT 'Name of the product group',
	PRODUCT_GROUP_DSC COMMENT 'Product group description',
	CREATE_TS COMMENT 'UPC added date time, e.g: \"2021-08-01T03:48:40.712Z\"',
	UPDATE_TS COMMENT 'UPC updated date time, e.g: \"2021-08-01T03:48:40.712Z\"',
	CREATE_USER_ID COMMENT 'Overall product group created or updated user id, e.g: \"rkasi01\"',
	CREATE_FIRST_NM COMMENT 'First name of the user who created this product group',
	CREATE_LAST_NM COMMENT 'Last name of the user who created or updated this product group',
	UPDATE_USER_ID COMMENT 'User ID of the user who created or updated this product group',
	UPDATE_FIRST_NM COMMENT 'First name of the user who updated this product group',
	PRODUCT_GROUP_VERSION_NBR COMMENT 'Product Group version number represents the message structure',
	PRODUCT_GROUP_TYPE_DSC COMMENT 'Description of the product group type',
	MOB_ID COMMENT 'Master Offer Bank Id or number, business users uses this id to uniquely identify product groups',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for OMS_Product_Group'
 as
select
Product_Group_Id,
DW_First_Effective_TS,
DW_Last_Effective_TS,
Product_Group_Nm,
Product_Group_Dsc,
Create_Ts,
Update_Ts,
Create_User_Id,
Create_First_Nm,
Create_Last_Nm,
Update_User_Id,
Update_First_Nm,
Product_Group_Version_Nbr,
Product_Group_Type_Dsc,
Mob_Id,
DW_CREATE_TS ,
DW_LAST_UPDATE_TS ,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND
from <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_Product_Group;
