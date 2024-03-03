--liquibase formatted sql
--changeset SYSTEM:OMS_Product_Group_Upc_Comment runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_PRODUCT_GROUP_UPC_COMMENT(
	UPC_CD COMMENT 'UPC number e.g: 3520450097',
	PRODUCT_GROUP_ID COMMENT 'Product Group Primary Key.',
	COMMENT_TS COMMENT 'Comment date time, e.g: 2021-08-01T03:48:40.712Z',
	DW_FIRST_EFFECTIVE_TS COMMENT 'The date the record was inserted. For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_TS COMMENT 'for the current record this is 12/31/9999. for updated records based on the primary key of the table, this is the new current records',
	COMMENT_DSC COMMENT 'History of comments how the UPC made to the list',
	COMMENT_BY_USER_ID COMMENT 'Commented by USER ID',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for OMS_Product_Group_Upc_Comment'
 as
select
Upc_Cd,
Product_Group_Id,
Comment_Ts,
DW_First_Effective_TS,
DW_Last_Effective_TS,
Comment_Dsc,
Comment_By_User_Id,
DW_CREATE_TS ,
DW_LAST_UPDATE_TS ,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND
from <<EDM_DB_NAME>>.DW_C_PRODUCT.OMS_Product_Group_Upc_Comment;
