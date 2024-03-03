--liquibase formatted sql
--changeset SYSTEM:Oms_Offer_Fulfillment_Channel_Type runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_OFFER_FULFILLMENT_CHANNEL_TYPE(
	FULFILLMENT_CHANNEL_TYPE_CD COMMENT '“It’s a Channel Thru which the offer will be fulfilled to customers. Eg .”delivery”, “dug”, “inStorePurchase”, “wug””',
	FULFILLMENT_CHANNEL_IND COMMENT '“It’s a indicator for the Fulfillment channel\"',
	FULFILLMENT_CHANNEL_DSC COMMENT '“It’s a description of Fulfillment Channel\"',
	OMS_OFFER_ID COMMENT 'Internal Ofer Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 day'
) COMMENT='VIEW for Oms_Offer_Fulfillment_Channel_Type'
 as

Select Fulfillment_Channel_Type_Cd,
	   Fulfillment_Channel_Ind,
	   Fulfillment_Channel_Dsc,
	   Oms_Offer_Id,
	   Dw_First_Effective_Dt,
	   Dw_Last_Effective_Dt,
	   Dw_Create_Ts,
	   Dw_Last_Update_Ts,
	   Dw_Logical_Delete_Ind,
	   Dw_Source_Create_Nm,
	   Dw_Source_Update_Nm,
	   Dw_Current_Version_Ind
From <<EDM_DB_NAME>>.DW_C_PRODUCT.Oms_Offer_Fulfillment_Channel_Type;
