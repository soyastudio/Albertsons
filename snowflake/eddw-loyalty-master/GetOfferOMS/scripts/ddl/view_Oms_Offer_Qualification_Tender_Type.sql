--liquibase formatted sql
--changeset SYSTEM:Oms_Offer_Qualification_Tender_Type runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view OMS_OFFER_QUALIFICATION_TENDER_TYPE(
	OMS_OFFER_ID COMMENT 'OMS Offer',
	TENDER_TYPE_DSC COMMENT 'TenderType Is a Payment Method',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	CONJUNCTION_TYPE_TXT COMMENT 'values can only be AND or OR',
	DISPLAY_ORDER_NBR COMMENT 'order of display in UI',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='View For Oms_Offer_Qualification_Tender_Type'
 as
Select
Oms_Offer_Id,
Tender_Type_Dsc  ,

Dw_First_Effective_Dt,
Dw_Last_Effective_Dt , 
Conjunction_Type_Txt,
Display_Order_Nbr ,
DW_CREATE_TS ,
DW_LAST_UPDATE_TS ,
DW_LOGICAL_DELETE_IND ,
DW_SOURCE_CREATE_NM ,
DW_SOURCE_UPDATE_NM ,
DW_CURRENT_VERSION_IND
From <<EDM_DB_NAME>>.DW_C_PRODUCT.Oms_Offer_Qualification_Tender_Type;
