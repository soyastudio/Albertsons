--liquibase formatted sql
--changeset SYSTEM:EPE_TRANSACTION_ITEM_SAVING_CLIPS runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;


create or replace view EPE_TRANSACTION_ITEM_SAVING_CLIPS(
	TRANSACTION_INTEGRATION_ID COMMENT 'Generated Integration Identifier of each transaction',
	UPC_NBR COMMENT 'Universal Product Code of the item',
	OFFER_ID COMMENT 'This is the Offer ID applied to the savings item',
	CLIP_ID COMMENT 'Identifier of Clip used for Header level savings',
	ITEM_SEQUENCE_ID COMMENT 'the sequence number for the items in the basket ',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for Epe_Transaction_Item_Saving_Clips'
 as
SELECT
Transaction_Integration_Id,
Upc_Nbr,
Offer_Id,
Clip_Id,
Item_Sequence_Id,
Dw_First_Effective_Dt,
Dw_Last_Effective_Dt,
Dw_Create_Ts,
Dw_Last_Update_Ts,
Dw_Source_Create_Nm,
Dw_Source_Update_Nm,
Dw_Logical_Delete_Ind,
Dw_Current_Version_Ind  
FROM <<EDM_DB_NAME>>.DW_C_RETAILSALE.Epe_Transaction_Item_Saving_Clips;
