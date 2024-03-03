--liquibase formatted sql
--changeset SYSTEM:EPE_TRANSACTION_ITEM runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view EPE_TRANSACTION_ITEM(
	TRANSACTION_INTEGRATION_ID COMMENT 'Generated Integration Identifier of each transaction',
	UPC_NBR COMMENT 'Universal Product Code',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DEPARTMENT_NBR COMMENT 'Department Number of the transaction item',
	DISCOUNT_ALLOWED_IND COMMENT 'Indicator to identify if the discount is allowed on the item',
	ITEM_SEQUENCE_ID COMMENT 'the sequence number for the items in the basket',
	POINTS_APPLY_ITEM_IND COMMENT 'Indicator to identify if the point  is applied on the item',
	ITEM_UOM_CD COMMENT 'Quantity Type : Either weight or count',
	SELL_BY_WEIGHT_CD COMMENT 'Sell By Weight Code',
	DEPARTMENT_GROUP_NBR COMMENT 'Department Group Number of the transaction item',
	LINK_PLU_NBR COMMENT 'Link PLU Number of the Item',
	ITEM_PLU_NBR COMMENT 'PLU Number of the item',
	CLIPPED_OFFER_START_TS COMMENT 'Clip Start Timestamp',
	CLIPPED_OFFER_END_TS COMMENT 'Clip End Timestamp',
	PRICE_PER_ITEM_AMT COMMENT 'Price per Item Amount',
	BASE_PRICE_AMT COMMENT 'Base Price of the item',
	ITEM_PRICE_AMT COMMENT 'Price Amount of the item',
	NET_PROMOTION_AMT COMMENT 'The net amount after promotion has been applied',
	UNIT_PRICE_AMT COMMENT 'Unit Price of the item',
	EXTENDED_PRICE_AMT COMMENT 'Unit price Amount*Quantity(Item Level) = Extended price Amount',
	BASE_PRICE_PER_AMT COMMENT 'Base price of the item',
	CLUB_CARD_SAVINGS_AMT COMMENT 'Club Card Saving Amount for the transaction item',
	AVERAGE_WEIGHT_QTY COMMENT 'Average Weight Quantity of the item',
	ITEM_UNIT_QTY COMMENT 'The unit of the quantity of the item',
	ITEM_QTY COMMENT 'Quantity of the item',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for EPE_TRANSACTION_ITEM'
 as
select                      
Transaction_Integration_Id,
UPC_Nbr,
DW_First_Effective_Dt,
DW_Last_Effective_Dt,
Department_Nbr,
Discount_Allowed_Ind,
Item_Sequence_Id,
Points_Apply_Item_Ind,
Item_UOM_Cd,
Sell_By_Weight_Cd,
Department_Group_Nbr,
Link_Plu_Nbr,
Item_Plu_Nbr ,
Clipped_Offer_Start_Ts ,
Clipped_Offer_End_Ts,
Price_Per_Item_Amt,
Base_Price_Amt,
Item_Price_Amt,
Net_Promotion_Amt,
Unit_Price_Amt,
Extended_Price_Amt,
Base_Price_Per_Amt,
Club_Card_Savings_Amt,
Average_Weight_Qty,
Item_Unit_Qty,
Item_Qty,
DW_CREATE_TS,
DW_LAST_UPDATE_TS,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND 
from  <<EDM_DB_NAME>>.DW_C_RETAILSALE.EPE_TRANSACTION_ITEM;



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


create or replace view EPE_TRANSACTION_HEADER_SAVING_CLIPS(
	TRANSACTION_INTEGRATION_ID COMMENT 'Generated Integration Identifier of each transaction',
	OFFER_ID COMMENT 'This is the Offer ID applied to the savings item',
	CLIP_ID COMMENT 'Identifier of Clip used for Header level savings',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for Epe_Transaction_Header_Saving_Clips'
 as
SELECT
Transaction_Integration_Id,
Offer_Id ,
Clip_Id,
Dw_First_Effective_Dt,
Dw_Last_Effective_Dt,
Dw_Create_Ts,
Dw_Last_Update_Ts,
Dw_Source_Create_Nm,
Dw_Source_Update_Nm,
Dw_Logical_Delete_Ind,
Dw_Current_Version_Ind  
FROM <<EDM_DB_NAME>>.DW_C_RETAILSALE.Epe_Transaction_Header_Saving_Clips;
