--liquibase formatted sql
--changeset SYSTEM:view_PARTNER_GROCERY_ORDER_DETAIL runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view PARTNER_GROCERY_ORDER_DETAIL(
	ORDER_ID COMMENT 'Order_Id',
	PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID COMMENT 'Partner_Grocery_Order_Customer_Integration_Id',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW_Last_Effective_Dt',
	ITEM_NBR COMMENT 'Item_Nbr',
	UPC_ID COMMENT 'UPC_Id',
	UPC_ID_TXT COMMENT 'UPC_Id_Txt',
	ITEM_DSC COMMENT 'Item_Dsc',
	ITEM_QTY COMMENT 'Item_Qty',
	ITEM_TAX_AMT COMMENT 'Item_Tax_Amt',
	RECEIPT_NBR COMMENT 'Receipt_Nbr',
	UNIT_PRICE_AMT COMMENT 'Unit_Price_Amt',
	REVENUE_AMT COMMENT 'Revenue_Amt',
	ALCOHOLIC_IND COMMENT 'Alcoholic_Ind',
	STORE_TRANSACTION_TS COMMENT 'Store_Transaction_Ts',
	LOYALTY_PHONE_NBR COMMENT 'Loyalty_Phone_Nbr',
	SNAP_IND COMMENT 'If EBT SNAP or EBT CASH was applied Boolean value that describes whether EBT was applied to the item. If amount of EBT applied is at least $0.01 then this will be TRUE, else it will be NULL',
	EBT_SNAP_AMT COMMENT 'The dollar ($) amount applied to purchase that was used with an EBT SNAP account',
	EBT_CASH_AMT COMMENT 'The customer dollar ($) amount applied  to purchase that was used with an EBT Cash account',
	UNADJUSTED_ITEM_TAX_RT COMMENT 'This is the aggregate sales tax rate used to calculate that would have been used on this item. This value will not be affected by EBT SNAP amount applied. Actual sales tax paid by dollar amount will be populated in Sales Tax (row 13)',
	UNADJUSTED_ITEM_TAX_AMT COMMENT 'What the tax amount would have been if the item was paid for with non-EBT; Online Revenue multiplied by Original Sales Tax rate (includes mark-up)',
	DW_CREATE_TS COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS COMMENT 'DW_LAST_UPDATE_TS',
	DW_SOURCE_CREATE_NM COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_LOGICAL_DELETE_IND COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_CURRENT_VERSION_IND COMMENT 'DW_CURRENT_VERSION_IND'
) COMMENT='VIEW for Partner_Grocery_Order_Detail'
 as
select 
Order_Id	,
Partner_Grocery_Order_Customer_Integration_Id	,
DW_First_Effective_Dt	,
DW_Last_Effective_Dt	,
Item_Nbr	,
UPC_Id	,
UPC_Id_Txt	,
Item_Dsc	,
Item_Qty	,
Item_Tax_Amt	,
Receipt_Nbr	,
Unit_Price_Amt	,
Revenue_Amt	,
Alcoholic_Ind	,
Store_Transaction_Ts	,
Loyalty_Phone_Nbr	,
Snap_Ind,
Ebt_Snap_Amt,
Ebt_Cash_Amt,
Unadjusted_Item_Tax_Rt,
Unadjusted_Item_Tax_Amt,
DW_CREATE_TS	,
DW_LAST_UPDATE_TS	,
DW_SOURCE_CREATE_NM	,
DW_SOURCE_UPDATE_NM,
DW_LOGICAL_DELETE_IND,
DW_CURRENT_VERSION_IND
 from  <<EDM_DB_NAME>>.DW_C_LOYALTY.Partner_Grocery_Order_Detail;
