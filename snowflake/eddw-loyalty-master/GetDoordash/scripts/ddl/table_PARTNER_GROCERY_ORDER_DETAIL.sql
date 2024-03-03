--liquibase formatted sql
--changeset SYSTEM:Partner_Grocery_Order_detail runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE PARTNER_GROCERY_ORDER_DETAIL (
	ORDER_ID VARCHAR(50) NOT NULL COMMENT 'Partner created Order ID, Must be numeric (not alphanumeric).  This is Unique for each Grocery Order placed by the  Customers through Partner channels.',
	PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Partner_Grocery_Order_Customer_Integration_Id',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date that this division instance became effective.',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The last date that this division instance was effective. Thid date for the current instance will be 9999/12/31.',
	ITEM_NBR NUMBER(14,0) COMMENT 'UPC of the Item',
	UPC_ID NUMBER(14,0) COMMENT 'UPC Code of the Item',
	UPC_ID_TXT VARCHAR(14) COMMENT 'UPC Code of the Item in Text format.',
	ITEM_DSC VARCHAR(500) COMMENT 'Full Description of the Item.',
	ITEM_QTY NUMBER(16,5) COMMENT 'Amount of item ordered.  ',
	ITEM_TAX_AMT NUMBER(16,2) COMMENT 'Tax appled at item level.',
	RECEIPT_NBR VARCHAR(50) COMMENT 'This is the scanned barcode number that you are currently capturing in receipts.csv | Receipt Barcode. this can go away once providing approval_cd in the order info log. (Fullfilment number).',
	UNIT_PRICE_AMT NUMBER(16,4) COMMENT 'Price of product ( net amount paid, including any card discounts)',
	REVENUE_AMT NUMBER(16,2) COMMENT 'Price times quantity.',
	ALCOHOLIC_IND BOOLEAN COMMENT 'Products identified as containing alcohol (bollean value or 0 or 1).',
	STORE_TRANSACTION_TS TIMESTAMP_LTZ(9) COMMENT 'Grocery Order Create Timestamp.',
	LOYALTY_PHONE_NBR VARCHAR(12) COMMENT 'Loyalty Phone Number of the Customer.  This Phone Number is used to tag the Loyalty information of the Customer.',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_LAST_UPDATE_TS',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW_CURRENT_VERSION_IND',
	SNAP_IND BOOLEAN COMMENT 'If EBT SNAP or EBT CASH was applied\nBoolean value that describes whether EBT was applied to the item. If amount of EBT applied is at least $0.01 then this will be TRUE, else it will be NULL',
	EBT_SNAP_AMT NUMBER(9,2) COMMENT 'The dollar ($) amount applied to purchase that was used with an EBT SNAP account',
	EBT_CASH_AMT NUMBER(9,2) COMMENT 'The customer dollar ($) amount applied  to purchase that was used with an EBT Cash account',
	UNADJUSTED_ITEM_TAX_RT NUMBER(9,3) COMMENT 'This is the aggregate sales tax rate used to calculate that would have been used on this item. This value will not be affected by EBT SNAP amount applied. Actual sales tax paid by dollar amount will be populated in Sales Tax (row 13)',
	UNADJUSTED_ITEM_TAX_AMT NUMBER(9,4) COMMENT 'What the tax amount would have been if the item was paid for with non-EBT; Online Revenue multiplied by Original Sales Tax rate (includes mark-up)',
	primary key (ORDER_ID, PARTNER_GROCERY_ORDER_CUSTOMER_INTEGRATION_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);
