--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Reward_Reconciliation runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE BUSINESS_PARTNER_REWARD_RECONCILIATION (
	TRANSACTION_ID VARCHAR(100) NOT NULL COMMENT 'Transaction Id',
	ALT_TRANSACTION_ID VARCHAR(100) COMMENT 'Alt_Transaction_Id',
	REFERENCE_NBR VARCHAR(100) COMMENT 'Reference Nbr',
	REWARD_STATUS_CD VARCHAR(50) COMMENT 'Reward Status Cd',
	REWARD_STATUS_TYPE_CD VARCHAR(65535) COMMENT 'Reward Status Type_Cd',
	REWARD_STATUS_DSC VARCHAR(50) COMMENT 'Reward Status Dsc',
	REWARD_STATUS_EFFECTIVE_TS TIMESTAMP_LTZ(9) COMMENT 'Reward Status Effective Ts',
	RECONCILATION_MESSAGE_ID VARCHAR(50) COMMENT 'Reconcilation Message Id',
	TOTAL_PURCHASE_QTY NUMBER(18,3) COMMENT 'Total Purchase Qty',
	PURCHASE_UOM_CD VARCHAR(50) COMMENT 'Purchase UOM Cd',
	PURCHASE_UOM_DSC VARCHAR(250) COMMENT 'Purchase UOM Dsc',
	PURCHASE_UOM_SHORT_DSC VARCHAR(50) COMMENT 'Purchase UOM Short Dsc',
	PURCHASE_DISCOUNT_LIMIT_QTY NUMBER(18,3) COMMENT 'Purchase Discount Limit Qty',
	TENDER_TYPE_CD VARCHAR(50) COMMENT 'Tender Type Cd',
	TENDER_TYPE_DSC VARCHAR(250) COMMENT 'Tender Type Dsc',
	TENDER_TYPE_SHORT_DSC VARCHAR(50) COMMENT 'Tender Type Short Dsc',
	PURCHASE_DISCOUNT_AMT NUMBER(18,2) COMMENT 'Purchase Discount Amt',
	REGULAR_PRICE_AMT NUMBER(18,2) COMMENT 'Regular Price Amt',
	CURRENCY_CD VARCHAR(25) COMMENT 'Currency Cd',
	PROMOTION_PRICE_AMT NUMBER(18,2) COMMENT 'Promotion Price Amt',
	TOTAL_SAVING_AMT NUMBER(18,2) COMMENT 'Total Saving Amt',
	TOTAL_FUEL_PURCHASE_AMT NUMBER(18,2) COMMENT 'Total Fuel Purchase Amt',
	NONFUEL_PURCHASE_AMT NUMBER(18,2) COMMENT 'Nonfuel Purchase Amt',
	TOTAL_PURCHASE_AMT NUMBER(18,2) COMMENT 'Total Purchase Amt',
	DISCOUNT_PURCHASE_AMT NUMBER(18,2) COMMENT 'Discount Purchase Amt',
	TRANSACTION_FEE_AMT NUMBER(18,2) COMMENT 'Transaction Fee Amt',
	NET_PAYMENT_AMT NUMBER(18,2) COMMENT 'Net Payment Amt',
	SETTLEMENT_AMT NUMBER(18,2) COMMENT 'Settlement Amt',
	ACCOUNT_ID VARCHAR(25) COMMENT 'Account Id',
	ACCOUNTING_UNIT_ID VARCHAR(20) COMMENT 'Accounting Unit Id',
	CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'Create Ts',
	CREATE_USER_ID VARCHAR(50) COMMENT 'Create User Id',
	UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'Update Ts',
	UPDATE_USER_ID VARCHAR(50) COMMENT 'Update User Id',
	TRANSACTION_TYPE_CD VARCHAR(50) COMMENT 'Transaction Type Cd',
	SEQUENCE_NBR NUMBER(38,0) NOT NULL COMMENT 'Sequence Nbr',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW First Effective Dt',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW Last Effective Dt',
	BUSINESS_PARTNER_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Business Partner Integration Id',
	TRANSACTION_TS TIMESTAMP_LTZ(9) NOT NULL COMMENT 'Transaction Ts',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW CREATE TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW LAST UPDATE TS',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW LOGICAL DELETE IND',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW SOURCE CREATE NM',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW SOURCE UPDATE NM',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW CURRENT VERSION IND',
	primary key (BUSINESS_PARTNER_INTEGRATION_ID, TRANSACTION_ID, TRANSACTION_TS, SEQUENCE_NBR, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);
