--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Reward_Reconciliation runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view BUSINESS_PARTNER_REWARD_RECONCILIATION(
	TRANSACTION_ID COMMENT 'Transaction Id',
	ALT_TRANSACTION_ID COMMENT 'Alt_Transaction_Id',
	REFERENCE_NBR COMMENT 'Reference Nbr',
	REWARD_STATUS_CD COMMENT 'Reward Status Cd',
	REWARD_STATUS_TYPE_CD COMMENT 'Reward Status Type_Cd',
	REWARD_STATUS_DSC COMMENT 'Reward Status Dsc',
	REWARD_STATUS_EFFECTIVE_TS COMMENT 'Reward Status Effective Ts',
	RECONCILATION_MESSAGE_ID COMMENT 'Reconcilation Message Id',
	TOTAL_PURCHASE_QTY COMMENT 'Total Purchase Qty',
	PURCHASE_UOM_CD COMMENT 'Purchase UOM Cd',
	PURCHASE_UOM_DSC COMMENT 'Purchase UOM Dsc',
	PURCHASE_UOM_SHORT_DSC COMMENT 'Purchase UOM Short Dsc',
	PURCHASE_DISCOUNT_LIMIT_QTY COMMENT 'Purchase Discount Limit Qty',
	TENDER_TYPE_CD COMMENT 'Tender Type Cd',
	TENDER_TYPE_DSC COMMENT 'Tender Type Dsc',
	TENDER_TYPE_SHORT_DSC COMMENT 'Tender Type Short Dsc',
	PURCHASE_DISCOUNT_AMT COMMENT 'Purchase Discount Amt',
	REGULAR_PRICE_AMT COMMENT 'Regular Price Amt',
	CURRENCY_CD COMMENT 'Currency Cd',
	PROMOTION_PRICE_AMT COMMENT 'Promotion Price Amt',
	TOTAL_SAVING_AMT COMMENT 'Total Saving Amt',
	TOTAL_FUEL_PURCHASE_AMT COMMENT 'Total Fuel Purchase Amt',
	NONFUEL_PURCHASE_AMT COMMENT 'Nonfuel Purchase Amt',
	TOTAL_PURCHASE_AMT COMMENT 'Total Purchase Amt',
	DISCOUNT_PURCHASE_AMT COMMENT 'Discount Purchase Amt',
	TRANSACTION_FEE_AMT COMMENT 'Transaction Fee Amt',
	NET_PAYMENT_AMT COMMENT 'Net Payment Amt',
	SETTLEMENT_AMT COMMENT 'Settlement Amt',
	ACCOUNT_ID COMMENT 'Account Id',
	ACCOUNTING_UNIT_ID COMMENT 'Accounting Unit Id',
	CREATE_TS COMMENT 'Create Ts',
	CREATE_USER_ID COMMENT 'Create User Id',
	UPDATE_TS COMMENT 'Update Ts',
	UPDATE_USER_ID COMMENT 'Update User Id',
	TRANSACTION_TYPE_CD COMMENT 'Transaction Type Cd',
	SEQUENCE_NBR COMMENT 'Sequence Nbr',
	DW_FIRST_EFFECTIVE_DT COMMENT 'DW First Effective Dt',
	DW_LAST_EFFECTIVE_DT COMMENT 'DW Last Effective Dt',
	BUSINESS_PARTNER_INTEGRATION_ID COMMENT 'Business Partner Integration Id',
	TRANSACTION_TS COMMENT 'Transaction Ts',
	DW_CREATE_TS COMMENT 'DW CREATE TS',
	DW_LAST_UPDATE_TS COMMENT 'DW LAST UPDATE TS',
	DW_LOGICAL_DELETE_IND COMMENT 'DW LOGICAL DELETE IND',
	DW_SOURCE_CREATE_NM COMMENT 'DW SOURCE CREATE NM',
	DW_SOURCE_UPDATE_NM COMMENT 'DW SOURCE UPDATE NM',
	DW_CURRENT_VERSION_IND COMMENT 'DW CURRENT VERSION IND'
) COMMENT='VIEW For Business_Partner_Reward_Reconciliation'
 as
SELECT
	Transaction_Id                ,
 Alt_Transaction_Id            ,	
 Reference_Nbr                ,
 Reward_Status_Cd             ,
 Reward_Status_Type_Cd       ,
 Reward_Status_Dsc           ,
 Reward_Status_Effective_Ts   ,
 Reconcilation_Message_Id    ,
 Total_Purchase_Qty          ,
 Purchase_UOM_Cd             ,
 Purchase_UOM_Dsc            ,
 Purchase_UOM_Short_Dsc       ,
 Purchase_Discount_Limit_Qty ,
 Tender_Type_Cd              ,
 Tender_Type_Dsc               ,
 Tender_Type_Short_Dsc        ,
 Purchase_Discount_Amt        ,
 Regular_Price_Amt           ,
 Currency_Cd                  ,
 Promotion_Price_Amt          ,
 Total_Saving_Amt             ,
 Total_Fuel_Purchase_Amt      ,
 Nonfuel_Purchase_Amt         ,
 Total_Purchase_Amt           ,
 Discount_Purchase_Amt       ,
 Transaction_Fee_Amt          ,
 Net_Payment_Amt              ,
 Settlement_Amt                ,
 Account_Id                   ,
 Accounting_Unit_Id           ,
 Create_Ts                    ,
 Create_User_Id               ,
 Update_Ts                    ,
 Update_User_Id              ,
 Transaction_Type_Cd        ,
 Sequence_Nbr                 ,
 DW_First_Effective_Dt       ,
 DW_Last_Effective_Dt         ,
 Business_Partner_Integration_Id  ,
 Transaction_Ts               ,
 DW_CREATE_TS               ,
 DW_LAST_UPDATE_TS             ,
 DW_LOGICAL_DELETE_IND      ,
 DW_SOURCE_CREATE_NM     ,    
 DW_SOURCE_UPDATE_NM      ,   
 DW_CURRENT_VERSION_IND      
FROM  <<EDM_DB_NAME>>.DW_C_Loyalty.Business_Partner_Reward_Reconciliation ;