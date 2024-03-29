--liquibase formatted sql
--changeset SYSTEM:Business_Partner_Reward_Transaction runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE BUSINESS_PARTNER_REWARD_TRANSACTION (
	TRANSACTION_ID VARCHAR(100) NOT NULL COMMENT 'Transaction_Id',
	TRANSACTION_TS TIMESTAMP_LTZ(9) NOT NULL COMMENT 'Transaction_Ts',
	TRANSACTION_TYPE_CD VARCHAR(50) NOT NULL COMMENT 'Transaction_Type_Cd',
	TRANSACTION_TYPE_DSC VARCHAR(250) COMMENT 'Transaction_Type_Dsc',
	TRANSACTION_TYPE_SHORT_DSC VARCHAR(50) COMMENT 'Transaction_Type_Short_Dsc',
	REFERENCE_NBR VARCHAR(100) COMMENT 'Reference_Nbr',
	ALT_TRANSACTION_ID VARCHAR(100) COMMENT 'Alt_Transaction_Id',
	ALT_TRANSACTION_TYPE_CD VARCHAR(50) COMMENT 'Alt_Transaction_Type_Cd',
	ALT_TRANSACTION_TYPE_DSC VARCHAR(250) COMMENT 'Alt_Transaction_Type_Dsc',
	ALT_TRANSACTION_TYPE_SHORT_DSC VARCHAR(50) COMMENT 'Alt_Transaction_Type_Short_Dsc',
	PARTNER_DIVISION_ID VARCHAR(10) COMMENT 'Partner_Division_Id',
	POSTAL_ZONE_CD VARCHAR(20) COMMENT 'Postal_Zone_Cd',
	CUSTOMER_DIVISION_ID VARCHAR(10) COMMENT 'Customer_Division_Id',
	STATUS_TYPE_DSC VARCHAR(50) COMMENT 'Status_Type_Dsc',
	STATUS_TYPE_EFFECTIVE_TS TIMESTAMP_LTZ(9) COMMENT 'Status_Type_Effective_Ts',
	FUEL_PUMP_ID VARCHAR(50) COMMENT 'Fuel_Pump_Id',
	REGISTER_ID VARCHAR(50) COMMENT 'Register_Id',
	FUEL_GRADE_CD VARCHAR(50) COMMENT 'Fuel_Grade_Cd',
	FUEL_GRADE_DSC VARCHAR(250) COMMENT 'Fuel_Grade_Dsc',
	FUEL_GRADE_SHORT_DSC VARCHAR(50) COMMENT 'Fuel_Grade_Short_Dsc',
	TENDER_TYPE_CD VARCHAR(50) COMMENT 'Tender_Type_Cd',
	TENDER_TYPE_DSC VARCHAR(250) COMMENT 'Tender_Type_Dsc',
	TENDER_TYPE_SHORT_DSC VARCHAR(50) COMMENT 'Tender_Type_Short_Dsc',
	REWARD_MESSAGE_ID VARCHAR(50) COMMENT 'Reward_Message_Id',
	REWARD_TOKEN_OFFERED_QTY NUMBER(38,0) COMMENT 'Reward_Token_Offered_Qty',
	TOTAL_PURCHASE_QTY NUMBER(18,3) COMMENT 'Total_Purchase_Qty',
	PURCHASE_UOM_CD VARCHAR(10) COMMENT 'Purchase_UOM_Cd',
	PURCHASE_UOM_NM VARCHAR(25) COMMENT 'Purchase_UOM_Nm',
	PURCHASE_DISCOUNT_LIMIT_QTY NUMBER(38,0) COMMENT 'Purchase_Discount_Limit_Qty',
	PURCHASE_DISCOUNT_AMT NUMBER(18,2) COMMENT 'Purchase_Discount_Amt',
	TOTAL_FUEL_PURCHASE_AMT NUMBER(18,2) COMMENT 'Total_Fuel_Purchase_Amt',
	NONFUEL_PURCHASE_AMT NUMBER(18,2) COMMENT 'Nonfuel_Purchase_Amt',
	TOTAL_PURCHASE_AMT NUMBER(18,2) COMMENT 'Total_Purchase_Amt',
	DISCOUNT_AMT NUMBER(18,2) COMMENT 'Discount_Amt',
	EXCEPTION_TYPE_DSC VARCHAR(250) COMMENT 'Exception_Type_Dsc',
	EXCEPTION_TYPE_SHORT_DSC VARCHAR(50) COMMENT 'Exception_Type_Short_Dsc',
	EXCEPTION_TRANSACTION_TS TIMESTAMP_LTZ(9) COMMENT 'Exception_Transaction_Ts',
	CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'Create_Ts',
	CREATE_USER_ID VARCHAR(50) COMMENT 'Create_User_Id',
	UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'Update_Ts',
	UPDATE_USER_ID VARCHAR(50) COMMENT 'Update_User_Id',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW_First_Effective_Dt',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'DW_Last_Effective_Dt',
	BUSINESS_PARTNER_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Business_Partner_Integration_Id',
	RETAIL_CUSTOMER_UUID VARCHAR(16777216) COMMENT 'Retail_Customer_UUID',
	LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'Last_Update_Ts',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_CREATE_TS',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'DW_LAST_UPDATE_TS',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'DW_LOGICAL_DELETE_IND',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_CREATE_NM',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'DW_SOURCE_UPDATE_NM',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'DW_CURRENT_VERSION_IND',
	STATUS_TYPE_CD VARCHAR(50) COMMENT 'Status_Type_Cd',
	OLD_CLUB_CARD_NBR VARCHAR(50) COMMENT 'Old_Club_Card_Nbr',
	CLUB_CARD_NBR VARCHAR(100) COMMENT 'Club_Card_Nbr',
	HOUSEHOLD_ID NUMBER(38,0) COMMENT 'House_Hold_Id',
	CUSTOMER_PHONE_NBR NUMBER(38,0) COMMENT 'Customer_Phone_Nbr',
	TOTAL_SAVINGS_VALUE_AMT NUMBER(18,2) COMMENT 'Total Savings Value Amt',
	primary key (BUSINESS_PARTNER_INTEGRATION_ID, TRANSACTION_ID, TRANSACTION_TS, TRANSACTION_TYPE_CD, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);

ALTER TABLE Business_Partner_Reward_Transaction
 DROP PRIMARY KEY;
 
 --DEsc table Business_Partner_Reward_Transaction; 
ALTER TABLE Business_Partner_Reward_Transaction
 ADD PRIMARY KEY (Business_Partner_Integration_Id, 
 Transaction_Id, Transaction_Type_Cd, Status_Type_Cd, 
 Dw_First_Effective_Dt, Dw_Last_Effective_Dt);
 
 ALTER TABLE Business_Partner_Reward_Transaction drop COLUMN if exists
 Alt_Transaction_Ts ;
 
ALTER TABLE Business_Partner_Reward_Transaction ADD COLUMN
 Alt_Transaction_Ts    TIMESTAMP;
 
 ALTER TABLE Business_Partner_Reward_Transaction modify COLUMN
 Status_Type_Cd    not null;
