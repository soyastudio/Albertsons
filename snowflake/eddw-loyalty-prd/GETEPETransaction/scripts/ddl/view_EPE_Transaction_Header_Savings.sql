--liquibase formatted sql
--changeset SYSTEM:EPE_Transaction_Header_Savings runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view EPE_TRANSACTION_HEADER_SAVINGS(
	TRANSACTION_INTEGRATION_ID COMMENT 'Generated Integration Identifier of each transaction',
	OFFER_ID COMMENT 'This is the Offer ID applied to the savings item',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	SAVING_DSC COMMENT 'Description of the savings at transaction level',
	SOURCE_SYSTEM_CD COMMENT 'Source from where the offer has been created',
	REDEMPTION_AMT COMMENT 'Total Redemption Amount for the transaction',
	REDEMPTION_CNT COMMENT 'The no of items the offers has been applied',
	NON_DIGITAL_OFFER_IND COMMENT 'Indicates whether the transaction level coupon is digital or non digital',
	CALCULATE_USAGE_IND COMMENT 'Indicator to whether this offer need to keep track of its redemption',
	DISCOUNT_LEVEL_TXT COMMENT 'This describes if the level of the discount. Item Level/Basket Level/Dept Level',
	DISCOUNT_MESSAGE_TXT COMMENT 'The message displayed for the discount. It’s the receipt text',
	DISCOUNT_TYPE_TXT COMMENT 'Source from where the offer has been created',
	NET_PROMOTION_AMT COMMENT 'The final discount amount on any particular item',
	SAVINGS_CATEGORY_ID COMMENT 'Savings Category will tells J4U Savings, Credit Card Savings, Club Card Savings and Reward Savings.',
	SAVINGS_CATEGORY_NM COMMENT 'Savings Category will tells J4U Savings, Credit Card Savings, Club Card Savings and Reward Savings.',
	USAGE_CNT COMMENT 'How many times this offer has been redeemed before',
	PROGRAM_CD COMMENT 'Program code of the offer being applied for the savings',
	PROGRAM_TYPE_CD COMMENT 'Program Type code of the Offer being applied for the savings',
	START_DT COMMENT 'Offer Start Date',
	END_DT COMMENT 'Offer End Date',
	EXTERNAL_OFFER_ID COMMENT 'External offer Id in OMS Offer',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW for EPE_Transaction_Header_Savings'
 as
select
Transaction_Integration_Id,
Offer_Id ,
DW_First_Effective_Dt,
DW_Last_Effective_Dt,
Saving_Dsc ,
Source_System_Cd ,
Redemption_Amt,
Redemption_Cnt ,
--Updated_Dt ,
Non_Digital_Offer_Ind ,
Calculate_Usage_Ind,
Discount_Level_Txt,
Discount_Message_Txt,
Discount_Type_Txt,
Net_Promotion_Amt,
Savings_Category_Id,
Savings_Category_Nm,
Usage_Cnt,
Program_Cd,
Program_Type_Cd,
Start_Dt,
End_Dt,
External_Offer_Id,
DW_CREATE_TS ,
DW_LAST_UPDATE_TS ,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM  ,
DW_SOURCE_UPDATE_NM  ,
DW_CURRENT_VERSION_IND     
from  <<EDM_DB_NAME>>.DW_C_RETAILSALE.EPE_Transaction_Header_Savings;
