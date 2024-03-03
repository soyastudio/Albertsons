--liquibase formatted sql
--changeset SYSTEM:FRESHPASS_SUBSCRIPTION_PLAN runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view FRESHPASS_SUBSCRIPTION_PLAN(
	SUBSCRIPTION_PLAN_INTEGRATION_ID COMMENT 'Generated Integration Identifier based on alternate keys',
	DW_LAST_EFFECTIVE_DT COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_FIRST_EFFECTIVE_DT COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	SUBSCRIPTION_PLAN_TYPE_NM COMMENT 'FreshPass Plan Type name',
	SUBSCRIPTION_PLAN_CD COMMENT 'FreshPass Plan Unique Identifier',
	SUBSCRIPTION_TYPE_NM COMMENT 'Current Fresh Plan Details for the User',
	CANCELLATION_GRACE_PERIOD_NBR COMMENT 'Grace Period (Number of Days) with in which the plan can be cancelled',
	DISCOUNT_TYPE_DSC COMMENT 'FreshPass Plan DiscountType',
	CURRENCY_CD COMMENT 'Code of the currency being used - USD',
	DISCOUNT_DURATION_DAYS_NBR COMMENT 'FreshPass Plan DiscountDuration',
	DISCOUNT_END_DT COMMENT 'FreshPass Plan DiscountEndDate',
	DISCOUNT_PLAN_END_DT COMMENT 'FreshPass DiscountedPlan EndDate',
	DISCOUNT_PLAN_START_DT COMMENT 'FreshPass DiscountPlan StartDate',
	DISCOUNTED_PRICE_IND COMMENT 'Discounted Plan Price for user- True or False',
	EXTENDED_TRIAL_DURATION_IND COMMENT 'Extended Trial Duration for user - True Or False',
	REGULAR_PLAN_PRICE_AMT COMMENT 'FreshPass Plan Regular Price',
	SUBSCRIPTION_PRICE_AMT COMMENT 'FreshPass Plan Price',
	TRIAL_DURATION_DAYS_NBR COMMENT 'FreshPass Initial Trial In days',
	DW_CREATE_TS COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS COMMENT 'When a record is updated  this would be the',
	DW_LOGICAL_DELETE_IND COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d'
) COMMENT='VIEW For Freshpass_Subscription_Plan'
 as
SELECT
Subscription_Plan_Integration_Id,
Dw_Last_Effective_Dt,
Dw_First_Effective_Dt,
Subscription_Plan_Type_Nm,
Subscription_Plan_Cd,
Subscription_Type_Nm,
Cancellation_Grace_Period_Nbr,
Discount_Type_Dsc,
Currency_Cd,
Discount_Duration_Days_Nbr,
Discount_End_Dt,
Discount_Plan_End_Dt,
Discount_Plan_Start_Dt,
Discounted_Price_Ind,
Extended_Trial_Duration_Ind,
Regular_Plan_Price_Amt,
Subscription_Price_Amt,
Trial_Duration_Days_Nbr,
Dw_Create_Ts,
Dw_Last_Update_Ts,
Dw_Logical_Delete_Ind,
Dw_Source_Create_Nm,
Dw_Source_Update_Nm,
Dw_Current_Version_Ind
FROM EDM_CONFIRMED_PRD.DW_C_LOYALTY.FRESHPASS_SUBSCRIPTION_PLAN;
