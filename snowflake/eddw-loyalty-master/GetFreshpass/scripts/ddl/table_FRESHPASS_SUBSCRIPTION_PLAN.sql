--liquibase formatted sql
--changeset SYSTEM:FRESHPASS_SUBSCRIPTION_PLAN runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_LOYALTY;

create or replace TABLE FRESHPASS_SUBSCRIPTION_PLAN (
	SUBSCRIPTION_PLAN_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Generated Integration Identifier based on alternate keys',
	DW_LAST_EFFECTIVE_DT DATE NOT NULL COMMENT 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day',
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL COMMENT 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key',
	SUBSCRIPTION_PLAN_TYPE_NM VARCHAR(16777216) COMMENT 'FreshPass Plan Type name',
	SUBSCRIPTION_PLAN_CD VARCHAR(16777216) COMMENT 'FreshPass Plan Unique Identifier',
	SUBSCRIPTION_TYPE_NM VARCHAR(16777216) COMMENT 'Current Fresh Plan Details for the User',
	CANCELLATION_GRACE_PERIOD_NBR NUMBER(38,0) COMMENT 'Grace Period (Number of Days) with in which the plan can be cancelled',
	DISCOUNT_TYPE_DSC VARCHAR(16777216) COMMENT 'FreshPass Plan DiscountType',
	CURRENCY_CD VARCHAR(16777216) COMMENT 'Code of the currency being used - USD',
	DISCOUNT_DURATION_DAYS_NBR NUMBER(38,0) COMMENT 'FreshPass Plan DiscountDuration',
	DISCOUNT_END_DT DATE COMMENT 'FreshPass Plan DiscountEndDate',
	DISCOUNT_PLAN_END_DT DATE COMMENT 'FreshPass DiscountedPlan EndDate',
	DISCOUNT_PLAN_START_DT DATE COMMENT 'FreshPass DiscountPlan StartDate',
	DISCOUNTED_PRICE_IND BOOLEAN COMMENT 'Discounted Plan Price for user- True or False',
	EXTENDED_TRIAL_DURATION_IND BOOLEAN COMMENT 'Extended Trial Duration for user - True Or False',
	REGULAR_PLAN_PRICE_AMT NUMBER(12,2) COMMENT 'FreshPass Plan Regular Price',
	SUBSCRIPTION_PRICE_AMT NUMBER(12,2) COMMENT 'FreshPass Plan Price',
	TRIAL_DURATION_DAYS_NBR NUMBER(38,0) COMMENT 'FreshPass Initial Trial In days',
	DW_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'The timestamp the record was inserted.',
	DW_LAST_UPDATE_TS TIMESTAMP_LTZ(9) COMMENT 'When a record is updated  this would be the current timestamp',
	DW_LOGICAL_DELETE_IND BOOLEAN COMMENT 'Set to True when we receive a delete record for the primary key, else False',
	DW_SOURCE_CREATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this insert.',
	DW_SOURCE_UPDATE_NM VARCHAR(255) COMMENT 'The Bod (data source) name of this update or delete.',
	DW_CURRENT_VERSION_IND BOOLEAN COMMENT 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d',
	constraint XPKFRESHPASS_SUBSCRIPTION_PLAN primary key (SUBSCRIPTION_PLAN_INTEGRATION_ID, DW_LAST_EFFECTIVE_DT, DW_FIRST_EFFECTIVE_DT)
);
