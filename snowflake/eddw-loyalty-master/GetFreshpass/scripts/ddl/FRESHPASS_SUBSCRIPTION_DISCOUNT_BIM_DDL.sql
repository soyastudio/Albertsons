--liquibase formatted sql
--changeset SYSTEM:FRESHPASS_SUBSCRIPTION_DISCOUNT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA DW_C_LOYALTY;

create or replace TRANSIENT TABLE FRESHPASS_SUBSCRIPTION_DISCOUNT (
	DISCOUNT_TYPE_NM VARCHAR(16777216) NOT NULL,
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL,
	DW_LAST_EFFECTIVE_DT DATE NOT NULL,
	SUBSCRIPTION_CD VARCHAR(16777216),
	DISCOUNT_FEE_AMT NUMBER(6,2),
	CAMPAIGN_START_TS TIMESTAMP_NTZ(9),
	CAMPAIGN_END_TS TIMESTAMP_NTZ(9),
	DISCOUNT_DURATION_QTY NUMBER(38,0),
	IS_ACTIVE_IND BOOLEAN,
	TRIAL_DURATION_QTY NUMBER(38,0),
	CAMPAIGN_THRESHOLD_QTY NUMBER(38,0),
	ENROLLMENT_CNT NUMBER(38,0),
	CMAPIGN_RULES_TXT VARCHAR(16777216),
	CAMPAIGN_TYPE_NM VARCHAR(16777216),
	CAMPAIGN_TITLE_NM VARCHAR(16777216),
	CAMPAIGN_SAVINGS_TXT VARCHAR(16777216),
	CATEGORY_NM VARCHAR(16777216),
	CREATE_TS TIMESTAMP_NTZ(9),
	CREATE_USER_ID VARCHAR(16777216),
	UPDATE_TS TIMESTAMP_NTZ(9),
	UPDATE_USER_ID VARCHAR(16777216),
	DW_CREATE_TS TIMESTAMP_NTZ(9),
	DW_LAST_UPDATE_TS TIMESTAMP_NTZ(9),
	DW_LOGICAL_DELETE_IND BOOLEAN,
	DW_SOURCE_CREATE_NM VARCHAR(255),
	DW_SOURCE_UPDATE_NM VARCHAR(255),
	DW_CURRENT_VERSION_IND BOOLEAN,
	primary key (DISCOUNT_TYPE_NM, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);


COMMENT ON COLUMN Freshpass_Subscription_Discount.Discount_Type_Nm IS 'Name of the discount type';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Subscription_Cd IS 'Numeric code for the subscription';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Discount_Fee_Amt IS 'Fee amount for the discount type';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Campaign_Start_Ts IS 'Start timestamp of the campaign';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Campaign_End_Ts IS 'End timestamp of the campaign';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Discount_Duration_Qty IS 'Duration of discount in months';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Is_Active_Ind IS 'Indicator if the discount is active';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Trial_Duration_Qty IS 'Duraiton of trial in months';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Campaign_Threshold_Qty IS 'Threshold limit for the campaign';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Enrollment_Cnt IS 'Count of enrollment for the discount';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Cmapign_Rules_Txt IS 'list of campaign rules';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Campaign_Type_Nm IS 'Type of campaign';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Campaign_Title_Nm IS 'Title of campaign';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Campaign_Savings_Txt IS 'Campaign Savings';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Category_Nm IS 'Name of campaign category';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Create_Ts IS 'Date initial record was created ';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Create_User_Id IS 'User ID who created initial record';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Update_Ts IS 'Date record was modified ';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Update_User_Id IS 'User ID who modified record';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Dw_Create_Ts IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Dw_Last_Update_Ts IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Dw_Logical_Delete_Ind IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Dw_Source_Create_Nm IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Dw_Source_Update_Nm IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Dw_Current_Version_Ind IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Dw_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Freshpass_Subscription_Discount.Dw_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

