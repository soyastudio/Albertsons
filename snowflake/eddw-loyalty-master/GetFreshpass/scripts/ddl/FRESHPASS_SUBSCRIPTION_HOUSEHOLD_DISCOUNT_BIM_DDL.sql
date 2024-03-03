--liquibase formatted sql
--changeset SYSTEM:FRESHPASS_SUBSCRIPTION_HOUSEHOLD_DISCOUNT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA DW_C_LOYALTY;

create or replace TRANSIENT TABLE FRESHPASS_SUBSCRIPTION_HOUSEHOLD_DISCOUNT (
	HOUSEHOLD_ID NUMBER(38,0) NOT NULL,
	DISCOUNT_TYPE_NM VARCHAR(16777216) NOT NULL,
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL,
	DW_LAST_EFFECTIVE_DT DATE NOT NULL,
	TRIBE_EXPIRY_TS TIMESTAMP_NTZ(9),
	MONTHS_USED_QTY NUMBER(38,0),
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
	primary key (HOUSEHOLD_ID, DISCOUNT_TYPE_NM, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);


COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Tribe_Expiry_Ts IS 'The expiry date for a tribe(SNAP, Student, military etc) until which the user is qualified for the discounted plan.';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Months_Used_Qty IS 'No. of months user used under the discounted plan';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Create_Ts IS 'Date initial record was created ';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Create_User_Id IS 'User ID who created initial record';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Update_Ts IS 'Date record was modified ';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Update_User_Id IS 'User ID who modified record';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Discount_Type_Nm IS 'Name of the discount type';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Household_Id IS 'Unique identifier of the Household. The ''aggregateId'' in CHMS service is the HHID unlike the other services where the ''aggegatedid'' we map to the CustomerId';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Dw_Create_Ts IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Dw_Last_Update_Ts IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Dw_Logical_Delete_Ind IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Dw_Source_Create_Nm IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Dw_Source_Update_Nm IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Dw_Current_Version_Ind IS 'set to yes when the current record is deleted,  the Last Effective date on this record is still set to be  current date -1 d';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Dw_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Freshpass_Subscription_Household_Discount.Dw_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';
