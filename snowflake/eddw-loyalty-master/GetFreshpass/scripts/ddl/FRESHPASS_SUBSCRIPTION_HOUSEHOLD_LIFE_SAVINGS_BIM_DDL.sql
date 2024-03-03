--liquibase formatted sql
--changeset SYSTEM:FRESHPASS_SUBSCRIPTION_HOUSEHOLD_LIFE_SAVINGS runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE <<EDM_DB_NAME>>;
USE SCHEMA DW_C_LOYALTY;

create or replace TRANSIENT TABLE FRESHPASS_SUBSCRIPTION_HOUSEHOLD_LIFE_SAVINGS (
	HOUSEHOLD_ID NUMBER(38,0) NOT NULL,
	DW_FIRST_EFFECTIVE_DT DATE NOT NULL,
	DW_LAST_EFFECTIVE_DT DATE NOT NULL,
	DELIVERY_SAVINGS_AMT NUMBER(8,2),
	PERK_SAVINGS_AMT NUMBER(8,2),
	TOTAL_SAVINGS_AMT NUMBER(8,2),
	PLAN_CLEBRATED_SAVINGS_AMT NUMBER(8,2),
	PLAN_CELEBRATED_TS TIMESTAMP_NTZ(9),
	DELIVERY_ORDER_CNT NUMBER(38,0),
	DUG_ORDER_CNT NUMBER(38,0),
	INSTORE_ORDER_CNT NUMBER(38,0),
	REWARDS_EARNED_QTY NUMBER(38,0),
	REWARD_POINTS_QTY NUMBER(38,0),
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
	primary key (HOUSEHOLD_ID, DW_FIRST_EFFECTIVE_DT, DW_LAST_EFFECTIVE_DT)
);


COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Delivery_Savings_Amt IS 'Delivery amount saved by FreshPass customer ';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Perk_Savings_Amt IS 'Amount saved by FreshPass customer using b4u coupons and promos';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Total_Savings_Amt IS 'Sum of Delivery and Perks Savings';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Plan_Clebrated_Savings_Amt IS 'Threshold when milestone savings value will be reached by customer';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Plan_Celebrated_Ts IS 'Date when customer reached milestone savings.';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Delivery_Order_Cnt IS 'No. of Delivery orders placed by the household';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Dug_Order_Cnt IS 'No. of DUG orders placed by the household';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Instore_Order_Cnt IS 'No. of in store orders placed by the household';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Rewards_Earned_Qty IS 'Rewards earned by the household';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Household_Id IS 'Unique identifier of the Household. The ''aggregateId'' in CHMS service is the HHID unlike the other services where the ''aggegatedid'' we map to the CustomerId';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Create_Ts IS 'Date initial record was created ';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Create_User_Id IS 'User ID who created initial record';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Update_Ts IS 'Date record was modified ';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Update_User_Id IS 'User ID who modified record';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Dw_Create_Ts IS 'The timestamp the record was inserted.';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Dw_Last_Update_Ts IS 'When a record is updated  this would be the current timestamp';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Dw_Logical_Delete_Ind IS 'Set to True when we receive a delete record for the primary key, else False';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Dw_Source_Create_Nm IS 'The Bod (data source) name of this insert.';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Dw_Source_Update_Nm IS 'The Bod (data source) name of this update or delete.';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Dw_Current_Version_Ind IS 'set to yes when the current record is deleted,Â  the Last Effective date on this record is still set to beÂ  current date -1 d';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Dw_First_Effective_Dt IS 'The date the record was inserted.  For update Primary Keys this values is used from the prior record of the primary key';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Dw_Last_Effective_Dt IS 'for the current record this is ''12/31/9999''.  for updated records based on the primary key of the table, this is the new current records DW_First_Effective_Dt -1 day';

COMMENT ON COLUMN Freshpass_Subscription_Household_Life_Savings.Reward_Points_Qty IS 'Points equivalant for the rewards earned.';
