--liquibase formatted sql
--changeset SYSTEM:FRESHPASS_SUBSCRIPTION_HOUSEHOLD_LIFE_SAVINGS runOnChange:true splitStatements:false OBJECT_TYPE:view

USE DATABASE <<EDM_DB_NAME_VIEW>>;
USE SCHEMA DW_VIEWS;

create or replace view FRESHPASS_SUBSCRIPTION_HOUSEHOLD_LIFE_SAVINGS (
												HOUSEHOLD_ID,
												DW_FIRST_EFFECTIVE_DT,
												DW_LAST_EFFECTIVE_DT,
												DELIVERY_SAVINGS_AMT,
												PERK_SAVINGS_AMT,
												TOTAL_SAVINGS_AMT,
												PLAN_CLEBRATED_SAVINGS_AMT,
												PLAN_CELEBRATED_TS,
												DELIVERY_ORDER_CNT,
												DUG_ORDER_CNT,
												INSTORE_ORDER_CNT,
												REWARDS_EARNED_QTY,
												REWARD_POINTS_QTY,
												CREATE_TS,
												CREATE_USER_ID,
												UPDATE_TS,
												UPDATE_USER_ID,
												DW_CREATE_TS,
												DW_LAST_UPDATE_TS,
												DW_LOGICAL_DELETE_IND,
												DW_SOURCE_CREATE_NM,
												DW_SOURCE_UPDATE_NM,
												DW_CURRENT_VERSION_IND
) COMMENT='VIEW For FRESHPASS_SUBSCRIPTION_HOUSEHOLD_LIFE_SAVINGS'
			as
			SELECT 
			HOUSEHOLD_ID,
			DW_FIRST_EFFECTIVE_DT,
			DW_LAST_EFFECTIVE_DT,
			DELIVERY_SAVINGS_AMT,
			PERK_SAVINGS_AMT,
			TOTAL_SAVINGS_AMT,
			PLAN_CLEBRATED_SAVINGS_AMT,
			PLAN_CELEBRATED_TS,
			DELIVERY_ORDER_CNT,
			DUG_ORDER_CNT,
			INSTORE_ORDER_CNT,
			REWARDS_EARNED_QTY,
			REWARD_POINTS_QTY,
			CREATE_TS,
			CREATE_USER_ID,
			UPDATE_TS,
			UPDATE_USER_ID,
			DW_CREATE_TS,
			DW_LAST_UPDATE_TS,
			DW_LOGICAL_DELETE_IND,
			DW_SOURCE_CREATE_NM,
			DW_SOURCE_UPDATE_NM,
			DW_CURRENT_VERSION_IND
	FROM EDM_CONFIRMED_<<ENV>>.DW_C_LOYALTY.FRESHPASS_SUBSCRIPTION_HOUSEHOLD_LIFE_SAVINGS;
