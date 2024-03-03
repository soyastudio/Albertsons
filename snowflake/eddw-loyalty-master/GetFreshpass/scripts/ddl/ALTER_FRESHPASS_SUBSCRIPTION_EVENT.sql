--liquibase formatted sql
--changeset SYSTEM:FRESHPASS_SUBSCRIPTION_EVENT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_C_LOYALTY;

ALTER TABLE EDM_CONFIRMED_PRD.DW_C_LOYALTY.FRESHPASS_SUBSCRIPTION_EVENT
ADD COLUMN
Cycle_Delivery_Order_Cnt NUMBER,
Cycle_Dug_Order_Cnt NUMBER,
Cycle_Rewards_Earned_Qty NUMBER,
Life_Delivery_Order_Cnt NUMBER,
Life_Dug_Order_Cnt NUMBER,
Life_Rewards_Earned_Qty NUMBER;
