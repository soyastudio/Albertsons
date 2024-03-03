--liquibase formatted sql
--changeset SYSTEM:GETFRESHPASS_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_REFINED_PRD;
use schema EDM_REFINED_PRD.DW_R_LOYALTY;

ALTER TABLE EDM_REFINED_PRD.DW_R_LOYALTY.GETFRESHPASS_FLAT
ADD COLUMN
Cycle_Delivery_Order_Cnt VARCHAR(16777216),
Cycle_Dug_Order_Cnt VARCHAR(16777216),
Cycle_Rewards_Earned_Qty VARCHAR(16777216),
Life_Delivery_Order_Cnt VARCHAR(16777216),
Life_Dug_Order_Cnt VARCHAR(16777216),
Life_Rewards_Earned_Qty VARCHAR(16777216);
