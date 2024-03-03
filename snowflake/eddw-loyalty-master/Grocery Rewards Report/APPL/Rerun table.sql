--liquibase formatted sql
--changeset SYSTEM:F_Grocery_Reward_Offer_Clips_Rerun runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

USE DATABASE EDM_ANALYTICS_PRD;
USE SCHEMA dw_stage;

create or replace table F_Grocery_Reward_Offer_Clips_Rerun
(
OFFER_ID number
);
