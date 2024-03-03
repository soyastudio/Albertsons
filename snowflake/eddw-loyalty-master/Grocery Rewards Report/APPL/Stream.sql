--liquibase formatted sql
--changeset SYSTEM:GR Report runOnChange:true splitStatements:false OBJECT_TYPE:STREAM

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

create or replace stream F_Grocery_Reward_Offer_Clips_Report_Stream 
on table EDM_CONFIRMED_PRD.dw_c_loyalty.clip_details;

create or replace stream F_Grocery_Reward_Offer_Clips_Report_EPE_Stream 
on table EDM_CONFIRMED_PRD.dw_c_retailsale.Epe_Transaction_header_Saving_Clips;

create or replace stream F_Grocery_Reward_Offer_Clips_Report_OMS_Stream 
on table EDM_CONFIRMED_PRD.dw_c_product.oms_offer;
