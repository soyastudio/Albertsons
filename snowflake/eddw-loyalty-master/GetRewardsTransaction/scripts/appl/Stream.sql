--liquibase formatted sql
--changeset SYSTEM:ESED_RewardTransaction_Json_R_STREAM runOnChange:true splitStatements:false OBJECT_TYPE:STREAM

use database EDM_REFINED_PRD;
use schema EDM_REFINED_PRD.DW_APPL;

create or replace stream ESED_RewardTransaction_Json_R_STREAM on 
table EDM_REFINED_PRD.dw_r_loyalty.ESED_RewardTransaction_Json;
