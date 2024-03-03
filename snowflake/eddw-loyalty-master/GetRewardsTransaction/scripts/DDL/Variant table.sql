--liquibase formatted sql
--changeset SYSTEM:ESED_RewardTransaction_Json runOnChange:true splitStatements:false OBJECT_TYPE:TABLE

use database EDM_REFINED_PRD;
use schema dw_r_loyalty;

create or replace TABLE ESED_RewardTransaction_Json (
FILENAME VARCHAR(5000),
SRC_JSON VARIANT,
CREATED_TS TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

alter table ESED_RewardTransaction_Json set change_tracking = true;
