--liquibase formatted sql
--changeset SYSTEM:ESED_InstantAllocation runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_REFINED_PRD;
use schema EDM_REFINED_PRD.DW_R_LOYALTY;

create or replace TABLE ESED_INSTANTALLOCATION (
	FILENAME VARCHAR(5000),
	SRC_JSON VARIANT,
	CREATED_TS TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);