--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_CONTROL_TABLE runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_USER_ACTIVITY;

create or replace TABLE CLICK_STREAM_CONTROL_TABLE (
	CLICK_STREAM_INTEGRATION_ID NUMBER(38,0) NOT NULL COMMENT 'Unique Key generated for each record in the Adobe transaction data',
	HIT_ID_HIGH VARCHAR(16777216) NOT NULL COMMENT 'Used in combination with hitid_low to uniquely identify a hit.',
	HIT_ID_LOW VARCHAR(16777216) NOT NULL COMMENT 'Used in combination with Hit_Id_Low to uniquely identify a hit.',
	VISIT_PAGE_NBR VARCHAR(16777216) NOT NULL COMMENT 'Variable used in the Hit depth dimension. Increases by 1 for each hit the user generates. Resets each visit.',
	VISIT_NBR NUMBER(38,0) NOT NULL COMMENT 'Variable used in the Hit depth dimension. Increases by 1 for each hit the user generates. Resets each visit.',
	SOURCE_CREATE_TS TIMESTAMP_LTZ(9) COMMENT 'name of the source from where data is updated',
	DW_CREATE_TS TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP() COMMENT 'When a record is created this would be the current timestamp',
	unique (CLICK_STREAM_INTEGRATION_ID),
	primary key (HIT_ID_HIGH, HIT_ID_LOW, VISIT_PAGE_NBR, VISIT_NBR)
);