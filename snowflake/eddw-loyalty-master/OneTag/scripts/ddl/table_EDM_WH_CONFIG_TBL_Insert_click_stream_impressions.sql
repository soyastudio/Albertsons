--liquibase formatted sql
--changeset SYSTEM:EDM_WH_CONFIG_TBL runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_REFINED_<<ENV>>;
use schema DW_R_MASTERDATA;

delete from EDM_WH_CONFIG_TBL where TABLE_NAME='ONE_TAG_CAROUSEL_Flat_wrk';

insert into EDM_WH_CONFIG_TBL
(TABLE_NAME,RECORD_COUNT_XSMALL_WH,RECORD_COUNT_MEDIUM_WH,RECORD_COUNT_LARGE_WH,RECORD_COUNT_XLARGE_WH)
values('ONE_TAG_CAROUSEL_Flat_wrk','10000','100000','500000','1000000');
