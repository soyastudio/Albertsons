--liquibase formatted sql
--changeset SYSTEM:ESED_PartnerRewardReconciliation runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_LOYALTY;

create or replace TABLE ESED_PARTNERREWARDRECONCILIATION (
	FILENAME VARCHAR(5000),
	SRC_XML VARIANT,
	CREATED_TS TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);
