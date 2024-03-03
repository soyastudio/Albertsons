use database <<EDM_DB_NAME_R>>;
use schema dw_r_loyalty;

create or replace TABLE ESED_REWARDTRANSACTION_JSON (
	FILENAME VARCHAR(5000),
	SRC_JSON VARIANT,
	CREATED_TS TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);

create or replace TABLE ESED_REWARDTRANSACTION (
	FILENAME VARCHAR(5000),
	SRC_XML VARIANT,
	CREATED_TS TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP()
);
