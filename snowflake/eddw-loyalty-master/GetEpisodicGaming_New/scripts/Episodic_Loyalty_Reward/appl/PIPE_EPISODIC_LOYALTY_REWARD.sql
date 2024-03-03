--liquibase formatted sql
--changeset SYSTEM:EDM_EPISODIC_LOYALTY_REWARD_PIPE_PRDBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:pipe

USE DATABASE EDM_REFINED_<<ENV>>;
USE SCHEMA DW_APPL;

CREATE OR REPLACE PIPE EDM_EPISODIC_LOYALTY_REWARD_PIPE_<<ENV>>BLOB_INC
AUTO_INGEST = TRUE
INTEGRATION = <<SNOWPIPE_INTEGRATION>>
AS
COPY INTO EDM_REFINED_<<ENV>>.DW_R_LOYALTY.EPISODIC_LOYALTY_REWARD_FLAT
FROM
(SELECT   $8, $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7, $9 ,CURRENT_TIMESTAMP ,METADATA$FILENAME
	FROM @EDM_EPISODIC_GAMING_STAGE_<<ENV>>BLOB_INC/Merkle/ 
)
FILE_FORMAT = 'CSV_EPISODIC_GAMING'
PATTERN = '.*merkle_loyalty_rewards.*[.]*'
ON_ERROR = 'SKIP_FILE';

Create or replace stream EPISODIC_LOYALTY_REWARD_FLAT_R_STREAM ON TABLE EDM_REFINED_<<ENV>>.DW_R_LOYALTY.EPISODIC_LOYALTY_REWARD_FLAT;

--ROLLBACK DROP PIPE EDM_REFINED_<<ENV>>.DW_APPL.EPISODIC_LOYALTY_REWARD_PIPE_<<ENV>>BLOB_INC;
