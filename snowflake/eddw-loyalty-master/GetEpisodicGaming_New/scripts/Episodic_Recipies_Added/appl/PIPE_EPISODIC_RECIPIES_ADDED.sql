--liquibase formatted sql
--changeset SYSTEM:EDM_EPISODIC_RECIPIES_ADDED_PIPE_PRDBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:pipe

USE DATABASE EDM_REFINED_<<ENV>>;
USE SCHEMA DW_APPL;

CREATE OR REPLACE PIPE EDM_EPISODIC_RECIPIES_ADDED_PIPE_<<ENV>>BLOB_INC
AUTO_INGEST = TRUE
INTEGRATION = <<SNOWPIPE_INTEGRATION>>
AS
COPY INTO EDM_REFINED_<<ENV>>.DW_R_LOYALTY.EPISODIC_RECIPIES_ADDED_FLAT
FROM
(SELECT   $11,$1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7,$8,$9,$10,$12 ,CURRENT_TIMESTAMP ,METADATA$FILENAME
	FROM @EDM_EPISODIC_GAMING_STAGE_<<ENV>>BLOB_INC/Merkle/ 
)
FILE_FORMAT = 'CSV_EPISODIC_GAMING'
PATTERN = '.*merkle_recipes_added.*[.]*'
ON_ERROR = 'SKIP_FILE';

--ROLLBACK DROP PIPE EDM_REFINED_<<ENV>>.DW_APPL.EDM_EPISODIC_AVATAR_SELECTION_PIPE_<<ENV>>BLOB_INC;

Create or replace stream EPISODIC_RECIPIES_ADDED_FLAT_R_STREAM ON TABLE EDM_REFINED_<<ENV>>.DW_R_LOYALTY.EPISODIC_RECIPIES_ADDED_FLAT;
