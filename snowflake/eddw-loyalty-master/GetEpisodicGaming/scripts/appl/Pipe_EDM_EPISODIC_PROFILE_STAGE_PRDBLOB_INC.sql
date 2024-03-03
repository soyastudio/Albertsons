--liquibase formatted sql
--changeset SYSTEM:EDM_EPISODIC_PROFILE_STAGE_PRDBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:PIPE
use database EDM_REFINED_PRD;
use schema DW_APPL;

Create or replace pipe EDM_EPISODIC_PROFILE_STAGE_PRDBLOB_INC
auto_ingest = true
integration = EDDW_PRD_SNOWPIPEINTEGRATION
as
COPY INTO EDM_REFINED_PRD.DW_R_LOYALTY.EPISODIC_PROFILE_FLAT
FROM
(select  $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 , current_timestamp ,metadata$filename
	from @EDM_Episodic_Gaming_STAGE_PRDBLOB_INC/Merkle/
)
file_format = 'CSV_EPISODIC_GAMING'
pattern='.*merkle_profile.*[.]*'
on_error='SKIP_FILE'

--rollback DROP PIPE EDM_REFINED_PRD.DW_APPL.EDM_EPISODIC_PROFILE_STAGE_PRDBLOB_INC;