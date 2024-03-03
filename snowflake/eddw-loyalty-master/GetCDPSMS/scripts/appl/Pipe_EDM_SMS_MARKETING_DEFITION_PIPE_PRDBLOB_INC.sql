--liquibase formatted sql
--changeset SYSTEM:EDM_SMS_MARKETING_DEFITION_PIPE_PRDBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:PIPE
use database EDM_REFINED_PRD;
use schema DW_APPL;

Create or replace pipe EDM_SMS_MARKETING_DEFITION_PIPE_PRDBLOB_INC
auto_ingest = true
integration = EDDW_PRD_SNOWPIPEINTEGRATION
as
COPY INTO EDM_REFINED_PRD.DW_R_LOYALTY.Get_SMS_Marketing_Defition_FLAT
	FROM
	(select $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10, metadata$filename, CURRENT_TIMESTAMP
		from @EDM_SMS_Marketing_Defition_STAGE_PRDBLOB_INC/CDP_Marketing_Content_Definition/ 
	 )
	 file_format = 'CSV_SMS_MCD'
	 pattern='.*.*[.]csv'
     on_error = 'SKIP_FILE'

--rollback DROP PIPE EDM_REFINED_PRD.DW_APPL.EDM_SMS_MARKETING_DEFITION_PIPE_PRDBLOB_INC;