--liquibase formatted sql
--changeset SYSTEM:EDDW_FRESHPASS_PIPE_PRDBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:PIPE
use database EDM_REFINED_PRD;
use schema DW_APPL;

Create or replace pipe EDDW_FRESHPASS_PIPE_PRDBLOB_INC
auto_ingest = true
integration = EDDW_PRD_SNOWPIPEINTEGRATION
as
copy into EDM_REFINED_PRD.DW_R_LOYALTY.ESED_Freshpass(filename, src_json) from
	(
        select metadata$filename, $1
	    from @EDDW_Freshpass_STAGE_PRDBLOB_INC/OCSP_C02_SubsriptionStatusChange_PROD/ 
	)
	file_format = (type='JSON')
	on_error = 'SKIP_FILE'

--rollback DROP PIPE EDM_REFINED_PRD.DW_APPL.EDDW_FRESHPASS_PIPE_PRDBLOB_INC;