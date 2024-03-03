--liquibase formatted sql
--changeset SYSTEM:EDDW_BUSINESSPARTNER_PIPE_PRDBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:PIPE
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

Create or replace pipe EDDW_BUSINESSPARTNER_PIPE_PRDBLOB_INC
auto_ingest = true
integration = EDDW_PRD_SNOWPIPEINTEGRATION
as
copy into <<EDM_DB_NAME_R>>.DW_R_LOYALTY.ESED_BusinessPartner(filename, src_xml) from
	(
        select metadata$filename, $1
	    from @EDDW_BusinessPartner_STAGE_PRDBLOB_INC/ESED_C01_BusinessPartner/
	)
	file_format = (type='XML')
	on_error = 'SKIP_FILE'

--rollback DROP PIPE <<EDM_DB_NAME_R>>.DW_APPL.EDDW_BUSINESSPARTNER_PIPE_PRDBLOB_INC
