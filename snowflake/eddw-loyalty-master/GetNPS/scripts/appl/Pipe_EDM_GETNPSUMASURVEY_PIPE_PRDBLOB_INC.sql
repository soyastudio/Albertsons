--liquibase formatted sql
--changeset SYSTEM:EDM_GETNPSUMASURVEY_PIPE_PRDBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:PIPE
use database EDM_REFINED_PRD;
use schema DW_APPL;

Create or replace pipe EDM_GETNPSUMASURVEY_PIPE_PRDBLOB_INC
auto_ingest = true
integration = EDDW_PRD_SNOWPIPEINTEGRATION
as
COPY INTO EDM_REFINED_PRD.DW_R_LOYALTY.GETNPSUMASURVEY_FLAT
FROM
	(select $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10 ,$11 ,$12 ,$13 ,$14 ,$15 ,$16 ,$17 ,$18 ,$19 ,$20 ,
			$21 ,$22 ,$23 ,$24 ,$25 ,$26 ,$27 ,$28 ,$29 ,$30 ,$31 ,$32 ,$33 ,$34 ,$35 ,$36 ,$37 ,$38 ,$39 ,$40 ,
			$41 ,$42 ,$43 ,$44 ,$45 ,$46 ,$47 ,metadata$filename ,CURRENT_TIMESTAMP
		from @EDM_GETNPSUMASURVEY_FLAT_STAGE_PRDBLOB_INC/Medallia/ 
	 )
	 file_format = 'CSV_NPS_SURVEY'
	 pattern='.*Albertsons_digital_surveys.*[.]csv'
     on_error = 'SKIP_FILE'

--rollback DROP PIPE EDM_REFINED_PRD.DW_APPL.EDM_GETNPSUMASURVEY_PIPE_PRDBLOB_INC;