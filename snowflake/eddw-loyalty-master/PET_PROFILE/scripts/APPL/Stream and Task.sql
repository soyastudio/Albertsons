use database EDM_REFINED_PRD;
use schema EDM_REFINED_PRD.DW_APPL;

drop stream if exists ESED_PETPROFILE_R_STREAM;
create or replace stream GETPETPROFILE_ADF_FLAT_R_STREAM on table EDM_REFINED_PRD.dw_r_loyalty.GETPETPROFILE_ADF_FLAT;

use database EDM_REFINED_PRD;
use schema DW_APPL;

create or replace task ESED_PETPROFILE_R_TASK
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_APPL.GETPETPROFILE_ADF_FLAT_R_STREAM')
	as call SP_GETPETPROFILE_TO_FLAT_LOAD();
	
alter task ESED_PETPROFILE_R_TASK resume;
