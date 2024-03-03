create or replace task ESED_PETPROFILE_R_TASK
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_APPL.ESED_PetProfile_R_STREAM')
	as call SP_GETPETPROFILE_TO_FLAT_LOAD();