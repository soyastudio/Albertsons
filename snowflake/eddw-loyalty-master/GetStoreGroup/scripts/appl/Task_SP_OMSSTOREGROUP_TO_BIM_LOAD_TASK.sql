create or replace task SP_OMSSTOREGROUP_TO_BIM_LOAD_TASK
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_APPL.OMSStoreGroup_FLAT_R_STREAM')
	as CALL SP_OMSStoreGroup_To_BIM_load();
