create or replace task ESED_BUSINESSPARTNER_R_TASK
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('ESED_BusinessPartner_R_STREAM')
	as call sp_GetBusinessPartner_To_FLAT_load();
