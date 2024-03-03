create or replace task GET_NPSSURVEYUMASCORE_TASK_NEW
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='60 minutes'
	QUERY_TAG='{"OBJECT_TYPE":"TASK", "OBJECT_NAME":"GET_Freshpass_Subscription_Task", "APPCODE":"OCSP"}'
	when SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_APPL.GETNPSUMASURVEY_FLAT_R_STREAM')
	as CALL SP_GETNPSUMASurvey_To_BIM_load();
