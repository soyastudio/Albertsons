create or replace task SP_GETPARTNERREWARDTRANSACTION_TO_BIM_LOAD_TASK
	warehouse=PROD_INGESTION_MEDIUM_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('EDM_REFINED_PRD.DW_APPL.GetPartnerRewardTransaction_Flat_R_STREAM')
	as CALL SP_GetPartnerRewardTransaction_To_BIM_load();
