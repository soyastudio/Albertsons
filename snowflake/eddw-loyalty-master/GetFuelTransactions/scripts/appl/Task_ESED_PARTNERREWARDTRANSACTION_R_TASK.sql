create or replace task ESED_PARTNERREWARDTRANSACTION_R_TASK
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('ESED_PartnerRewardTransaction_R_STREAM')
	as call sp_GetPartnerRewardTransaction_To_FLAT_load();
