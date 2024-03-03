create or replace task OFFEROMS_FLAT_ANALYTICAL_R_TASK
	warehouse=PROD_INGESTION_MEDIUM_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.DW_APPL.OfferOMS_Flat_Analytical_R_Stream')
	as call EDM_CONFIRMED_PRD.DW_APPL.SP_GetOfferOMS_To_Analytical_LOAD_Fact_Offer_Reports();
