--liquibase formatted sql
--changeset SYSTEM:GRReport runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

create or replace task SP_F_Grocery_Reward_Offer_Clips_Report_Task
	warehouse=PROD_INGESTION_SMALL_WH
	schedule='1 minutes'
	when SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.dw_appl.F_Grocery_Reward_Offer_Clips_Report_Stream') OR
	 SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.dw_appl.F_Grocery_Reward_Offer_Clips_Report_EPE_Stream') OR
	 SYSTEM$STREAM_HAS_DATA('EDM_CONFIRMED_PRD.dw_appl.F_Grocery_Reward_Offer_Clips_Report_OMS_Stream') 
	as CALL SP_GROCERYREWARD_TO_ANALYTICAL_LOAD(); 
