--liquibase formatted sql
--changeset SYSTEM:EDDW_REWARDTRANSACTION_JSON_PIPE runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_REFINED_PRD;
use schema DW_APPL;

create or replace pipe EDDW_REWARDTRANSACTION_JSON_PIPE_PRDBLOB_INC 
auto_ingest=true 
integration='EDDW_PRD_SNOWPIPEINTEGRATION'
as copy into EDM_REFINED_PRD.DW_R_LOYALTY.ESED_RewardTransaction_Json(filename, SRC_JSON) from
	(
        select metadata$filename, $1
	    from @EDDW_RewardTransaction_STAGE_PRDBLOB_INC/OCRP_C02_EDM_REWARDS_TRANSACTION_OUTBOUND/
	)
	file_format = (type='JSON')
	on_error = 'SKIP_FILE';
