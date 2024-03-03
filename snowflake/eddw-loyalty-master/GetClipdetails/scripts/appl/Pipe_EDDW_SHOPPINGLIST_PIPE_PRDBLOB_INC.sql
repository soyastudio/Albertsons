--liquibase formatted sql
--changeset SYSTEM:EDDW_SHOPPINGLIST_PIPE_PRDBLOB_INC runOnChange:true splitStatements:false OBJECT_TYPE:PIPE
use database EDM_REFINED_PRD;
use schema DW_APPL;

Create or replace pipe EDDW_SHOPPINGLIST_PIPE_PRDBLOB_INC
auto_ingest = true
integration = EDDW_PRD_SNOWPIPEINTEGRATION
as
copy into EDM_REFINED_PRD.DW_R_LOYALTY.ESED_ShoppingList_Temp(filename, src_txt) from
	(
        select metadata$filename, $1
	    from @EDDW_ShoppingList_STAGE_PRDBLOB_INC/EMJU_C02_ShoppingList/ 
	)
	file_format = 'csv_no_delimiter'
	on_error = 'SKIP_FILE';

--rollback DROP PIPE EDM_REFINED_PRD.DW_APPL.EDDW_SHOPPINGLIST_PIPE_PRDBLOB_INC;
