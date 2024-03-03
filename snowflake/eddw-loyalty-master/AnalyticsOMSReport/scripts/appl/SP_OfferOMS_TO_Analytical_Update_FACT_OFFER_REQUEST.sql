--liquibase formatted sql
--changeset SYSTEM:SP_OfferOMS_TO_Analytical_Update_FACT_OFFER_REQUEST runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_ANALYTICS_PRD;
use schema DW_RETAIL_EXP;

CREATE OR REPLACE PROCEDURE EDM_ANALYTICS_PRD.DW_RETAIL_EXP.SP_OFFEROMS_TO_ANALYTICAL_UPDATE_FACT_OFFER_REQUEST(SRC_WRK_TBL VARCHAR, ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
    // **************       Update for Fact_Offer_Request table from storegroup BEGIN *****************
    var src_wrk_tbl = SRC_WRK_TBL;
    var anl_db = ANL_DB;
    var anl_schema = ANL_SCHEMA;
    var wrk_schema = WRK_SCHEMA;

    var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".Fact_copient_update_wrk";
    var tgt_tbl = anl_db + "." + anl_schema + ".Fact_Offer_Request";
    
    
    var cr_tgt_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl + ` AS
        select src.copient_id
        ,src.offer_id
		,src.offer_request_id
        from
        (select distinct payload_redemptionSystemId as copient_id
        ,payload_externalOfferId as offer_id
		,PAYLOAD_OFFERREQUESTID as offer_request_id
        from ` + src_wrk_tbl + `
        ) src
        inner join
        (
        select distinct copient_id
        ,offer_id
		,offer_request_id
        from  ` + tgt_tbl + ` 
        )tgt
        on src.offer_id = tgt.offer_id
		and src.offer_request_id = tgt.offer_request_id
        where NVL(src.copient_id,'-1') <> NVL(tgt.copient_id,'-1')	;`;

    try {
        snowflake.execute (
            {sqlText: cr_tgt_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Creation of table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
    }

    var update_sql = `update `+ tgt_tbl+ ` as tgt
        set copient_id = src.copient_id	
        ,dw_last_update_ts = current_timestamp()
        from (select distinct copient_id
            ,offer_id
            ,offer_request_id			
            from ` + tgt_wrk_tbl + `) src
        where tgt.offer_id = src.offer_id
		and  tgt.offer_request_id = src.offer_request_id`;

    try {
        snowflake.execute (
            {sqlText: update_sql  }
        )
    }
    catch (err)  {
        return "Update to Fact_Offer_Request tgt_tbl table "+ tgt_tbl +" Failed with error: " + err;   // Return a error message.
    }

    // **************       Update for Fact_Offer_Request ENDs *****************
            
    return "Done"


$$;