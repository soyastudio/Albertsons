--liquibase formatted sql
--changeset SYSTEM:Sp_productgroup_to_analytical_load_fact_offer_request_update runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_ANALYTICS_PRD;
use schema DW_RETAIL_EXP;

CREATE OR REPLACE PROCEDURE EDM_ANALYTICS_PRD.DW_RETAIL_EXP.SP_PRODUCTGROUP_TO_ANALYTICAL_LOAD_FACT_OFFER_REQUEST_UPDATE(SRC_WRK_TBL VARCHAR, ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
    // **************       Update for Fact_Offer_Request table from storegroup BEGIN *****************
    var src_wrk_tbl = SRC_WRK_TBL;
    var anl_db = ANL_DB;
    var anl_schema = ANL_SCHEMA;
    var wrk_schema = WRK_SCHEMA;

    var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".Fact_Pg_update_wrk";
    var tgt_tbl = anl_db + "." + anl_schema + ".Fact_Offer_Request";
    
    
    var cr_tgt_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl + ` AS
        select src.product_group_id
        ,src.product_group_nm
        from
        (select distinct payload_id as product_group_id
        ,payload_name as product_group_nm
        from ` + src_wrk_tbl + `
        ) src
        inner join
        (
        select distinct product_group_id
        ,product_group_nm
        from  ` + tgt_tbl + `
        
        )tgt
        on src.product_group_id = tgt.product_group_id
        where src.product_group_nm<> tgt.product_group_nm;`;

    try {
        snowflake.execute (
            {sqlText: cr_tgt_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Creation of table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
    }

    var update_sql = `update `+ tgt_tbl+ ` as tgt
        set product_group_nm = src.product_group_nm 
        ,dw_last_update_ts = current_timestamp()
        from (select product_group_id
            ,product_group_nm 
            from ` + tgt_wrk_tbl + `) src
        where tgt.product_group_id = src.product_group_id`;

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