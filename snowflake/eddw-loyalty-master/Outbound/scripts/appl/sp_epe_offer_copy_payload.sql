--liquibase formatted sql
--changeset SYSTEM:sp_epe_offer_copy_payload runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_OUT_PRD;
use schema DW_DCAT;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_OUT_PRD.DW_DCAT.SP_EPE_OFFER_COPY_PAYLOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


    // Globa Variables
var cnf_out_db = "EDM_CONFIRMED_OUT_PRD";
var wrk_schema = "DW_DCAT";
var cnf_out_schema = "DW_DCAT";
var src_db = "EDM_CONFIRMED_OUT_PRD";
var src_schema = "DW_DCAT";
var src_tbl = src_db + "." + src_schema + ".EPE_OFFER_JSON_O_STREAM";
var src_wrk_tbl = cnf_out_db + "." + wrk_schema + ".EPE_OFFER_JSON_WRK";
var src_rerun_tbl = cnf_out_db + "." + wrk_schema + ".EPE_OFFER_JSON_RERUN";
var kafka = cnf_out_db + "." + cnf_out_schema + ".KAFKAOUTQUEUE";
var audit_table = cnf_out_db + "." + cnf_out_schema + ".EPE_OFFER_JSON_AUDIT";
   // var azure_past = "@EDDW_EPE_STAGE_OUTBOUND/epe_offers_PRD";
var azure_future = "@EDDW_EPE_STAGE_OUTBOUND/epe_offers";

    //check if rerun queue table exists otherwise create it
    
    var sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0 AS 
								SELECT * FROM `+ src_wrk_tbl +` where 1=2 `;
								
    try {
        snowflake.execute (
            {sqlText: sql_crt_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }
        
    // persist stream data in work table for the current transaction, includes data from previous failed run
    var sql_crt_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ src_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 0 as
                                SELECT * FROM `+ src_tbl +` 
                                UNION ALL 
                                SELECT * FROM `+ src_rerun_tbl +``;
    try {
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
        
     // Empty the rerun queue table
     
    var sql_empty_rerun_tbl = `TRUNCATE TABLE `+ src_rerun_tbl + ``;
    try {
        snowflake.execute (
            {sqlText: sql_empty_rerun_tbl  }
            );
        }
    catch (err)  {
        throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
        }
        
    // query to load rerun queue table when encountered a failure
	
    var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE `+ src_rerun_tbl+`  as SELECT * FROM `+ src_wrk_tbl+``;
   
    
    var filter_copy_future = " WHERE SPLIT_PART(KEY,'|',2) >= DATE(CURRENT_TIMESTAMP)+1";
    var filter_copy_today = " WHERE SPLIT_PART(KEY,'|',2) = DATE(CURRENT_TIMESTAMP) AND SPLIT_PART(KEY,'|',3) >= DATE(CURRENT_TIMESTAMP) ";
    var filter_copy_current = " WHERE SPLIT_PART(KEY,'|',2) < DATE(CURRENT_TIMESTAMP) AND SPLIT_PART(KEY,'|',3) >= DATE(CURRENT_TIMESTAMP) ";
    var filter_copy_past = " WHERE SPLIT_PART(KEY,'|',2) < DATE(CURRENT_TIMESTAMP) AND SPLIT_PART(KEY,'|',3) < DATE(CURRENT_TIMESTAMP) ";
    
    var sql_copy_future = `copy into ` + azure_future + `
                              from (SELECT to_variant(parse_json(payload)) FROM `+ src_wrk_tbl + filter_copy_future+ `)
                               file_format=(type = 'JSON')
                              MAX_FILE_SIZE = 5000000
                              INCLUDE_QUERY_ID = TRUE`;
   
    var sql_copy_today = `copy into ` + azure_future + `
                              from (SELECT to_variant(parse_json(payload)) FROM `+ src_wrk_tbl + filter_copy_today+ `)
                               file_format=(type = 'JSON')
                              MAX_FILE_SIZE = 5000000
                              INCLUDE_QUERY_ID = TRUE`;
                              
    var sql_copy_current = `copy into ` + azure_future + `
                              from (SELECT to_variant(parse_json(payload)) FROM `+ src_wrk_tbl + filter_copy_current+ `)
                               file_format=(type = 'JSON')
                              MAX_FILE_SIZE = 5000000
                              INCLUDE_QUERY_ID = TRUE`;
                              
    var sql_copy_past = `copy into ` + azure_future + `
                              from (SELECT to_variant(parse_json(payload)) FROM `+ src_wrk_tbl + filter_copy_past+ `)
                               file_format=(type = 'JSON')
                              MAX_FILE_SIZE = 5000000
                              INCLUDE_QUERY_ID = TRUE`;
    var get_query_id = `set query_id =  (select last_query_id())`;
    var sql_audit = `Insert into ` + audit_table + `
                    (
                      topic, 
                      filename,
                      key, 
                      dw_create_ts
                    )
                    select 
                    topic,
                    'data_' || filename_id as filename , 
                     key,
                    dw_create_ts
                    from
                    (select  topic,
                    key,
                    $query_id  as filename_id,
                    CURRENT_TIMESTAMP() AS dw_create_ts FROM `+ src_wrk_tbl 
                    
    var sql_audit_copy_future = sql_audit + filter_copy_future+ `)`;
    var sql_audit_copy_today = sql_audit + filter_copy_today+ `)`;
    var sql_audit_copy_current = sql_audit + filter_copy_current+ `)`;
    var sql_audit_copy_past = sql_audit + filter_copy_past+ `)`;
   
                  
                         
    var sql_begin = "BEGIN"
    var sql_commit = "COMMIT"
    var sql_rollback = "ROLLBACK"
  
	
	var sub_proc_nms = ['sql_copy_future', 'sql_copy_today','sql_copy_current','sql_copy_past']
						
	
     try {
       sub_proc_nm = sub_proc_nms[0];
        if (sub_proc_nm = sql_copy_future)   {                  
       
        var get_query_id = `set query_id =  (select last_query_id())`; 
        
        snowflake.execute (
            {sqlText: sub_proc_nm  }
        );
        
        snowflake.execute (
            {sqlText: get_query_id  }
        );
        snowflake.execute (
            {sqlText: sql_audit_copy_future }
        );
        }//sql_copy_future
        
         sub_proc_nm = sub_proc_nms[1];
        if (sub_proc_nm = sql_copy_today)   {                  
       
        var get_query_id = `set query_id =  (select last_query_id())`; 
        
        snowflake.execute (
            {sqlText: sub_proc_nm  }
        );
        
        snowflake.execute (
            {sqlText: get_query_id  }
        );
        snowflake.execute (
            {sqlText: sql_audit_copy_today }
        );
        }//sql_copy_today
        
         sub_proc_nm = sub_proc_nms[2];
        if (sub_proc_nm = sql_copy_current)   {                  
       
        var get_query_id = `set query_id =  (select last_query_id())`; 
        
        snowflake.execute (
            {sqlText: sub_proc_nm  }
        );
        
        snowflake.execute (
            {sqlText: get_query_id  }
        );
        snowflake.execute (
            {sqlText: sql_audit_copy_current }
        );
        }//sql_copy_current
        
         sub_proc_nm = sub_proc_nms[3];
        if (sub_proc_nm = sql_copy_past)   {                  
       
        var get_query_id = `set query_id =  (select last_query_id())`; 
        
        snowflake.execute (
            {sqlText: sub_proc_nm  }
        );
        
        snowflake.execute (
            {sqlText: get_query_id  }
        );
        snowflake.execute (
            {sqlText: sql_audit_copy_past }
        );
        }// sql_copy_past		 
      }//try
         
      
       catch (err) {
            snowflake.execute (
                {sqlText: sql_ins_rerun_tbl  }
            );
        return "Loading of Json to Azure and KafkaoutQueue Failed with error: " + err;   // Return a error message.
        }

$$;