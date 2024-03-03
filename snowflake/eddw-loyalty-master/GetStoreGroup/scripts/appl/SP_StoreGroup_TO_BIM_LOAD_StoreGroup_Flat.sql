--liquibase formatted sql
--changeset SYSTEM:SP_StoreGroup_TO_BIM_LOAD_StoreGroup_Flat runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_C_PRODUCT;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_C_PRODUCT.SP_STOREGROUP_TO_BIM_LOAD_STOREGROUP_FLAT(FLAT_STREAM VARCHAR, CNF_DB VARCHAR, DW_PRD_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  

    // **************        Load for Storegroup_Flat table BEGIN *****************

    var src_flat_strm  = FLAT_STREAM;	
    var cnf_db = CNF_DB;
    var cnf_schema = DW_PRD_SCHEMA;
    var wrk_schema = WRK_SCHEMA;	
	var Refined_db = '<<EDM_DB_NAME_R>>';
	var Refined_schema ='DW_R_PRODUCT';


    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
    var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Storegroup_Flat_Wrk";
    var tgt_tbl = cnf_db +"."+ cnf_schema +".Storegroup_Flat";
	var flat_tbl =  Refined_db +"."+ Refined_schema +".StoreGroup_Flat";
	
	var sql_tgt_wrk_tbl = `TRUNCATE TABLE `+ tgt_wrk_tbl +` `;
	try {
        snowflake.execute ({sqlText: sql_tgt_wrk_tbl });
		}
	catch (err) { 
		throw "Truncation of wrk table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
	}

	
		var wrk_tbl = `INSERT INTO `+ tgt_wrk_tbl +` 
		
		with CTE as (
		
		Select PAYLOAD_ID, MAX(lastUpdateTs) AS lastUpdateTs from ` + flat_tbl+` where payload_id in (Select payload_id from ` + src_flat_strm +`)
		GROUP BY PAYLOAD_ID)
		
             SELECT   filename
                            ,PayloadPart
                            ,PageNum
                            ,TotalPages
                            ,EntityId
                            ,PayLoadType
                            ,EntityType
                            ,SourceAction
                            ,payload_id
                            ,payload_name
                            ,payload_description
							,payload_createTs
							,payload_updateTs
							,payload_createdUser_userId
							,payload_CreatedUser_firstname 
							,payload_CreatedUser_lastname 
                            ,payload_updatedUser_userid 
                            ,payload_updatedUser_firstName 
							,payload_updatedUser_lastname
							,payload_stores
							,lastUpdateTs
							,DW_CREATE_TS
   FROM (
              SELECT   ftab.filename
                            ,ftab.PayloadPart
                            ,ftab.PageNum
                            ,ftab.TotalPages
                            ,ftab.EntityId
                            ,ftab.PayLoadType
                            ,ftab.EntityType
                            ,ftab.SourceAction
                            ,ftab.payload_id
                            ,ftab.payload_name
                            ,ftab.payload_description
							,ftab.payload_createTs
							,ftab.payload_updateTs
							,ftab.payload_createdUser_userId
							,ftab.payload_CreatedUser_firstname 
							,ftab.payload_CreatedUser_lastname 
                            ,ftab.payload_updatedUser_userid 
                            ,ftab.payload_updatedUser_firstName 
							,ftab.payload_updatedUser_lastname
							,ftab.payload_stores
							,ftab.lastUpdateTs
							,ftab.DW_CREATE_TS
                            ,row_number() over(PARTITION BY ftab.payload_id,ftab.payload_stores ORDER BY to_timestamp_ntz(ftab.lastUpdateTs) desc) as rn
			         FROM `+ flat_tbl +` FTAB
					 INNER JOIN CTE CT ON CT.PAYLOAD_ID = FTAB.PAYLOAD_ID AND CT.lastUpdateTs = FTAB.lastUpdateTs
                     WHERE   FTAB.payload_id is not null
                     And payload_stores is not null
                            
					) where rn = 1 `;
                       
					

		    try {
        snowflake.execute ( 
            {sqlText: wrk_tbl  }
        );
    }
    catch (err)  { 
    return "Creation of Storegroup_Flat tgt_wrk_tbl table  Failed with error: " + err;   // Return a error message.
    }
    
   
	var delete_duplicate = `DELETE FROM ` + tgt_tbl + ` WHERE (payload_id) IN (SELECT payload_id  FROM `+ tgt_wrk_tbl +`)`;
                      
							
	var sql_begin = "BEGIN"
    // Processing Inserts
    var sql_inserts = `insert INTO ` + tgt_tbl + `
	                     (filename
                            ,PayloadPart
                            ,PageNum
                            ,TotalPages
                            ,EntityId
                            ,PayLoadType
                            ,EntityType
                            ,SourceAction
                            ,payload_id
                            ,payload_name
                            ,payload_description
							,payload_createTs
							,payload_updateTs
							,payload_createdUser_userId
							,payload_CreatedUser_firstname 
							,payload_CreatedUser_lastname 
                            ,payload_updatedUser_userid 
                            ,payload_updatedUser_firstName 
							,payload_updatedUser_lastname
							,payload_stores
							,lastUpdateTs
							,DW_CREATE_TS)
				Select 
                            filename
                            ,PayloadPart
                            ,PageNum
                            ,TotalPages
                            ,EntityId
                            ,PayLoadType
                            ,EntityType
                            ,SourceAction
                            ,payload_id
                            ,payload_name
                            ,payload_description
							,payload_createTs
							,payload_updateTs
							,payload_createdUser_userId
							,payload_CreatedUser_firstname 
							,payload_CreatedUser_lastname 
                            ,payload_updatedUser_userid 
                            ,payload_updatedUser_firstName 
							,payload_updatedUser_lastname
							,payload_stores
							,lastUpdateTs
							,DW_CREATE_TS
							from `+ tgt_wrk_tbl +` 
							`;
                      
    var sql_commit = "COMMIT"
    var sql_rollback = "ROLLBACK"
    try {
       snowflake.execute (
            {sqlText: sql_begin}
        );
        snowflake.execute(
			{sqlText: delete_duplicate}
			);              
        snowflake.execute (
            {sqlText: sql_inserts}
        );
        snowflake.execute (
            {sqlText: sql_commit}
        );    
    }
    catch (err) {
        snowflake.execute (
            {sqlText: sql_rollback}
        );
         return "Loading of "  + tgt_tbl + " Failed with error: " + err;   // Return a error message.
    }
            // **************        Load for Storegroup_Flat  ENDs *****************
 return "Done"
$$;
