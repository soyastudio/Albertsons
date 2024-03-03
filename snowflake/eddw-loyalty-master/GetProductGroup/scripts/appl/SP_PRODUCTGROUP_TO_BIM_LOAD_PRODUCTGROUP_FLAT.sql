--liquibase formatted sql
--changeset SYSTEM:SP_PRODUCTGROUP_TO_BIM_LOAD_PRODUCTGROUP_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_PRODUCTGROUP_TO_BIM_LOAD_PRODUCTGROUP_FLAT
("FLAT_STREAM" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "DW_SCHEMA" VARCHAR(16777216), "WRK_SCHEMA" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$

    // **************        Load for ProductGroup_Flat table BEGIN *****************

  
    var cnf_db = CNF_DB;
    var cnf_schema = DW_SCHEMA;
    var wrk_schema = WRK_SCHEMA;
    var src_flat_strm =  FLAT_STREAM;	
	var Refined_db = 'EDM_REFINED_PRD';
	var Refined_schema ='DW_R_PRODUCT';

    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
    var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".ProductGroup_Flat_wrk";
    var tgt_tbl = cnf_db +"."+ cnf_schema +".ProductGroup_Flat";
	var flat_tbl =  Refined_db +"."+ Refined_schema +".ProductGroup_Flat";

	
		var wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS	
		with CTE as (
		
		Select PAYLOAD_ID, MAX(lastUpdateTs) AS lastUpdateTs from ` + flat_tbl+` where payload_id in (Select payload_id from ` + src_flat_strm +`)
		GROUP BY PAYLOAD_ID)
	             select src.filename
                            ,src.PayloadPart
                            ,src.PageNum
                            ,src.TotalPages
                            ,src.EntityId
                            ,src.PayLoadType
                            ,src.EntityType
                            ,src.SourceAction
                            ,src.payload_id
                            ,src.payload_name
                            ,src.payload_description
                            ,src.payload_productGroupIds_upcIds
                            ,src.payload_productGroupIds_departmentSectionIds
                            ,src.payload_productGroupIds_manufactureIds
                            ,src.payload_createTs
                            ,src.payload_updateTs
                            ,src.payload_createdUser_userId
                            ,src.payload_createdUser_firstName
                            ,src.payload_createduser_lastName
                            ,src.payload_updatedUser_userId
                            ,src.payload_updatedUser_firstName
                            ,src.payload_updatedUser_lastName
                            ,src.lastUpdateTs
                            ,src.DW_CREATE_TS
                            From (
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
							,payload_productGroupIds_upcIds
							,payload_productGroupIds_departmentSectionIds
							,payload_productGroupIds_manufactureIds
							,payload_createTs
							,payload_updateTs
							,payload_createdUser_userId
							,payload_createdUser_firstName
							,payload_createduser_lastName
							,payload_updatedUser_userId
							,payload_updatedUser_firstName
							,payload_updatedUser_lastName
                            ,lastUpdateTs
							,current_timestamp() as DW_CREATE_TS
   FROM (
              SELECT   FTAB.filename
							,FTAB.PayloadPart 
							,FTAB.PageNum 
							,FTAB.TotalPages 
							,FTAB.EntityId 
							,FTAB.PayLoadType 
							,FTAB.EntityType 
							,FTAB.SourceAction
							,FTAB.payload_id
							,FTAB.payload_name
							,FTAB.payload_description
							,FTAB.payload_productGroupIds_upcIds
							,FTAB.payload_productGroupIds_departmentSectionIds
							,FTAB.payload_productGroupIds_manufactureIds
							,FTAB.payload_createTs
							,FTAB.payload_updateTs
							,FTAB.payload_createdUser_userId
							,FTAB.payload_createdUser_firstName
							,FTAB.payload_createduser_lastName
							,FTAB.payload_updatedUser_userId
							,FTAB.payload_updatedUser_firstName
							,FTAB.payload_updatedUser_lastName
                            ,FTAB.lastUpdateTs
							,current_timestamp() as DW_CREATE_TS
                            ,row_number() over(PARTITION BY FTAB.payload_id,FTAB.payload_productGroupIds_upcIds ORDER BY to_timestamp_ntz(FTAB.lastUpdateTs) desc) as rn
			         FROM `+ flat_tbl +` FTAB
					 INNER JOIN CTE CT ON CT.PAYLOAD_ID = FTAB.PAYLOAD_ID AND CT.lastUpdateTs = FTAB.lastUpdateTs
                         WHERE   FTAB.payload_id is not null
						 AND upper(FTAB.PAYLOAD_PRODUCTGROUPINFO_LISTTYPE) IN ('FINAL')						 
                            
					) where rn = 1 
                       )src
					`;

		    try {
        snowflake.execute ( 
            {sqlText: wrk_tbl  }
        );
    }
    catch (err)  { 
    return "Creation of ProductGroup_Flat tgt_wrk_tbl table  Failed with error: " + err;   // Return a error message.
    }
    
   
	var delete_duplicate = `DELETE FROM ` + tgt_tbl + ` WHERE payload_id IN (SELECT payload_id FROM `+ tgt_wrk_tbl +`)
							`;
                      
							
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
							,payload_productGroupIds_upcIds
							,payload_productGroupIds_departmentSectionIds
							,payload_productGroupIds_manufactureIds
							,payload_createTs
							,payload_updateTs
							,payload_createdUser_userId
							,payload_createdUser_firstName
							,payload_createduser_lastName
							,payload_updatedUser_userId
							,payload_updatedUser_firstName
							,payload_updatedUser_lastName
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
							,payload_productGroupIds_upcIds
							,payload_productGroupIds_departmentSectionIds
							,payload_productGroupIds_manufactureIds
							,payload_createTs
							,payload_updateTs
							,payload_createdUser_userId
							,payload_createdUser_firstName
							,payload_createduser_lastName
							,payload_updatedUser_userId
							,payload_updatedUser_firstName
							,payload_updatedUser_lastName
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
            // **************        Load for ProductGroup_Flat  ENDs *****************
 return "Done"
$$;
