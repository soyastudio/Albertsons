--liquibase formatted sql
--changeset SYSTEM:sp_StoreGroup_To_FLAT_load_Rerun runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_PRODUCT;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME_R>>.DW_R_PRODUCT.SP_STOREGROUP_TO_FLAT_LOAD_RERUN()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    // Global Variable Declaration
    var wrk_schema = "DW_R_STAGE";
    var ref_db = "<<EDM_DB_NAME_R>>";
    var ref_schema = "DW_R_PRODUCT";
    var src_tbl = ref_db + "." + ref_schema + ".ESED_StoreGroup_R_STREAM";
    var src_wrk_tbl = ref_db + "." + wrk_schema + ".ESED_StoreGroup_wrk";
	var src_rerun_tbl = ref_db + "." + wrk_schema + ".ESED_StoreGroup_Rerun";
    var tgt_flat_tbl = ref_db + "." + ref_schema + ".StoreGroup_Flat";
	
  	
		// check if rerun queue table exists otherwise create it
		//rerun table has been created using work table as a part of new change ,previously it was creating using the streams
		
	
	var sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0
    AS SELECT * FROM `+ src_wrk_tbl +` where 1=2;`;
	try {
      snowflake.execute (
          {sqlText: sql_crt_rerun_tbl  }
          );
  }
  catch (err)  {
    throw "Creation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
  }
	
	//TRUNCATE WORK TABLE
	
	var sql_truncate_wrk_tbl = `TRUNCATE table `+ src_wrk_tbl +``;
	
	// persist stream data in work table for the current transaction, includes data from previous failed run
	
	  var sql_crt_src_wrk_tbl = `INSERT INTO `+ src_wrk_tbl +`
								select * from `+ src_tbl +` 
								UNION ALL 
								select * from `+ src_rerun_tbl +` `;
    try {
	
	 snowflake.execute (
            {sqlText: sql_truncate_wrk_tbl  }
            );
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
	
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE `+ src_rerun_tbl +` `;
	try {
        snowflake.execute ({sqlText: sql_empty_rerun_tbl });
  }
  catch (err) { 
    throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
  }
	
	// query to load rerun queue table when encountered a failure
    var sql_ins_rerun_tbl = `INSERT OVERWRITE INTO  `+ src_rerun_tbl+` SELECT * FROM `+ src_wrk_tbl +``;
	

    var insert_into_flat_dml =`INSERT INTO `+ tgt_flat_tbl +`
			with LVL_1_FLATTEN as
            (select 
            tbl.filename as filename
             ,tbl.src_json as src_json
            ,storegroup.seq as seq
            from `+ src_wrk_tbl +` tbl
            ,LATERAL FLATTEN(tbl.SRC_JSON) storegroup
            )
			select distinct
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
            ,payload_CreateTs 
            ,payload_updateTs 
            ,payload_CreatedUser_userid 
            ,payload_CreatedUser_firstname 
            ,payload_CreatedUser_lastname 
            ,payload_updatedUser_userid 
            ,payload_updatedUser_firstName 
            ,payload_updatedUser_lastname 
            ,payload_stores 
            ,lastUpdateTs
			,current_timestamp()
			FROM
			(select filename
             ,sg.src_json:payloadPart::string as PayloadPart
            ,sg.src_json:pageNum::string as PageNum
            ,sg.src_json:totalPages::string as totalPages
            ,sg.src_json:entityId::string as entityId
            ,sg.src_json:payLoadType::string as payLoadType
            ,sg.src_json:entityType::string as entityType
            ,sg.src_json:sourceAction::string as sourceAction
			,sg.src_json:payload:id::string as payload_Id
			,sg.src_json:payload:name::string as payload_Name
			,sg.src_json:payload:description::string as payload_Description
			,sg.src_json:payload:createTs::string as payload_createTs
			,sg.src_json:payload:updateTs::string as  payload_updateTs
			,sg.src_json:payload:createdUser:userId::string as payload_createdUser_userId
			,sg.src_json:payload:createdUser:firstName::string as payload_createdUser_firstName
			,sg.src_json:payload:createdUser:lastName::string as payload_createdUser_LastName
			,sg.src_json:payload:updatedUser:userId::string as payload_updatedUser_userId
			,sg.src_json:payload:updatedUser:firstName::string as payload_updatedUser_firstName
			,sg.src_json:payload:updatedUser:lastName::string as payload_updatedUser_LastName
            ,sg.src_json:lastUpdateTs::string as lastUpdateTs
			,sg_stores.value as payload_Stores
			from LVL_1_FLATTEN sg
            ,LATERAL FLATTEN(input => src_json:payload:stores, outer => True) as sg_stores ) sg;`

	try {
            snowflake.execute (
            {sqlText: insert_into_flat_dml  }
            );
        }
    catch (err)  { 
		    snowflake.execute ( 
						{sqlText: sql_ins_rerun_tbl }
						); 
            throw "Loading of table "+ tgt_flat_tbl +" Failed with error: " + err;   // Return a error message.
        }
	
	
	
$$;
