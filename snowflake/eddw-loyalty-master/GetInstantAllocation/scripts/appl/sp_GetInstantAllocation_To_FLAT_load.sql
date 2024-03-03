--liquibase formatted sql
--changeset SYSTEM:sp_GetInstantAllocation_To_FLAT_load runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_REFINED_PRD;
use schema EDM_REFINED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_REFINED_PRD.DW_APPL.SP_GETINSTANTALLOCATION_TO_FLAT_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
	
    var cur_db = snowflake.execute( {sqlText: `Select current_database()`} ); 
	cur_db.next(); 
	var env = cur_db.getColumnValue(1); 
	env = env.split('_'); 
	env = env[env.length - 1]; 
	var env_tbl_nm = `EDM_Environment_Variable_${env}`; 
	var env_schema_nm = 'DW_R_MASTERDATA'; 
	var env_db_nm = `EDM_REFINED_${env}`; 

	try { 
    var rs = snowflake.execute( {sqlText: `SELECT * FROM ${env_db_nm}.${env_schema_nm}.${env_tbl_nm}`} ); 
    var metaparams = {};
    while (rs.next()){
      metaparams[rs.getColumnValue(1)] = rs.getColumnValue(2);
    }
    var ref_db = metaparams['REF_DB']; 
    var ref_schema = metaparams['R_LOYAL']; 
    var app_schema = metaparams['APPL']; 
    var wrk_schema = metaparams['R_STAGE']; 
	} catch (err) { 
    throw `Error while fetching data from EDM_Environment_Variable_${env}`; 
	}
    var variant_nm = 'ESED_InstantAllocation';
	var bod_nm = 'GetInstantAllocation';
    var src_tbl = `${ref_db}.${app_schema}.${variant_nm}_R_STREAM`;
    var src_wrk_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_wrk`;
    var src_rerun_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_Rerun`;
    var tgt_flat_tbl = `${ref_db}.${ref_schema}.${bod_nm}_FLAT`;
	
	
	var sql_empty_wrk_tbl = `TRUNCATE TABLE `+ src_wrk_tbl +` `;
	try {
        snowflake.execute ({sqlText: sql_empty_wrk_tbl });
  }
  catch (err) { 
    throw "Truncation of wrk table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
  }
	
	// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `INSERT INTO ${src_wrk_tbl} 
                            select * from ${src_tbl} 
                            UNION ALL 
                            select * from ${src_rerun_tbl}`;
    try {
        snowflake.execute ({ sqlText: sql_crt_src_wrk_tbl });
    } catch (err)  {
        throw `Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;
	
    try {
        snowflake.execute ({sqlText: sql_empty_rerun_tbl });
    } catch (err) { 
        throw `Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
	// query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TABLE ${src_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS 
							            SELECT * FROM ${src_wrk_tbl}`;
							
	var insert_into_flat_dml =`INSERT INTO ${tgt_flat_tbl}
					with LVL_1_FLATTEN as 
(select
tbl.filename as filename
,tbl.src_json as src_json
from ${src_wrk_tbl} tbl
,LATERAL FLATTEN(tbl.SRC_JSON) profile
)
SELECT DISTINCT
profile.SRC_JSON:household_id::string AS householdid
,profile.SRC_JSON:offer_id::string AS offerid
,profile.SRC_JSON:region_id::string AS regionid
,profile.SRC_JSON:allocation_start_dt::string AS allocationstartdate
,profile.SRC_JSON:allocation_end_dt::string AS allocationenddate
,profile.SRC_JSON:allocation_qty::string AS allocationqty
,profile.SRC_JSON:event_ts::string AS eventts
,profile.SRC_JSON:event_name::string AS eventname
,filename
,current_timestamp() as DW_CREATETS
,profile.SRC_JSON:event_source::string AS eventsource
from LVL_1_FLATTEN profile
`;

    try {
        snowflake.execute ( {sqlText: insert_into_flat_dml} );
    } catch (err) { 
        snowflake.execute ( {sqlText: sql_ins_rerun_tbl} ); 
        throw `Loading of table ${tgt_flat_tbl} Failed with error: ${err}`;   // Return a error message.
    }

$$;
