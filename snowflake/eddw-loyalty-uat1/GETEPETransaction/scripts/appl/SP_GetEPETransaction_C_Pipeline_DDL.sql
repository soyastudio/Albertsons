--liquibase formatted sql
--changeset SYSTEM:SP_GetEPETransaction_C_Pipeline_DDL runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETEPETRANSACTION_C_PIPELINE_DDL()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    // Get Metadata from EDM_Environment_Variable Table

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
        var cnf_db = metaparams['CNF_DB'];
        var cnf_schema = metaparams['C_RETAIL'];
        var wrk_schema = metaparams['C_STAGE'];
        var ref_db = metaparams['REF_DB'];
		var ref_schema = metaparams['R_RETAIL'];
        var app_schema = metaparams['APPL'];
        var views_db = `${metaparams['VIEWS_DB']}_${env}`;
        var views_schema = metaparams['VIEWS'];
        var warehouse = metaparams['WAREHOUSE'];
    } catch (err) {
        throw `Error while fetching data from EDM_Environment_Variable_${env}`;
    }

    // Task for FLAT Table
	var create_task = `CREATE OR REPLACE TASK SP_GetEPETransaction_To_BIM_load_TASK
	WAREHOUSE = '${warehouse}'
	SCHEDULE = '1 minutes'
	WHEN     SYSTEM$STREAM_HAS_DATA('${ref_db}.${app_schema}.GetEPETransaction_Flat_R_STREAM')
	AS
	CALL SP_GetEPETransaction_To_BIM_load();`

	try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }

    // To resume the task
	var resume_task = `ALTER TASK SP_GetEPETransaction_To_BIM_load_TASK resume;`

	try {
        snowflake.execute({ sqlText: resume_task });
    }
    catch (err)  {
        throw `Resume of Task Failed with error: ${err}`;   // Return a error message.
    }


$$;
