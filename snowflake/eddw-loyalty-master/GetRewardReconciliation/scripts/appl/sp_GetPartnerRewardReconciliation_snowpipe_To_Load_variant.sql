--liquibase formatted sql
--changeset SYSTEM:sp_GetPartnerRewardReconciliation_snowpipe_To_Load_variant runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME_R>>.DW_APPL.SP_GETPARTNERREWARDRECONCILIATION_SNOWPIPE_TO_LOAD_VARIANT()
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
		while (rs.next()) {
			metaparams[rs.getColumnValue(1)] = rs.getColumnValue(2);
		}
		// Global variables
		var ref_db = metaparams['REF_DB']; 
		var ref_schema = metaparams['R_LOYAL']; 
		var app_schema = metaparams['APPL']; 
		var wrk_schema = metaparams['R_STAGE'];
		var azure_blob_url = metaparams['AZURE_BLOB_URL']; 
		var pipe_integration = metaparams['PIPE_INTEGRATION'];
		var storage_integration = metaparams['STORAGE_INTEGRATION'];
		var warehouse = metaparams['WAREHOUSE'];
	} catch (err) { 
		throw `Error while fetching data from EDM_Environment_Variable_${env}`;
	}

	// Global variables
	var variant_nm = 'ESED_PartnerRewardReconciliation';
	var bod_nm = 'GetPartnerRewardReconciliation';
    var short_bod_nm = bod_nm.substring(3);
	var stream_nm = `${variant_nm}_R_STREAM`;
	var stage_nm = `EDDW_${short_bod_nm}_STAGE_${env}BLOB_INC`;
	var pipe_nm = `EDDW_${short_bod_nm}_PIPE_${env}BLOB_INC`;
	var variant_src_tbl = `${ref_db}.${app_schema}.${variant_nm}_R_STREAM`;
	var variant_src_rerun_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_Rerun`;
	var flat_src_tbl = `${ref_db}.${app_schema}.GetPartnerRewardReconciliation_Flat_R_STREAM`;
	var flat_src_rerun_tbl = `${ref_db}.${wrk_schema}.GetPartnerRewardReconciliation_Flat_Rerun`;
	// If the topic folder ESED_Competitor is directly inside its-dev-kafka-topics or its-qa-kafka-topics
	// Then just give topic_nm_location = 'ESED_Competitor/'
	// if it's inside of inside its-dev-kafka-topics/EDM_PHASE2 then topic_nm_location = 'EDM_Phase2/ESED_Competitor/   
	var topic_nm_location = 'ESED_C01_PartnerRewardReconciliation/';

	//create stage
	var create_stage = `CREATE OR REPLACE stage ${stage_nm}
	url = '${azure_blob_url}'
	STORAGE_INTEGRATION = ${storage_integration}
	;`
	try {
		snowflake.execute({ sqlText: create_stage });
	} catch (err) {
		throw `Creation of stage failed with error: ${err}`;   // Return a error message.
	}


	// Create Stream on Variant table
	var create_stream_on_variant = `CREATE OR REPLACE STREAM ${stream_nm} ON TABLE ${ref_db}.${ref_schema}.${variant_nm}`;
	try {
        snowflake.execute({ sqlText: create_stream_on_variant });
    } catch (err) {
        throw `Creation of stream on variant table failed with error: ${err}`;   // Return a error message.
    }


	// check if rerun queue table exists otherwise create it
	var variant_sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS ${variant_src_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT * FROM ${variant_src_tbl} where 1=2`;
	try {
		snowflake.execute({ sqlText: variant_sql_crt_rerun_tbl });
	} catch (err) {
		throw `Creation of rerun queue table ${variant_src_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
	}
	
	// create pipe  
	var create_pipe = `CREATE OR REPLACE pipe ${pipe_nm}
	auto_ingest = true
	integration = '${pipe_integration}'
	as
	copy into ${ref_db}.${ref_schema}.${variant_nm}(filename, src_xml) from
	(
        select metadata$filename, $1
	    from @${stage_nm}/${topic_nm_location}
	)
	file_format = (type='XML')
	on_error = 'SKIP_FILE';`
	try {
        snowflake.execute({ sqlText: create_pipe });
    } catch (err) {
        throw `Creation of Pipe failed with error: ${err}`;   // Return a error message.
    }


	var create_stream_on_flat = `CREATE OR REPLACE STREAM ${bod_nm}_Flat_R_STREAM ON TABLE ${ref_db}.${ref_schema}.${bod_nm}_Flat`;

	try {
        snowflake.execute({ sqlText: create_stream_on_flat });
    } catch (err) {
        throw `Creation of stream on flat table failed with error: ${err}`;   // Return a error message.
    }   

	var flat_sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS ${flat_src_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS SELECT * FROM ${flat_src_tbl} where 1=2 `;
	try {
        snowflake.execute({ sqlText: flat_sql_crt_rerun_tbl });
    }
    catch (err)  {
        throw `Creation of rerun queue table ${flat_src_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }

	// Task for FLAT Table
	var create_task = `CREATE OR REPLACE TASK ESED_PartnerRewardReconciliation_R_TASK
	WAREHOUSE = 'ESED_PARTNERREWARDRECONCILIATION_R_TASK'
	SCHEDULE = '1 minutes'
	WHEN
	SYSTEM$STREAM_HAS_DATA('ESED_PartnerRewardReconciliation_R_STREAM')
	AS 
	call sp_GetPartnerRewardReconciliation_To_FLAT_load();`

	try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task ${sqlText} Failed with error: ${err}`;   // Return a error message.
    }

	// To resume the task
	var resume_task = `ALTER TASK ESED_PartnerRewardReconciliation_R_TASK resume;`

	try {
        snowflake.execute({ sqlText: resume_task });
    }
    catch (err)  {
        throw `Creation of Task ${sqlText} Failed with error: ${err}`;   // Return a error message.
    }


$$;
