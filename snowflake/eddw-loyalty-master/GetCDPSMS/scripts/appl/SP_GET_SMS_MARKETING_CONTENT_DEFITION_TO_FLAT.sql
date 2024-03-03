--liquibase formatted sql
--changeset SYSTEM:SP_GET_SMS_MARKETING_CONTENT_DEFITION_TO_FLAT runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME_R>>.DW_APPL.SP_GET_SMS_MARKETING_CONTENT_DEFITION_TO_FLAT()
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
		var azure_blob_url = metaparams['AZURE_DIRECT_FEEDS_URL']; 
		var pipe_integration = metaparams['PIPE_INTEGRATION'];
		var storage_integration = metaparams['STORAGE_INTEGRATION'];
		var warehouse = metaparams['WAREHOUSE'];
		var topic_nm_location = 'CDP_Marketing_Content_Definition/';
	} catch (err) { 
		throw `Error while fetching data from EDM_Environment_Variable_${env}`;
	}
	
	var flat_nm = 'Get_SMS_Marketing_Defition_FLAT';
    var bod_nm = 'Get_SMS_Marketing_Defition';
    var short_bod_nm = bod_nm.substring(4);
    var stage_nm = `EDM_${short_bod_nm}_STAGE_${env}BLOB_INC`;
	var pipe_nm = `EDM_${short_bod_nm}_PIPE_${env}BLOB_INC`;
	var flat_src_tbl = `${ref_db}.${app_schema}.Get_SMS_Marketing_Defition_Flat_R_STREAM`;
	var flat_src_rerun_tbl = `${ref_db}.${wrk_schema}.Get_SMS_Marketing_Defition_Flat_Rerun`;

//create stage
	var create_stage = `CREATE OR REPLACE stage ${stage_nm}
	url = '${azure_blob_url}' 
	STORAGE_INTEGRATION = ${storage_integration};`
	try {
		snowflake.execute({ sqlText: create_stage });
	} catch (err) {
		throw `Creation of stage failed with error: ${err}`;   // Return a error message.
	}


// Create Fileformat type
  var create_file_format =`CREATE OR REPLACE FILE FORMAT CSV_SMS_MCD  TYPE =csv COMPRESSION = 'AUTO'
                            FIELD_DELIMITER = ',' RECORD_DELIMITER = '\\n' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY ='"'
							TRIM_SPACE = FALSE ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' ESCAPE_UNENCLOSED_FIELD = '\\134'
				            DATE_FORMAT = 'AUTO' TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('\\\\N')`;
	try {
        snowflake.execute({ sqlText: create_file_format });
    } catch (err) {
        throw `Creation of Fileformat failed with error: ${err}`;  // Return a error message.
	                }						

// Create Pipe
	var create_pipe = `CREATE OR REPLACE pipe ${pipe_nm}
	auto_ingest = true
	integration = '${pipe_integration}'
	as
	COPY INTO ${ref_db}.${ref_schema}.${flat_nm}
	FROM
	(select $1 ,$2 ,$3 ,$4 ,$5 ,$6 ,$7 ,$8 ,$9 ,$10, metadata$filename, CURRENT_TIMESTAMP
		from @${stage_nm}/${topic_nm_location} 
	 )
	 file_format = 'CSV_SMS_MCD'
	 pattern='.*.*[.]csv'
     on_error = 'SKIP_FILE';`
	try {
		snowflake.execute({ sqlText: create_pipe });
		} catch (err) {
		throw `Creation of Pipe failed with error: ${err}`;   // Return a error message.
		}
		

// Create Stream on flat table		
var create_stream_on_flat = `CREATE OR REPLACE STREAM ${bod_nm}_Flat_R_STREAM ON TABLE ${ref_db}.${ref_schema}.${bod_nm}_Flat`;

	try {
        snowflake.execute({ sqlText: create_stream_on_flat });
    } catch (err) {
        throw `Creation of stream on flat table failed with error: ${err}`;   // Return a error message.
    }   
	

// Create Rerun table on Stream
	var flat_sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS ${flat_src_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS 
	SELECT * FROM ${flat_src_tbl} where 1=2`;

	try {
        snowflake.execute({ sqlText: flat_sql_crt_rerun_tbl });
    } catch (err) {
        throw `Creation of rerun table on stream failed with error: ${err}`;   // Return a error message.
    } 
	

$$;
