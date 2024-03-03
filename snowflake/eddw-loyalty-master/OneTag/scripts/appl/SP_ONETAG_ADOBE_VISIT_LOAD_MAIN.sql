--liquibase formatted sql
--changeset SYSTEM:SP_ONETAG_ADOBE_VISIT_LOAD_MAIN runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_<<ENV>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_ONETAG_ADOBE_VISIT_LOAD_MAIN()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER --required for autoscale
--EXECUTE AS OWNER
AS
$$
	// Global Variable Declaration
	var cur_db = snowflake.execute({ sqlText: `Select current_database()` });
	cur_db.next();
	var env = cur_db.getColumnValue(1);
	env = env.split('_');
	env = env [env.length - 1];
	var env_tbl_nm = `EDM_Environment_Variable_${env}`;
	var env_schema_nm = 'DW_R_MASTERDATA';
	var env_db_nm = `EDM_REFINED_${env}`;

	try {
		var rs = snowflake.execute({ sqlText: `SELECT * FROM ${env_db_nm}.${env_schema_nm}.${env_tbl_nm}` });
		var metaparams = {};
		while (rs.next()) { metaparams[rs.getColumnValue(1)] = rs.getColumnValue(2); }
		var ref_db = metaparams['REF_DB'];
        var cnf_db = metaparams['CNF_DB'];
        var cnf_schema = metaparams['C_USER_ACT'];
		var appl_schema = metaparams['APPL'];
        var tgt_schema = metaparams['A_EXP']; // DW_DIGITAL_EXP
		var tgt_db = metaparams['ANLYS_DB']; // EDM_ANALYTICS_DEV
		var ref_db = metaparams['REF_DB'];
        var wrk_schema = metaparams['C_STAGE'];
   
	} catch (err) {
		throw `Error while fetching data from EDM_Environment_Variable_${env}`;
	}
	
	var analytical_adobe =  `${cnf_db}.${cnf_schema}.CLICK_STREAM_OTHER`;
	var analytical_onetag = `${cnf_db}.${cnf_schema}.CUSTOMER_SESSION_EVENT_MASTER`;
    var analytical_visit = `${cnf_db}.${wrk_schema}.ONE_TAG_CLICK_STREAM_VISIT_WRK`;

	 // create table for the current transaction, 
	var sql_crt_src_wrk_tbl = `insert into ${analytical_visit}
                               select distinct OT.EVENT_ID,OT.session_id,AD.visit_id,AD.visit_page_nbr,OT.EVENT_TS,create_ts as ADOBE_CREATE_TS
                                from ${analytical_onetag} OT join 
                                ${analytical_adobe} AD 
                                ON OT.EVENT_ID=AD.ONETAG_EVENT_ID
                                where ad.ONETAG_EVENT_ID is not null
                                AND exclude_hit_flg = '0'
                                AND hit_source_cd NOT IN ('5','7','8','9')
                                AND AD.dw_current_version_ind='TRUE'
								and OT.EVENT_ID 
not in (select event_id from ${analytical_visit})`;
  
  //Autoscaling Code Start
   
	var sql_cmdl = `select count(*) as recordcount from ${analytical_onetag}`;
	var src_wrk_tbl_nm=`${analytical_onetag}`.toUpperCase();
	try {
		var stmtl=snowflake.createStatement({sqlText: sql_cmdl});
		var rsl=stmtl.execute();
		while (rsl.next()) {var record_count=rsl.getColumnValue(1);}
		var wh_selected = snowflake.execute({sqlText:`CALL ${ref_db}.${appl_schema}.SP_AutoScaling_based_on_data_Generic('${src_wrk_tbl_nm}','${record_count}');`});
	} catch (err) {
		throw `sp_autoscaling_based_on_data_Generic call failed with error:${err}`;
	}

	// Autoscaling Code end
	
    try {
    ret_obj = snowflake.execute({sqlText: sql_crt_src_wrk_tbl});
    ret_obj.next();
    return_msg = ret_obj.getColumnValue(1);
    } catch (err) {
    throw `Creation of ONE_TAG_CLICK_STREAM_VISIT Table Failed with error: ${err}`;
    }

$$;