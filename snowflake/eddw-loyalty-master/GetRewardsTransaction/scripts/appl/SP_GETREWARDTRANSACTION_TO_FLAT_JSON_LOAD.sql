--liquibase formatted sql
--changeset SYSTEM:SP_GETREWARDTRANSACTION_TO_FLAT_JSON_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_REFINED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETREWARDTRANSACTION_TO_FLAT_JSON_LOAD()
RETURNS VARCHAR(16777216)
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
	var anlys_ref_db = metaparams['ANLYS_DB'];
	var anlys_ref_schema = metaparams['DATA_GOVRNC'];
	var anlys_wrk_schema = metaparams['A_STAGE'];
	} catch (err) { 
    throw `Error while fetching data from EDM_Environment_Variable_${env}`; 
	}
    var variant_nm = 'ESED_RewardTransaction_Json';
	var bod_nm = 'GetRewardTransaction';
    var src_tbl = `${ref_db}.${app_schema}.${variant_nm}_R_STREAM`;
    var src_wrk_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_wrk`;
    var src_rerun_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_Rerun`;
    var tgt_flat_tbl = `${ref_db}.${ref_schema}.${bod_nm}_FLAT`;
	
	var bodName = 'GetRewardTransaction';	
	
	// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace table ${src_wrk_tbl} as
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
			,LATERAL FLATTEN(tbl.src_json) 			
			) 
SELECT distinct FILENAME
          ,'GetRewardTransaction' as BODNm
          ,null as DocumentID
		,null as TargetApplicationCd
		,null as SourceApplicationCd
		,null as Description
		,null as InternalFileTransferInd
		,null as DocumentNm
		,null as InboundOutboundInd
		,null as AlternateDocumentID
		,null as CreationDt
		,null as PHIdataInd
		,null as PCIdataInd
		,null as PIIdataInd
		,null as BusinessSensitivityLevel_Code
		,null as DataClassificationLevel_Code
		,null as ActionTypeCd
		,null as RecordTypeCd
		,TransactionDetailTxt
		,null as CustomerId
		,HouseholdId
		,UpdateTs
		,CreateTs
		,CreateUserId
		,UpdateUserId
		,CustomerTierCd_Code
		,LoyaltyProgramCd
		,RewardDollarStartTs
		,RewardDollarEndTs
		,RewardDollarPointsQty
		,RewardOriginCd
		,RewardOriginTs
		,StatusCd
		,TransactionId
		,AltTransactionId
		,TransactionTypeCd_Code
		,null as Document_ReleaseId
		,null as Document_VersionId
		,null as Document_SystemEnvironmentCd
		,CURRENT_TIMESTAMP AS DW_CreateTs
		,BeforeTransactionSnapshot
		,AfterTransactionSnapshot
    FROM
    (
			select distinct reward.src_json:hhId::string as HouseholdId
							,filename
							,reward.src_json:transaction_id::string as TransactionId
							,reward.src_json:tier::string as CustomerTierCd_Code
							,reward.src_json:event_source::string as RewardOriginCd
							,reward.src_json:create_user::string as CreateUserId
							,reward.src_json:modified_user::string as UpdateUserId
							,a.value:program_cd::string as LoyaltyProgramCd
							,a.value:status::string as StatusCd
							,a.value:validity_end_dt::string as RewardDollarEndTs
							,a.value:transaction_type::string as TransactionTypeCd_Code
							,a.value:partner_transaction_id::string as AltTransactionId
							,a.value:points::string as RewardDollarPointsQty
							,a.value:event_ts::string as RewardOriginTs
							,a.value:after_snapShot::string as AfterTransactionSnapshot
							,a.value:before_snapshot::string as BeforeTransactionSnapshot
							,a.value:validity_start_dt::string as RewardDollarStartTs
							,a.value:transaction_breakdown_details::string as TransactionDetailTxt
							,a.value:create_ts::string as CreateTs
							,a.value:modified_ts::string as UpdateTs
			from lvl_1_flatten reward
			,LATERAL FLATTEN(input => reward.src_json:transactions, outer => TRUE) as a
)`;

    try {
        snowflake.execute ( {sqlText: insert_into_flat_dml} );
    } catch (err) { 
        snowflake.execute ( {sqlText: sql_ins_rerun_tbl} ); 
        throw `Loading of table ${tgt_flat_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// EDM Remediation sp call starts
function execEdmRemProc(rem_proc_nm, rem_params) 
    {
		try {
			 ret_obj = snowflake.execute (
						{sqlText: "call " + rem_proc_nm + "("+ rem_params +")"  }
						);
             ret_obj.next();
             ret_msg = ret_obj.getColumnValue(1);            

			}
		catch (err)  {
			return "Error executing stored procedure "+ rem_proc_nm + "("+ rem_params +")" + err;   // Return a error message.
			}
		return ret_msg;
	}
	
	var sub_proc_list = ['SP_EDM_Remediation_Recon_P1load']		
	 
for (index = 0; index < sub_proc_list.length; index++) 
    {
            rem_proc_nm = sub_proc_list[index];
               rem_params = "'"+ bodName +"','"+ src_wrk_tbl +"','"+ anlys_ref_db +"','"+ anlys_ref_schema +"','"+ anlys_wrk_schema +"'"; 
            return_msg = execEdmRemProc(rem_proc_nm, rem_params);
           
			/*
			if (return_msg)
            {
                snowflake.execute (
                        {sqlText: sql_ins_rerun_tbl }
                        );
                throw return_msg;
            }
			*/
    }

	// EDM Remediation sp call ends
$$;
