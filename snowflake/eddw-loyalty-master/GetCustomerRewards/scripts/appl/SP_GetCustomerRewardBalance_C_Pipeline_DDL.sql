--liquibase formatted sql
--changeset SYSTEM:SP_GetCustomerRewardBalance_C_Pipeline_DDL runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETCUSTOMERREWARDBALANCE_C_PIPELINE_DDL()
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
        var cnf_schema = metaparams['C_LOYAL'];
        var ref_db = metaparams['REF_DB'];
        var app_schema = metaparams['APPL']; 
        var views_db = `${metaparams['VIEWS_DB']}_${env}`;
        var views_schema = metaparams['VIEWS'];
        var warehouse = metaparams['WAREHOUSE'];
    } catch (err) {
        throw `Error while fetching data from EDM_Environment_Variable_${env}`;
    }


    try {
	var create_tbl=`CREATE OR REPLACE TABLE ${cnf_db}.${cnf_schema}.Customer_Reward_Balance
(
 Household_Id          NUMBER  NOT NULL ,
 Balance_Update_Ts     TIMESTAMP  NOT NULL ,
 DW_First_Effective_Dt  DATE  NOT NULL ,
 DW_Last_Effective_Dt  DATE  NOT NULL ,
 Reward_Origin_Cd      VARCHAR(20)  ,
 Reward_Origin_Dsc     VARCHAR(50)  ,
 Reward_Dollar_Point_Qty  NUMBER  ,
 Reward_Token_Point_Qty  NUMBER  ,
 Reward_Token_Point_Expiry_Qty  NUMBER  ,
 Reward_Dollar_Start_Ts  TIMESTAMP  ,
 Reward_Dollar_End_Ts  TIMESTAMP  ,
 Reward_Token_End_Ts   TIMESTAMP  ,
 Reward_Token_Will_Expiry_End_Ts  TIMESTAMP  ,
 DW_CREATE_TS          TIMESTAMP  ,
 DW_LAST_UPDATE_TS     TIMESTAMP  ,
 DW_LOGICAL_DELETE_IND  BOOLEAN  ,
 DW_SOURCE_CREATE_NM   VARCHAR(255)  ,
 DW_SOURCE_UPDATE_NM   VARCHAR(255)  ,
 DW_CURRENT_VERSION_IND  BOOLEAN 
)
COPY GRANTS;`

snowflake.execute( {sqlText: create_tbl} );
} catch (err) {
throw `Error while creating table: ${err}`;
}


    
    try {
	var alter_tbl=`ALTER TABLE ${cnf_db}.${cnf_schema}.Customer_Reward_Balance
 ADD PRIMARY KEY (Household_Id, Balance_Update_Ts, DW_First_Effective_Dt, DW_Last_Effective_Dt);`

snowflake.execute( {sqlText: alter_tbl} );
} catch (err) {
throw `Error while creating table: ${err}`;
}


    
    try {
	var create_view=`CREATE OR REPLACE VIEW ${views_db}.${views_schema}.Customer_Reward_Balance COPY GRANTS COMMENT='View For Customer_Reward_Balance' AS
SELECT * FROM ${cnf_db}.${cnf_schema}.Customer_Reward_Balance;`

snowflake.execute( {sqlText: create_view} );
} catch (err) {
throw `Error while creating table: ${err}`;
}


    

    // Task for FLAT Table
	var create_task = `CREATE OR REPLACE TASK SP_GetCustomerRewardBalance_To_BIM_load_TASK
	WAREHOUSE = '${warehouse}'
	SCHEDULE = '1 minutes'
	WHEN SYSTEM$STREAM_HAS_DATA('${ref_db}.${app_schema}.GetCustomerRewardBalance_FLAT_R_STREAM')
	AS 
	CALL SP_GetCustomerRewardBalance_To_BIM_load();`

	try {
        snowflake.execute({ sqlText: create_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }

    // To resume the task
	var resume_task = `ALTER TASK SP_GetCustomerRewardBalance_To_BIM_load_TASK resume;`

	try {
        snowflake.execute({ sqlText: resume_task });
    }
    catch (err)  {
        throw `Creation of Task Failed with error: ${err}`;   // Return a error message.
    }


$$;
