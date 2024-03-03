--liquibase formatted sql
--changeset SYSTEM:SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_SELECTION runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_SELECTION(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

						var src_wrk_tbl = SRC_WRK_TBL;
						var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.Episodic_Selection_tmp_WRK`;
						var cnf_schema = C_LOYAL;
						var cnf_schema_lkp = 'DW_C_LOCATION';
						var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Episodic_Selection_WRK`;
						var tgt_tbl = `${CNF_DB}.${cnf_schema}.Episodic_Selection`;
						var lkp_tbl = `${CNF_DB}.${cnf_schema_lkp}.Retail_Store`;
						var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.EPISODIC_SELECTIONS_FLAT_RERUN`;
						

// Empty the rerun queue table
var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;




//query to load rerun queue table when encountered a failure
var sql_ins_rerun_tbl = `INSERT INTO ${src_rerun_tbl} SELECT * FROM ${temp_wrk_tbl}`;




// persist stream data in work table for the current transaction, includes data from previous failed run



var sql_crt_src_wrk_tbl = `create or replace TRANSIENT table `+ temp_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 0 as
SELECT * FROM `+ src_wrk_tbl +`
UNION ALL
SELECT * FROM `+ src_rerun_tbl+``;

try {
snowflake.execute (
{sqlText: sql_crt_src_wrk_tbl }
);
}
catch (err) {
throw "Creation of Source Work table "+ temp_wrk_tbl +" Failed with error: " + err; // Return a error message.
}


						
						// ************** Load for Episodic_Selection table BEGIN *****************
						// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
						var create_tgt_wrk_table = `CREATE OR REPLACE TRANSIENT  TABLE ${tgt_wrk_tbl} as
							SELECT DISTINCT
								src.Program_Id
								,src.Select_Id
								,src.Household_Id
								,src.Facility_Integration_Id
								,src.Action_Cd
								,src.Retail_Store_Id
								,src.Reference_Id
								,src.Banner_Nm
								,src.Created_Ts
								,src.Extract_Ts
								,src.filename
								,src.DW_LOGICAL_DELETE_IND
								,CASE
								WHEN (
								tgt.Program_Id IS NULL
								and tgt.Select_Id is NULL
								)
								THEN 'I'
								ELSE 'U'
								END AS DML_Type
								,CASE
								WHEN tgt.DW_First_Effective_dt = CURRENT_DATE
								THEN 1
								Else 0
								END as Sameday_chg_ind
								FROM ( SELECT
								Program_Id
								,Select_Id
								,Household_Id
								,Facility_Integration_Id
								,Action_Cd
								,Retail_Store_Id
								,Reference_Id
								,Banner_Nm
								,Created_Ts
								,Extract_Ts
								,filename
								,DW_LOGICAL_DELETE_IND
								FROM (
								SELECT
								Program_Id
								,Select_Id
								,Household_Id
								,Facility_Integration_Id
								,Action_Cd
								,Retail_Store_Id
								,Reference_Id
								,Banner_Nm
								,Created_Ts
								,Extract_Ts
								,filename
								,DW_Create_Ts
								,false as DW_LOGICAL_DELETE_IND
								,Row_number() OVER (
								PARTITION BY Program_Id,Select_Id
								order by to_timestamp_ntz(Extract_Ts) DESC) as rn
								FROM(
								SELECT
								Program_Id
								,Select_Id
								,Household_Id
								,Facility_Integration_Id
								,Action_Cd
								,Retail_Store_Id
								,Reference_Id
								,Banner_Nm
								,Created_Ts
								,Extract_Ts
								,filename
								,DW_Create_Ts
								FROM
								(
								SELECT
								Program_Id
								,Select_Id
								,Household_Id
								//,Facility_Integration_Id
								,Action as Action_Cd
								,Store_Id as Retail_Store_Id
								,Reference_Id
								,Banner as Banner_Nm
								,to_timestamp_ntz(Created_Ts) as Created_Ts
								,to_timestamp_ntz(Extract_Ts) as Extract_Ts
								,File_name as filename
								,DW_Create_TS
								FROM
								${temp_wrk_tbl} S
								)src
								LEFT JOIN
								(
								SELECT Facility_Integration_Id,Facility_Nbr FROM ${CNF_DB}.${cnf_schema_lkp}.RETAIL_STORE
								WHERE
								DW_CURRENT_VERSION_IND=TRUE AND DW_LOGICAL_DELETE_IND=FALSE
								) AS RS ON
								RS.Facility_Nbr = src.Retail_Store_Id
								)
								) where rn=1 
                                  AND Program_Id is NOT NULL
								AND Select_Id is NOT NULL
								) src
								LEFT JOIN
								(
								SELECT DISTINCT
								Program_Id
								,Select_Id
								,Household_Id
								,Facility_Integration_Id
								,Action_Cd
								,Retail_Store_Id
								,Reference_Id
								,Banner_Nm
								,Created_Ts
								,Extract_Ts
								,DW_First_Effective_dt
								,DW_LOGICAL_DELETE_IND
								FROM
								${tgt_tbl} tgt
								WHERE DW_CURRENT_VERSION_IND = TRUE
								)as tgt
								ON
								nvl(src.Program_Id ,'-1') = nvl(tgt.Program_Id ,'-1')
								and nvl(src.Select_Id,'-1') = nvl(tgt.Select_Id ,'-1')
								WHERE (
								tgt.Program_Id IS NULL
								AND tgt.Select_Id is NULL
								)
								OR
								(
								NVL(src.Program_Id,'-1') <> NVL(tgt.Program_Id,'-1')
								OR NVL(src.Select_Id ,'-1') <> NVL(tgt.Select_Id ,'-1')
								OR NVL(src.Household_Id,'-1') <> NVL(tgt.Household_Id,'-1')
								OR NVL(src.Facility_Integration_Id,'-1') <> NVL(tgt.Facility_Integration_Id,'-1')
								OR NVL(src.Action_Cd,'-1') <> NVL(tgt.Action_Cd,'-1')
								OR NVL(src.Retail_Store_Id,'-1') <> NVL(tgt.Retail_Store_Id,'-1')
								OR NVL(src.Reference_Id,'-1') <>NVL(tgt.Reference_Id,'-1')
								OR NVL(src.Banner_Nm,'-1') <> NVL(tgt.Banner_Nm,'-1')
								OR NVL(src.Created_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Created_Ts,'9999-12-31 00:00:00.000')
								OR NVL(src.Extract_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Extract_Ts,'9999-12-31 00:00:00.000')
								OR src.DW_LOGICAL_DELETE_IND <> tgt.DW_LOGICAL_DELETE_IND
								)`;try {
								snowflake.execute ({ sqlText: create_tgt_wrk_table });
								}
					catch (err) {
					
					snowflake.execute ({ sqlText: sql_ins_rerun_tbl});


	return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`; // Return a error message.
								}
		// Transaction for Updates, Insert begins
		var sql_begin = "BEGIN"
		// SCD Type2 - Processing Different day updates
		var sql_updates = `UPDATE ${tgt_tbl} as tgt
								SET
								DW_Last_Effective_dt = CURRENT_DATE - 1,
								DW_CURRENT_VERSION_IND = FALSE,
								DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
								DW_SOURCE_UPDATE_NM = filename
								FROM (
								SELECT
								Program_Id
								,Select_Id
								,filename
								FROM ${tgt_wrk_tbl}
								WHERE
								DML_Type = 'U'
								AND Sameday_chg_ind = 0
								) src
								WHERE
								nvl(src.Program_Id,'-1') = nvl(tgt.Program_Id,'-1')
								AND nvl(src.Select_Id,'-1') = nvl(tgt.Select_Id,'-1')
								AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
		// SCD Type1 - Processing Sameday updates
		var sql_sameday = `UPDATE ${tgt_tbl} as tgt
								SET Program_Id = src.Program_Id,
								Select_Id = src.Select_Id,
								Household_Id = src.Household_Id,
								Facility_Integration_Id=src.Facility_Integration_Id,
								Action_Cd = src.Action_Cd,
								Retail_Store_Id = src.Retail_Store_Id,
								Reference_Id=src.Reference_Id,
								Banner_Nm = src.Banner_Nm,
								Created_Ts = src.Created_Ts,
								Extract_Ts = src.Extract_Ts,
								DW_Logical_delete_ind = src.DW_Logical_delete_ind,
								DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
								DW_SOURCE_UPDATE_NM = filename
								FROM (
								SELECT
								Program_Id
								,Select_Id
								,Household_Id
								,Facility_Integration_Id
								,Action_Cd
								,Retail_Store_Id
								,Reference_Id
								,Banner_Nm
								,Created_Ts
								,Extract_Ts
								,filename
								,DW_Logical_delete_ind
								FROM ${tgt_wrk_tbl}
								WHERE
								DML_Type = 'U'
								AND Sameday_chg_ind = 1
								) src WHERE
								nvl(src.Program_Id,'-1') = nvl(tgt.Program_Id,'-1')
								AND nvl(src.Select_Id,'-1') = nvl(tgt.Select_Id,'-1')
								AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
		// Processing Inserts
		var sql_inserts = `INSERT INTO ${tgt_tbl}
								(
								Program_Id
								,Select_Id
								,Household_Id
								,Facility_Integration_Id
								,Action_Cd
								,Retail_Store_Id
								,Reference_Id
								,Banner_Nm
								,Created_Ts
								,Extract_Ts
								,DW_First_Effective_Dt
								,DW_Last_Effective_Dt
								,Dw_Create_Ts
								//,Dw_Last_Update_Ts
								//,Dw_Source_Update_Nm
								,Dw_Logical_Delete_Ind
								,Dw_Source_Create_Nm
								,Dw_Current_Version_Ind
								)
								SELECT
								Program_Id
								,Select_Id
								,Household_Id
								,Facility_Integration_Id
								,Action_Cd
								,Retail_Store_Id
								,Reference_Id
								,Banner_Nm
								,Created_Ts
								,Extract_Ts
								,CURRENT_DATE
								,'31-DEC-9999'
								,CURRENT_TIMESTAMP
								//,Dw_Last_Update_Ts
								//,Dw_Source_Update_Nm
								,Dw_Logical_Delete_Ind
								,filename
								,TRUE
								FROM ${tgt_wrk_tbl}
								WHERE
								Sameday_chg_ind = 0`;
	var sql_commit = "COMMIT";
	var sql_rollback = "ROLLBACK";
	try {
	snowflake.execute ({ sqlText: sql_begin });
	snowflake.execute({ sqlText: sql_empty_rerun_tbl });
	snowflake.execute ({ sqlText: sql_updates });
	snowflake.execute ({ sqlText: sql_sameday });
	snowflake.execute ({ sqlText: sql_inserts });
	snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
	snowflake.execute ({ sqlText: sql_rollback });
	
	snowflake.execute ({ sqlText: sql_ins_rerun_tbl});
	
	
	return `Loading of table ${tgt_tbl} Failed with error: ${err}` ; // Return a error message.
	}
	// ************** Load for Episodic_Selection Table ENDs *****************

$$;
