--liquibase formatted sql
--changeset SYSTEM:SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_LAND_DETAIL runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_LAND_DETAIL(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var src_wrk_tbl = SRC_WRK_TBL;
		var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.Episodic_Land_Detail_tmp_WRK`;
		var cnf_schema = C_LOYAL;
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Episodic_Land_Detail_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.Episodic_Land_Detail`;
		var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.EPISODIC_LANDDETAILS_FLAT_RERUN`;





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






		// ************** Load for Episodic_LandDetail table BEGIN *****************
		// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
					var create_tgt_wrk_table = `CREATE OR REPLACE TRANSIENT  TABLE ${tgt_wrk_tbl} as
											SELECT DISTINCT
											src.Land_Detail_id
											,src.Household_Id
											,src.Land_Nm
											,src.Iteration_Cnt
											,src.Completed_Flg
											,src.Created_Ts
											,src.Last_Updated_Ts
											,src.Program_Id
											,src.Extract_Ts
											,src.DW_CREATE_TS
											,src.FILE_NAME
											,src.DW_LOGICAL_DELETE_IND
											,CASE
											WHEN (
											tgt.Program_Id IS NULL
											and tgt.Land_Detail_id is NULL
											)
											THEN 'I'
											ELSE 'U'
											END AS DML_Type
											,CASE
											WHEN tgt.DW_First_Effective_dt = CURRENT_DATE
											THEN 1
											Else 0
											END as Sameday_chg_ind
											FROM (
											SELECT
											Land_Detail_id
											,Household_Id
											,Land_Nm
											,Iteration_Cnt
											,Completed_Flg
											,Created_Ts
											,Last_Updated_Ts
											,Program_Id
											,Extract_Ts
											,DW_CREATE_TS
											,FILE_NAME
											,DW_LOGICAL_DELETE_IND





											FROM (
											SELECT
											Land_Detail_id
											,Household_Id
											,Land_Nm
											,Iteration_Cnt
											,Completed_Flg
											,Created_Ts
											,Last_Updated_Ts
											,Program_Id
											,Extract_Ts
											,FILE_NAME
											,DW_CREATE_TS
											,false as DW_LOGICAL_DELETE_IND
											,Row_number() OVER (
											PARTITION BY Program_Id,Land_Detail_id
											ORDER BY to_timestamp_ntz(Extract_Ts) DESC) as rn
											FROM(
											SELECT
											Land_Detail_id
											,Household_Id
											,Land_Nm
											,Iteration_Cnt
											,Completed_Flg
											,Created_Ts
											,Last_Updated_Ts
											,Program_Id
											,Extract_Ts
											,FILE_NAME
											,DW_CREATE_TS
											FROM
											(



											SELECT
											Land_Details_Id as Land_Detail_id
											,Household_Id
											,Land_Name as Land_Nm
											,Iteration as Iteration_Cnt
											,Completed as Completed_Flg
											,to_timestamp_ntz(Created_Ts) as Created_Ts
											,to_timestamp_ntz(Last_Updated) as Last_Updated_Ts
											,Program_Id
											,to_timestamp_ntz(Extract_Ts) as Extract_Ts
											,FILE_NAME
											,DW_CREATE_TS
											FROM ${temp_wrk_tbl} S
											) 
										)
										) where rn=1
												AND Program_Id is NOT NULL
												AND Land_Detail_id is NOT NULL
										) src



										LEFT JOIN
										(
										SELECT DISTINCT
										Land_Detail_id
										,Household_Id
										,Land_Nm
										,Iteration_Cnt
										,Completed_Flg
										,Created_Ts
										,Last_Updated_Ts
										,Program_Id
										,Extract_Ts
										,DW_First_Effective_dt
										,DW_LOGICAL_DELETE_IND
										FROM ${tgt_tbl} tgt
										WHERE DW_CURRENT_VERSION_IND = TRUE
										)AS tgt
										ON
										NVL(src.Program_Id ,'-1') = NVL(tgt.Program_Id ,'-1')
										and NVL(src.Land_Detail_id,'-1') = NVL(tgt.Land_Detail_id ,'-1')
										WHERE (
										tgt.Program_Id IS NULL
										AND tgt.Land_Detail_id is NULL
										)
										OR
										(
										NVL(src.Program_Id,'-1') <> NVL(tgt.Program_Id,'-1')
										OR NVL(src.Land_Detail_id ,'-1') <> NVL(tgt.Land_Detail_id ,'-1')
										OR NVL(src.Household_Id,'-1') <> NVL(tgt.Household_Id,'-1')
										OR NVL(src.Land_Nm,'-1') <> NVL(tgt.Land_Nm,'-1')
										OR NVL(src.Iteration_Cnt,'-1') <> NVL(tgt.Iteration_Cnt,'-1')
										OR NVL(src.Completed_Flg,'-1') <> NVL(tgt.Completed_Flg,'-1')
										OR NVL(src.Created_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Created_Ts,'9999-12-31 00:00:00.000')
										OR NVL(src.Last_Updated_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Last_Updated_Ts,'9999-12-31 00:00:00.000')
										OR NVL(src.Extract_Ts,'9999-12-31 00:00:00.000') <>NVL(tgt.Extract_Ts,'9999-12-31 00:00:00.000')
										OR src.DW_LOGICAL_DELETE_IND <> tgt.DW_LOGICAL_DELETE_IND
										)`;
//return create_tgt_wrk_table;



try {
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
					DW_SOURCE_UPDATE_NM = FILE_NAME
					FROM (
					SELECT
					Program_Id
					,Land_Detail_id
					,FILE_NAME
					FROM ${tgt_wrk_tbl}
					WHERE
					DML_Type = 'U'
					AND Sameday_chg_ind = 0
					) src
					WHERE
					NVL(src.Program_Id,'-1') = NVL(tgt.Program_Id,'-1')
					AND NVL(src.Land_Detail_id,'-1') = NVL(tgt.Land_Detail_id,'-1')
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;




		// SCD Type1 - Processing Sameday updates
		var sql_sameday = `UPDATE ${tgt_tbl} as tgt
					SET Program_Id = src.Program_Id,
					Land_Detail_id = src.Land_Detail_id,
					Household_Id = src.Household_Id,
					Land_Nm = src.Land_Nm,
					Iteration_Cnt = src.Iteration_Cnt,
					Completed_Flg = src.Completed_Flg,
					Created_Ts = src.Created_Ts,
					Last_Updated_Ts=src.Last_Updated_Ts,
					Extract_Ts=src.Extract_Ts,
					DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = FILE_NAME
					FROM (
					SELECT
					Program_Id
					,Land_Detail_id
					,Household_Id
					,Land_Nm
					,Iteration_Cnt
					,Completed_Flg
					,Created_Ts
					,Last_Updated_Ts
					,Extract_Ts
					,FILE_NAME
					,DW_Logical_delete_ind
					FROM ${tgt_wrk_tbl}
					WHERE
					DML_Type = 'U'
					AND Sameday_chg_ind = 1
					) src WHERE
					NVL(src.Program_Id,'-1') = NVL(tgt.Program_Id,'-1')
					AND NVL(src.Land_Detail_id,'-1') = NVL(tgt.Land_Detail_id,'-1')
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;



		// Processing Inserts
		var sql_inserts = `INSERT INTO ${tgt_tbl}
				(
				Program_Id
				,Land_Detail_id
				,DW_First_Effective_Dt
				,DW_Last_Effective_Dt
				,Household_Id
				,Land_Nm
				,Iteration_Cnt
				,Completed_Flg
				,Created_Ts
				,Last_Updated_Ts
				,Extract_Ts
				,DW_CREATE_TS
				,DW_LOGICAL_DELETE_IND
				,DW_SOURCE_CREATE_NM
				,DW_CURRENT_VERSION_IND
				)
				SELECT
				Program_Id
				,Land_Detail_id
				,CURRENT_DATE
				,'31-DEC-9999'
				,Household_Id
				,Land_Nm
				,Iteration_Cnt
				,Completed_Flg
				,Created_Ts
				,Last_Updated_Ts
				,Extract_Ts
				,CURRENT_TIMESTAMP
				,DW_LOGICAL_DELETE_IND
				,FILE_NAME
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





// ************** Load for Episodic_Land_Detail Table ENDs *****************






$$;
