--liquibase formatted sql
--changeset SYSTEM:SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_LOYALTY_REWARD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_LOYALTY_REWARD(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, VIEW_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

	var src_wrk_tbl = SRC_WRK_TBL;
	var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.EPISODIC_LOYALTY_REWARD_TMP_WRK`;
	var cnf_schema = C_LOYAL;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.EPISODIC_LOYALTY_REWARD_WRK`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.EPISODIC_LOYALTY_REWARD`;
	var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.EPISODIC_LOYALTY_REWARD_FLAT_RERUN`;
	var lkp_tbl_facility = `${VIEW_DB}.DW_VIEWS.FACILITY`;


	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;


	//query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `INSERT INTO ${src_rerun_tbl} SELECT * FROM ${temp_wrk_tbl}`;


	// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace Transient table `+ temp_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 0 as
	SELECT * FROM `+ src_wrk_tbl +`
	UNION ALL
	SELECT * FROM `+ src_rerun_tbl+``;
	
	try {
		snowflake.execute (	{sqlText: sql_crt_src_wrk_tbl });
	}
	catch (err) {
	throw "Creation of Source Work table "+ temp_wrk_tbl +" Failed with error: " + err; // Return a error message.
	}


	// **************        Load for Episodic_Drawing table BEGIN *****************
	// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `CREATE OR REPLACE TRANSIENT TABLE ${tgt_wrk_tbl} as 
	SELECT DISTINCT
	src.PROGRAM_ID,
	src.REWARD_ID,
	src.HOUSEHOLD_ID,
	src.DIVISION_NM,
	src.FACILITY_INTEGRATION_ID,
	src.RETAIL_STORE_ID,
	src.BANNER_NM,
	src.REWARD_ISSUED_CNT,
	src.CREATED_TS,
	src.EXTRACT_TS,
	src.FILE_NAME,
	src.DW_LOGICAL_DELETE_IND
	,CASE 
	WHEN (
	tgt.PROGRAM_ID IS NULL 
	and  tgt.REWARD_ID is NULL
	) 
	THEN 'I' 
	ELSE 'U' 
	END AS DML_Type
	,CASE   
	WHEN tgt.DW_FIRST_EFFECTIVE_DT = CURRENT_DATE 
	THEN 1 
	Else 0 
	END as SAMEDAY_CHG_IND
	FROM (
		SELECT
		PROGRAM_ID,
		REWARD_ID,
		HOUSEHOLD_ID,
		DIVISION_NM,
		FACILITY_INTEGRATION_ID,
		RETAIL_STORE_ID,
		BANNER_NM,
		REWARD_ISSUED_CNT,
		CREATED_TS,
		EXTRACT_TS,
		DW_CREATE_TS,
		FILE_NAME,
		DW_LOGICAL_DELETE_IND
		FROM( 
			SELECT
			PROGRAM_ID,
			REWARD_ID,
			HOUSEHOLD_ID,
			DIVISION_NM,
			FACILITY_INTEGRATION_ID,
			RETAIL_STORE_ID,
			BANNER_NM,
			REWARD_ISSUED_CNT,
			CREATED_TS,
			EXTRACT_TS,
			DW_CREATE_TS,
			FILE_NAME,
			false as  DW_LOGICAL_DELETE_IND,
			Row_number() OVER (
			PARTITION BY PROGRAM_ID,REWARD_ID order by DW_CREATE_TS DESC,to_timestamp_ntz(EXTRACT_TS) DESC) as rn
			FROM(
				SELECT
				PROGRAM_ID,
				REWARD_ID,
				HOUSEHOLD_ID,
				DIVISION_NM,
				FACILITY_INTEGRATION_ID,
				RETAIL_STORE_ID,
				BANNER_NM,
				REWARD_ISSUED_CNT,
				CREATED_TS,
				EXTRACT_TS,
				DW_CREATE_TS,
				FILE_NAME 
				FROM
					(
					SELECT  
					PROGRAM_ID, 
					to_number(REWARD_ID) AS REWARD_ID, 
					to_number(HOUSEHOLD_ID) AS HOUSEHOLD_ID, 
					DIVISION AS DIVISION_NM, 
					STORE_ID AS RETAIL_STORE_ID, 
					BANNER AS BANNER_NM, 
					to_number(REWARDS_AMOUNT) AS REWARD_ISSUED_CNT, 
					to_timestamp_ntz(CREATED_TS) AS CREATED_TS, 
					to_timestamp_ntz(EXTRACT_TS) AS EXTRACT_TS, 
					DW_CREATE_TS, 
					FILE_NAME
					FROM 
					${temp_wrk_tbl}
					) S
					LEFT JOIN 
					(
					SELECT 
					FACILITY_INTEGRATION_ID,
					CORPORATION_ID,
					FACILITY_NBR
					from ${lkp_tbl_facility}
					) F ON S.RETAIL_STORE_ID = F.FACILITY_NBR AND F.CORPORATION_ID = '001'
				)
			) where rn=1 AND PROGRAM_ID is NOT NULL AND REWARD_ID is NOT NULL
		)  src
	LEFT JOIN
		( 
		SELECT  DISTINCT
		PROGRAM_ID,
		REWARD_ID,
		HOUSEHOLD_ID,
		DIVISION_NM,
		FACILITY_INTEGRATION_ID,
		RETAIL_STORE_ID,
		BANNER_NM,
		REWARD_ISSUED_CNT,
		CREATED_TS,
		EXTRACT_TS,
		DW_CREATE_TS,
		DW_LOGICAL_DELETE_IND,
		DW_FIRST_EFFECTIVE_DT
		FROM
		${tgt_tbl} tgt
		WHERE DW_CURRENT_VERSION_IND = TRUE
		)as tgt 
		ON
		nvl(src.PROGRAM_ID ,'-1') = nvl(tgt.PROGRAM_ID ,'-1')
		and  nvl(src.REWARD_ID,'-1') = nvl(tgt.REWARD_ID ,'-1')
	WHERE(
	tgt.PROGRAM_ID IS  NULL
	AND tgt.REWARD_ID is  NULL
	)
	OR
	(
	NVL(src.HOUSEHOLD_ID,'-1') <> NVL(tgt.HOUSEHOLD_ID,'-1')  
	OR NVL(src.Division_Nm,'-1') <> NVL(tgt.Division_Nm,'-1')
	OR NVL(src.FACILITY_INTEGRATION_ID,-1) <> NVL(tgt.FACILITY_INTEGRATION_ID,-1)
	OR NVL(src.RETAIL_STORE_ID,'-1') <> NVL(tgt.RETAIL_STORE_ID,'-1')
	OR NVL(src.BANNER_NM,-1) <> NVL(tgt.BANNER_NM,-1)
	OR NVL(src.REWARD_ISSUED_CNT,-1) <> NVL(tgt.REWARD_ISSUED_CNT,-1)
	OR NVL(src.CREATED_TS,'9999-12-31 00:00:00.000') <> NVL(tgt.CREATED_TS,'9999-12-31 00:00:00.000')
	OR NVL(src.EXTRACT_TS,'9999-12-31 00:00:00.000') <>NVL(tgt.EXTRACT_TS,'9999-12-31 00:00:00.000')
	OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND  
	)`;   

	
	try {
		snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
	catch (err) {
		snowflake.execute ({ sqlText: sql_ins_rerun_tbl});
		return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
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
	PROGRAM_ID
	,REWARD_ID
	,FILE_NAME
	FROM ${tgt_wrk_tbl}
	WHERE 
	DML_Type = 'U' 
	AND SAMEDAY_CHG_IND = 0
	) src
	WHERE
	nvl(src.PROGRAM_ID,'-1') = nvl(tgt.PROGRAM_ID,'-1')
	AND nvl(src.REWARD_ID,'-1') = nvl(tgt.REWARD_ID,'-1')					
	AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
	

	// SCD Type1 - Processing Sameday updates
	var sql_sameday = `UPDATE ${tgt_tbl} as tgt
	SET
	HOUSEHOLD_ID = src.HOUSEHOLD_ID,
	DIVISION_NM = src.DIVISION_NM,
	FACILITY_INTEGRATION_ID = src.FACILITY_INTEGRATION_ID,
	RETAIL_STORE_ID = src.RETAIL_STORE_ID,
	BANNER_NM = src.BANNER_NM,
	REWARD_ISSUED_CNT = src.REWARD_ISSUED_CNT,
	CREATED_TS = src.CREATED_TS,
	EXTRACT_TS = src.EXTRACT_TS,
	DW_LOGICAL_DELETE_IND = src.DW_LOGICAL_DELETE_IND,
	DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
	DW_SOURCE_UPDATE_NM = src.FILE_NAME
	FROM ( 
	SELECT
	PROGRAM_ID,
	REWARD_ID,
	HOUSEHOLD_ID,
	DIVISION_NM,
	FACILITY_INTEGRATION_ID,
	RETAIL_STORE_ID,
	BANNER_NM,
	REWARD_ISSUED_CNT,
	CREATED_TS,
	EXTRACT_TS,
	FILE_NAME,
	DW_LOGICAL_DELETE_IND
	FROM ${tgt_wrk_tbl}
	WHERE 
	DML_Type = 'U' 
	AND Sameday_chg_ind = 1
	) src WHERE
	nvl(src.PROGRAM_ID,'-1') = nvl(tgt.PROGRAM_ID,'-1')
	AND nvl(src.REWARD_ID,'-1') = nvl(tgt.REWARD_ID,'-1')	
	AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
	
	
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} 
	(
	PROGRAM_ID,
	REWARD_ID,
	DW_FIRST_EFFECTIVE_DT,
	DW_LAST_EFFECTIVE_DT,
	HOUSEHOLD_ID,
	DIVISION_NM,
	FACILITY_INTEGRATION_ID,
	RETAIL_STORE_ID,
	BANNER_NM,
	REWARD_ISSUED_CNT,
	CREATED_TS,
	EXTRACT_TS,
	DW_CREATE_TS,
	DW_LAST_UPDATE_TS,
	DW_LOGICAL_DELETE_IND,
	DW_SOURCE_CREATE_NM,
	DW_SOURCE_UPDATE_NM,
	DW_CURRENT_VERSION_IND
	)
	SELECT
	PROGRAM_ID,
	REWARD_ID,
	CURRENT_DATE,
	'31-DEC-9999',
	HOUSEHOLD_ID,
	DIVISION_NM,
	FACILITY_INTEGRATION_ID,
	RETAIL_STORE_ID,
	BANNER_NM,
	REWARD_ISSUED_CNT,
	CREATED_TS,
	EXTRACT_TS,
	CURRENT_TIMESTAMP,
	NULL,
	DW_LOGICAL_DELETE_IND,
	FILE_NAME,
	NULL,
	TRUE 
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


return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
}		

// **************        Load for Episodic_Drawing Table ENDs *****************


$$;
