--liquibase formatted sql
--changeset SYSTEM:SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_PROFILE runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETEPISODIC_TO_BIM_LOAD_EPISODIC_PROFILE(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$



var src_wrk_tbl = SRC_WRK_TBL;
var temp_wrk_tbl = `${CNF_DB}.${C_STAGE}.Episodic_Profile_tmp_WRK`;
var cnf_schema = C_LOYAL;
var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.EPISODIC_PROFILE_WRK`;
var tgt_tbl = `${CNF_DB}.${cnf_schema}.EPISODIC_PROFILE`;
var src_rerun_tbl = `${CNF_DB}.${C_STAGE}.EPISODIC_PROFILE_FLAT_RERUN`;

// Empty the rerun queue table
var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;

//query to load rerun queue table when encountered a failure
var sql_ins_rerun_tbl = `INSERT INTO ${src_rerun_tbl} SELECT HOUSEHOLD_ID,PROFILE_ID,REGISTRATION_DATE,RULES_ACCEPTED_TS,LAST_UPDATED,
PROGRAM_ID,EXTRACT_TS,DW_CREATE_TS,FILE_NAME,METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID,
PRIMARYUUID,IS_EMPLOYEE,RULES_ACCEPTED FROM ${temp_wrk_tbl}`;

// persist stream data in work table for the current transaction, includes data from previous failed run

var sql_empty_tmp_wrk_tbl = `TRUNCATE TABLE `+ temp_wrk_tbl +``;

var sql_crt_src_wrk_tbl = `create or replace Transient table `+ temp_wrk_tbl +` DATA_RETENTION_TIME_IN_DAYS = 0 as
SELECT * FROM `+ src_wrk_tbl +`
UNION ALL
SELECT HOUSEHOLD_ID,PROFILE_ID,REGISTRATION_DATE,RULES_ACCEPTED_TS,LAST_UPDATED,PROGRAM_ID,EXTRACT_TS,DW_CREATE_TS,
FILE_NAME,PRIMARYUUID,IS_EMPLOYEE,RULES_ACCEPTED,METADATA$ACTION,METADATA$ISUPDATE,METADATA$ROW_ID FROM `+ src_rerun_tbl+``;

try {
snowflake.execute ({sqlText: sql_empty_tmp_wrk_tbl});
snowflake.execute ({sqlText: sql_crt_src_wrk_tbl });
}
catch (err) {
throw "Creation of Source Work table "+ temp_wrk_tbl +" Failed with error: " + err; // Return a error message.
}

// ************** Load for EPISODIC_PROFILE table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var empty_tgt_wrk_table = `TRUNCATE TABLE ${tgt_wrk_tbl}`;

var create_tgt_wrk_table = `CREATE OR REPLACE TRANSIENT TABLE ${tgt_wrk_tbl} as 
SELECT DISTINCT
SRC.Household_Id
,SRC.Profile_Id
,SRC.Registration_Dt
,SRC.Rules_Accepted_Ts
,SRC.Last_Updated_TS
,SRC.Program_Id
,SRC.Extract_Ts
,SRC.filename
,SRC.DW_LOGICAL_DELETE_IND
,CASE
WHEN (
tgt.Household_Id IS NULL
and tgt.Program_Id is NULL
)
THEN 'I'
ELSE 'U'
END AS DML_Type
,CASE
WHEN tgt.DW_First_Effective_dt = CURRENT_DATE
THEN 1
Else 0
END as Sameday_chg_ind
,SRC.RETAIL_CUSTOMER_UUID
,SRC.EMPLOYEE_PROFILE_IND
,SRC.RULES_ACCEPTED_IND
FROM



(
SELECT
Household_Id
,Profile_Id
,Registration_Dt
,Rules_Accepted_Ts
,Last_Updated_TS
,Program_Id
,Extract_Ts
,DW_Create_Ts
,FILENAME
,DW_LOGICAL_DELETE_IND
,RETAIL_CUSTOMER_UUID
,EMPLOYEE_PROFILE_IND
,RULES_ACCEPTED_IND

FROM (
SELECT
Household_Id
,Profile_Id
,Registration_Dt
,Rules_Accepted_Ts
,Last_Updated_TS
,Program_Id
,Extract_Ts
,DW_Create_Ts
,FILENAME
,false as DW_LOGICAL_DELETE_IND
,RETAIL_CUSTOMER_UUID
,EMPLOYEE_PROFILE_IND
,RULES_ACCEPTED_IND
,Row_number() OVER (
PARTITION BY Household_Id,Program_Id
order by to_timestamp_ntz(Extract_Ts) DESC
) as rn
FROM
(SELECT
Household_Id
,Profile_Id
,Registration_Dt
,Rules_Accepted_Ts
,Last_Updated_TS
,Program_Id
,Extract_Ts
,DW_Create_Ts
,FILENAME
,RETAIL_CUSTOMER_UUID
,EMPLOYEE_PROFILE_IND
,RULES_ACCEPTED_IND

FROM
(
SELECT
 Household_Id
,Profile_Id
,to_timestamp_ntz(Registration_Date) as Registration_Dt
,to_timestamp_ntz(Rules_Accepted_Ts) as Rules_Accepted_Ts
,to_timestamp_ntz(Last_Updated) as Last_Updated_TS
,Program_Id
,to_timestamp_ntz(Extract_Ts) as Extract_Ts
,to_timestamp_ntz(DW_Create_Ts) as DW_Create_Ts
,File_Name as FILENAME
,PRIMARYUUID AS RETAIL_CUSTOMER_UUID
,to_boolean(IS_EMPLOYEE) AS EMPLOYEE_PROFILE_IND
,to_boolean(RULES_ACCEPTED) AS RULES_ACCEPTED_IND
FROM ${temp_wrk_tbl} S
)
)
)
WHERE rn=1
AND Household_Id IS NOT NULL
AND Program_Id IS NOT NULL
) SRC



LEFT JOIN
(
SELECT
Household_Id
,Profile_Id
,Registration_Dt
,Rules_Accepted_Ts
,Last_Updated_TS
,Program_Id
,Extract_Ts
,DW_First_Effective_dt
,DW_LOGICAL_DELETE_IND
,RETAIL_CUSTOMER_UUID
,EMPLOYEE_PROFILE_IND
,RULES_ACCEPTED_IND
FROM
${tgt_tbl} tgt
WHERE DW_CURRENT_VERSION_IND = TRUE
)as tgt
ON
NVL(src.Household_Id,'-1')=NVL(tgt.Household_Id,'-1')
AND NVL(src.Program_Id,'-1')=NVL(tgt.Program_Id,'-1')
WHERE
(
tgt.Household_Id IS NULL
AND tgt.Program_Id IS NULL
)
OR
(
NVL(src.Household_Id,'-1')<>NVL(tgt.Household_Id,'-1')
OR NVL(src.Profile_Id,'-1')<>NVL(tgt.Profile_Id,'-1')
OR NVL(src.Registration_Dt,'9999-12-31 00:00:00.000')<>NVL(tgt.Registration_Dt,'9999-12-31 00:00:00.000')
OR NVL(src.Rules_Accepted_Ts,'9999-12-31 00:00:00.000')<>NVL(tgt.Rules_Accepted_Ts,'9999-12-31 00:00:00.000')
OR NVL(src.Last_Updated_TS,'9999-12-31 00:00:00.000')<>NVL(tgt.Last_Updated_TS,'9999-12-31 00:00:00.000')
OR NVL(src.Program_Id,'-1')<>NVL(tgt.Program_Id,'-1')
OR NVL(src.Extract_Ts,'9999-12-31 00:00:00.000')<>NVL(tgt.Extract_Ts,'9999-12-31 00:00:00.000')
OR NVL(src.RETAIL_CUSTOMER_UUID,'-1')<>NVL(tgt.RETAIL_CUSTOMER_UUID,'-1')
OR NVL(src.EMPLOYEE_PROFILE_IND,-1)<>NVL(tgt.EMPLOYEE_PROFILE_IND,-1)
OR NVL(src.RULES_ACCEPTED_IND,-1)<>NVL(tgt.RULES_ACCEPTED_IND,-1)
)`;

//return create_tgt_wrk_table;

try {
snowflake.execute ({sqlText: empty_tgt_wrk_table});
snowflake.execute ({ sqlText: create_tgt_wrk_table });

}
catch (err) {
snowflake.execute ({ sqlText: sql_ins_rerun_tbl});

throw `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`; // Return a error message.
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

FROM ( SELECT
HOUSEHOLD_ID
,PROGRAM_ID
,FILENAME
FROM ${tgt_wrk_tbl}
WHERE
DML_Type = 'U'
AND Sameday_chg_ind = 0
)src
WHERE
nvl(src.Household_Id, '-1') = nvl(tgt.Household_Id, '-1')
AND nvl(src.Program_Id, '-1') = nvl(tgt.Program_Id, '-1')
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;



// SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
SET
Household_Id = src.Household_Id
,Profile_Id = src.Profile_Id
,Registration_Dt = src.Registration_Dt
,Rules_Accepted_Ts = src.Rules_Accepted_Ts
,Last_Updated_TS = src.Last_Updated_TS
,Program_Id = src.Program_Id
,Extract_Ts = src.Extract_Ts
,DW_Logical_delete_ind = src.DW_Logical_delete_ind
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = filename
,RETAIL_CUSTOMER_UUID = src.RETAIL_CUSTOMER_UUID
,EMPLOYEE_PROFILE_IND = src.EMPLOYEE_PROFILE_IND
,RULES_ACCEPTED_IND = src.RULES_ACCEPTED_IND

FROM (
SELECT
Household_Id
,Profile_Id
,Registration_Dt
,Rules_Accepted_Ts
,Last_Updated_TS
,Program_Id
,Extract_Ts
,FILENAME
,DW_LOGICAL_DELETE_IND
,RETAIL_CUSTOMER_UUID
,EMPLOYEE_PROFILE_IND
,RULES_ACCEPTED_IND
FROM ${tgt_wrk_tbl}
WHERE
DML_Type = 'U'
AND Sameday_chg_ind = 1
)src
WHERE
nvl(src.Household_Id, '-1') = nvl(tgt.Household_Id, '-1')
AND nvl(src.Program_Id, '-1') = nvl(tgt.Program_Id, '-1')
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
(
Household_Id
,Profile_Id
,Registration_Dt
,Rules_Accepted_Ts
,Last_Updated_TS
,Extract_Ts
,DW_CREATE_TS
,DW_LOGICAL_DELETE_IND
,DW_SOURCE_CREATE_NM
,DW_CURRENT_VERSION_IND
,DW_FIRST_EFFECTIVE_DT
,DW_LAST_EFFECTIVE_DT
,PROGRAM_ID
,RETAIL_CUSTOMER_UUID
,EMPLOYEE_PROFILE_IND
,RULES_ACCEPTED_IND

)
SELECT
Household_Id
,Profile_Id
,Registration_Dt
,Rules_Accepted_Ts
,Last_Updated_TS
,Extract_Ts
,CURRENT_TIMESTAMP
,DW_LOGICAL_DELETE_IND
,filename
,TRUE
,CURRENT_DATE
,'31-DEC-9999'
,PROGRAM_ID
,RETAIL_CUSTOMER_UUID
,EMPLOYEE_PROFILE_IND
,RULES_ACCEPTED_IND
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

throw `Loading of table ${tgt_tbl} Failed with error: ${err}` ; // Return a error message.
}

// ************** Load for EPISODIC_PROFILE Table ENDs *****************$$;
