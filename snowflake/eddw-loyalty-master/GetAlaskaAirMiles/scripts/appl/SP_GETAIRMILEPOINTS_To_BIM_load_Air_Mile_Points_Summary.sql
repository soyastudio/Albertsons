--liquibase formatted sql
--changeset SYSTEM:SP_GETAIRMILEPOINTS_To_BIM_load_Air_Mile_Points_Summary runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETAIRMILEPOINTS_TO_BIM_LOAD_AIR_MILE_POINTS_SUMMARY(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

var cnf_db = CNF_DB ;
var cnf_schema = C_LOYAL;
var wrk_schema = C_STAGE;
var src_tbl = SRC_WRK_TBL;
var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Air_Mile_Points_Summary_WRK`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Air_Mile_Points_Summary`;
// ************** Load for EPE_Error table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.



var sql_command = `CREATE or Replace table  ${tgt_wrk_tbl} as
WITH src_wrk_tbl_recs as
(
select distinct
Batch_Id,
Batch_Start_Date_Txt,
Batch_End_Date_Txt,
Total_Air_Mile_Points_Qty,
Record_Cnt,
Total_Rejected_Air_Mile_Points_Qty,
Rejected_Record_Cnt,
Create_Ts,
Create_User_Id,
Update_Ts,
Update_User_Id,
Source_Type_Cd,
filename,
Row_Number() over(partition by Batch_Id order by(Create_Ts) desc) as rn
from
(
select
AIRMILEPOINTSSUMMARY_BATCHID AS Batch_Id,
AIRMILEPOINTSSUMMARY_BATCHSTARTDT AS Batch_Start_Date_Txt,
AIRMILEPOINTSSUMMARY_BATCHENDDT AS Batch_End_Date_Txt,
AIRMILEPOINTSSUMMARY_TOTALAIRMILEPOINTSQTY AS Total_Air_Mile_Points_Qty,
AIRMILEPOINTSSUMMARY_RECORDCNT AS Record_Cnt,
AIRMILEPOINTSSUMMARY_TOTALREJECTEDAIRMILEPOINTSQTY AS Total_Rejected_Air_Mile_Points_Qty,
AIRMILEPOINTSSUMMARY_REJECTEDRECORDCNT AS Rejected_Record_Cnt,
AIRMILEPOINTSSUMMARY_CREATETS AS Create_Ts,
AIRMILEPOINTSSUMMARY_CREATEUSERID AS Create_User_Id,
AIRMILEPOINTSSUMMARY_UPDATETS AS Update_Ts,
AIRMILEPOINTSSUMMARY_UPDATEUSERID AS Update_User_Id,
AIRMILEPOINTSSOURCETYPE_CODE AS Source_Type_Cd,
AIRMILEPOINTSSOURCETYPE_CODE,
filename
from
${SRC_WRK_TBL}
Where AIRMILEPOINTSSOURCETYPE_CODE = 'SUMMARY'
)
)
select
src.Batch_Id,
src.Batch_Start_Date_Txt,
src.Batch_End_Date_Txt,
src.Total_Air_Mile_Points_Qty,
src.Record_Cnt,
src.Total_Rejected_Air_Mile_Points_Qty,
src.Rejected_Record_Cnt,
src.Create_Ts,
src.Create_User_Id,
src.Update_Ts,
src.Update_User_Id,
src.Source_Type_Cd,
src.DW_Logical_delete_ind,
src.filename,
CASE WHEN (tgt.Batch_Id is NULL) THEN 'I' ELSE 'U' END AS DML_TYPE,
CASE WHEN tgt.DW_First_Effective_Dt = CURRENT_DATE THEN 1 ELSE 0 END AS SAMEDAY_CHG_IND
from
(select
Batch_Id,
Batch_Start_Date_Txt,
Batch_End_Date_Txt,
Total_Air_Mile_Points_Qty,
Record_Cnt,
Total_Rejected_Air_Mile_Points_Qty,
Rejected_Record_Cnt,
Create_Ts,
Create_User_Id,
Update_Ts,
Update_User_Id,
Source_Type_Cd,
false as DW_Logical_delete_ind,
filename
From src_wrk_tbl_recs
Where rn=1
and Batch_Id is not null
) src

left join
(select distinct
tgt.Batch_Id,
tgt.Batch_Start_Date_Txt,
tgt.Batch_End_Date_Txt,
tgt.Total_Air_Mile_Points_Qty,
tgt.Record_Cnt,
tgt.Total_Rejected_Air_Mile_Points_Qty,
tgt.Rejected_Record_Cnt,
tgt.Create_Ts,
tgt.Create_User_Id,
tgt.Update_Ts,
tgt.Update_User_Id,
tgt.Source_Type_Cd,
tgt.DW_First_Effective_Dt,
tgt.DW_LOGICAL_DELETE_IND
From ${tgt_tbl} as tgt
WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
)tgt
on tgt.Batch_Id = src.Batch_Id
Where (tgt.Batch_Id is Null)
or(
NVL(src.Batch_Start_Date_Txt,'-1') <> NVL(tgt.Batch_Start_Date_Txt,'-1')
or NVL(src.Batch_End_Date_Txt,'-1') <> NVL(tgt.Batch_End_Date_Txt,'-1')
or NVL(src.Total_Air_Mile_Points_Qty,'-1') <> NVL(tgt.Total_Air_Mile_Points_Qty,'-1')
or NVL(src.Record_Cnt,'-1') <> NVL(tgt.Record_Cnt,'-1')
or NVL(src.Total_Rejected_Air_Mile_Points_Qty,'-1') <> NVL(tgt.Total_Rejected_Air_Mile_Points_Qty,'-1')
or NVL(src.Rejected_Record_Cnt,'-1') <> NVL(tgt.Rejected_Record_Cnt,'-1')
or NVL(src.Create_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Create_Ts,'9999-12-31 00:00:00.000')
or NVL(src.Create_User_Id,'-1') <> NVL(tgt.Create_User_Id,'-1')
or NVL(src.Update_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Update_Ts,'9999-12-31 00:00:00.000')
or NVL(src.Update_User_Id,'-1') <> NVL(tgt.Update_User_Id,'-1')
or NVL(src.Source_Type_Cd,'-1') <> NVL(tgt.Source_Type_Cd,'-1')
or src.DW_LOGICAL_DELETE_IND <> tgt.DW_LOGICAL_DELETE_IND
)`;


try {
       
        snowflake.execute ({sqlText: sql_command});
        }
    catch (err)  {
        return "Creation of Air_Mile_Points_Summary work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }


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
                Batch_Id,
   filename
FROM ${tgt_wrk_tbl}
                WHERE DML_Type = 'U'
                AND Sameday_chg_ind = 0                                      
                AND Batch_Id is not NULL                              
) src
                WHERE tgt.Batch_Id = src.Batch_Id
   AND tgt.DW_CURRENT_VERSION_IND = TRUE
AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;
// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
SET Batch_Start_Date_Txt = src.Batch_Start_Date_Txt,
Batch_End_Date_Txt = src.Batch_End_Date_Txt,
Total_Air_Mile_Points_Qty = src.Total_Air_Mile_Points_Qty,
Record_Cnt = src.Record_Cnt,
Total_Rejected_Air_Mile_Points_Qty = src.Total_Rejected_Air_Mile_Points_Qty,
Rejected_Record_Cnt = src.Rejected_Record_Cnt,
Create_Ts = src.Create_Ts,
Create_User_Id = src.Create_User_Id,
Update_Ts = src.Update_Ts,
Update_User_Id = src.Update_User_Id,
Source_Type_Cd = src.Source_Type_Cd,
DW_Logical_delete_ind = src.DW_Logical_delete_ind,
DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
DW_SOURCE_UPDATE_NM   = FileName
FROM ( SELECT
    Batch_Id,
Batch_Start_Date_Txt,
Batch_End_Date_Txt,
Total_Air_Mile_Points_Qty,
Record_Cnt,
Total_Rejected_Air_Mile_Points_Qty,
Rejected_Record_Cnt,
Create_Ts,
Create_User_Id,
Update_Ts,
Update_User_Id,
Source_Type_Cd,
    DW_Logical_delete_ind,
    filename
  FROM ${tgt_wrk_tbl}
  WHERE DML_Type = 'U'
  AND Sameday_chg_ind = 1
  AND Batch_Id IS NOT NULL  
) src
WHERE tgt.Batch_Id = src.Batch_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                    Batch_Id,
                    DW_First_Effective_Dt,
                    DW_Last_Effective_Dt,
                    Batch_Start_Date_Txt,
Batch_End_Date_Txt,
Total_Air_Mile_Points_Qty,
Record_Cnt,
Total_Rejected_Air_Mile_Points_Qty,
Rejected_Record_Cnt,
Create_Ts,
Create_User_Id,
Update_Ts,
Update_User_Id,
Source_Type_Cd,
DW_Create_Ts,
--DW_LAST_UPDATE_TS,
DW_LOGICAL_DELETE_IND,
DW_SOURCE_CREATE_NM,
--DW_SOURCE_UPDATE_NM,
DW_CURRENT_VERSION_IND
                   )

SELECT DISTINCT
                    Batch_Id,
CURRENT_DATE as DW_First_Effective_dt,
'31-DEC-9999' ,
Batch_Start_Date_Txt,
Batch_End_Date_Txt,
Total_Air_Mile_Points_Qty,
Record_Cnt,
Total_Rejected_Air_Mile_Points_Qty,
Rejected_Record_Cnt,
Create_Ts,
Create_User_Id,
Update_Ts,
Update_User_Id,
Source_Type_Cd,                    
CURRENT_TIMESTAMP,
--DW_LAST_UPDATE_TS,
                    DW_Logical_delete_ind,
                    FileName,
--DW_SOURCE_UPDATE_NM,
                    TRUE
FROM ${tgt_wrk_tbl}
                where Sameday_chg_ind = 0
and Batch_Id is not null
`;
var sql_commit = "COMMIT";
var sql_rollback = "ROLLBACK";
try {
        snowflake.execute({sqlText: sql_begin});
   snowflake.execute({sqlText: sql_updates});
        snowflake.execute({sqlText: sql_sameday});
        snowflake.execute({sqlText: sql_inserts});
        snowflake.execute({sqlText: sql_commit});
}
    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback}
            );

        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }

// ************** Load for EPE_Error table ENDs *****************


$$;
