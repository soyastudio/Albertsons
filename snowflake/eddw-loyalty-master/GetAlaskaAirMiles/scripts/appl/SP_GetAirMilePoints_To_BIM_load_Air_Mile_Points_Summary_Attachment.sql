--liquibase formatted sql
--changeset SYSTEM:SP_GetAirMilePoints_To_BIM_load_Air_Mile_Points_Summary_Attachment runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETAIRMILEPOINTS_TO_BIM_LOAD_AIR_MILE_POINTS_SUMMARY_ATTACHMENT(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

var cnf_db = CNF_DB ;
var src_wrk_tbl = SRC_WRK_TBL;
var cnf_schema = C_LOYAL;
var wrk_schema = C_STAGE;
var tgt_wrk_tbl = `${CNF_DB}.${wrk_schema}.Air_Mile_Points_Summary_Attachment_wrk`;
var tgt_tbl = `${CNF_DB}.${cnf_schema}.Air_Mile_Points_Summary_Attachment`;

// ************** Load for EPE_Error table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.


var sql_command = `Create or REPLACE table  ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            ( SELECT DISTINCT  Batch_Id,
File_Nm,
Link_URL_Txt,
AIRMILEPOINTSSOURCETYPE_CODE,
CreationDt,
filename,
Row_Number() over(partition by Batch_Id, File_Nm order by(CreationDt) desc) as rn
from
                            (
                            SELECT DISTINCT
AIRMILEPOINTSSUMMARY_BATCHID as Batch_Id,
AIRMILEPOINTSSUMMARY_FILENM as File_Nm,
AIRMILEPOINTSSUMMARY_LINKURL as Link_URL_Txt,
AIRMILEPOINTSSOURCETYPE_CODE,
CreationDt,
filename
FROM ${src_wrk_tbl}
       where AIRMILEPOINTSSOURCETYPE_CODE = 'SUMMARY'
)
                          )      
SELECT
       src.Batch_Id,
src.File_Nm,
src.Link_URL_Txt,
src.DW_Logical_delete_ind,
src.CreationDt,
src.filename,
                               CASE WHEN tgt.Batch_Id IS NULL AND tgt.File_Nm IS NULL THEN 'I' ELSE 'U' END AS DML_Type
                               ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
from
                          (
select
Batch_Id,
File_Nm,
Link_URL_Txt,
FALSE AS DW_Logical_delete_ind,
CreationDt,
filename
from src_wrk_tbl_recs
WHERE rn = 1
AND Batch_Id is not null
AND File_Nm is not null
)src
LEFT JOIN
                          (SELECT  DISTINCT
       tgt.Batch_Id,
tgt.File_Nm,
tgt.Link_URL_Txt,
tgt.dw_logical_delete_ind,
tgt.dw_first_effective_dt  
                          FROM ${tgt_tbl} tgt
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt
                          ON tgt.Batch_Id = src.Batch_Id
 AND tgt.File_Nm = src.File_Nm
 WHERE  (tgt.Batch_Id is null and tgt.File_Nm is null)
or(
NVL(src.Link_URL_Txt,'-1') <> NVL(tgt.Link_URL_Txt,'-1')
 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )  `;        

try {
       
        snowflake.execute ({sqlText: sql_command});
        }
    catch (err)  {
        return "Creation of Air_Mile_Points_Detail_Attachment work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
  File_Nm,
  filename
FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U'
                             AND Sameday_chg_ind = 0                                      
                             AND Batch_Id is not NULL                              
AND File_Nm is not null
) src
                              WHERE tgt.Batch_Id = src.Batch_Id
AND tgt.File_Nm = src.File_Nm
AND tgt.DW_CURRENT_VERSION_IND = TRUE
AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
SET Link_URL_Txt = src.Link_URL_Txt,
DW_Logical_delete_ind = src.DW_Logical_delete_ind,
DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
DW_SOURCE_UPDATE_NM   = FileName
FROM ( SELECT
    Batch_Id,
    File_Nm,
    Link_URL_Txt,
    DW_Logical_delete_ind,
    CreationDt,
    filename
   
  FROM ${tgt_wrk_tbl}
  WHERE DML_Type = 'U'
  AND Sameday_chg_ind = 1
  AND Batch_Id IS NOT NULL  
  AND File_Nm IS NOT NULL
) src
WHERE tgt.Batch_Id = src.Batch_Id
AND tgt.File_Nm = src.File_Nm
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                                        Batch_Id,
File_Nm,
DW_First_Effective_Dt,
                    DW_Last_Effective_Dt,  
Link_URL_Txt,          
                    DW_CREATE_TS,
                    DW_LOGICAL_DELETE_IND,
                    DW_SOURCE_CREATE_NM,
                    DW_CURRENT_VERSION_IND
                   )

SELECT DISTINCT
                      Batch_Id,
File_Nm,
CURRENT_DATE as DW_First_Effective_dt,
'31-DEC-9999' ,
Link_URL_Txt,                    
CURRENT_TIMESTAMP,
                      DW_Logical_delete_ind,
                      FileName,
                      TRUE
FROM ${tgt_wrk_tbl}
                where Sameday_chg_ind = 0
and Batch_Id is not null
and File_Nm is not null
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
            {sqlText: sql_rollback  }
            );

        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }

// ************** Load for Air_Mile_Points_Summary_Attachment table ENDs *****************


$$;
