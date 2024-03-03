--liquibase formatted sql
--changeset SYSTEM:SP_GETAIRMILEPOINTS_To_BIM_load_Air_Mile_Points_DETAIL runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETAIRMILEPOINTS_TO_BIM_LOAD_AIR_MILE_POINTS_DETAIL(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

 var cnf_db = CNF_DB;
var cnf_schema = C_LOYAL;
var wrk_schema = C_STAGE;
var src_tbl = SRC_WRK_TBL ;
var tgt_wrk_tbl = `${cnf_db}.${ wrk_schema}.Air_Mile_Points_DETAIL_WRK`;
var tgt_tbl = `${cnf_db}.${ cnf_schema}.Air_Mile_Points_DETAIL`;
var tgt_exp_tbl = `${cnf_db}.${ wrk_schema}.Air_Mile_Points_DETAIL_Exceptions`;

// **************        Load for Air_Mile_Points_DETAIL  table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
var sql_command = `Create OR Replace TABLE ${ tgt_wrk_tbl} AS
WITH src_wrk_tbl_recs AS (
SELECT DISTINCT Batch_Id
,Household_Id
,Transaction_Id
,Transaction_Ts
,Transaction_Date_Txt
,Transaction_Type_Cd
,Transaction_Type_Dsc
,Transaction_Type_Short_Dsc
,Transaction_Reason_Cd
,Transaction_Reason_Dsc
,Transaction_Reason_Short_Dsc
,Transaction_Reference_Nbr
,Alternate_Transaction_Id
,Alternate_Transaction_Type_Cd
,Alternate_Transaction_Type_Dsc
,Alternate_Transaction_Type_Short_Dsc
,Alternate_Transaction_Ts
,Record_Type_Cd
,Record_Type_Dsc
,Record_Type_Short_Dsc
,Air_Mile_Program_Id
,Air_Mile_Program_Nm
,Air_Mile_Tier_Nm
,Air_Mile_Point_Qty
,Customer_Formatted_Nm
,Customer_Preferred_Salution_Cd
,Customer_Title_Cd
,Customer_Given_Nm
,Customer_Nick_Nm
,Customer_Middle_Nm
,Customer_Family_Nm
,Customer_Maiden_Nm
,Customer_Generation_Affix_Cd
,Customer_Qualification_Affix_Cd
,Create_Ts
,Create_User_Id
,Update_Ts
,Update_User_Id
,Source_Extract_Ts
,Source_Type_cd
,CreationDt
,filename
,Row_Number() OVER (PARTITION BY Batch_Id,Household_Id,Transaction_Id,Transaction_Ts ORDER BY (CreationDt) DESC) AS rn
FROM (
SELECT AIRMILEPOINTSDETAIL_BATCHID AS Batch_Id
,AIRMILEPOINTSDETAIL_HOUSEHOLDID AS Household_Id
,TransactionId AS Transaction_Id
,TransactionTs AS Transaction_Ts
,TransactionDt AS Transaction_Date_Txt
,TRANSACTIONTYPECD_CODE AS Transaction_Type_Cd
,TRANSACTIONTYPECD_DESCRIPTION AS Transaction_Type_Dsc
,TRANSACTIONTYPECD_SHORTDESCRIPTION AS Transaction_Type_Short_Dsc
,TRANSACTIONREASONCD_CODE AS Transaction_Reason_Cd
,TRANSACTIONREASONCD_DESCRIPTION AS Transaction_Reason_Dsc
,TRANSACTIONREASONCD_SHORTDESCRIPTION AS Transaction_Reason_Short_Dsc
,ReferenceNbr AS Transaction_Reference_Nbr
,AltTransactionId AS Alternate_Transaction_Id
,ALTTRANSACTIONTYPE_CODE AS Alternate_Transaction_Type_Cd
,ALTTRANSACTIONTYPE_DESCRIPTION AS Alternate_Transaction_Type_Dsc
,ALTTRANSACTIONTYPE_SHORTDESCRIPTION AS Alternate_Transaction_Type_Short_Dsc
,AltTransactionDtTs AS Alternate_Transaction_Ts
,AirMilePointsDetail_RecordType_Code AS Record_Type_Cd
,AirMilePointsDetail_RecordType_Description AS Record_Type_Dsc
,AirMilePointsDetail_RecordType_ShortDescription AS Record_Type_Short_Dsc
,AirMileProgramId AS Air_Mile_Program_Id
,AirMileProgramNm AS Air_Mile_Program_Nm
,AirMileTierNm AS Air_Mile_Tier_Nm
,AirMilePointQty AS Air_Mile_Point_Qty
,AirMilePointsDetail_FormattedNm AS Customer_Formatted_Nm
,AirMilePointsDetail_PreferredSalutationCd AS Customer_Preferred_Salution_Cd
,AirMilePointsDetail_TitleCd AS Customer_Title_Cd
,AirMilePointsDetail_GivenNm AS Customer_Given_Nm
,AirMilePointsDetail_NickNm AS Customer_Nick_Nm
,AirMilePointsDetail_MiddleNm AS Customer_Middle_Nm
,AirMilePointsDetail_FamilyNm AS Customer_Family_Nm
,AirMilePointsDetail_MaidenNm AS Customer_Maiden_Nm
,AirMilePointsDetail_GenerationAffixCd AS Customer_Generation_Affix_Cd
,AirMilePointsDetail_QualificationAffixCd AS Customer_Qualification_Affix_Cd
,AirMilePointsDetail_CreateTs AS Create_Ts
,AirMilePointsDetail_CreateUserId AS Create_User_Id
,AirMilePointsDetail_UpdateTs AS Update_Ts
,AirMilePointsDetail_UpdateUserId AS Update_User_Id
,AIRMILEPOINTSDETAIL_CREATEDTTM AS Source_Extract_Ts
,AirMilePointsSourceType_code AS Source_Type_cd
,CreationDt
,filename
FROM ${ src_tbl}
WHERE airmilepointssourcetype_code = 'DETAILS'
)
)

SELECT src.Batch_Id
,src.Household_Id
,src.Transaction_Id
,src.Transaction_Ts
,src.Transaction_Date_Txt
,src.Transaction_Type_Cd
,src.Transaction_Type_Dsc
,src.Transaction_Type_Short_Dsc
,src.Transaction_Reason_Cd
,src.Transaction_Reason_Dsc
,src.Transaction_Reason_Short_Dsc
,src.Transaction_Reference_Nbr
,src.Alternate_Transaction_Id
,src.Alternate_Transaction_Type_Cd
,src.Alternate_Transaction_Type_Dsc
,src.Alternate_Transaction_Type_Short_Dsc
,src.Alternate_Transaction_Ts
,src.Record_Type_Cd
,src.Record_Type_Dsc
,src.Record_Type_Short_Dsc
,src.Air_Mile_Program_Id
,src.Air_Mile_Program_Nm
,src.Air_Mile_Tier_Nm
,src.Air_Mile_Point_Qty
,src.Customer_Formatted_Nm
,src.Customer_Preferred_Salution_Cd
,src.Customer_Title_Cd
,src.Customer_Given_Nm
,src.Customer_Nick_Nm
,src.Customer_Middle_Nm
,src.Customer_Family_Nm
,src.Customer_Maiden_Nm
,src.Customer_Generation_Affix_Cd
,src.Customer_Qualification_Affix_Cd
,src.Create_Ts
,src.Create_User_Id
,src.Update_Ts
,src.Update_User_Id
,src.Source_Extract_Ts
,src.Source_Type_cd
,src.DW_Logical_delete_ind
,src.CreationDt
,src.filename
,CASE WHEN (
tgt.Batch_Id IS NULL
AND tgt.Household_Id IS NULL
AND tgt.Transaction_Id IS NULL
AND tgt.Transaction_Ts IS NULL
)THEN 'I' ELSE 'U' END AS DML_TYPE
,CASE WHEN tgt.DW_First_Effective_Dt = CURRENT_DATE THEN 1 ELSE 0 END AS SAMEDAY_CHG_IND
FROM (
SELECT DISTINCT Batch_Id
,Household_Id
,Transaction_Id
,Transaction_Ts
,Transaction_Date_Txt
,Transaction_Type_Cd
,Transaction_Type_Dsc
,Transaction_Type_Short_Dsc
,Transaction_Reason_Cd
,Transaction_Reason_Dsc
,Transaction_Reason_Short_Dsc
,Transaction_Reference_Nbr
,Alternate_Transaction_Id
,Alternate_Transaction_Type_Cd
,Alternate_Transaction_Type_Dsc
,Alternate_Transaction_Type_Short_Dsc
,Alternate_Transaction_Ts
,Record_Type_Cd
,Record_Type_Dsc
,Record_Type_Short_Dsc
,Air_Mile_Program_Id
,Air_Mile_Program_Nm
,Air_Mile_Tier_Nm
,Air_Mile_Point_Qty
,Customer_Formatted_Nm
,Customer_Preferred_Salution_Cd
,Customer_Title_Cd
,Customer_Given_Nm
,Customer_Nick_Nm
,Customer_Middle_Nm
,Customer_Family_Nm
,Customer_Maiden_Nm
,Customer_Generation_Affix_Cd
,Customer_Qualification_Affix_Cd
,Create_Ts
,Create_User_Id
,Update_Ts
,Update_User_Id
,Source_Extract_Ts
,Source_Type_cd
,false AS DW_Logical_delete_ind
,CreationDt
,filename
FROM src_wrk_tbl_recs
WHERE rn = 1
AND Batch_Id IS NOT NULL
AND Household_Id IS NOT NULL
AND Transaction_Id IS NOT NULL
AND Transaction_Ts IS NOT NULL
) src
LEFT JOIN (
SELECT DISTINCT tgt.Batch_Id
,tgt.Household_Id
,tgt.Transaction_Id
,tgt.Transaction_Ts
,tgt.Transaction_Date_Txt
,tgt.Transaction_Type_Cd
,tgt.Transaction_Type_Dsc
,tgt.Transaction_Type_Short_Dsc
,tgt.Transaction_Reason_Cd
,tgt.Transaction_Reason_Dsc
,tgt.Transaction_Reason_Short_Dsc
,tgt.Transaction_Reference_Nbr
,tgt.Alternate_Transaction_Id
,tgt.Alternate_Transaction_Type_Cd
,tgt.Alternate_Transaction_Type_Dsc
,tgt.Alternate_Transaction_Type_Short_Dsc
,tgt.Alternate_Transaction_Ts
,tgt.Record_Type_Cd
,tgt.Record_Type_Dsc
,tgt.Record_Type_Short_Dsc
,tgt.Air_Mile_Program_Id
,tgt.Air_Mile_Program_Nm
,tgt.Air_Mile_Tier_Nm
,tgt.Air_Mile_Point_Qty
,tgt.Customer_Formatted_Nm
,tgt.Customer_Preferred_Salution_Cd
,tgt.Customer_Title_Cd
,tgt.Customer_Given_Nm
,tgt.Customer_Nick_Nm
,tgt.Customer_Middle_Nm
,tgt.Customer_Family_Nm
,tgt.Customer_Maiden_Nm
,tgt.Customer_Generation_Affix_Cd
,tgt.Customer_Qualification_Affix_Cd
,tgt.Create_Ts
,tgt.Create_User_Id
,tgt.Update_Ts
,tgt.Update_User_Id
,tgt.Source_Extract_Ts
,tgt.Source_Type_cd
,tgt.DW_First_Effective_Dt
,tgt.DW_LOGICAL_DELETE_IND
FROM ${ tgt_tbl} tgt
WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
) tgt ON tgt.Batch_Id = src.Batch_Id
AND tgt.Household_Id = src.Household_Id
AND tgt.Transaction_Id = src.Transaction_Id
AND tgt.Transaction_Ts = src.Transaction_Ts
WHERE (
tgt.Batch_Id IS NULL
AND tgt.Household_Id IS NULL
AND tgt.Transaction_Id IS NULL
AND tgt.Transaction_Ts IS NULL
)
OR (
  nvl(src.Transaction_Date_Txt,'-1') <> nvl(tgt.Transaction_Date_Txt,'-1')
OR nvl(src.Transaction_Type_Cd, '-1') <> nvl(tgt.Transaction_Type_Cd, '-1')
OR nvl(src.Transaction_Type_Dsc, '-1') <> nvl(tgt.Transaction_Type_Dsc, '-1')
OR nvl(src.Transaction_Type_Short_Dsc, '-1') <> nvl(tgt.Transaction_Type_Short_Dsc, '-1')
OR nvl(src.Transaction_Reason_Cd, '-1') <> nvl(tgt.Transaction_Reason_Cd, '-1')
OR nvl(src.Transaction_Reason_Dsc, '-1') <> nvl(tgt.Transaction_Reason_Dsc, '-1')
OR nvl(src.Transaction_Reason_Short_Dsc, '-1') <> nvl(tgt.Transaction_Reason_Short_Dsc, '-1')
OR nvl(src.Transaction_Reference_Nbr, '-1') <> nvl(tgt.Transaction_Reference_Nbr, '-1')
OR nvl(src.Alternate_Transaction_Id, '-1') <> nvl(tgt.Alternate_Transaction_Id, '-1')
OR nvl(src.Alternate_Transaction_Type_Cd, '-1') <> nvl(tgt.Alternate_Transaction_Type_Cd, '-1')
OR nvl(src.Alternate_Transaction_Type_Dsc, '-1') <> nvl(tgt.Alternate_Transaction_Type_Dsc, '-1')
OR nvl(src.Alternate_Transaction_Type_Short_Dsc, '-1') <> nvl(tgt.Alternate_Transaction_Type_Short_Dsc, '-1')
OR nvl(src.Alternate_Transaction_Ts, '9999-12-31 00:00:00.000') <> nvl(tgt.Alternate_Transaction_Ts, '9999-12-31 00:00:00.000')
OR nvl(src.Record_Type_Cd, '-1') <> nvl(tgt.Record_Type_Cd, '-1')
OR nvl(src.Record_Type_Dsc, '-1') <> nvl(tgt.Record_Type_Dsc, '-1')
OR nvl(src.Record_Type_Short_Dsc, '-1') <> nvl(tgt.Record_Type_Short_Dsc, '-1')
OR nvl(src.Air_Mile_Program_Id, '-1') <> nvl(tgt.Air_Mile_Program_Id, '-1')
OR nvl(src.Air_Mile_Program_Nm, '-1') <> nvl(tgt.Air_Mile_Program_Nm, '-1')
OR nvl(src.Air_Mile_Tier_Nm, '-1') <> nvl(tgt.Air_Mile_Tier_Nm, '-1')
OR nvl(src.Air_Mile_Point_Qty, '-1') <> nvl(tgt.Air_Mile_Point_Qty, '-1')
OR nvl(src.Customer_Formatted_Nm, '-1') <> nvl(tgt.Customer_Formatted_Nm, '-1')
OR nvl(src.Customer_Preferred_Salution_Cd, '-1') <> nvl(tgt.Customer_Preferred_Salution_Cd, '-1')
OR nvl(src.Customer_Title_Cd, '-1') <> nvl(tgt.Customer_Title_Cd, '-1')
OR nvl(src.Customer_Given_Nm, '-1') <> nvl(tgt.Customer_Given_Nm, '-1')
OR nvl(src.Customer_Nick_Nm, '-1') <> nvl(tgt.Customer_Nick_Nm, '-1')
OR nvl(src.Customer_Middle_Nm, '-1') <> nvl(tgt.Customer_Middle_Nm, '-1')
OR nvl(src.Customer_Family_Nm, '-1') <> nvl(tgt.Customer_Family_Nm, '-1')
OR nvl(src.Customer_Maiden_Nm, '-1') <> nvl(tgt.Customer_Maiden_Nm, '-1')
OR nvl(src.Customer_Generation_Affix_Cd, '-1') <> nvl(tgt.Customer_Generation_Affix_Cd, '-1')
OR nvl(src.Customer_Qualification_Affix_Cd, '-1') <> nvl(tgt.Customer_Qualification_Affix_Cd, '-1')
OR nvl(src.Create_Ts, '9999-12-31 00:00:00.000') <> nvl(tgt.Create_Ts, '9999-12-31 00:00:00.000')
OR nvl(src.Create_User_Id, '-1') <> nvl(tgt.Create_User_Id, '-1')
OR nvl(src.Update_Ts, '9999-12-31 00:00:00.000') <> nvl(tgt.Update_Ts, '9999-12-31 00:00:00.000')
OR nvl(src.Update_User_Id, '-1') <> nvl(tgt.Update_User_Id, '-1')
OR nvl(src.Source_Extract_Ts, '9999-12-31 00:00:00.000') <> nvl(tgt.Source_Extract_Ts, '9999-12-31 00:00:00.000')
OR nvl(src.Source_Type_cd, '-1') <> nvl(tgt.Source_Type_cd, '-1')
OR src.DW_LOGICAL_DELETE_IND <> tgt.DW_LOGICAL_DELETE_IND
) `;

try {
snowflake.execute ({sqlText: sql_command});
} catch(err) {
return "Creation of Air_Mile_Points_DETAIL work table " + tgt_wrk_tbl + " Failed with error: " + err;// Return a error message.
}
var sql_begin = "BEGIN"
// SCD Type2 - Processing Different day updates
var sql_updates = `UPDATE ${ tgt_tbl} AS TGT

SET DW_Last_Effective_dt = CURRENT_DATE - 1
,DW_CURRENT_VERSION_IND = FALSE
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = filename
FROM (
SELECT Batch_Id
,Household_Id
,Transaction_Id
,Transaction_Ts
,filename
FROM ${ tgt_wrk_tbl}
WHERE DML_TYPE = 'U'     
AND Sameday_chg_ind = 0  
AND Batch_Id IS NOT NULL
AND Household_Id IS NOT NULL
AND Transaction_Id IS NOT NULL
AND Transaction_TS IS NOT NULL
) SRC
WHERE TGT.Batch_Id = SRC.Batch_Id
AND TGT.Household_Id = SRC.Household_Id
AND TGT.Transaction_Id = SRC.Transaction_Id
AND TGT.Transaction_Ts = SRC.Transaction_Ts
AND TGT.DW_CURRENT_VERSION_IND = TRUE
AND TGT.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = `UPDATE ${ tgt_tbl} AS tgt
SET  Transaction_Date_Txt = src.Transaction_Date_Txt
,Transaction_Type_Cd = src.Transaction_Type_Cd
,Transaction_Type_Dsc = src.Transaction_Type_Dsc
,Transaction_Type_Short_Dsc = src.Transaction_Type_Short_Dsc
,Transaction_Reason_Cd = src.Transaction_Reason_Cd
,Transaction_Reason_Dsc = src.Transaction_Reason_Dsc
,Transaction_Reason_Short_Dsc = src.Transaction_Reason_Short_Dsc
,Transaction_Reference_Nbr = src.Transaction_Reference_Nbr
,Alternate_Transaction_Id = src.Alternate_Transaction_Id
,Alternate_Transaction_Type_Cd = src.Alternate_Transaction_Type_Cd
,Alternate_Transaction_Type_Dsc = src.Alternate_Transaction_Type_Dsc
,Alternate_Transaction_Type_Short_Dsc = src.Alternate_Transaction_Type_Short_Dsc
,Alternate_Transaction_Ts = src.Alternate_Transaction_Ts
,Record_Type_Cd = src.Record_Type_Cd
,Record_Type_Dsc = src.Record_Type_Dsc
,Record_Type_Short_Dsc = src.Record_Type_Short_Dsc
,Air_Mile_Program_Id = src.Air_Mile_Program_Id
,Air_Mile_Program_Nm = src.Air_Mile_Program_Nm
,Air_Mile_Tier_Nm = src.Air_Mile_Tier_Nm
,Air_Mile_Point_Qty = src.Air_Mile_Point_Qty
,Customer_Formatted_Nm = src.Customer_Formatted_Nm
,Customer_Preferred_Salution_Cd = src.Customer_Preferred_Salution_Cd
,Customer_Title_Cd = src.Customer_Title_Cd
,Customer_Given_Nm = src.Customer_Given_Nm
,Customer_Nick_Nm = src.Customer_Nick_Nm
,Customer_Middle_Nm = src.Customer_Middle_Nm
,Customer_Family_Nm = src.Customer_Family_Nm
,Customer_Maiden_Nm = src.Customer_Maiden_Nm
,Customer_Generation_Affix_Cd = src.Customer_Generation_Affix_Cd
,Customer_Qualification_Affix_Cd = src.Customer_Qualification_Affix_Cd
,Create_Ts = src.Create_Ts
,Create_User_Id = src.Create_User_Id
,Update_Ts = src.Update_Ts
,Update_User_Id = src.Update_User_Id
,Source_Extract_Ts = src.Source_Extract_Ts
,Source_Type_cd = src.Source_Type_cd
,DW_Logical_delete_ind = src.DW_Logical_delete_ind
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = FileName
FROM (
SELECT Batch_Id
,Household_Id
,Transaction_Id
,Transaction_Ts
,Transaction_Date_Txt
,Transaction_Type_Cd
,Transaction_Type_Dsc
,Transaction_Type_Short_Dsc
,Transaction_Reason_Cd
,Transaction_Reason_Dsc
,Transaction_Reason_Short_Dsc
,Transaction_Reference_Nbr
,Alternate_Transaction_Id
,Alternate_Transaction_Type_Cd
,Alternate_Transaction_Type_Dsc
,Alternate_Transaction_Type_Short_Dsc
,Alternate_Transaction_Ts
,Record_Type_Cd
,Record_Type_Dsc
,Record_Type_Short_Dsc
,Air_Mile_Program_Id
,Air_Mile_Program_Nm
,Air_Mile_Tier_Nm
,Air_Mile_Point_Qty
,Customer_Formatted_Nm
,Customer_Preferred_Salution_Cd
,Customer_Title_Cd
,Customer_Given_Nm
,Customer_Nick_Nm
,Customer_Middle_Nm
,Customer_Family_Nm
,Customer_Maiden_Nm
,Customer_Generation_Affix_Cd
,Customer_Qualification_Affix_Cd
,Create_Ts
,Create_User_Id
,Update_Ts
,Update_User_Id
,Source_Extract_Ts
,Source_Type_cd
,DW_Logical_delete_ind
,creationDt
,FILENAME
FROM ${ tgt_wrk_tbl}
WHERE DML_Type = 'U'    
AND Sameday_chg_ind = '1'
AND Batch_Id IS NOT NULL
AND Household_Id IS NOT NULL
AND Transaction_Id IS NOT NULL
AND Transaction_Ts IS NOT NULL
) src
WHERE tgt.Batch_Id = src.Batch_Id
AND tgt.Household_Id = src.Household_Id
AND tgt.Transaction_Id = tgt.Transaction_Id
AND tgt.Transaction_Ts = tgt.Transaction_Ts
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `Insert INTO ${ tgt_tbl}(
Batch_Id,
Household_Id,
Transaction_Id,
Transaction_Ts,
Transaction_Date_Txt,
Transaction_Type_Cd,
Transaction_Type_Dsc,
Transaction_Type_Short_Dsc,
Transaction_Reason_Cd,
Transaction_Reason_Dsc,
Transaction_Reason_Short_Dsc,
Transaction_Reference_Nbr,
Alternate_Transaction_Id,
Alternate_Transaction_Type_Cd,
Alternate_Transaction_Type_Dsc,
Alternate_Transaction_Type_Short_Dsc,
Alternate_Transaction_Ts,
Record_Type_Cd,
Record_Type_Dsc,
Record_Type_Short_Dsc,
Air_Mile_Program_Id,
Air_Mile_Program_Nm,
Air_Mile_Tier_Nm,
Air_Mile_Point_Qty,
Customer_Formatted_Nm,
 Customer_Preferred_Salution_Cd,
 Customer_Title_Cd,
 Customer_Given_Nm,
 Customer_Nick_Nm,
 Customer_Middle_Nm,
 Customer_Family_Nm,
 Customer_Maiden_Nm,
 Customer_Generation_Affix_Cd, 
 Customer_Qualification_Affix_Cd,
 Create_Ts,
 Create_User_Id,
 Update_Ts,
 Update_User_Id,
 Source_Extract_Ts,
 Source_Type_cd,
 DW_First_Effective_Dt,
 DW_Last_Effective_Dt,
 DW_CREATE_TS,
 DW_LOGICAL_DELETE_IND,
 DW_SOURCE_CREATE_NM,
 DW_CURRENT_VERSION_IND)

SELECT DISTINCT Batch_Id
,Household_Id
,Transaction_Id
,Transaction_Ts
,Transaction_Date_Txt
,Transaction_Type_Cd
,Transaction_Type_Dsc
,Transaction_Type_Short_Dsc
,Transaction_Reason_Cd
,Transaction_Reason_Dsc
,Transaction_Reason_Short_Dsc
,Transaction_Reference_Nbr
,Alternate_Transaction_Id
,Alternate_Transaction_Type_Cd
,Alternate_Transaction_Type_Dsc
,Alternate_Transaction_Type_Short_Dsc
,Alternate_Transaction_Ts
,Record_Type_Cd
,Record_Type_Dsc
,Record_Type_Short_Dsc
,Air_Mile_Program_Id
,Air_Mile_Program_Nm
,Air_Mile_Tier_Nm
,Air_Mile_Point_Qty
,Customer_Formatted_Nm
,Customer_Preferred_Salution_Cd
,Customer_Title_Cd
,Customer_Given_Nm
,Customer_Nick_Nm
,Customer_Middle_Nm
,Customer_Family_Nm
,Customer_Maiden_Nm
,Customer_Generation_Affix_Cd
,Customer_Qualification_Affix_Cd
,Create_Ts
,Create_User_Id
,Update_Ts
,Update_User_Id
,Source_Extract_Ts
,Source_Type_cd
,CURRENT_DATE AS DW_First_Effective_Dt
,'31-DEC-9999'
,Current_timestamp AS DW_CREATE_TS
,DW_Logical_delete_ind
,Filename
,True
FROM ${ tgt_wrk_tbl}
WHERE Batch_Id IS NOT NULL
AND Household_Id IS NOT NULL
AND Transaction_Id IS NOT NULL
AND Transaction_Ts IS NOT NULL
AND Sameday_chg_Ind = 0 `;

var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
snowflake.execute ({sqlText: sql_begin});

snowflake.execute ({sqlText: sql_updates});

snowflake.execute ({sqlText: sql_sameday});

snowflake.execute ({sqlText: sql_inserts});

snowflake.execute ({sqlText: sql_commit});

} catch(err) {
snowflake.execute ({sqlText: sql_rollback });

return `Loading OF TABLE ${ tgt_tbl} Failed WITH error: ${ err}`;// Return a error message.

}
// ************** Load for Air_Mile_Points_DETAIL table ENDs *****************

$$;
