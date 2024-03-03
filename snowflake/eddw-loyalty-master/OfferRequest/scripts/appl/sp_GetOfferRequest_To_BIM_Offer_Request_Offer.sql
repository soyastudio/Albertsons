--liquibase formatted sql
--changeset SYSTEM:sp_GetOfferRequest_To_BIM_Offer_Request_Offer runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETOFFERREQUEST_TO_BIM_OFFER_REQUEST_OFFER(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR, LKP_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

  
   
    var cnf_db = CNF_DB ;
var wrk_schema = WRK_SCHEMA ;
var cnf_schema = CNF_SCHEMA;
var src_wrk_tbl = SRC_WRK_TBL;
var lkp_schema = LKP_SCHEMA;
// ************** Load for Offer_Request_Offer table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Offer_wrk";
var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request_Offer";
var LKP_tbl = cnf_db +"."+ lkp_schema +".Offer";
var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Offer_Exceptions";
var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as
   WITH src_wrk_tbl_recs as
( SELECT
Offer_External_Id

, Copient_Offer_Id
, Offer_Request_Id
, Offer_Id
, User_Interface_Unique_Id
, Product_Group_Version_Id
, Discount_Version_Id
, Instant_Win_Version_Id
, Discount_Id
, Attached_Offer_Status_Type_Cd
, Attached_Offer_Status_Dsc
, Attached_Offer_Status_Effective_Ts
, Applied_Program_Nm
, Program_Applied_Ind
, Distinct_Id
, Offer_Rank_Nbr
, creationdt
, actiontypecd
, FileName
, Row_number() OVER ( partition BY Offer_External_Id ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
FROM (SELECT * FROM
(SELECT DISTINCT
ReferenceOfferId AS Offer_External_Id
,OfferRequestId AS Offer_Request_Id
,OfferId AS Offer_Id
,AttachedOfferTypeId AS User_Interface_Unique_Id
,ProductGroup_ProductGroupVersionId AS Product_Group_Version_Id
,AttachedOffer_DiscountVersionId AS Discount_Version_Id
,Attachedoffer_instantwinversionid AS Instant_Win_Version_Id
,AttachedOffer_DiscountId AS Discount_Id
,Status_StatusTypeCd_Type AS Attached_Offer_Status_Type_Cd
,Status_Description AS Attached_Offer_Status_Dsc
,Status_EffectiveDtTm AS Attached_Offer_Status_Effective_Ts
,ProgramNm AS Applied_Program_Nm
,AppliedInd AS Program_Applied_Ind
,DistinctId AS Distinct_Id
,Attachedoffer_Offerranknbr AS Offer_Rank_Nbr
, REPLACE(IFF(regexp_instr(ReferenceOfferId, '-') > 0 , SPLIT(TRIM(substr(ReferenceOfferId, 1, regexp_instr(ReferenceOfferId, '-')-1)),'-')[0],NULL ),'"','') AS Reference_Offer_Id
, creationdt
, actiontypecd
, FileName
FROM ` + src_wrk_tbl +`) FLT_SRC
LEFT JOIN
(SELECT Offer_id AS Copient_Offer_id FROM ` + LKP_tbl +`) LKP1 ON (LKP1.Copient_Offer_id = FLT_SRC.Reference_Offer_Id)
 )
)
SELECT SRC. Offer_External_Id
, SRC. Copient_Offer_Id
, SRC. Offer_Request_Id
, SRC. Offer_Id
, SRC. User_Interface_Unique_Id
, SRC. Product_Group_Version_Id
, SRC. Discount_Version_Id
, SRC. Instant_Win_Version_Id
, SRC. Discount_Id
, SRC. Attached_Offer_Status_Type_Cd
, SRC. Attached_Offer_Status_Dsc
, SRC. Attached_Offer_Status_Effective_Ts
, SRC. Applied_Program_Nm
, SRC. Program_Applied_Ind
, SRC. Distinct_Id
, SRC. Offer_Rank_Nbr
, src.dw_logical_delete_ind
, src.FileName
, CASE WHEN (tgt.Offer_External_Id IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
, CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
FROM   (SELECT Offer_External_Id
, Copient_Offer_Id
, Offer_Request_Id
, Offer_Id
, User_Interface_Unique_Id
, Product_Group_Version_Id
, Discount_Version_Id
, Instant_Win_Version_Id
, Discount_Id
, Attached_Offer_Status_Type_Cd
, Attached_Offer_Status_Dsc
, Attached_Offer_Status_Effective_Ts
, Applied_Program_Nm
, Program_Applied_Ind
, Distinct_Id
, Offer_Rank_Nbr
, creationdt
, FALSE AS DW_Logical_delete_ind
, FileName
FROM src_wrk_tbl_recs
WHERE  rn = 1

AND User_Interface_Unique_Id  IS NOT NULL
//AND Store_Group_Version_Id  IS NOT NULL
AND UPPER(ActionTypeCd) <> 'DELETE'
) src
LEFT JOIN (SELECT tgt.Offer_External_Id
, tgt.Copient_Offer_Id
, tgt.Offer_Request_Id
, tgt.Offer_Id
, tgt.User_Interface_Unique_Id
, tgt.Product_Group_Version_Id
, tgt.Discount_Version_Id
, tgt.Instant_Win_Version_Id
, tgt.Discount_Id
, tgt.Attached_Offer_Status_Type_Cd
, tgt.Attached_Offer_Status_Dsc
, tgt.Attached_Offer_Status_Effective_Ts
, tgt.Applied_Program_Nm
, tgt.Program_Applied_Ind
, tgt.Distinct_Id
, tgt.Offer_Rank_Nbr
, tgt.dw_logical_delete_ind
, tgt.dw_first_effective_dt
FROM ` + tgt_tbl + ` tgt
WHERE DW_CURRENT_VERSION_IND = TRUE
) tgt
ON tgt.Offer_External_Id = src.Offer_External_Id
WHERE  (tgt.Offer_External_Id  IS NULL )
OR (
NVL(src.Offer_Request_Id,'-1') <> NVL(tgt.Offer_Request_Id,'-1')
OR NVL(src.Applied_Program_Nm,'-1') <> NVL(tgt.Applied_Program_Nm,'-1')
OR NVL(src.Attached_Offer_Status_Dsc,'-1') <> NVL(tgt.Attached_Offer_Status_Dsc,'-1')
OR NVL(src.Attached_Offer_Status_Effective_Ts,'9999-12-31 00:00:00') <> NVL(tgt.Attached_Offer_Status_Effective_Ts,'9999-12-31 00:00:00')
OR NVL(src.Attached_Offer_Status_Type_Cd,'-1') <> NVL(tgt.Attached_Offer_Status_Type_Cd,'-1')
OR NVL(src.user_interface_unique_id,'-1') <> NVL(tgt.user_interface_unique_id,'-1')
OR NVL(src.Copient_Offer_Id,'-1') <> NVL(tgt.Copient_Offer_Id,'-1')
OR NVL(src.Discount_Id,'-1') <> NVL(tgt.Discount_Id,'-1')
OR NVL(src.Discount_Version_Id,'-1') <> NVL(tgt.Discount_Version_Id,'-1')
OR NVL(src.Distinct_Id,'-1') <> NVL(tgt.Distinct_Id,'-1')
OR NVL(src.Offer_Id,'-1') <> NVL(tgt.Offer_Id,'-1')
OR NVL(src.Product_Group_Version_Id,'-1') <> NVL(tgt.Product_Group_Version_Id,'-1')
OR NVL(src.Program_Applied_Ind,'-1') <> NVL(tgt.Program_Applied_Ind,'-1')
OR NVL(src.Instant_Win_Version_Id,'-1') <> NVL(tgt.Instant_Win_Version_Id,'-1')
                                     OR NVL(src.Offer_Rank_Nbr,'-1') <> NVL(tgt.Offer_Rank_Nbr,'-1')
OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
)
UNION ALL
SELECT tgt.Offer_External_Id
, tgt.Copient_Offer_Id
, tgt.Offer_Request_Id
, tgt.Offer_Id
, tgt.User_Interface_Unique_Id
, tgt.Product_Group_Version_Id
, tgt.Discount_Version_Id
, tgt.Instant_Win_Version_Id
, tgt.Discount_Id
, tgt.Attached_Offer_Status_Type_Cd
, tgt.Attached_Offer_Status_Dsc
, tgt.Attached_Offer_Status_Effective_Ts
, tgt.Applied_Program_Nm
, tgt.Program_Applied_Ind
, tgt.Distinct_Id
, tgt.Offer_Rank_Nbr
, TRUE AS DW_Logical_delete_ind
,src.Filename
, 'U' as DML_Type
, CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
FROM ` + tgt_tbl + ` tgt
inner join src_wrk_tbl_recs src on src.Offer_Request_Id = tgt.Offer_Request_Id

WHERE DW_CURRENT_VERSION_IND = TRUE
and rn = 1
AND upper(ActionTypeCd) = 'DELETE'
AND DW_LOGICAL_DELETE_IND = FALSE
AND (tgt.Offer_Request_Id) in
(
SELECT DISTINCT Offer_Request_Id
//, Store_Group_Version_Id
FROM src_wrk_tbl_recs src
WHERE rn = 1
AND upper(ActionTypeCd) = 'DELETE'
AND Offer_Request_Id  IS NOT NULL

)  
 
`;
try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Offer_Request_Offer work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
//SCD Type2 transaction begins
    var sql_begin = "BEGIN"
var sql_updates = `// Processing Updates of Type 2 SCD
UPDATE ` + tgt_tbl + ` as tgt
SET DW_Last_Effective_dt = CURRENT_DATE-1
,DW_CURRENT_VERSION_IND = FALSE
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = FileName
FROM ( SELECT Offer_External_Id
,FileName
FROM `+ tgt_wrk_tbl +`
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 0
AND Offer_External_Id  IS NOT NULL
) src
WHERE tgt.Offer_External_Id = src.Offer_External_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
    var sql_sameday = `// Processing Sameday updates
UPDATE ` + tgt_tbl + ` as tgt
SET Copient_Offer_Id = src.Copient_Offer_Id
,Offer_Request_Id = src.Offer_Request_Id
, Offer_Id = src.Offer_Id
, User_Interface_Unique_Id = src.User_Interface_Unique_Id
, Product_Group_Version_Id = src.Product_Group_Version_Id
,Discount_Version_Id = src.Discount_Version_Id
, Instant_Win_Version_Id = src.Instant_Win_Version_Id
, Discount_Id = src.Discount_Id
, Attached_Offer_Status_Type_Cd = src.Attached_Offer_Status_Type_Cd
, Attached_Offer_Status_Dsc = src.Attached_Offer_Status_Dsc
, Attached_Offer_Status_Effective_Ts = src.Attached_Offer_Status_Effective_Ts
, Applied_Program_Nm = src.Applied_Program_Nm
, Program_Applied_Ind = src.Program_Applied_Ind
//, Store_Group_Version_Id = src.Store_Group_Version_Id
,Distinct_Id=src.Distinct_Id
, Offer_Rank_Nbr =src.Offer_Rank_Nbr
, DW_Logical_delete_ind = src.DW_Logical_delete_ind
, DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
, DW_SOURCE_UPDATE_NM = FileName
FROM ( SELECT Offer_External_Id
, Copient_Offer_Id
, Offer_Request_Id
, Offer_Id
, User_Interface_Unique_Id
, Product_Group_Version_Id
, Discount_Version_Id
, Instant_Win_Version_Id
, Discount_Id
, Attached_Offer_Status_Type_Cd
, Attached_Offer_Status_Dsc
, Attached_Offer_Status_Effective_Ts
, Applied_Program_Nm
, Program_Applied_Ind
, Distinct_Id
, Offer_Rank_Nbr
, DW_Logical_delete_ind
, FileName
FROM `+ tgt_wrk_tbl +`
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 1
AND Offer_External_Id  IS NOT NULL
) src
WHERE tgt.Offer_External_Id = src.Offer_External_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ` + tgt_tbl + `
( Offer_External_Id
, Copient_Offer_Id
, Offer_Request_Id
, Offer_Id
, User_Interface_Unique_Id
, Product_Group_Version_Id
, Discount_Version_Id
, Instant_Win_Version_Id
, Discount_Id
, Attached_Offer_Status_Type_Cd
, Attached_Offer_Status_Dsc
, Attached_Offer_Status_Effective_Ts
, Applied_Program_Nm
, Program_Applied_Ind
, Distinct_Id
, Offer_Rank_Nbr
, DW_First_Effective_Dt    
, DW_Last_Effective_Dt    
, DW_CREATE_TS
, DW_LOGICAL_DELETE_IND
, DW_SOURCE_CREATE_NM
, DW_CURRENT_VERSION_IND
)
SELECT Offer_External_Id
, Copient_Offer_Id
, Offer_Request_Id
, Offer_Id
, User_Interface_Unique_Id
, Product_Group_Version_Id
, Discount_Version_Id
, Instant_Win_Version_Id
, Discount_Id
, Attached_Offer_Status_Type_Cd
, Attached_Offer_Status_Dsc
, Attached_Offer_Status_Effective_Ts
, Applied_Program_Nm
, Program_Applied_Ind
, Distinct_Id
, Offer_Rank_Nbr
, CURRENT_DATE
, '31-DEC-9999'
, CURRENT_TIMESTAMP
, DW_Logical_delete_ind
, FileName
, TRUE
FROM `+ tgt_wrk_tbl +`
WHERE Sameday_chg_ind = 0
AND Offer_External_Id  IS NOT NULL `;
    var sql_commit = "COMMIT"
    var sql_rollback = "ROLLBACK"
try {
        snowflake.execute (
            {sqlText: sql_begin  }
            );
snowflake.execute (
            {sqlText: sql_updates  }
            );
        snowflake.execute (
            {sqlText: sql_sameday  }
            );
        snowflake.execute (
            {sqlText: sql_inserts  }
            );
        snowflake.execute (
            {sqlText: sql_commit  }
            );    

        }
    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback  }
            );
        return "Loading of Offer_Request_Offer table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
var sql_delexceptions =`Delete From ` + tgt_exp_tbl +`;`;
       
var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl +`  
                              select    Offer_Request_Id
, Applied_Program_Nm
, Attached_Offer_Status_Dsc
, Attached_Offer_Status_Effective_Ts
, Attached_Offer_Status_Type_Cd
, user_interface_unique_id
, Copient_Offer_Id
, Discount_Id
, Discount_Version_Id
, Distinct_Id
, Offer_External_Id
, Offer_Id
, Product_Group_Version_Id
, Program_Applied_Ind
//, Store_Group_Version_Id
                                                     ,Instant_Win_Version_Id
                                                    ,Offer_Rank_Nbr
, FileName
, DW_Logical_delete_ind
   , DML_Type
   , Sameday_chg_ind
   ,CASE WHEN Offer_External_Id is NULL THEN 'Offer_External_Id is NULL'
WHEN Copient_Offer_Id is NULL THEN 'Copient_Offer_Id is NULL'
WHEN Offer_Request_Id is NULL THEN 'Offer_Request_Id is NULL'
WHEN user_interface_unique_id is NULL THEN 'user_interface_unique_id is NULL'
//WHEN Store_Group_Version_Id is NULL THEN 'Store_Group_Version_Id is NULL'
ELSE NULL END AS Exception_Reason
   ,CURRENT_TIMESTAMP AS DW_CREATE_TS
FROM `+ tgt_wrk_tbl +`
WHERE Offer_External_Id  IS NULL
 or Copient_Offer_Id  IS NULL
or Offer_Request_Id  IS NULL
or user_interface_unique_id  IS NULL`;
//AND Store_Group_Version_Id  IS NULL`;
     try {
      snowflake.execute (
            {sqlText: sql_delexceptions  }
            );
        snowflake.execute (
             {sqlText: sql_exceptions  }
            );
        }
    catch (err)  {
        return "Insert into tgt Exception table "+ tgt_exp_tbl +" Failed with error: " + err;   // Return a error message.
        }
// ************** Load for Offer_Request_Offer table ENDs *****************

$$;
