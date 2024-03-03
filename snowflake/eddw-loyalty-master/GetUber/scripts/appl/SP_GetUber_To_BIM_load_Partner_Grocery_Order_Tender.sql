USE DATABASE EDM_CONFIRMED_PRD;
USE SCHEMA DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETUBER_TO_BIM_LOAD_PARTNER_GROCERY_ORDER_TENDER(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYALTY VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

var src_wrk_tbl = SRC_WRK_TBL;
var cnf_schema = C_LOYALTY;
var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Partner_Grocery_Order_Tender_wrk`;
    var tgt_tbl = `${CNF_DB}.${cnf_schema}.Partner_Grocery_Order_Tender`;
var lkp_tbl = `${CNF_DB}.${cnf_schema}.Partner_Grocery_Order_Detail`;
var tgt_exp_tbl = `${CNF_DB}.${C_STAGE}.Partner_Grocery_Order_Tender_EXCEPTIONS`;
// ************** Load for Partner_Grocery_Order_Tender table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;
var sql_command = `INSERT INTO ${tgt_wrk_tbl} 
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT
                             a.Order_Id
,a.Approval_Cd
,a.LOADDATE
,a.FileName
,a.Masked_Credit_Card_Nbr
,row_number() over (PARTITION BY a.order_id ORDER BY b.STORE_TXN_TS desc )as rn
FROM
(
SELECT DISTINCT
ORDER_ID as Order_Id
,Approval_Cd as Approval_Cd
,LOADDATE
 ,filename as FileName
 ,NULL as Masked_Credit_Card_Nbr

FROM  ${src_wrk_tbl}
UNION ALL
SELECT DISTINCT
      Order_Id
 ,Approval_Cd
 ,LOADDATE
 ,FileName
 ,Masked_Credit_Card_Nbr
FROM ${tgt_exp_tbl}
WHERE upper(filename) like '%UBER%'
 ) a
 left join ${src_wrk_tbl} b on a.Order_Id = b.Order_Id
 )                          
SELECT
src.Order_Id
  ,src.Partner_Grocery_Order_Customer_Integration_Id
  ,src.Approval_Cd
  ,src.LOADDATE
  ,src.DW_Logical_delete_ind  
  ,src.Filename
  ,src.Masked_Credit_Card_Nbr
 ,CASE WHEN tgt.Order_Id IS NULL AND tgt.Partner_Grocery_Order_Customer_Integration_Id IS NULL THEN 'I' ELSE 'U' END AS DML_Type
  ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind  
  from
  (SELECT
s.Order_Id
,B.Partner_Grocery_Order_Customer_Integration_Id
,s.Approval_Cd
,s.LOADDATE
,s.DW_Logical_delete_ind
,s.Filename
,s.Masked_Credit_Card_Nbr
FROM
(
select
Order_Id
   ,Approval_Cd
,LOADDATE
,FALSE AS DW_Logical_delete_ind
,filename
,Masked_Credit_Card_Nbr
from src_wrk_tbl_recs
WHERE rn = 1
and Order_Id is not null    
) s  
  LEFT JOIN
( SELECT Partner_Grocery_Order_Customer_Integration_Id
 ,ORDER_ID
FROM ${lkp_tbl}
WHERE DW_CURRENT_VERSION_IND = TRUE
AND DW_LOGICAL_DELETE_IND = FALSE
) B ON S.ORDER_ID = B.ORDER_ID    
)src
                        LEFT JOIN
                          (SELECT  DISTINCT
tgt.Order_Id
,tgt.Partner_Grocery_Order_Customer_Integration_Id
,tgt.Approval_Cd
,tgt.dw_logical_delete_ind
,tgt.dw_first_effective_dt 
,tgt.Masked_Credit_Card_Nbr                                                                                                                                                                                                                                                             
                          FROM ${tgt_tbl} tgt
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt
                          ON tgt.Order_Id = src.Order_Id
 AND tgt.Partner_Grocery_Order_Customer_Integration_Id = src.Partner_Grocery_Order_Customer_Integration_Id
                          WHERE  (tgt.Order_Id is null and tgt.Partner_Grocery_Order_Customer_Integration_Id is null)  
                          or(
                           NVL(src.Approval_Cd,'-1') <> NVL(tgt.Approval_Cd,'-1')    
						or  NVL(src.Masked_Credit_Card_Nbr,'-1') <> NVL(tgt.Masked_Credit_Card_Nbr,'-1') 
 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )
 `;        
try {
		snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Partner_Grocery_Order_Tender work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
                                           Order_Id,                              
                                           filename,
  Partner_Grocery_Order_Customer_Integration_Id
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U'
                             AND Sameday_chg_ind = 0                                      
                             AND Order_Id is not NULL                              
AND Partner_Grocery_Order_Customer_Integration_Id is not null
                             ) src
                             WHERE tgt.Order_Id = src.Order_Id
AND tgt.Partner_Grocery_Order_Customer_Integration_Id = src.Partner_Grocery_Order_Customer_Integration_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE
AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;
// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
SET Approval_Cd = src.Approval_Cd
  ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
  ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
  ,DW_SOURCE_UPDATE_NM = FileName
  ,Masked_Credit_Card_Nbr = src.Masked_Credit_Card_Nbr
FROM ( SELECT
    Order_Id
   ,Partner_Grocery_Order_Customer_Integration_Id
,Approval_Cd
,DW_Logical_delete_ind
,FileName
,Masked_Credit_Card_Nbr
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 1
AND Order_Id IS NOT NULL
AND Partner_Grocery_Order_Customer_Integration_Id IS NOT NULL
) src
WHERE tgt.Order_Id = src.Order_Id
AND tgt.Partner_Grocery_Order_Customer_Integration_Id = src.Partner_Grocery_Order_Customer_Integration_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                     Order_Id
,Partner_Grocery_Order_Customer_Integration_Id
,Approval_Cd
                    ,DW_First_Effective_Dt
                    ,DW_Last_Effective_Dt              
                    ,DW_CREATE_TS
                    ,DW_LOGICAL_DELETE_IND
                    ,DW_SOURCE_CREATE_NM
                    ,DW_CURRENT_VERSION_IND   
					,Masked_Credit_Card_Nbr
                   )
                   SELECT DISTINCT
                      Order_Id
,Partner_Grocery_Order_Customer_Integration_Id
,Approval_Cd
                     ,CURRENT_DATE as DW_First_Effective_dt
,'31-DEC-9999'                    
,CURRENT_TIMESTAMP
                     ,DW_Logical_delete_ind
                     ,FileName
                     ,TRUE
					,Masked_Credit_Card_Nbr
FROM ${tgt_wrk_tbl}
                where Order_Id is not null
and Partner_Grocery_Order_Customer_Integration_Id is not null
and Sameday_chg_ind = 0`;
var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl} WHERE upper(filename) like '%UBER%'`;
/* WHERE
 ((Order_Id) in
 (SELECT Order_Id from ${tgt_tbl} WHERE DW_CURRENT_VERSION_IND = TRUE
 and Partner_Grocery_Order_Customer_Integration_Id is not null
 )
 OR
 (Order_Id, Partner_Grocery_Order_Customer_Integration_Id) in
 (SELECT Order_Id, Partner_Grocery_Order_Customer_Integration_Id from ${tgt_wrk_tbl} WHERE
  Partner_Grocery_Order_Customer_Integration_Id is null
 )
)`;*/
 
var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl  + `
(
   Order_Id
  ,Partner_Grocery_Order_Customer_Integration_Id
  ,Approval_Cd
  ,LOADDATE
  ,filename
  ,Exception_Reason
  ,dw_create_ts
  ,Masked_Credit_Card_Nbr
)
SELECT DISTINCT 
Order_Id
       ,Partner_Grocery_Order_Customer_Integration_Id
       ,Approval_Cd
       ,LOADDATE
        ,filename
       ,CASE WHEN Partner_Grocery_Order_Customer_Integration_Id IS NULL THEN 'Partner_Grocery_Order_Customer_Integration_Id is NULL'
        END AS Exception_Reason
        ,current_timestamp AS dw_create_ts
        ,Masked_Credit_Card_Nbr
FROM `+ tgt_wrk_tbl +`
WHERE (Partner_Grocery_Order_Customer_Integration_Id is NULL
or Order_Id is null) 
AND upper(filename) like '%UBER%'
`;
var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute({sqlText: sql_begin});
snowflake.execute({sqlText: sql_updates});
        snowflake.execute({sqlText: sql_sameday});
        snowflake.execute({sqlText: sql_inserts});
        snowflake.execute({sqlText: sql_commit});
snowflake.execute({sqlText: truncate_exceptions});
snowflake.execute({sqlText: sql_exceptions});
}
    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback  }
            );
        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }
// ************** Load for Partner_Grocery_Order_Tender table ENDs *****************



$$;
