--liquibase formatted sql
--changeset SYSTEM:SP_GetUber_To_BIM_LOAD_Partner_Grocery_Order_Detail runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETUBER_TO_BIM_LOAD_PARTNER_GROCERY_ORDER_DETAIL
(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYALTY VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_LOYALTY;
var lkp_schema = C_LOYALTY;
var src_wrk_tbl = SRC_WRK_TBL;
var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.PARTNER_GROCERY_ORDER_DETAIL_WRK`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Partner_Grocery_Order_Detail`;
var lkp_tb1 =`${cnf_db}.${lkp_schema}.Partner_Grocery_Order_Customer`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.Partner_Grocery_Order_Detail_Exceptions`;
// ************** Load for Partner_Grocery_Order_Detail table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;
var sql_command = `INSERT INTO ${tgt_wrk_tbl} 
WITH src_wrk_tbl_recs as
(SELECT DISTINCT
Order_Id
,Item_Nbr
,UPC_Id
                            ,UPC_Id_Txt
                            ,Item_Dsc
                            ,Item_Qty
                            ,Item_Tax_Amt
                            ,Receipt_Nbr
                            ,Unit_Price_Amt
                            ,Revenue_Amt
                            ,Alcoholic_Ind
                            ,Store_Transaction_Ts
                            ,Loyalty_Phone_Nbr
                            ,lower(user_id) as user_id
,FileName
,row_number() over (PARTITION BY order_id,Item_Dsc,UPC_ID ORDER BY Store_Transaction_Ts desc)as rn
FROM
(
SELECT DISTINCT
                             ORDER_ID as Order_Id
,ORDER_ITM_ID as Item_Nbr
,NVL(UPC_Id,'0') as UPC_Id
,LPAD(upc_id,14,0) as UPC_Id_Txt
,ORDERED_PROD_DSC as Item_Dsc
,ITEM_QTY as Item_Qty
,ITM_TAX_AMT as Item_Tax_Amt
,RTL_RCPT_NBR as Receipt_Nbr
,UNIT_PRC_AMT as Unit_Price_Amt
,ONLINE_REVENUE_AMT as Revenue_Amt
,ALCOHOLIC_IND as Alcoholic_Ind
,STORE_TXN_TS as Store_Transaction_Ts
,LOYALTY_PHONE_NBR as Loyalty_Phone_Nbr
,lower(user_id) as user_id
,FileName
                       
FROM  ${src_wrk_tbl}

UNION ALL
SELECT DISTINCT
Order_Id
,Item_Nbr
,UPC_Id
,UPC_Id_Txt
,Item_Dsc
,Item_Qty
,Item_Tax_Amt
,Receipt_Nbr
,Unit_Price_Amt
,Revenue_Amt
,Alcoholic_Ind
,Store_Transaction_Ts
,Loyalty_Phone_Nbr
 ,lower(user_id) as user_id
,FileName
FROM ${tgt_exp_tbl}
WHERE upper(filename) like '%UBER%'
           ))
,src_wrk_tbl_recs_TEMP as
(
select
src1.Order_Id
,LKP_Partner_Grocery_Order_Customer.Partner_Grocery_Order_Customer_Integration_Id AS Partner_Grocery_Order_Customer_Integration_Id
,src1.Item_Nbr
,src1.UPC_Id
,src1.UPC_Id_Txt
,src1.Item_Dsc
,src1.Item_Qty  
,src1.Item_Tax_Amt
,SRC1.Receipt_Nbr
,src1.Unit_Price_Amt
,src1.Revenue_Amt
,SRC1.Alcoholic_Ind
,SRC1.Store_Transaction_Ts
,SRC1.Loyalty_Phone_Nbr
,lower(SRC1.user_id) as user_id 
,src1.DW_Logical_delete_ind
,src1.FileName
from
(
SELECT
Order_Id
,Item_Nbr
,UPC_Id
,UPC_Id_Txt
,Item_Dsc
,Item_Qty
,Item_Tax_Amt
,Receipt_Nbr
,Unit_Price_Amt
,Revenue_Amt
,Alcoholic_Ind
,Store_Transaction_Ts
,Loyalty_Phone_Nbr
,lower(user_id) as user_id
,false AS DW_Logical_delete_ind
,FileName
FROM   src_wrk_tbl_recs
WHERE rn=1
and Order_Id  IS NOT NULL  --and UPC_Id is not null
   ) src1
LEFT JOIN
(SELECT DISTINCT Partner_Grocery_Order_Customer_Integration_Id,lower(Source_Customer_Id) as Source_Customer_Id,LOYALTY_PHONE_NBR
FROM ${lkp_tb1}
WHERE DW_CURRENT_VERSION_IND = TRUE
AND DW_LOGICAL_DELETE_IND = FALSE
) LKP_Partner_Grocery_Order_Customer
ON nvl(lower(src1.user_id),'-1') = nvl(lower(LKP_Partner_Grocery_Order_Customer.Source_Customer_Id),'-1')
//AND nvl(src1.LOYALTY_PHONE_NBR,'-1') = nvl(LKP_Partner_Grocery_Order_Customer.LOYALTY_PHONE_NBR,'-1')
)
select
src.Order_Id
,src.Partner_Grocery_Order_Customer_Integration_Id
,src.Item_Nbr
,src.UPC_Id
,src.UPC_Id_Txt
,SRC.Item_Dsc
,SRC.Item_Qty
,src.Item_Tax_Amt  
,src.Receipt_Nbr
,SRC.Unit_Price_Amt
,src.Revenue_Amt
,src.Alcoholic_Ind
,src.Store_Transaction_Ts
,src.Loyalty_Phone_Nbr
,lower(src.User_id) as user_id
,src.FileName
,src.DW_Logical_delete_ind
  ,CASE WHEN (tgt.Order_Id IS NULL AND tgt.Partner_Grocery_Order_Customer_Integration_Id IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
from
src_wrk_tbl_recs_TEMP src
LEFT JOIN (SELECT
tgt.Order_Id
,tgt.Partner_Grocery_Order_Customer_Integration_Id
,tgt.Item_Nbr
,tgt.UPC_Id
,tgt.UPC_Id_Txt
,tgt.Item_Dsc
,tgt.Item_Qty
,tgt.Item_Tax_Amt
,tgt.Receipt_Nbr
,tgt.Unit_Price_Amt
,tgt.Revenue_Amt
,tgt.Alcoholic_Ind
,tgt.Store_Transaction_Ts
,tgt.Loyalty_Phone_Nbr
,tgt.dw_logical_delete_ind
,tgt.dw_first_effective_dt
FROM ${tgt_tbl} tgt
WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
) tgt
ON tgt.Order_Id = src.Order_Id
//AND tgt.Partner_Grocery_Order_Customer_Integration_Id = src.Partner_Grocery_Order_Customer_Integration_Id 
WHERE  (tgt.Order_Id  IS NULL)
//WHERE  (tgt.Order_Id  IS NULL AND tgt.Partner_Grocery_Order_Customer_Integration_Id IS NULL)
OR (NVL(src.Item_Nbr,'-1') <> NVL(tgt.Item_Nbr,'-1')
OR NVL(src.UPC_Id,'-1') <> NVL(tgt.UPC_Id,'-1')
OR NVL(src.UPC_Id_Txt,'-1') <> NVL(tgt.UPC_Id_Txt,'-1')
OR NVL(src.Item_Dsc,'-1') <> NVL(tgt.Item_Dsc,'-1')
OR NVL(src.Item_Qty,'-1') <> NVL(tgt.Item_Qty,'-1')
OR NVL(src.Item_Tax_Amt,'-1') <> NVL(tgt.Item_Tax_Amt,'-1')
OR NVL(src.Receipt_Nbr,'-1') <> NVL(tgt.Receipt_Nbr,'-1')
OR NVL(src.Unit_Price_Amt,'-1') <> NVL(tgt.Unit_Price_Amt,'-1')
OR NVL(src.Revenue_Amt,'-1') <> NVL(tgt.Revenue_Amt,'-1')
OR NVL(to_boolean(src.Alcoholic_Ind),-1) <> NVL(tgt.Alcoholic_Ind,-1)
OR NVL(src.Store_Transaction_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Store_Transaction_Ts,'9999-12-31 00:00:00.000')
OR NVL(src.Loyalty_Phone_Nbr,'-1') <> NVL(tgt.Loyalty_Phone_Nbr,'-1')
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
        return `Creation of Partner_Grocery_Order_Detail work table ${tgt_wrk_tbl} Failed with error:  ${err}`;   // Return a error message.
        }
//SCD Type2 transaction begins
// Processing Updates of Type 2 SCD
var sql_begin = "BEGIN"
var sql_updates =
`UPDATE ${tgt_tbl} as tgt
SET DW_Last_Effective_dt = CURRENT_DATE-1
,DW_CURRENT_VERSION_IND = FALSE
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = FileName
FROM ( SELECT
Order_Id
,Partner_Grocery_Order_Customer_Integration_Id
,FileName
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
--AND Sameday_chg_ind = 0
AND Order_Id  IS NOT NULL
//AND Partner_Grocery_Order_Customer_Integration_Id  IS NOT NULL
) src
WHERE tgt.Order_Id = src.Order_Id  
//AND tgt.Partner_Grocery_Order_Customer_Integration_Id = src.Partner_Grocery_Order_Customer_Integration_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
   
// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
SET  Item_Nbr= src.Item_Nbr
,UPC_Id= src.UPC_Id
,UPC_Id_Txt=src.UPC_Id_Txt
,Item_Dsc= src.Item_Dsc
,Item_Qty= src.Item_Qty
,Item_Tax_Amt=src.Item_Tax_Amt
,Receipt_Nbr=src.Receipt_Nbr
,Unit_Price_Amt=src. Unit_Price_Amt
,Revenue_Amt= src.Revenue_Amt
,Alcoholic_Ind=src.Alcoholic_Ind
,Store_Transaction_Ts=src.Store_Transaction_Ts
,Loyalty_Phone_Nbr=src.Loyalty_Phone_Nbr
,DW_Logical_delete_ind = src.DW_Logical_delete_ind
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = FileName
FROM ( SELECT
Order_Id
,Partner_Grocery_Order_Customer_Integration_Id
,Item_Nbr
,UPC_Id
,UPC_Id_Txt
,Item_Dsc
,Item_Qty
,Item_Tax_Amt
,Receipt_Nbr
,Unit_Price_Amt
,Revenue_Amt
,Alcoholic_Ind
,Store_Transaction_Ts
,Loyalty_Phone_Nbr
,DW_Logical_delete_ind
,FileName
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 1
AND Order_Id  IS NOT NULL
//AND Partner_Grocery_Order_Customer_Integration_Id  IS NOT NULL
) src
WHERE tgt.Order_Id = src.Order_Id  
//AND tgt.Partner_Grocery_Order_Customer_Integration_Id = src.Partner_Grocery_Order_Customer_Integration_Id  
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
(Order_Id
,Partner_Grocery_Order_Customer_Integration_Id
,DW_First_Effective_Dt
,DW_Last_Effective_Dt
,Item_Nbr
,UPC_Id
,UPC_Id_Txt
,Item_Dsc
,Item_Qty
,Item_Tax_Amt
,Receipt_Nbr
,Unit_Price_Amt
,Revenue_Amt
,Alcoholic_Ind
,Store_Transaction_Ts
,Loyalty_Phone_Nbr
,DW_CREATE_TS
,DW_SOURCE_CREATE_NM
,DW_LOGICAL_DELETE_IND
,DW_CURRENT_VERSION_IND
)
SELECT DISTINCT
Order_Id
,Partner_Grocery_Order_Customer_Integration_Id
,CURRENT_DATE
,'31-DEC-9999'
,Item_Nbr
,UPC_Id
,UPC_Id_Txt
,Item_Dsc
,Item_Qty
,Item_Tax_Amt
,Receipt_Nbr
,Unit_Price_Amt
,Revenue_Amt
,Alcoholic_Ind
,Store_Transaction_Ts
,Loyalty_Phone_Nbr
,CURRENT_TIMESTAMP
,FileName
,DW_Logical_delete_ind
,TRUE
FROM ${tgt_wrk_tbl}
WHERE --Sameday_chg_ind = 0 AND
 Order_Id  IS NOT NULL
AND Partner_Grocery_Order_Customer_Integration_Id  IS NOT NULL
`;
    var sql_commit = "COMMIT"
    var sql_rollback = "ROLLBACK"
try {
        snowflake.execute (
            {sqlText: sql_begin  }
            );
        snowflake.execute (
            {sqlText: sql_updates  }
            );
        /*snowflake.execute (
            {sqlText: sql_sameday  }
            );*///same day is commented, as it is aways delete old and insert new
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
        return `Loading of Partner_Grocery_Order_Detail table ${tgt_tbl} Failed with error:  ${err}`;   // Return a error message.
       
}
 var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl} WHERE upper(filename) like '%UBER%'`;
						  /*WHERE
						  ((Order_Id) in
						  (SELECT Order_Id from ${tgt_tbl} WHERE DW_CURRENT_VERSION_IND = TRUE
						  and Partner_Grocery_Order_Customer_Integration_Id is not null
						  )
						  OR
						  (Order_Id, Partner_Grocery_Order_Customer_Integration_Id) in
						  (SELECT Order_Id, Partner_Grocery_Order_Customer_Integration_Id from ${tgt_wrk_tbl} WHERE 
						   Partner_Grocery_Order_Customer_Integration_Id is null
						  ))`;*/
var sql_exceptions = `INSERT INTO  ${tgt_exp_tbl}  
(
   Order_Id,
Partner_Grocery_Order_Customer_Integration_Id,
Item_Nbr,
UPC_Id,
UPC_Id_Txt,
Item_Dsc,
Item_Qty,
Item_Tax_Amt,
Receipt_Nbr,
Unit_Price_Amt,
Revenue_Amt,
Alcoholic_Ind,
Store_Transaction_Ts,
Loyalty_Phone_Nbr,
 user_id,
FileName,
DML_Type,
Sameday_chg_ind,
Exception_Reason,
DW_CREATE_TS
   )
select Distinct
   Order_Id,
Partner_Grocery_Order_Customer_Integration_Id,
Item_Nbr,
UPC_Id,
UPC_Id_Txt,
Item_Dsc,
Item_Qty,
Item_Tax_Amt,
Receipt_Nbr,
Unit_Price_Amt,
Revenue_Amt,
Alcoholic_Ind,
Store_Transaction_Ts,
Loyalty_Phone_Nbr,
lower(user_id) as user_id,
FileName,
DML_Type,
Sameday_chg_ind,
CASE WHEN Order_Id is NULL THEN 'Order_Id is NULL'
WHEN Partner_Grocery_Order_Customer_Integration_Id is NULL THEN 'Partner_Grocery_Order_Customer_Integration_Id is NULL'
ELSE NULL END AS Exception_Reason,
   CURRENT_TIMESTAMP AS DW_CREATE_TS
FROM  ${tgt_wrk_tbl}
WHERE (Order_Id IS NULL
or Partner_Grocery_Order_Customer_Integration_Id IS NULL)
AND upper(filename) like '%UBER%'  
`;
         
              try
              {
                     snowflake.execute (
                     {sqlText: sql_begin }
                     );
                     snowflake.execute(
                     {sqlText: truncate_exceptions}
                     );  
                     snowflake.execute (
                     {sqlText: sql_exceptions  }
                     );
                     snowflake.execute (
                     {sqlText: sql_commit  }
                     );
              }
              catch (err)  {
                     snowflake.execute (
                     {sqlText: sql_rollback  }
                     );
        return `Insert into tgt Exception table ${tgt_exp_tbl} Failed with error:  ${err}`;   // Return a error message.
              }
// ************** Load for Partner_Grocery_Order_Detail table ENDs *****************

$$;
