--liquibase formatted sql
--changeset SYSTEM:SP_GETDOORDASH_TO_BIM_LOAD_PARTNER_GROCERY_ORDER_CUSTOMER runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETDOORDASH_TO_BIM_LOAD_PARTNER_GROCERY_ORDER_CUSTOMER(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


var src_wrk_tbl = SRC_WRK_TBL;
var cnf_schema = C_LOYAL;
var wrk_schema = C_STAGE;
var tgt_wrk_tbl = `${CNF_DB}.${wrk_schema}.PARTNER_GROCERY_ORDER_CUSTOMER_DOORDASH_WRK`;
var tgt_tbl = `${CNF_DB}.${cnf_schema}.Partner_Grocery_Order_Customer`;
var src_wrk_tmp_tbl = `${CNF_DB}.${wrk_schema}.Partner_Grocery_Order_Customer_SRC_WRK`;
var tgt_exp_tbl = `${CNF_DB}.${wrk_schema}.Partner_Grocery_Order_Customer_EXCEPTIONS`;
                       
    // **************  Load for Partner_Grocery_Order_Customer table BEGIN *****************
	
	var sql_truncate_tmp_tbl = `Truncate table  ${src_wrk_tmp_tbl}`;
    var cr_src_wrk_tbl = `INSERT INTO ${src_wrk_tmp_tbl} 
WITH src_wrk_tbl_recs as
(
SELECT DISTINCT
lower(Source_Customer_Id) as Source_Customer_Id,
Loyalty_Phone_Nbr,
First_Nm,
Last_Nm,
Email_Address_Txt,
Contact_Phone_Nbr,
Retail_Customer_UUID,
filename,
DW_Logical_delete_ind,
row_number() over (PARTITION BY lower(Source_Customer_Id) ORDER BY lower(Source_Customer_Id),Loyalty_Phone_Nbr,First_Nm,Last_Nm desc )as rn
--row_number() over (PARTITION BY lower(Source_Customer_Id),Loyalty_Phone_Nbr ORDER BY (select 1)desc )as rn
--row_number() over (PARTITION BY lower(Source_Customer_Id),Loyalty_Phone_Nbr ORDER BY to_timestamp_ntz(creationdt) desc) as rn
FROM
(
SELECT DISTINCT
lower(src.Source_Customer_Id) as Source_Customer_Id,
src.Loyalty_Phone_Nbr,
src.First_Nm,
src.Last_Nm,
src.Email_Address_Txt,
src.Contact_Phone_Nbr,
src.Retail_Customer_UUID,
src.filename,
FALSE AS DW_Logical_delete_ind
FROM
( SELECT DISTINCT
lower(Source_Customer_Id) as Source_Customer_Id,
flat1.Loyalty_Phone_Nbr,
First_Nm,
Last_Nm,
Email_Address_Txt,
Contact_Phone_Nbr,
Retail_Customer_UUID,
filename
from
(
(SELECT DISTINCT
lower(USER_ID) as Source_Customer_Id,
LOYALTY_PHONE_NBR as  Loyalty_Phone_Nbr,
NULL as First_Nm,
NULL as Last_Nm,
NULL as Email_Address_Txt,
NULL as Contact_Phone_Nbr,
NULL as Retail_Customer_UUID,
filename
FROM ${src_wrk_tbl}
--where lower(USER_ID) is not null
)flat1
/*left join
(
SELECT DISTINCT LOYALTY_PHONE_NBR, lower(USER_ID) as USER_ID
FROM ${src_wrk_tbl}
where lower(USER_ID) is not null
)flat2 on lower(flat1.Source_Customer_Id) = lower(flat2.USER_ID) */
)
where lower(Source_Customer_Id) IS NOT NULL
OR  flat1.LOYALTY_PHONE_NBR IS NOT NULL
UNION ALL
SELECT DISTINCT
lower(Source_Customer_Id) as Source_Customer_Id,
Loyalty_Phone_Nbr,
First_Nm,
Last_Nm,
Email_Address_Txt,
Contact_Phone_Nbr,
Retail_Customer_UUID,  
filename
FROM ${tgt_exp_tbl}
WHERE upper(filename) like '%DOORDASH%'
) src
)
 
 
)
SELECT DISTINCT
lower(Source_Customer_Id) as Source_Customer_Id,
Loyalty_Phone_Nbr,
First_Nm,
Last_Nm,
Email_Address_Txt,
Contact_Phone_Nbr,
Retail_Customer_UUID,
filename,
DW_Logical_delete_ind
from src_wrk_tbl_recs
where rn=1`;
try {
snowflake.execute ({ sqlText: sql_truncate_tmp_tbl});
snowflake.execute ({ sqlText: cr_src_wrk_tbl });
}
    catch (err) {
        return `Creation of Source work temp table table ${src_wrk_tmp_tbl} Failed with error: ${err}`;   // Return a error message.
    }
       
// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;
var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl} 
SELECT DISTINCT
CASE WHEN DML_TYPE = 'I' THEN mx_det_intg_id + rnum ELSE Partner_Grocery_Order_Customer_Integration_Id END AS Partner_Grocery_Order_Customer_Integration_Id,
lower(Source_Customer_Id) as Source_Customer_Id,
Loyalty_Phone_Nbr,
First_Nm,
Last_Nm,
Email_Address_Txt,
Contact_Phone_Nbr,
Retail_Customer_UUID,
DML_TYPE,
Sameday_chg_ind,
filename,
DW_Logical_delete_ind
from
(
SELECT seq_gen.*
,row_number() over ( PARTITION BY DML_TYPE ORDER BY Partner_Grocery_Order_Customer_Integration_Id ) as rnum
FROM
(
SELECT DISTINCT
tgt.Partner_Grocery_Order_Customer_Integration_Id,
lower(src.Source_Customer_Id) as Source_Customer_Id,
src.Loyalty_Phone_Nbr,
src.First_Nm,
src.Last_Nm,
src.Email_Address_Txt,
src.Contact_Phone_Nbr,
src.Retail_Customer_UUID,
src.filename,
src.DW_Logical_delete_ind,
CASE WHEN tgt.Partner_Grocery_Order_Customer_Integration_Id is NULL THEN 'I' ELSE 'U' END as DML_Type,
CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind,
mx_det_intg_id
FROM ${src_wrk_tmp_tbl} src
JOIN
(
SELECT NVL(MAX(Partner_Grocery_Order_Customer_Integration_Id),0) as mx_det_intg_id
FROM ${tgt_tbl}
WHERE DW_CURRENT_VERSION_IND = TRUE
) mx on 1=1
LEFT JOIN
(
SELECT
Partner_Grocery_Order_Customer_Integration_Id,
lower(Source_Customer_Id) as Source_Customer_Id,
Loyalty_Phone_Nbr,
First_Nm,
Last_Nm,
Email_Address_Txt,
Contact_Phone_Nbr,
Retail_Customer_UUID,
tgt.DW_Logical_delete_ind,
tgt.DW_First_Effective_dt
FROM ${tgt_tbl} tgt
WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
) as tgt
on nvl(lower(src.Source_Customer_Id),'-1') = nvl(lower(tgt.Source_Customer_Id),'-1')

WHERE
tgt.Partner_Grocery_Order_Customer_Integration_Id is NULL
OR (
NVL(src.Loyalty_Phone_Nbr,'-1') <> nvl(tgt.Loyalty_Phone_Nbr,'-1')OR
NVL(tgt.First_Nm,'-1') <> NVL(src.First_Nm,'-1') OR
NVL(tgt.Last_Nm,'-1') <> NVL(src.Last_Nm,'-1') OR
NVL(tgt.Email_Address_Txt,'-1') <> NVL(src.Email_Address_Txt,'-1') OR
NVL(tgt.Contact_Phone_Nbr,'-1') <> NVL(src.Contact_Phone_Nbr,'-1') OR
NVL(tgt.Retail_Customer_UUID,'-1') <> NVL(src.Retail_Customer_UUID,'-1') OR
tgt.dw_logical_delete_ind  <>  src.dw_logical_delete_ind
)
) seq_gen
)`;  
try {
snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
snowflake.execute ({ sqlText: create_tgt_wrk_table });
}
    catch (err) {
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
DW_SOURCE_UPDATE_NM = filename
FROM (
SELECT
Partner_Grocery_Order_Customer_Integration_Id,
filename
FROM ${tgt_wrk_tbl}
WHERE
DML_Type = 'U' AND
Sameday_chg_ind = 0 AND
Partner_Grocery_Order_Customer_Integration_Id is not NULL
) src
WHERE
tgt.Partner_Grocery_Order_Customer_Integration_Id = src.Partner_Grocery_Order_Customer_Integration_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
 
// SCD Type1 - Processing Sameday updates
var sql_sameday = `
UPDATE ${tgt_tbl} as tgt
SET      
// npks
Source_Customer_Id= src.Source_Customer_Id,
Loyalty_Phone_Nbr= src.Loyalty_Phone_Nbr,
First_Nm= src.First_Nm,
Last_Nm= src.Last_Nm,
Email_Address_Txt= src.Email_Address_Txt,
Contact_Phone_Nbr= src.Contact_Phone_Nbr,
Retail_Customer_UUID= src.Retail_Customer_UUID,
DW_Logical_delete_ind = src.DW_Logical_delete_ind,
DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
DW_SOURCE_UPDATE_NM = filename
FROM (
SELECT
Partner_Grocery_Order_Customer_Integration_Id,
lower(Source_Customer_Id) as Source_Customer_Id,
Loyalty_Phone_Nbr,
First_Nm,
Last_Nm,
Email_Address_Txt,
Contact_Phone_Nbr,
Retail_Customer_UUID,
DW_Logical_delete_ind,
filename
FROM ${tgt_wrk_tbl}
WHERE
DML_Type = 'U' AND
Sameday_chg_ind = 1 AND
Partner_Grocery_Order_Customer_Integration_Id is not NULL
) src
WHERE
tgt.Partner_Grocery_Order_Customer_Integration_Id = src.Partner_Grocery_Order_Customer_Integration_Id AND
tgt.DW_CURRENT_VERSION_IND = TRUE`;
                               
                           
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} (
Partner_Grocery_Order_Customer_Integration_Id,
DW_First_Effective_Dt,
DW_Last_Effective_Dt,
Source_Customer_Id,
Loyalty_Phone_Nbr,
First_Nm,
Last_Nm,
Email_Address_Txt,
Contact_Phone_Nbr,
Retail_Customer_UUID,
DW_CREATE_TS,          
DW_LOGICAL_DELETE_IND,  
DW_SOURCE_CREATE_NM,  
DW_CURRENT_VERSION_IND  
)
SELECT distinct
Partner_Grocery_Order_Customer_Integration_Id,
CURRENT_DATE,
'31-DEC-9999',
lower(Source_Customer_Id) as Source_Customer_Id,
Loyalty_Phone_Nbr,
First_Nm,
Last_Nm,
Email_Address_Txt,
Contact_Phone_Nbr,
Retail_Customer_UUID,
CURRENT_TIMESTAMP,
DW_Logical_delete_ind,
filename,
TRUE
FROM ${tgt_wrk_tbl}
WHERE
Sameday_chg_ind = 0 AND
Partner_Grocery_Order_Customer_Integration_Id is not NULL
--Retail_Customer_UUID is not NULL
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
        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }
       
   
var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl} WHERE upper(filename) like '%DOORDASH%'`;
      /*WHERE
      ((Partner_Grocery_Order_Customer_Integration_Id) in
      (SELECT Partner_Grocery_Order_Customer_Integration_Id  from ${tgt_tbl} WHERE DW_CURRENT_VERSION_IND = TRUE and
       Retail_Customer_UUID is not null))
      OR
      (( Partner_Grocery_Order_Customer_Integration_Id) in
      (SELECT Partner_Grocery_Order_Customer_Integration_Id  from ${tgt_wrk_tbl} WHERE Retail_Customer_UUID is null))`;*/
var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl  + `
SELECT
Partner_Grocery_Order_Customer_Integration_Id,
lower(Source_Customer_Id) as Source_Customer_Id,
Loyalty_Phone_Nbr,
First_Nm,
Last_Nm,
Email_Address_Txt,
Contact_Phone_Nbr,
Retail_Customer_UUID,
filename,
CASE WHEN lower(Source_Customer_Id) IS NULL THEN 'Source_Customer_Id is NULL'
  WHEN Loyalty_Phone_Nbr IS NULL THEN 'Loyalty_Phone_Nbr is NULL'
END AS Exception_Reason,
current_timestamp AS dw_create_ts
FROM `+ tgt_wrk_tbl +`
WHERE lower(Source_Customer_Id) is NULL
and upper(filename) like '%DOORDASH%'
--AND  Loyalty_Phone_Nbr is NULL
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
        return `Insert into tgt Exception table  ${tgt_exp_tbl} Failed with error:  ${err}`;   // Return a error message.
}
                			
                // **************        Load for Partner_Grocery_Order_Customer table ENDs *****************

$$;
