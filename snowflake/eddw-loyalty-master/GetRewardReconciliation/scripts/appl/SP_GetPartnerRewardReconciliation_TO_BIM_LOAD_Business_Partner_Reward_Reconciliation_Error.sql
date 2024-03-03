--liquibase formatted sql
--changeset SYSTEM:SP_GetPartnerRewardReconciliation_TO_BIM_LOAD_Business_Partner_Reward_Reconciliation_Error runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETPARTNERREWARDRECONCILIATION_TO_BIM_LOAD_BUSINESS_PARTNER_REWARD_RECONCILIATION_ERROR(SRC_WRK_TBL VARCHAR, SRC_WRK_TBL1 VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
     
 
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_LOYAL;
var src_wrk_tbl = SRC_WRK_TBL;
var src_wrk_tbl1 = SRC_WRK_TBL1;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Reward_Reconciliation_Error_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Business_Partner_Reward_Reconciliation_Error`;
var lkp_tbl =`${cnf_db}.${cnf_schema}.Business_Partner`;
var lkp_tbl_Cust = `${cnf_db}.${cnf_schema}.Business_Partner_Reward_Transaction`; 
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Reward_Reconciliation_Error_Exceptions`;

// ************** Load for Business_Partner_Reward_Reconciliation_Error table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
WITH src_wrk_tbl_recs as
(SELECT DISTINCT
Transaction_Id,
Reconcilation_Error_Type_Cd,
--Transaction_Ts,
row_number() over (partition by Transaction_Id order by To_timestamp_ntz(CREATIONDT)desc) as Sequence_Nbr,
Reconcilation_Error_Type_Dsc,
Reconcilation_Error_Type_Short_Dsc,
Partner_Participant_Id,
Partner_Site_Id,
Partner_Id,
FileName,
creationdt,
row_number() over (PARTITION BY Reconcilation_Error_Type_Cd, Transaction_Id,Partner_Participant_Id,Partner_Site_Id, Partner_Id order by To_timestamp_ntz(CREATIONDT) DESC)as rn
FROM
(
SELECT DISTINCT
TransactionId as Transaction_Id ,
LPAD(sp_val.value::string,4,0) as Reconcilation_Error_Type_Cd  ,
--TransactionTs as Transaction_Ts ,
--Sequence_Nbr ,
ReconErrorTypeCd_Description as Reconcilation_Error_Type_Dsc  ,
ReconErrorTypeCd_ShortDescription as Reconcilation_Error_Type_Short_Dsc, 
PartnerParticipantId as Partner_Participant_Id,
PartnerSiteId as Partner_Site_Id,
PartnerId as Partner_Id,  
FileName,
creationdt
FROM  ${src_wrk_tbl}
,LATERAL FLATTEN(input => ReconErrorTypeCd_Code, outer => TRUE) as ReconErrorTypeCd_Code
,lateral flatten(input => parse_json(ReconErrorTypeCd_Code.value), outer => true) as sp_val							 
) 
)
   select 
src.Transaction_Id,
src.Reconcilation_Error_Type_Cd, 
src.Business_Partner_Integration_Id,
--src.Retail_Customer_UUID,
src.Transaction_Ts,
src.Sequence_Nbr ,
src.Reconcilation_Error_Type_Dsc,
src.Reconcilation_Error_Type_Short_Dsc,
src.Partner_Participant_Id,
src.Partner_Site_Id,
src.Partner_Id, 
src.FileName,
src.DW_Logical_delete_ind,
src.creationdt,
CASE WHEN (tgt.Business_Partner_Integration_Id IS NULL AND tgt.Transaction_Id IS NULL AND tgt.Reconcilation_Error_Type_Cd IS NULL AND tgt.Transaction_Ts IS NULL AND tgt.Sequence_Nbr IS NULL) THEN 'I' ELSE 'U' END AS DML_Type,
CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
from 
(select 
src1.Transaction_Id,
src1.Reconcilation_Error_Type_Cd,
B.Business_Partner_Integration_Id as Business_Partner_Integration_Id, 
--C.Retail_Customer_UUID as Retail_Customer_UUID,
C.Transaction_Ts,
src1.Sequence_Nbr,
src1.Reconcilation_Error_Type_Dsc,
src1.Reconcilation_Error_Type_Short_Dsc,
src1.Partner_Participant_Id,
src1.Partner_Site_Id,
src1.Partner_Id, 
src1.DW_Logical_delete_ind,
src1.FileName,
src1.creationdt
from
(
select
Transaction_Id,
Reconcilation_Error_Type_Cd,
--Transaction_Ts,
Sequence_Nbr,
Reconcilation_Error_Type_Dsc,
Reconcilation_Error_Type_Short_Dsc,
Partner_Participant_Id,
Partner_Site_Id,
Partner_Id, 
false AS DW_Logical_delete_ind,
FileName,
creationdt
FROM src_wrk_tbl_recs
where Transaction_Id is not null
and (Partner_Participant_Id is not null
AND Partner_Site_Id is not null
AND Partner_Id is not null)
and rn=1
) src1
LEFT JOIN 
   ( SELECT distinct Transaction_Id
                                                                  ,Transaction_Ts
FROM ${lkp_tbl_Cust} 
WHERE DW_CURRENT_VERSION_IND = TRUE 
AND DW_LOGICAL_DELETE_IND = FALSE
) C ON src1.Transaction_Id = C.Transaction_Id
LEFT JOIN 
( SELECT distinct Business_Partner_Integration_Id
 ,Partner_Participant_Id
     ,Partner_Site_Id
     ,Partner_Id 
FROM ${lkp_tbl} 
WHERE DW_CURRENT_VERSION_IND = TRUE 
AND DW_LOGICAL_DELETE_IND = FALSE 
) B ON  ((NVL(src1.Partner_Participant_Id,'-1') = NVL(B.Partner_Participant_Id,'-1')
AND NVL(src1.Partner_Site_Id,'-1') = NVL(B.Partner_Site_Id,'-1')
AND NVL(src1.Partner_Id,'-1') = NVL(B.Partner_Id,'-1'))
OR (NVL(src1.Partner_Participant_Id,'-1') = NVL(B.Partner_Participant_Id,'-1')
AND NVL(src1.Partner_Site_Id,'-1') = NVL(B.Partner_Site_Id,'-1'))) 
  
) src
LEFT JOIN (SELECT
tgt.Transaction_Id,
tgt.Reconcilation_Error_Type_Cd,
tgt.Business_Partner_Integration_Id,
--tgt.Retail_Customer_UUID,
tgt.Transaction_Ts,
tgt.Sequence_Nbr, 
tgt.Reconcilation_Error_Type_Dsc,
tgt.Reconcilation_Error_Type_Short_Dsc,
tgt.dw_logical_delete_ind,
tgt.dw_first_effective_dt
FROM ${tgt_tbl} tgt
WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
) as tgt
ON tgt.Transaction_Id = src.Transaction_Id
AND tgt.Reconcilation_Error_Type_Cd = src.Reconcilation_Error_Type_Cd 
AND tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
--AND tgt.Retail_Customer_UUID = src.Retail_Customer_UUID
AND tgt.Transaction_Ts = src.Transaction_Ts
--AND tgt.Sequence_Nbr = src.Sequence_Nbr 
where (tgt.Transaction_Id IS NULL  AND tgt.Business_Partner_Integration_Id IS NULL  AND tgt.Transaction_Ts IS NULL AND tgt.Reconcilation_Error_Type_Cd IS NULL AND tgt.Sequence_Nbr IS NULL)
OR (NVL(src.Reconcilation_Error_Type_Dsc,'-1') <> NVL(tgt.Reconcilation_Error_Type_Dsc,'-1')
OR NVL(src.Reconcilation_Error_Type_Short_Dsc,'-1') <> NVL(tgt.Reconcilation_Error_Type_Short_Dsc,'-1')
OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
)`;
try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        throw "Creation of Business_Partner_Reward_Reconciliation_Error work table Failed with error: "+ err;   // Return a error message.
        }
//SCD Type2 transaction begins 
// Processing Updates of Type 2 SCD
var sql_begin = `BEGIN`
                    var sql_updates =`UPDATE ${tgt_tbl} as tgt
SET DW_Last_Effective_dt = CURRENT_DATE-1,
DW_CURRENT_VERSION_IND = FALSE,
DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
DW_SOURCE_UPDATE_NM = FileName
FROM ( SELECT
Transaction_Id,
Reconcilation_Error_Type_Cd,
Business_Partner_Integration_Id,
--Retail_Customer_UUID,
Transaction_Ts,
Sequence_Nbr,
FileName
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 0
AND Transaction_Id IS NOT NULL
AND Reconcilation_Error_Type_Cd IS NOT NULL
AND Business_Partner_Integration_Id IS NOT NULL 
AND Transaction_Ts IS NOT NULL
AND Sequence_Nbr IS NOT NULL
) src
WHERE tgt.Transaction_Id = src.Transaction_Id
AND tgt.Reconcilation_Error_Type_Cd = src.Reconcilation_Error_Type_Cd
AND tgt.Business_Partner_Integration_Id  = src.Business_Partner_Integration_Id 
--AND tgt.Retail_Customer_UUID = src.Retail_Customer_UUID
AND tgt.Transaction_Ts = src.Transaction_Ts
AND tgt.Sequence_Nbr = src.Sequence_Nbr 
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                              
 //Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
SET Reconcilation_Error_Type_Dsc = src.Reconcilation_Error_Type_Dsc,
Reconcilation_Error_Type_Short_Dsc = src.Reconcilation_Error_Type_Short_Dsc
FROM ( SELECT
Transaction_Id,
Reconcilation_Error_Type_Cd, 
Business_Partner_Integration_Id,
--Retail_Customer_UUID,
Transaction_Ts,
Sequence_Nbr, 
                            Reconcilation_Error_Type_Dsc,
Reconcilation_Error_Type_Short_Dsc,
FileName,
DW_Logical_delete_ind
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 1
AND Transaction_Id IS NOT NULL
AND Reconcilation_Error_Type_Cd IS NOT NULL 
AND Business_Partner_Integration_Id IS NOT NULL 
--AND Retail_Customer_UUID IS NOT NULL 
AND Transaction_Ts IS NOT NULL
AND Sequence_Nbr IS NOT NULL
) src
WHERE tgt.Transaction_Id = src.Transaction_Id
AND tgt.Reconcilation_Error_Type_Cd = src.Reconcilation_Error_Type_Cd
AND tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id 
--AND tgt.Retail_Customer_UUID = src.Retail_Customer_UUID
AND tgt.Transaction_Ts = src.Transaction_Ts 
AND tgt.Sequence_Nbr = src.Sequence_Nbr 
AND    tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                        (Transaction_Id,
Reconcilation_Error_Type_Cd, 
Business_Partner_Integration_Id,
--Retail_Customer_UUID,
Transaction_Ts,
Sequence_Nbr, 
DW_First_Effective_Dt,  
   DW_Last_Effective_Dt,  
  Reconcilation_Error_Type_Dsc,
Reconcilation_Error_Type_Short_Dsc,
DW_CREATE_TS,        
  DW_LOGICAL_DELETE_IND, 
  DW_SOURCE_CREATE_NM,   
  DW_CURRENT_VERSION_IND  
)
SELECT DISTINCT
Transaction_Id,
Reconcilation_Error_Type_Cd, 
Business_Partner_Integration_Id,
--Retail_Customer_UUID,
Transaction_Ts,
Sequence_Nbr, 
CURRENT_DATE,
'31-DEC-9999',
Reconcilation_Error_Type_Dsc,
Reconcilation_Error_Type_Short_Dsc,
CURRENT_TIMESTAMP,
                        DW_Logical_delete_ind,
FileName,
TRUE
FROM ${tgt_wrk_tbl}
WHERE Sameday_chg_ind = 0
AND Transaction_Id IS NOT NULL
AND Reconcilation_Error_Type_Cd IS NOT NULL 
AND Business_Partner_Integration_Id IS NOT NULL 
--AND Retail_Customer_UUID IS NOT NULL 
AND Transaction_Ts IS NOT NULL
AND Sequence_Nbr IS NOT NULL`;
                          
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
    catch (err){
        snowflake.execute (
            {sqlText: sql_rollback  }
            );
        return "Loading of Business_Partner_Reward_Reconciliation_Error table Failed with error: "+ err;   // Return a error message.
        
       }
  
  
var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl}`;
 
var sql_exceptions =`INSERT INTO  ${tgt_exp_tbl} 
SELECT DISTINCT 
Transaction_Id,
Reconcilation_Error_Type_Cd, 
Business_Partner_Integration_Id,
--Retail_Customer_UUID,
Transaction_Ts,
Sequence_Nbr,
Reconcilation_Error_Type_Dsc,
Reconcilation_Error_Type_Short_Dsc,
Partner_Participant_Id,
Partner_Site_Id,
Partner_Id,
--CustomerId, 
FileName,
DW_Logical_delete_ind,
CREATIONDT,
DML_Type,
Sameday_chg_ind,
CASE WHEN Business_Partner_Integration_Id is NULL THEN 'Business_Partner_Integration_Id is NULL' 
ELSE NULL END AS Exception_Reason,
CURRENT_TIMESTAMP AS DW_CREATE_TS 
FROM  ${tgt_wrk_tbl}
WHERE Business_Partner_Integration_Id is NULL
--or Retail_Customer_UUID is NULL 
or Transaction_Id is NULL
or Reconcilation_Error_Type_Cd is null
or Transaction_Ts is null
or Sequence_Nbr is null

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
  
// ************** Load for Business_Partner_Reward_Reconciliation_Error table ENDs *****************

$$;
