--liquibase formatted sql
--changeset SYSTEM:SP_GETEPETRANSACTION_TO_BIM_EPE_TRANSACTION_HEADER_SAVING_CLIPS runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETEPETRANSACTION_TO_BIM_EPE_TRANSACTION_HEADER_SAVING_CLIPS
(SRC_WRK_TBL VARCHAR(16777216), CNF_DB VARCHAR(16777216), C_RETAIL VARCHAR(16777216), C_STAGE VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
var ref_db = "EDM_REFINED_PRD" ; 
var ref_schema = "DW_R_RETAILSALE";
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_RETAIL;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Header_Saving_Clips_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Epe_Transaction_Header_Saving_Clips`;
var src_flat_tbl =`${ref_db}.${ref_schema}.EPETRANSACTION_FLAT`;
var src_header_tbl = `${cnf_db}.${cnf_schema}.EPE_TRANSACTION_HEADER`;


// ************** Load for Epe_Transaction_Header_Saving_Clips table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.




var sql_command = `Create or replace transient table  ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            (
						with EPE_FLAT as
(select CASE WHEN TRANSACTIONSOURCE = 'STORE' THEN to_timestamp_tz(CASE 
				WHEN (
						STRTOK(TransactionTimestamp, '+', 2) <> ''
						AND CONTAINS (
							STRTOK(TransactionTimestamp, '+', 2)
							,':'
							) = false
						)
					OR (
						CONTAINS (
							TransactionTimestamp
							,'T'
							) = true
						AND STRTOK(TransactionTimestamp, '-', 4) <> ''
						AND CONTAINS (
							STRTOK(TransactionTimestamp, '-', 4)
							,':'
							) = false
						)
					THEN to_timestamp_tz(TransactionTimestamp, 'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
						--when TransactionTimestamp like '%T%' then to_timestamp_ntz(TransactionTimestamp,'YYYY-MM-DD HH24:MI:SS')
				ELSE to_timestamp_tz(TransactionTimestamp)
				END) 
                
                ELSE
                
               udf_ntz_to_tz(CASE 
				WHEN (
						STRTOK(TransactionTimestamp, '+', 2) <> ''
						AND CONTAINS (
							STRTOK(TransactionTimestamp, '+', 2)
							,':'
							) = false
						)
					OR (
						CONTAINS (
							TransactionTimestamp
							,'T'
							) = true
						AND STRTOK(TransactionTimestamp, '-', 4) <> ''
						AND CONTAINS (
							STRTOK(TransactionTimestamp, '-', 4)
							,':'
							) = false
						)
					THEN to_timestamp_ntz(TransactionTimestamp, 'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
						--when TransactionTimestamp like '%T%' then to_timestamp_ntz(TransactionTimestamp,'YYYY-MM-DD HH24:MI:SS')
				ELSE to_timestamp_ntz(TransactionTimestamp)
				END,STORETIMEZONE) END AS Transaction_Ts,
try_to_numeric(TRANSACTIONNUMBER) as TRANSACTIONNUMBER,
case when TRANSACTION_MEMBERID='' then NULL else TRANSACTION_MEMBERID end as TRANSACTION_MEMBERID,
TERMINALNUMBER,
clip.value::string as TXNLEVEL_CLIPIDS,
TXNLEVEL_OFFERID,
FileName
from `+ src_wrk_tbl +`
, LATERAL FLATTEN(input => parse_json(TXNLEVEL_CLIPIDS), outer => TRUE) as clip
)
select distinct h.TRANSACTION_INTEGRATION_ID,
                f.TXNLEVEL_CLIPIDS Clip_Id,
                f.TXNLEVEL_OFFERID Offer_Id,
                CURRENT_DATE Dw_First_Effective_Dt,
                '31-DEC-9999' Dw_Last_Effective_Dt,
                CURRENT_TIMESTAMP Dw_Create_Ts ,
                --null Dw_Last_Update_Ts,
                f.FileName Dw_Source_Create_Nm
                --null Dw_Source_Update_Nm,
                --False Dw_Logical_Delete_Ind,
                --True Dw_Current_Version_Ind
            ,Row_number() OVER (
			PARTITION BY TRANSACTION_INTEGRATION_ID,Clip_Id,Offer_Id ORDER BY (Dw_Create_Ts) DESC
			) AS rn                
from EPE_FLAT f,
`+ src_header_tbl +` h
where h.TRANSACTION_ID = f.TRANSACTIONNUMBER and nvl(h.HOUSEHOLD_ID,-1) = nvl(f.TRANSACTION_MEMBERID,-1)
and h.TERMINAL_NBR = f.TERMINALNUMBER
--and h.TRANSACTION_ID= f.TRANSACTIONNUMBER
and h.Transaction_Ts= f.Transaction_Ts
and f.TXNLEVEL_CLIPIDS is not null
) 

select 
 src.Transaction_Integration_Id
,src.Offer_Id
,src.Clip_Id
,src.DW_Logical_delete_ind
,src.Dw_Source_Create_Nm
,CASE WHEN tgt.Transaction_Integration_Id IS NULL  AND tgt.Offer_Id IS NULL AND tgt.Clip_Id IS NULL THEN 'I' ELSE 'U' END AS DML_Type
,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
from 
(
select 
Transaction_Integration_Id
,Offer_Id 
,Clip_Id 
,false AS DW_Logical_delete_ind
,Dw_Source_Create_Nm
FROM src_wrk_tbl_recs
where rn=1
and Clip_Id is not null
and Offer_Id is not null
)src
LEFT JOIN (
SELECT
tgt.Transaction_Integration_Id
,tgt.Offer_Id
,tgt.Clip_Id
,tgt.dw_logical_delete_ind
,tgt.dw_first_effective_dt
FROM ${tgt_tbl} tgt
WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
) as tgt 
ON tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
AND tgt.Offer_Id = src.Offer_Id
AND tgt.Clip_Id = src.Clip_Id
 
where (tgt.Transaction_Integration_Id IS NULL  AND tgt.Offer_Id IS NULL  AND tgt.Clip_Id IS NULL)
OR ( 

                                                        
 src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
)
`;
try {
        
        snowflake.execute ({sqlText: sql_command});
            
        }
    catch (err)  {
        throw "Creation of EPE_Transaction_Header_Saving_Points work table Failed with error: "+ err;   // Return a error message.
        }
var sql_begin = 'BEGIN'


//SCD Type2 transaction begins
// Processing Different Day Updates of Type 2 SCD

var sql_updates =
`UPDATE ${tgt_tbl} as tgt
 SET DW_Last_Effective_dt = CURRENT_DATE-1
,DW_CURRENT_VERSION_IND = FALSE
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = src.DW_SOURCE_CREATE_NM
FROM ( SELECT
 Transaction_Integration_Id
,Offer_Id
,Clip_Id
,DW_SOURCE_CREATE_NM
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 0
AND Transaction_Integration_Id  IS NOT NULL
AND Offer_Id IS NOT NULL
AND Clip_Id IS NOT NULL
) src
WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id  
AND tgt.Offer_Id = src.Offer_Id
AND tgt.Clip_Id = src.Clip_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE
AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;
   

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
SET  
 Transaction_Integration_Id = src.Transaction_Integration_Id
,Offer_Id = src.Offer_Id 
,Clip_Id  = src.Clip_Id 
,DW_LOGICAL_DELETE_IND = src.DW_LOGICAL_DELETE_IND 
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = src.DW_SOURCE_CREATE_NM
FROM 
( SELECT
Transaction_Integration_Id
,Offer_Id 
,Clip_Id
,DW_Logical_delete_ind
,DW_SOURCE_CREATE_NM
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 1
AND Transaction_Integration_Id  IS NOT NULL
AND Offer_Id IS NOT NULL
AND Clip_Id IS NOT NULL
) src
WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
AND tgt.Offer_Id = src.Offer_Id
AND tgt.Clip_Id = src.Clip_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
(
Transaction_Integration_Id ,
Offer_Id,
Clip_Id,
DW_Last_Effective_Dt ,
DW_First_Effective_Dt ,
DW_CREATE_TS ,
DW_LOGICAL_DELETE_IND ,
DW_SOURCE_CREATE_NM ,
DW_CURRENT_VERSION_IND
)
 SELECT DISTINCT
Transaction_Integration_Id 
,Offer_Id
,Clip_Id
,'31-DEC-9999'
,CURRENT_DATE
,CURRENT_TIMESTAMP
,DW_Logical_delete_ind
,DW_SOURCE_CREATE_NM
,TRUE
FROM ${tgt_wrk_tbl}
WHERE Sameday_chg_ind = 0
AND Transaction_Integration_Id  IS NOT NULL
AND Offer_Id IS NOT NULL
AND Clip_Id IS NOT NULL
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
        return `Loading of EPE_Transaction_Header_Saving_Points table ${tgt_tbl} Failed with error:  ${err}`;   // Return a error message.
       
}


// ************** Load for EPE_Transaction_Header_Saving_Points table ENDs *****************
$$;
