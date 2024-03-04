--liquibase formatted sql
--changeset SYSTEM:SP_GetEPETransaction_To_BIM_load_EPE_Transaction_Header_Saving_Points runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETEPETRANSACTION_TO_BIM_LOAD_EPE_TRANSACTION_HEADER_SAVING_POINTS
("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_RETAIL" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
 
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_RETAIL;
var lkp_schema = C_RETAIL;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Header_Saving_Points_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.EPE_Transaction_Header_Saving_Points`;
var lkp_tbl =`${cnf_db}.${lkp_schema}.EPE_Transaction_Header`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Header_Saving_Points_Exceptions`;



// ************** Load for EPE_Transaction_Header_Saving_Points table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.




var sql_command = `Create or replace table  ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT
                                                                 Offer_Id
                                                                ,Points_Program_Nm
                                                                ,Points_Burned_Nbr
                                                                ,Points_Earned_Nbr
                                                                ,Scorecard_Txt
                                                                ,TERMINALNUMBER
                                                                ,TRANSACTIONNUMBER
                                                                ,TRANSACTIONTIMESTAMP
                                                                ,UpdatedDate
                                                                ,filename
                                                                ,Row_number() OVER ( partition BY TERMINALNUMBER,TRANSACTIONNUMBER,TRANSACTIONTIMESTAMP,Points_Program_Nm,Offer_Id ORDER BY
                                                                 To_timestamp_ntz(UpdatedDate ) desc) AS rn
                                                                 from
                            (
                            SELECT DISTINCT


                                                                        TxnLevel_OfferId  as Offer_Id
                                                                        ,Points.VALUE:programName::string as Points_Program_Nm
                                                                        ,Points.VALUE:burn::string as Points_Burned_Nbr
                                                                        ,Points.VALUE:earn::string as Points_Earned_Nbr
                                                                        ,Points.VALUE:scoreCardText::string as Scorecard_Txt

									,TERMINALNUMBER
                                                                        ,try_to_numeric(TRANSACTIONNUMBER) as TRANSACTIONNUMBER
																	  , CASE WHEN TRANSACTIONSOURCE = 'STORE' THEN to_timestamp_tz(CASE
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
				END,STORETIMEZONE) END as TRANSACTIONTIMESTAMP
	,case
when  (STRTOK( UpdatedDate,'+',2)<>'' and contains(STRTOK( UpdatedDate,'+',2),':')= false ) or
(STRTOK( UpdatedDate,'-',4) <>''  and contains(STRTOK( UpdatedDate,'-',4),':')= false)  then to_timestamp_ltz(UpdatedDate,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(UpdatedDate)
end as UpdatedDate



                                                                        ,filename
FROM ${src_wrk_tbl}
,LATERAL FLATTEN(input => TxnLevel_Points, outer => TRUE) as Points
Union All
Select Distinct

                                                                Offer_Id
                                                                ,Points_Program_Nm
                                                                ,Points_Burned_Nbr
                                                                ,Points_Earned_Nbr
                                                                ,Scorecard_Txt
                                                                ,TERMINALNUMBER
                                                                ,TRANSACTIONNUMBER
                                                                ,TRANSACTIONTIMESTAMP
                                                                ,cast(UpdatedDate as varchar)

                                                                ,filename
FROM ${tgt_exp_tbl}
                            )
                       )
select
 src.Transaction_Integration_Id
,src.Offer_Id
,src.Points_Program_Nm
,src.Points_Burned_Nbr
,src.Points_Earned_Nbr
,src.Scorecard_Txt
,src.TERMINALNUMBER
,src.TRANSACTIONNUMBER
,src.TRANSACTIONTIMESTAMP
,src.DW_Logical_delete_ind
,src.UpdatedDate
,src.filename
,CASE WHEN tgt.Transaction_Integration_Id IS NULL  AND tgt.Offer_Id IS NULL AND tgt.Points_Program_Nm IS NULL THEN 'I' ELSE 'U' END AS DML_Type
,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
from
(
select
 LKP_EPE_Transaction_Header.Transaction_Integration_Id AS Transaction_Integration_Id
,src1.Offer_Id
,src1.Points_Program_Nm
,src1.Points_Burned_Nbr
,src1.Points_Earned_Nbr
,src1.Scorecard_Txt
,src1.TERMINALNUMBER
,src1.TRANSACTIONNUMBER
,src1.TRANSACTIONTIMESTAMP
,src1.DW_Logical_delete_ind
,src1.UpdatedDate
,src1.filename
from
(
select
Offer_Id
,Points_Program_Nm
,Points_Burned_Nbr
,Points_Earned_Nbr
,Scorecard_Txt
,TERMINALNUMBER
,TRANSACTIONNUMBER
,TRANSACTIONTIMESTAMP
,UpdatedDate
,false AS DW_Logical_delete_ind
,Filename
FROM src_wrk_tbl_recs
where rn=1
and Points_Program_Nm is not null
and Offer_Id is not null
) src1
LEFT JOIN
(SELECT DISTINCT Transaction_Integration_Id,
Terminal_Nbr,Transaction_Id,Transaction_Ts,Source_System_Cd
FROM ${lkp_tbl}
WHERE DW_CURRENT_VERSION_IND = TRUE
AND DW_LOGICAL_DELETE_IND = FALSE
) LKP_EPE_Transaction_Header
ON src1.terminalnumber = LKP_EPE_Transaction_Header.Terminal_Nbr
AND src1.TRANSACTIONNUMBER = LKP_EPE_Transaction_Header.Transaction_Id
AND src1.TRANSACTIONTIMESTAMP = LKP_EPE_Transaction_Header.Transaction_Ts
)src
LEFT JOIN (SELECT
tgt.Transaction_Integration_Id
,tgt.Offer_Id
,tgt.Points_Program_Nm
,tgt.Points_Burned_Nbr
,tgt.Points_Earned_Nbr
,tgt.Scorecard_Txt
,tgt.dw_logical_delete_ind
,tgt.dw_first_effective_dt
FROM ${tgt_tbl} tgt
WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
) as tgt
ON tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
AND tgt.Offer_Id = src.Offer_Id
AND tgt.Points_Program_Nm = src.Points_Program_Nm

where (tgt.Transaction_Integration_Id IS NULL  AND tgt.Offer_Id IS NULL  AND tgt.Points_Program_Nm IS NULL)
OR (
NVL(src.Points_Burned_Nbr,'-1') <> NVL(tgt.Points_Burned_Nbr,'-1')
                                                        OR NVL(src.Points_Earned_Nbr,'-1') <> NVL(tgt.Points_Earned_Nbr,'-1')
                                                        OR NVL(src.Scorecard_Txt,'-1') <> NVL(tgt.Scorecard_Txt,'-1')

OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
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
// Processing Updates of Type 2 SCD

var sql_updates =
`UPDATE ${tgt_tbl} as tgt
 SET DW_Last_Effective_dt = CURRENT_DATE-1
,DW_CURRENT_VERSION_IND = FALSE
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = FileName
FROM ( SELECT
 Transaction_Integration_Id
,Offer_Id
,Points_Program_Nm
,FileName
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 0
AND Transaction_Integration_Id  IS NOT NULL
AND Offer_Id IS NOT NULL
AND Points_Program_Nm IS NOT NULL
) src
WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
AND tgt.Offer_Id = src.Offer_Id
AND tgt.Points_Program_Nm = src.Points_Program_Nm
AND tgt.DW_CURRENT_VERSION_IND = TRUE
AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;


// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
SET
     Points_Burned_Nbr = src.Points_Burned_Nbr
    ,Points_Earned_Nbr = src.Points_Earned_Nbr
    ,Scorecard_Txt  = src.Scorecard_Txt
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = FileName
FROM ( SELECT
Transaction_Integration_Id
,Offer_Id
,Points_Program_Nm
,Points_Burned_Nbr
,Points_Earned_Nbr
,Scorecard_Txt
,DW_Logical_delete_ind
,UpdatedDate
,FileName
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 1
AND Transaction_Integration_Id  IS NOT NULL
AND Offer_Id IS NOT NULL
AND Points_Program_Nm IS NOT NULL
) src
WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
AND tgt.Offer_Id = src.Offer_Id
AND tgt.Points_Program_Nm = src.Points_Program_Nm
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
(Transaction_Integration_Id ,
Offer_Id,
Points_Program_Nm,
DW_Last_Effective_Dt ,
DW_First_Effective_Dt ,
Points_Burned_Nbr,
Points_Earned_Nbr ,
Scorecard_Txt,
DW_CREATE_TS ,
DW_LOGICAL_DELETE_IND ,
DW_SOURCE_CREATE_NM ,
DW_CURRENT_VERSION_IND
)
   SELECT DISTINCT
Transaction_Integration_Id
,Offer_Id
,Points_Program_Nm
,'31-DEC-9999'
,CURRENT_DATE
,Points_Burned_Nbr
,Points_Earned_Nbr
,Scorecard_Txt
,CURRENT_TIMESTAMP
,DW_Logical_delete_ind
,FileName
,TRUE
FROM ${tgt_wrk_tbl}
WHERE Sameday_chg_ind = 0
AND Transaction_Integration_Id  IS NOT NULL
AND Offer_Id IS NOT NULL
AND Points_Program_Nm IS NOT NULL
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
 var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;

var sql_exceptions = `INSERT INTO  ${tgt_exp_tbl}
select Distinct
Transaction_Integration_Id,
Offer_Id ,
Points_Program_Nm,
Points_Burned_Nbr,
Points_Earned_Nbr,
Scorecard_Txt,
TERMINALNUMBER,
TRANSACTIONNUMBER,
UpdatedDate,
FileName,
DML_Type,
Sameday_chg_ind,
CASE WHEN Transaction_Integration_Id is NULL THEN 'Transaction_Integration_Id is NULL'
     WHEN Offer_Id IS NULL THEN 'Offer_Id IS NULL'
     WHEN Points_Program_Nm IS NULL THEN 'Points_Program_Nm IS NULL'
END AS Exception_Reason,
CURRENT_TIMESTAMP AS DW_CREATE_TS,
TO_TIMESTAMP_NTZ (TRANSACTIONTIMESTAMP) AS TRANSACTIONTIMESTAMP
FROM  ${tgt_wrk_tbl}
WHERE Transaction_Integration_Id IS NULL
or Offer_Id IS NULL
or Points_Program_Nm IS NULL
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


// ************** Load for EPE_Transaction_Header_Saving_Points table ENDs *****************

$$;
