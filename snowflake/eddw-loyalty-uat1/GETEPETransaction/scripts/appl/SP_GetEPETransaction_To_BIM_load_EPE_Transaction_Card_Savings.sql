--liquibase formatted sql
--changeset SYSTEM:SP_GetEPETransaction_To_BIM_load_EPE_Transaction_Card_Savings runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETEPETRANSACTION_TO_BIM_LOAD_EPE_TRANSACTION_CARD_SAVINGS
("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_RETAIL" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
var cnf_db = CNF_DB ;
var src_wrk_tbl = SRC_WRK_TBL;
var cnf_schema = C_RETAIL;
var wrk_schema = C_STAGE;
var lkp_schema = C_RETAIL;

var tgt_wrk_tbl = `${CNF_DB}.${wrk_schema}.EPE_Transaction_Card_Savings_wrk`;
var tgt_tbl = `${CNF_DB}.${cnf_schema}.EPE_Transaction_Card_Savings`;
var lkp_tb1 =`${cnf_db}.${lkp_schema}.EPE_Transaction_Header`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Card_Savings_Exceptions`;

// **************	Load for EPE_Transaction_Card_Savings table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.



var sql_command = `Create or replace table  ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT
						Savings_Category_Id,
						Savings_Category_Nm,
						Savings_Amt,
                        TERMINALNUMBER,
                        TRANSACTIONNUMBER,
                        TRANSACTIONTIMESTAMP,
					    updateddate,
						filename,

						Row_number() OVER ( partition BY TERMINALNUMBER,TRANSACTIONNUMBER,TRANSACTIONTIMESTAMP,Savings_Category_Id ORDER BY
					    To_timestamp_ntz(updateddate) DESC) AS rn
				from

				  (

                            	SELECT DISTINCT
								CARDSAVINGS_SAVINGSCATEGORYID AS Savings_Category_Id,
                                CARDSAVINGS_SAVINGSCATEGORYNAME AS Savings_Category_Nm,
                                CARDSAVINGS_SAVINGSAMOUNT AS Savings_Amt,
                                TERMINALNUMBER,
								try_to_numeric(TRANSACTIONNUMBER) as TRANSACTIONNUMBER,
								--TRANSACTIONTIMESTAMP,
								CASE WHEN TRANSACTIONSOURCE = 'STORE' THEN to_timestamp_tz(CASE
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
,
								--updatedDate,
								case
when  (STRTOK( UpdatedDate,'+',2)<>'' and contains(STRTOK( UpdatedDate,'+',2),':')= false ) or
(contains(UpdatedDate,'T')=true and STRTOK(UpdatedDate,'-',4) <>'' and contains(STRTOK( UpdatedDate,'-',4),':')= false)
 then to_timestamp_ltz(UpdatedDate,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(UpdatedDate)
end as UpdatedDate ,



								filename as filename

						FROM ${src_wrk_tbl}



					UNION ALL

						select DISTINCT
						Savings_Category_Id,
						Savings_Category_Nm,
						Savings_Amt,
                                                TERMINALNUMBER,
						TRANSACTIONNUMBER,
						TRANSACTIONTIMESTAMP,
						cast(updateddate as varchar),
						filename
						FROM ${tgt_exp_tbl}
                          )
                       )
			SELECT
						        src.Transaction_Integration_Id,
							src.Savings_Category_Id,
							src.Savings_Category_Nm,
							src.Savings_Amt,
                                                        src.TERMINALNUMBER,
							src.TRANSACTIONNUMBER,
							src.TRANSACTIONTIMESTAMP,
							src.updatedDate,
							src.DW_Logical_delete_ind,
							src.filename,
                               CASE WHEN (tgt.Transaction_Integration_Id is NULL AND tgt.Savings_Category_Id IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
                               ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
				from
                          		(
						select
						 LKP_EPE_Transaction_Header.Transaction_Integration_Id AS Transaction_Integration_Id,
						 src1.Savings_Category_Id,
							src1.Savings_Category_Nm,
							src1.Savings_Amt,
							src1.TERMINALNUMBER,
							src1.TRANSACTIONNUMBER,
							src1.TRANSACTIONTIMESTAMP,
							src1.updatedDate,
							src1.DW_Logical_delete_ind,
							src1.filename
						from
							(
							select distinct
							Savings_Category_Id,
							Savings_Category_Nm,
							Savings_Amt,
							TERMINALNUMBER,
							TRANSACTIONNUMBER,
							TRANSACTIONTIMESTAMP,
							updatedDate,
							FALSE AS DW_Logical_delete_ind,
							filename
							from src_wrk_tbl_recs --src1
							WHERE rn = 1
							--Transaction_Integration_Id is NOT NULL
							AND Savings_Category_Id is not null

						)src1

						LEFT JOIN
							(SELECT DISTINCT  Transaction_Integration_Id,Terminal_Nbr,Transaction_Id,Transaction_Ts,Source_System_Cd
							 FROM ${lkp_tb1}
							 WHERE  DW_CURRENT_VERSION_IND = TRUE
							 AND DW_LOGICAL_DELETE_IND = FALSE
							) LKP_EPE_Transaction_Header
							 ON src1.TERMINALNUMBER = LKP_EPE_Transaction_Header.Terminal_Nbr
							AND src1.TRANSACTIONNUMBER = LKP_EPE_Transaction_Header.Transaction_Id
							AND src1.TRANSACTIONTIMESTAMP = LKP_EPE_Transaction_Header.Transaction_Ts
							-- AND Source_System_Cd = 'STORE'
							--AND src1.Facility_Integration_ID = LKP_Transaction_HDR.Facility_Integration_ID
						)src

						LEFT JOIN
                          (SELECT  DISTINCT
						        tgt.Transaction_Integration_Id,
							tgt.Savings_Category_Id,
							tgt.Savings_Category_Nm,
							tgt.Savings_Amt,
							tgt.dw_logical_delete_ind,
							tgt.dw_first_effective_dt
                          FROM ${tgt_tbl} tgt
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt
                          ON 			tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
						AND tgt.Savings_Category_Id = src.Savings_Category_Id

						  WHERE  (tgt.Transaction_Integration_Id  IS NULL
							and tgt.Savings_Category_Id is null )
					or(
						NVL(src.Savings_Category_Nm,'-1') <> NVL(tgt.Savings_Category_Nm,'-1')
                                                or NVL(src.Savings_Amt,'-1') <> NVL(tgt.Savings_Amt,'-1')
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )  `;

try {


        snowflake.execute ({sqlText: sql_command});

        }
    catch (err)  {
        return "Creation of EPE_Transaction_Card_Savings work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
                                           Transaction_Integration_Id,
					   Savings_Category_Id,
					   filename
FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U'
                             AND Sameday_chg_ind = 0
                             AND 	Transaction_Integration_Id  IS NOT NULL
					AND Savings_Category_Id is not null

				) src
                             WHERE 			tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
							AND tgt.Savings_Category_Id = src.Savings_Category_Id
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE
							 --AND tgt.DW_LOGICAL_DELETE_IND = FALSE
							`;
// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET Savings_Category_Nm = src.Savings_Category_Nm,
                                        Savings_Amt = src.Savings_Amt,
					DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					DW_LAST_UPDATE_TS	= CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM   = FileName
					FROM ( SELECT Transaction_Integration_Id,
								     Savings_Category_Id,
								     Savings_Category_Nm,
								     Savings_Amt,
								     updatedDate,
								     DW_Logical_delete_ind,
								     filename

							   FROM ${tgt_wrk_tbl}
							   WHERE DML_Type = 'U'
							   AND Sameday_chg_ind = 1
							   AND Transaction_Integration_Id  IS NOT NULL
							   AND Savings_Category_Id IS NOT NULL
							) src
							WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
						AND tgt.Savings_Category_Id = src.Savings_Category_Id
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                                        Transaction_Integration_Id,
				        Savings_Category_Id,
					DW_First_Effective_Dt,
                    			DW_Last_Effective_Dt,
					Savings_Category_Nm,
                                        Savings_Amt,
                    			DW_CREATE_TS,
                    			DW_LOGICAL_DELETE_IND,
                    			DW_SOURCE_CREATE_NM,
                    			DW_CURRENT_VERSION_IND
                   )
SELECT DISTINCT				Transaction_Integration_Id,
                      			Savings_Category_Id,
                                        CURRENT_DATE,
                                        '31-DEC-9999' ,
					Savings_Category_Nm,
					Savings_Amt,
					CURRENT_TIMESTAMP,
                     			DW_Logical_delete_ind,
                     			FileName,
                     			TRUE
				FROM ${tgt_wrk_tbl}
                	where 	Transaction_Integration_Id  IS NOT NULL
				and Savings_Category_Id is not null
                and Sameday_chg_ind = 0`;

var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
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

var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;

var sql_exceptions = `INSERT INTO  ${tgt_exp_tbl}
					select Transaction_Integration_Id,
								    	Savings_Category_Id,
									Savings_Category_Nm,
									Savings_Amt,
									TERMINALNUMBER,
									TRANSACTIONNUMBER,
									updatedDate,
									FileName,

									DML_Type,
									Sameday_chg_ind,
									CASE WHEN Transaction_Integration_Id is NULL THEN 'Transaction_Integration_Id is NULL'
									WHEN Savings_Category_Id is NULL THEN 'Savings_Category_Id  is NULL'
									END AS Exception_Reason,
								    	CURRENT_TIMESTAMP AS DW_CREATE_TS,
									TO_TIMESTAMP_NTZ (TRANSACTIONTIMESTAMP) AS TRANSACTIONTIMESTAMP
									FROM  ${tgt_wrk_tbl}
									WHERE Transaction_Integration_Id IS NULL
									or Savings_Category_Id is null
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


// ************** Load for EPE_Transaction_Card_Savings table ENDs *****************


$$;
