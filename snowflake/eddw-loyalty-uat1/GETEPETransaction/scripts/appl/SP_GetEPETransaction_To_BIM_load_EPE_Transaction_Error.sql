--liquibase formatted sql
--changeset SYSTEM:SP_GetEPETransaction_To_BIM_load_EPE_Transaction_Error runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETEPETRANSACTION_TO_BIM_LOAD_EPE_TRANSACTION_ERROR
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

var tgt_wrk_tbl = `${CNF_DB}.${wrk_schema}.EPE_Transaction_Error_wrk`;
var tgt_tbl = `${CNF_DB}.${cnf_schema}.EPE_Transaction_Error`;
var lkp_tb1 =`${cnf_db}.${lkp_schema}.EPE_Transaction_Header`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Error_Exceptions`;

// **************	Load for EPE_Transaction_Error table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.




var sql_command = `Create or replace transient table  ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT
						UPC_Nbr,
						Item_Sequence_Id,
						Pricing_Error_Cd,
						Pricing_Error_Dsc,
						TERMINALNUMBER,
						TRANSACTIONNUMBER,
						TRANSACTIONTIMESTAMP,
						UpdatedDate,
						filename,

						Row_number() OVER ( partition BY TERMINALNUMBER,TRANSACTIONNUMBER,TRANSACTIONTIMESTAMP,UPC_Nbr,Item_Sequence_Id, Pricing_Error_Cd ORDER BY
						To_timestamp_ntz(UpdatedDate) desc) AS rn
				from
                            	(
                                  (
                            	SELECT DISTINCT
								Items_itemcode as UPC_Nbr,
								ITEMS_ENTRYID as Item_Sequence_Id,
								Epe_Errors.value:epeErrorCode::string AS Pricing_Error_Cd,
								Epe_Errors.value:epeErrorDesc::string AS Pricing_Error_Dsc,
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
								--UpdatedDate,

                             case
when  (STRTOK( UpdatedDate,'+',2)<>'' and contains(STRTOK( UpdatedDate,'+',2),':')= false ) or
(contains(UpdatedDate,'T')=true and STRTOK(UpdatedDate,'-',4) <>'' and contains(STRTOK( UpdatedDate,'-',4),':')= false)
 then to_timestamp_ltz(UpdatedDate,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(UpdatedDate)
end as UpdatedDate ,



								filename as filename


						FROM ${src_wrk_tbl}
						,LATERAL FLATTEN(input => Items_EpeErrors, outer => TRUE) AS Epe_Errors)


					UNION ALL
						select DISTINCT
						 UPC_Nbr,
						 Item_Sequence_Id,
						Pricing_Error_Cd,
						Pricing_Error_Dsc,
						TERMINALNUMBER,
						TRANSACTIONNUMBER,
						TRANSACTIONTIMESTAMP,
						cast(UpdatedDate as varchar),
						filename

						FROM ${tgt_exp_tbl}
                          )      )
			SELECT
						        src.Transaction_Integration_Id,
							src.UPC_Nbr,
							src.Item_Sequence_Id,
							src.Pricing_Error_Cd,
							src.Pricing_Error_Dsc,
							src.TERMINALNUMBER,
							src.TRANSACTIONNUMBER,
							src.TRANSACTIONTIMESTAMP,
							src.UpdatedDate,
							src.DW_Logical_delete_ind,
							src.filename,


                               CASE WHEN (tgt.Transaction_Integration_Id is NULL AND tgt.UPC_Nbr IS NULL AND tgt.Item_Sequence_Id IS NULL AND tgt.Pricing_Error_Cd IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
                               ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
				from
                          		(
						select
						 LKP_EPE_Transaction_Header.Transaction_Integration_Id AS Transaction_Integration_Id,
						 src1.UPC_Nbr,
						 src1.Item_Sequence_Id,
							src1.Pricing_Error_Cd,
							src1.Pricing_Error_Dsc,
							src1.TERMINALNUMBER,
							src1.TRANSACTIONNUMBER,
							src1.TRANSACTIONTIMESTAMP,
							src1.UpdatedDate,
							src1.DW_Logical_delete_ind,
							src1.filename


						from
							(
							select distinct
							UPC_Nbr,
							Item_Sequence_Id,
							Pricing_Error_Cd,
							Pricing_Error_Dsc,
							TERMINALNUMBER,
							TRANSACTIONNUMBER,
							TRANSACTIONTIMESTAMP,
							UpdatedDate,
							FALSE AS DW_Logical_delete_ind,
							filename


							from src_wrk_tbl_recs --src1
							WHERE rn = 1
							--Transaction_Integration_Id is NOT NULL
							AND UPC_Nbr is not null
							AND Item_Sequence_Id is not null
							AND Pricing_Error_Cd is not null
						)src1

						LEFT JOIN
							(SELECT DISTINCT Transaction_Integration_Id,Terminal_Nbr,Transaction_Id,Transaction_Ts,Source_System_Cd
							 FROM ${lkp_tb1}
							 WHERE DW_CURRENT_VERSION_IND = TRUE
							 AND DW_LOGICAL_DELETE_IND = FALSE
							) LKP_EPE_Transaction_Header
							 ON src1.TERMINALNUMBER = LKP_EPE_Transaction_Header.Terminal_Nbr
							AND src1.TRANSACTIONNUMBER = LKP_EPE_Transaction_Header.Transaction_Id
							AND src1.TRANSACTIONTIMESTAMP = LKP_EPE_Transaction_Header.Transaction_Ts
							--AND Source_System_Cd = 'STORE'
							--AND src1.Facility_Integration_ID = LKP_Transaction_HDR.Facility_Integration_ID
						)src

						LEFT JOIN
                          (SELECT  DISTINCT
						        tgt.Transaction_Integration_Id,
							tgt.UPC_Nbr,
							tgt.Item_Sequence_Id,
							tgt.Pricing_Error_Cd,
							tgt.Pricing_Error_Dsc,
							tgt.dw_logical_delete_ind,
							tgt.dw_first_effective_dt
                          FROM ${tgt_tbl} tgt
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt
                          ON 			tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
						AND tgt.UPC_Nbr = src.UPC_Nbr
						AND tgt.Item_Sequence_Id= src.Item_Sequence_Id
						AND tgt.Pricing_Error_Cd = src.Pricing_Error_Cd
						  WHERE  (tgt.Transaction_Integration_Id  IS NULL
							and tgt.UPC_Nbr is null
							and tgt.Item_Sequence_Id is null
							and tgt.Pricing_Error_Cd is null)
					or(
						NVL(src.Pricing_Error_Dsc,'-1') <> NVL(tgt.Pricing_Error_Dsc,'-1')
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )  `;

try {


        snowflake.execute ({sqlText: sql_command});

        }
    catch (err)  {
        return "creation of EPE_Transaction_Error work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
					   UPC_Nbr,
					   Item_Sequence_Id,
					   Pricing_Error_Cd,
					   filename
FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U'
                             AND Sameday_chg_ind = 0
                             AND 	Transaction_Integration_Id  IS NOT NULL
					AND UPC_Nbr is not null
					AND Item_Sequence_Id is not null
							 AND Pricing_Error_Cd is not null
				) src
                             WHERE 			tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
							AND tgt.UPC_Nbr = src.UPC_Nbr
							AND tgt.Item_Sequence_Id = src.Item_Sequence_Id
							 AND tgt.Pricing_Error_Cd= src.Pricing_Error_Cd
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE
							 --AND tgt.DW_LOGICAL_DELETE_IND = FALSE
							`;
// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET Pricing_Error_Dsc = src.Pricing_Error_Dsc,
					DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					DW_LAST_UPDATE_TS	= CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM   = FileName
					FROM ( SELECT Transaction_Integration_Id,
								     UPC_Nbr,
									 Item_Sequence_Id,
								     Pricing_Error_Cd,
								     Pricing_Error_Dsc,
								     Updateddate,
								     DW_Logical_delete_ind,
								     filename

							   FROM ${tgt_wrk_tbl}
							   WHERE DML_Type = 'U'
							   AND Sameday_chg_ind = 1
							   AND Transaction_Integration_Id  IS NOT NULL
							   AND UPC_Nbr IS NOT NULL
							   AND Item_Sequence_Id IS NOT NULL
							   AND Pricing_Error_Cd IS NOT NULL
							) src
							WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
						AND tgt.UPC_Nbr = src.UPC_Nbr
						AND tgt.Item_Sequence_Id = src.Item_Sequence_Id
					        AND tgt.Pricing_Error_Cd = src.Pricing_Error_Cd
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                                        Transaction_Integration_Id,
				        UPC_Nbr,
						Item_Sequence_Id,
					Pricing_Error_Cd,
					DW_First_Effective_Dt,
                    			DW_Last_Effective_Dt,
					Pricing_Error_Dsc,
                    			DW_CREATE_TS,
                    			DW_LOGICAL_DELETE_IND,
                    			DW_SOURCE_CREATE_NM,
                    			DW_CURRENT_VERSION_IND
                   )
SELECT DISTINCT				Transaction_Integration_Id,
                      			UPC_Nbr,
								Item_Sequence_Id,
					Pricing_Error_Cd,
					CURRENT_DATE,
					'31-DEC-9999' ,
					Pricing_Error_Dsc,
					CURRENT_TIMESTAMP,
                     			DW_Logical_delete_ind,
                     			FileName,
                     			TRUE
				FROM ${tgt_wrk_tbl}
                	where 	Transaction_Integration_Id  IS NOT NULL
				and UPC_Nbr is not null
				and Item_Sequence_Id is not null
				and Pricing_Error_Cd is not null
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
								    	UPC_Nbr,
										Item_Sequence_Id,
									Pricing_Error_Cd,
									Pricing_Error_Dsc,
									TERMINALNUMBER,
									TRANSACTIONNUMBER,
									UpdatedDate,
									FileName,

									DML_Type,
									Sameday_chg_ind,
									CASE WHEN Transaction_Integration_Id is NULL THEN 'Transaction_Integration_Id is NULL'
									WHEN UPC_Nbr is NULL THEN 'UPC_Nbr  is NULL'
									WHEN Item_Sequence_Id is NULL THEN 'Item_Sequence_Id is NULL'
									WHEN Pricing_Error_Cd is NULL THEN 'Pricing_Error_Cd is NULL'
									END AS Exception_Reason,
								    	CURRENT_TIMESTAMP AS DW_CREATE_TS,
									TO_TIMESTAMP_NTZ (TRANSACTIONTIMESTAMP) AS TRANSACTIONTIMESTAMP
									FROM  ${tgt_wrk_tbl}
									WHERE Transaction_Integration_Id IS NULL
									or UPC_Nbr is null
									or Item_Sequence_Id is NULL
									or Pricing_Error_Cd is null
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


// ************** Load for EPE_Transaction_Error table ENDs *****************

$$;
