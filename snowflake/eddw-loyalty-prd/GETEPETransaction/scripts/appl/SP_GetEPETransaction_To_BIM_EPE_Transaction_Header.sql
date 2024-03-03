

var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_RETAIL;
var src_wrk_tbl = SRC_WRK_TBL;
var tgt_wrk_tbl = `${CNF_DB}.${wrk_schema}.EPE_Transaction_Header_wrk`;
var tgt_tbl = `${CNF_DB}.${cnf_schema}.EPE_Transaction_Header`;
var tgt_exp_tbl = `${CNF_DB}.${wrk_schema}.EPE_Transaction_Header_EXCEPTIONS`;

// ************** Load for  EPE_Transaction_Header table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_command = `Create or replace table  ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs
AS (
	SELECT DISTINCT
		case when Source_system_cd = 'ECOM' then
												CAST(replace(replace(replace(replace(concat(
													left(concat(try_to_numeric(Transaction_Id),
													NVL(CAST(Terminal_Nbr as varchar),'0')),21),
													NVL(left(cast(Transaction_Ts as varchar),23),'0')),'-',''),' ',''),':',''),'.','') AS NUMBER
       )
            when Source_system_cd = 'STORE' then try_to_numeric(order_id) end as TRANSACTION_INTEGRATION_ID
	   ,Store_Nbr
       ,Register_Transaction_Sequence_Nbr
		,Terminal_Nbr
		,Transaction_Id
		,Transaction_Ts
		,household_id
		,Status_Cd
		,Create_Dt
		,Source_System_Cd
		,Total_Card_Savings_Amt
		,Transaction_Total_Amt
		,UpdatedDate
        ,order_id
		,Filename
		,FULFILLMENT_STORE_NBR
		,Row_number() OVER (
			PARTITION BY TRANSACTION_INTEGRATION_ID ORDER BY To_timestamp_ntz(UpdatedDate) DESC
			) AS rn
	FROM (
		SELECT DISTINCT
		transactionUniqueID
		,storenumber as Store_Nbr
		,try_to_numeric(TRANSACTIONNUMBER) as Register_Transaction_Sequence_Nbr
		,TERMINALNUMBER AS Terminal_Nbr
			,try_to_numeric(TRANSACTIONNUMBER) AS Transaction_Id
			,CASE WHEN TRANSACTIONSOURCE = 'STORE' THEN to_timestamp_tz(CASE
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
				END,STORETIMEZONE) END AS Transaction_Ts
			,
			--   to_timestamp_ntz(transactiontimestamp,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM') as  Transaction_Ts,
			--              try_to_timestamp(transactiontimestamp) as Transaction_Ts,
			-- to_timestamp_ntz(transactiontimestamp) as  Transaction_Ts,
			case when TRANSACTION_MEMBERID= '' then NULL else TRANSACTION_MEMBERID end AS Household_id
			,TRANSACTION_STATUS AS Status_Cd
			,CASE
				WHEN (
						STRTOK(createddate, '+', 2) <> ''
						AND CONTAINS (
							STRTOK(createddate, '+', 2)
							,':'
							) = false
						)
					OR (
						CONTAINS (
							createddate
							,'T'
							) = true
						AND STRTOK(createddate, '-', 4) <> ''
						AND CONTAINS (
							STRTOK(createddate, '-', 4)
							,':'
							) = false
						)
					THEN to_timestamp_ltz(createddate, 'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
				ELSE to_timestamp_ltz(createddate)
				END AS create_Dt
			,TRANSACTIONSOURCE AS Source_System_Cd
			,TOTALCARDSAVINGS AS Total_Card_Savings_Amt
			,TRANSACTIONTOTAL AS Transaction_Total_Amt

			,CASE
				WHEN (
						STRTOK(UpdatedDate, '+', 2) <> ''
						AND CONTAINS (
							STRTOK(UpdatedDate, '+', 2)
							,':'
							) = false
						)
					OR (
						CONTAINS (
							UpdatedDate
							,'T'
							) = true
						AND STRTOK(UpdatedDate, '-', 4) <> ''
						AND CONTAINS (
							STRTOK(UpdatedDate, '-', 4)
							,':'
							) = false
						)
					THEN to_timestamp_ltz(UpdatedDate, 'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
				ELSE to_timestamp_ltz(UpdatedDate)
				END AS UpdatedDate
				,Case
                 When Source_system_cd = 'ECOM' then transactionnumber
                 When Source_system_cd = 'STORE' then transactionUniqueID
                 else null
                 End as order_id
			,Filename ,STORETIMEZONE,
			fulfillmentStoreNumber as FULFILLMENT_STORE_NBR

		FROM ${src_wrk_tbl}
		/*
		UNION ALL

		SELECT DISTINCT
			TRANSACTION_INTEGRATION_ID as transactionUniqueID
			,Store_Nbr
			,Register_Transaction_Sequence_Nbr
		    ,Terminal_Nbr
			,Transaction_Id
			,Transaction_Ts
			,household_id
			,Status_Cd
			,Create_Dt
			,Source_System_Cd
			,Total_Card_Savings_Amt
			,Transaction_Total_Amt

			,cast(UpdatedDate AS VARCHAR)
            ,order_id
			,Filename ,NULL AS STORETIMEZONE, FULFILLMENT_STORE_NBR
		FROM ${tgt_exp_tbl}  */
		)
        )


		SELECT Distinct src.TRANSACTION_INTEGRATION_ID
			,src.Store_Nbr
			,src.Register_Transaction_Sequence_Nbr
			,src.Terminal_Nbr
			,src.Transaction_Id
			,src.Transaction_Ts
			,src.order_id
			,src.household_id
			,src.Status_Cd
			,src.Create_Dt
			,src.Source_System_Cd
			,src.Total_Card_Savings_Amt
			,src.Transaction_Total_Amt
			,src.UpdatedDate
			,src.filename
			,src.DW_Logical_delete_ind
			,CASE
				WHEN tgt.TRANSACTION_INTEGRATION_ID IS NULL
					THEN 'I'
				ELSE 'U'
				END AS DML_Type
			,CASE
				WHEN tgt.DW_First_Effective_dt = CURRENT_DATE
					THEN 1
				ELSE 0
				END AS Sameday_chg_ind
			,src.FULFILLMENT_STORE_NBR
		FROM (

		                                        SELECT
					                             TRANSACTION_INTEGRATION_ID
												 ,Store_Nbr
												 ,Register_Transaction_Sequence_Nbr
					                             ,Terminal_Nbr
					                             ,Transaction_Id
					                             ,Transaction_Ts
												 ,order_id
					                             ,household_id
					                             ,Status_Cd
					                             ,Create_Dt
					                             ,Source_System_Cd
					                             ,Total_Card_Savings_Amt
					                             ,Transaction_Total_Amt
					                             ,FALSE AS DW_Logical_delete_ind
					                             ,UpdatedDate
					                             ,Filename
												 ,FULFILLMENT_STORE_NBR
				                                  FROM src_wrk_tbl_recs
												  where rn=1



				) as src
				LEFT JOIN
				(
			   SELECT
			     tgt.TRANSACTION_INTEGRATION_ID
				 ,tgt.Store_Nbr
				 ,tgt.Register_Transaction_Sequence_Nbr
				,tgt.Terminal_Nbr
				,tgt.Transaction_Id
				,tgt.Transaction_Ts
				,tgt.Order_Id
				,tgt.Household_Id
				,tgt.Status_Cd
				,tgt.Create_Dt
				,tgt.Source_System_Cd
				,tgt.Total_Card_Savings_Amt
				,tgt.Transaction_Total_Amt
				,tgt.dw_logical_delete_ind
				,tgt.dw_first_effective_dt
				,tgt.FULFILLMENT_STORE_NBR
			FROM ${tgt_tbl} tgt
			WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
			) AS tgt
			ON src.TRANSACTION_INTEGRATION_ID = tgt.TRANSACTION_INTEGRATION_ID
		    WHERE tgt.TRANSACTION_INTEGRATION_ID IS NULL
			OR (
				NVL(src.Store_Nbr, '-1') <> NVL(tgt.Store_Nbr, '-1')
				OR NVL(src.Register_Transaction_Sequence_Nbr, '-1') <> NVL(tgt.Register_Transaction_Sequence_Nbr, '-1')
				OR NVL(src.Terminal_Nbr, '-1') <> NVL(tgt.Terminal_Nbr, '-1')
				OR NVL(src.Transaction_Id, '-1') <> NVL(tgt.Transaction_Id, '-1')
				OR NVL(src.Transaction_Ts, '9999-12-31 00:00:00.000') <> NVL(tgt.Transaction_Ts, '9999-12-31 00:00:00.000')
				OR NVL(src.Order_Id, '-1') <> NVL(tgt.Order_Id, '-1')
				OR NVL(src.Household_Id, '-1') <> NVL(tgt.Household_Id, '-1')
				OR NVL(src.Status_Cd, '-1') <> NVL(tgt.Status_Cd, '-1')
				OR NVL(src.Create_Dt, '9999-12-31') <> NVL(tgt.Create_Dt, '9999-12-31')
				OR NVL(src.Source_System_Cd, '-1') <> NVL(tgt.Source_System_Cd, '-1')
				OR NVL(src.Total_Card_Savings_Amt, '-1') <> NVL(tgt.Total_Card_Savings_Amt, '-1')
				OR NVL(src.Transaction_Total_Amt, '-1') <> NVL(tgt.Transaction_Total_Amt, '-1')
				OR NVL(src.FULFILLMENT_STORE_NBR,'-1') <> NVL(tgt.FULFILLMENT_STORE_NBR, '-1')
				OR src.DW_LOGICAL_DELETE_IND <> tgt.DW_LOGICAL_DELETE_IND
					)

 `;
try {

        snowflake.execute ({sqlText: sql_command});
        }
    catch (err)  {
        return "Creation of EPE_Transaction_Header_wrk "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

 var sql_begin = "BEGIN"


/// SCD Type2 - Processing Different day updates
              var sql_updates = `UPDATE ${tgt_tbl} as tgt
              SET
 DW_Last_Effective_dt = CURRENT_DATE-1
,DW_CURRENT_VERSION_IND = FALSE
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = FileName
 FROM (
                             SELECT
                                           TRANSACTION_INTEGRATION_ID
                                          ,filename
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U'
                             AND Sameday_chg_ind = 0
                             AND TRANSACTION_INTEGRATION_ID is not NULL

                             ) src
                             WHERE tgt.TRANSACTION_INTEGRATION_ID = src.TRANSACTION_INTEGRATION_ID

							 AND tgt.DW_CURRENT_VERSION_IND = TRUE
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;




// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET       Store_Nbr               = src.Store_Nbr
							  ,Register_Transaction_Sequence_Nbr = src.Register_Transaction_Sequence_Nbr
							  ,Terminal_Nbr                 = src.Terminal_Nbr
							  ,Transaction_Id           = src.Transaction_Id
							  ,Transaction_Ts               = src.Transaction_Ts
							  ,Order_Id                    = src.Order_Id
							  ,Household_Id              = src.Household_Id
							  ,Status_Cd           = src.Status_Cd
							  ,Create_Dt                   = src.Create_Dt
							  ,Source_System_Cd                   = src.Source_System_Cd
							  ,Total_Card_Savings_Amt         = src.Total_Card_Savings_Amt
                              ,Transaction_Total_Amt           = src.Transaction_Total_Amt

					   ,DW_Logical_delete_ind 			   = src.DW_Logical_delete_ind
					   ,DW_LAST_UPDATE_TS                  = CURRENT_TIMESTAMP
					   ,DW_SOURCE_UPDATE_NM                = FileName
					   ,FULFILLMENT_STORE_NBR              = src.FULFILLMENT_STORE_NBR
					   FROM ( SELECT
								  TRANSACTION_INTEGRATION_ID
								  ,Store_Nbr
								  ,Register_Transaction_Sequence_Nbr
                                 ,Terminal_Nbr
                                 ,Transaction_Id
                                 ,TO_TIMESTAMP_TZ(Transaction_Ts) AS Transaction_Ts
                                 ,Order_Id
                                 ,Household_Id
                                 ,Status_Cd
                                 ,Create_Dt
                                 ,Source_System_Cd
                                 ,Total_Card_Savings_Amt
                                 ,Transaction_Total_Amt
                                 ,CURRENT_DATE
                                 ,'31-DEC-9999'
                                 ,CURRENT_TIMESTAMP
								 ,DW_Logical_delete_ind
                                 ,FileName
								 ,FULFILLMENT_STORE_NBR
							   FROM ${tgt_wrk_tbl}
							   WHERE DML_Type = 'U'
							   AND Sameday_chg_ind = 1
							   AND TRANSACTION_INTEGRATION_ID IS NOT NULL

							) src
							WHERE tgt.TRANSACTION_INTEGRATION_ID = src.TRANSACTION_INTEGRATION_ID
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
(                                TRANSACTION_INTEGRATION_ID

                                 ,Terminal_Nbr
                                 ,Transaction_Id
                                 ,Transaction_Ts
                                 ,Order_Id
                                 ,Household_Id
                                 ,Status_Cd
                                 ,Create_Dt
                                 ,Source_System_Cd
                                 ,Total_Card_Savings_Amt
                                 ,Transaction_Total_Amt
                                 ,DW_First_Effective_Dt
                                 ,DW_Last_Effective_Dt
                                 ,DW_CREATE_TS
                                 ,DW_LOGICAL_DELETE_IND
                                 ,DW_SOURCE_CREATE_NM
                                 ,DW_CURRENT_VERSION_IND
								 ,Store_Nbr
								 ,Register_Transaction_Sequence_Nbr
								 ,FULFILLMENT_STORE_NBR
)
   SELECT DISTINCT
TRANSACTION_INTEGRATION_ID
,Terminal_Nbr
,Transaction_Id
,TO_TIMESTAMP_TZ(Transaction_Ts) AS Transaction_Ts
,Order_Id
,Household_Id
,Status_Cd
,Create_Dt
,Source_System_Cd
,Total_Card_Savings_Amt
,Transaction_Total_Amt
,CURRENT_DATE as DW_First_Effective_dt
,'31-DEC-9999'
,CURRENT_TIMESTAMP
,DW_Logical_delete_ind
,FileName
,TRUE
,Store_Nbr
,Register_Transaction_Sequence_Nbr
,FULFILLMENT_STORE_NBR
FROM ${tgt_wrk_tbl}
WHERE TRANSACTION_INTEGRATION_ID  IS NOT NULL
AND Sameday_chg_ind = 0
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
		return `Loading of EPE_Transaction_Header table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }


 /*var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;*/

var sql_exceptions = `INSERT INTO  ${tgt_exp_tbl} (TRANSACTION_INTEGRATION_ID
    ,Terminal_Nbr
,Transaction_Id
,Transaction_Ts
,order_id
,household_id
,Status_Cd
,Create_Dt
,Source_System_Cd
,Total_Card_Savings_Amt
,Transaction_Total_Amt
,UpdatedDate
,Filename
,DML_Type
,Sameday_chg_ind
,Exception_Reason
,DW_CREATE_TS
   ,Store_Nbr
   ,Register_Transaction_Sequence_Nbr
   ,FULFILLMENT_STORE_NBR)
							select Distinct
   TRANSACTION_INTEGRATION_ID
    ,Terminal_Nbr
,Transaction_Id
,TO_TIMESTAMP_NTZ (Transaction_Ts) AS Transaction_Ts
,order_id
,household_id
,Status_Cd
,Create_Dt
,Source_System_Cd
,Total_Card_Savings_Amt
,Transaction_Total_Amt
,UpdatedDate
,Filename
,DML_Type
,Sameday_chg_ind
,CASE WHEN TRANSACTION_INTEGRATION_ID is NULL THEN 'TRANSACTION_INTEGRATION_ID is NULL'
END AS Exception_Reason,
   CURRENT_TIMESTAMP AS DW_CREATE_TS,
   Store_Nbr,
   Register_Transaction_Sequence_Nbr,
   FULFILLMENT_STORE_NBR

FROM  ${tgt_wrk_tbl}
WHERE  TRANSACTION_INTEGRATION_ID IS NULL
`;



              try
              {
                     snowflake.execute (
                     {sqlText: sql_begin }
                     );
                     /*snowflake.execute(
                     {sqlText: truncate_exceptions}
                     );*/
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

// ************** Load for EPE_Transaction_Header table ENDs *****************
