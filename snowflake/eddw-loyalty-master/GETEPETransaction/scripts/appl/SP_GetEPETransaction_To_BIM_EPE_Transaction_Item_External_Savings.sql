--liquibase formatted sql
--changeset SYSTEM:SP_GetEPETransaction_To_BIM_EPE_Transaction_Item_External_Savings runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema EDM_CONFIRMED_PRD.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETEPETRANSACTION_TO_BIM_EPE_TRANSACTION_ITEM_EXTERNAL_SAVINGS
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

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Item_External_Savings_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.EPE_Transaction_Item_External_Savings`;
var lkp_tb1 =`${cnf_db}.${lkp_schema}.EPE_TRANSACTION_Header`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Item_External_Savings_Exceptions`;


// ************** Load for EPE_Transaction_Item_External_Savings table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.



var sql_command = `Create or replace transient table  ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
			   (SELECT DISTINCT
				  UPC_Nbr,
                  Adjustment_Type_Cd,
				  Item_Sequence_Id,
                  Item_UOM_Cd,
                  Promotion_Cd,
                  Item_Unit_Qty,
                  External_Saving_Amt,
				  TERMINALNUMBER,
				  TRANSACTIONNUMBER,
				  TRANSACTIONTIMESTAMP,
				  UpdatedDate,
				  filename,
				  Row_Number() over(partition by TERMINALNUMBER,TRANSACTIONNUMBER,TRANSACTIONTIMESTAMP,UPC_Nbr,Adjustment_Type_Cd,Item_Sequence_Id
				  ORDER BY To_timestamp_ntz(UpdatedDate) desc) AS rn
				  FROM
				  (
				      SELECT DISTINCT
                        ITEMS_ITEMCODE  As UPC_Nbr,
                         External_Savings.VALUE:adjustmentType::string As Adjustment_Type_Cd,
						 ITEMS_ENTRYID as Item_Sequence_Id ,
                         External_Savings.VALUE:quantityType::string as Item_UOM_Cd,
                         External_Savings.VALUE:promotionCode::string  As Promotion_Cd,
                         External_Savings.VALUE:quantityValue::string as Item_Unit_Qty,
                         External_Savings.VALUE:extendedPrice::string As External_Saving_Amt,
						 TERMINALNUMBER ,
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
                        -- UpdatedDate,
						 case
when  (STRTOK( UpdatedDate,'+',2)<>'' and contains(STRTOK( UpdatedDate,'+',2),':')= false ) or
(contains(UpdatedDate,'T')=true and STRTOK(UpdatedDate,'-',4) <>'' and contains(STRTOK( UpdatedDate,'-',4),':')= false) 
 then to_timestamp_ltz(UpdatedDate,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(UpdatedDate)
end as UpdatedDate ,
							
                         filename
                        FROM  ${src_wrk_tbl}
					   ,LATERAL FLATTEN(input => Items_ExternalSavings, outer => TRUE) as External_Savings
						UNION ALL
						SELECT DISTINCT
						         UPC_Nbr,
							     Adjustment_Type_Cd,
								 Item_Sequence_Id,
                                 Item_UOM_Cd,
								 Promotion_Cd,
								 Item_Unit_Qty,
								 External_Saving_Amt,
								 TERMINALNUMBER,
								 TRANSACTIONNUMBER,
								 TRANSACTIONTIMESTAMP,
								 cast(UpdatedDate as varchar),
							     FileName
						FROM ${tgt_exp_tbl}
						)
			            )
						SELECT
						 src.Transaction_Integration_Id
						,src.UPC_Nbr
						,src.Adjustment_Type_Cd
						,src.Item_Sequence_Id
						,src.Item_UOM_Cd
						,src.Promotion_Cd
						,src.Item_Unit_Qty
						,src.External_Saving_Amt
						,src.TERMINALNUMBER
						,src.TRANSACTIONNUMBER
						,src.TRANSACTIONTIMESTAMP
						,src.UpdatedDate
						,src.DW_Logical_delete_ind
						,src.filename
						,CASE WHEN(tgt.Transaction_Integration_Id is NULL 
						AND tgt.UPC_Nbr is NULL 
						AND tgt.Adjustment_Type_Cd is NULL
						AND tgt.Item_Sequence_Id is NULL) THEN 'I' ELSE 'U' END as DML_Type
						,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
						FROM
					    (
						select
						 LKP_EPE_TRANSACTION_Header.Transaction_Integration_Id AS Transaction_Integration_Id
			            ,src1.UPC_Nbr
						,src1.Adjustment_Type_Cd
						,src1.Item_Sequence_Id
						,src1.Item_UOM_Cd
						,src1.Promotion_Cd
						,src1.Item_Unit_Qty
						,src1.External_Saving_Amt
						,src1.TERMINALNUMBER
						,src1.TRANSACTIONNUMBER
						,src1.TRANSACTIONTIMESTAMP
                        ,src1.UpdatedDate
						,src1.DW_Logical_delete_ind
						,src1.FileName
						from
						(
						SELECT distinct
						UPC_Nbr,
                        Adjustment_Type_Cd,
						Item_Sequence_Id,
                        Item_UOM_Cd,
                        Promotion_Cd,
                        Item_Unit_Qty,
                        External_Saving_Amt,
						TERMINALNUMBER,
						TRANSACTIONNUMBER,
						TRANSACTIONTIMESTAMP,
				        UpdatedDate,
						false AS DW_Logical_delete_ind,
						FileName
						FROM   src_wrk_tbl_recs --src1
						WHERE rn = 1
						AND Adjustment_Type_Cd IS NOT NULL
						AND Item_Sequence_Id IS NOT NULL
						AND UPC_Nbr IS NOT NULL
					    ) src1
						LEFT JOIN
                        (SELECT DISTINCT Transaction_Integration_Id,
							Terminal_Nbr,
						    Transaction_ID,
						    Transaction_Ts,
							Source_System_Cd
						--Facility_Integration_ID
                         FROM ${lkp_tb1}
                         WHERE DW_CURRENT_VERSION_IND = TRUE 
                           and DW_LOGICAL_DELETE_IND = FALSE
                        ) LKP_EPE_Transaction_Header
                         ON src1. TERMINALNUMBER  = LKP_EPE_Transaction_Header.Terminal_Nbr
						 AND src1.TRANSACTIONNUMBER = LKP_EPE_Transaction_Header.Transaction_ID
						 AND src1.TRANSACTIONTIMESTAMP = LKP_EPE_Transaction_Header.Transaction_Ts
						 -- AND Upper(Source_System_Cd) ='STORE'
                        )src
						LEFT JOIN (SELECT distinct
						 tgt.Transaction_Integration_Id
						,tgt.UPC_Nbr
						,tgt.Adjustment_Type_Cd
						,tgt.Item_Sequence_Id
						,tgt.Item_UOM_Cd
						,tgt.Promotion_Cd
						,tgt.Item_Unit_Qty
						,tgt.External_Saving_Amt
						,tgt.dw_logical_delete_ind
						,tgt.dw_first_effective_dt
						FROM ${tgt_tbl} tgt
						WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
						) tgt
						ON tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
						AND tgt.UPC_Nbr = src.UPC_Nbr
                        AND tgt.Adjustment_Type_Cd = src.Adjustment_Type_Cd
						AND tgt.Item_Sequence_Id = src.Item_Sequence_Id
						WHERE  (tgt.Transaction_Integration_Id  IS NULL AND tgt.UPC_Nbr IS NULL AND tgt.Adjustment_Type_Cd IS NULL)
						OR NVL(src.Item_UOM_Cd,'-1') <> NVL(tgt.Item_UOM_Cd,'-1')
						or NVL(src.Promotion_Cd,'-1') <> NVL(tgt.Promotion_Cd,'-1')
						or NVL(src.Item_Unit_Qty,'-1') <> NVL(tgt.Item_Unit_Qty,'-1')
						or NVL(src.External_Saving_Amt,'-1') <> NVL(tgt.External_Saving_Amt,'-1')
						OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
						`;


try {
        
        
        snowflake.execute ({sqlText: sql_command});
           
        }
    catch (err)  {
        return `Creation of EPE_Transaction_Item_External_Savings work table ${tgt_wrk_tbl} Failed with error:  ${err}`;   // Return a error message.
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
						 Transaction_Integration_Id
						,UPC_Nbr
						,Adjustment_Type_Cd
						,Item_Sequence_Id
						,FileName
				FROM ${tgt_wrk_tbl}
					WHERE DML_Type = 'U'
						AND Sameday_chg_ind = 0
						AND Transaction_Integration_Id IS NOT NULL
						AND UPC_Nbr IS NOT NULL
						AND Adjustment_Type_Cd IS NOT NULL
						AND Item_Sequence_Id IS NOT NULL
					) src
					WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
						AND tgt.UPC_Nbr = src.UPC_Nbr
                        AND tgt.Adjustment_Type_Cd = src.Adjustment_Type_Cd
						AND tgt.Item_Sequence_Id = src.Item_Sequence_Id
						AND tgt.DW_CURRENT_VERSION_IND = TRUE
						AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;


// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
						SET  Item_UOM_Cd = src.Item_UOM_Cd
						    ,Promotion_Cd = src.Promotion_Cd
							,Item_Unit_Qty = src.Item_Unit_Qty
							,External_Saving_Amt = src.External_Saving_Amt
						    ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
							,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
							,DW_SOURCE_UPDATE_NM = FileName
					FROM ( SELECT
							Transaction_Integration_Id
							,UPC_Nbr
							,Adjustment_Type_Cd
							,Item_Sequence_Id
							,Item_UOM_Cd
							,Promotion_Cd
							,Item_Unit_Qty
							,External_Saving_Amt
							,UpdatedDate
							,DW_Logical_delete_ind
							,FileName
					FROM ${tgt_wrk_tbl}
					WHERE DML_Type = 'U'
							AND Sameday_chg_ind = 1
							AND Transaction_Integration_Id IS NOT NULL
							AND UPC_Nbr IS NOT NULL
							AND Item_Sequence_Id IS NOT NULL
							 ) src
					WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
							AND tgt.UPC_Nbr = src.UPC_Nbr
                            AND tgt.Adjustment_Type_Cd = src.Adjustment_Type_Cd
							AND tgt.Item_Sequence_Id = src.Item_Sequence_Id
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
								(Transaction_Integration_Id ,
								 UPC_Nbr,
								 Adjustment_Type_Cd,
								 Item_Sequence_Id,
								 DW_First_Effective_Dt,
								 DW_Last_Effective_Dt,
								 Item_UOM_Cd,
								 Promotion_Cd,
								 Item_Unit_Qty,
								 External_Saving_Amt,
								 DW_CREATE_TS,
								 DW_LOGICAL_DELETE_IND,
								 DW_SOURCE_CREATE_NM ,
								 DW_CURRENT_VERSION_IND
								)
							    SELECT DISTINCT
								Transaction_Integration_Id
								,UPC_Nbr
								,Adjustment_Type_Cd
								,Item_Sequence_Id
								,CURRENT_DATE
								,'31-DEC-9999'
								,Item_UOM_Cd
								,Promotion_Cd
								,Item_Unit_Qty
								,External_Saving_Amt
								,CURRENT_TIMESTAMP
								,DW_Logical_delete_ind
								,FileName
								,TRUE
							FROM ${tgt_wrk_tbl}
							WHERE Sameday_chg_ind = 0
							AND Transaction_Integration_Id IS NOT NULL
							AND UPC_Nbr IS NOT NULL
							AND Adjustment_Type_Cd IS NOT NULL
							AND Item_Sequence_Id IS NOT NULL
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
        return `Loading of EPE_Transaction_Item_External_Savings table ${tgt_tbl} Failed with error:  ${err}`;   // Return a error message.

}
 var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;

var sql_exceptions = `INSERT INTO  ${tgt_exp_tbl}
							select Distinct
								    Transaction_Integration_Id,
								    UPC_Nbr,
								    Adjustment_Type_Cd,
									Item_Sequence_Id,
								    Item_UOM_Cd,
									Promotion_Cd,
									Item_Unit_Qty,
							        External_Saving_Amt,
									TERMINALNUMBER,
									TRANSACTIONNUMBER,									
									UpdatedDate,
									FileName,
									DML_Type,
									Sameday_chg_ind,
									CASE WHEN Transaction_Integration_Id is NULL THEN 'Transaction_Integration_Id is NULL'
                                         WHEN UPC_NBR IS NULL THEN 'UPC_NBR'
                                         WHEN ADJUSTMENT_TYPE_CD IS NULL THEN 'ADJUSTMENT_TYPE_CD IS NULL'
									END AS Exception_Reason,
								    CURRENT_TIMESTAMP AS DW_CREATE_TS,
								    TO_TIMESTAMP_NTZ (TRANSACTIONTIMESTAMP) AS TRANSACTIONTIMESTAMP
								    
									FROM  ${tgt_wrk_tbl}
									WHERE  Transaction_Integration_Id IS NULL
									or UPC_Nbr IS NULL
									or Adjustment_Type_Cd IS NULL
									or Item_Sequence_Id is NULL 
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

// ************** Load for EPE_Transaction_Item_External_Savings table ENDs *****************
$$;
