



var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE;
var cnf_schema = C_RETAIL
var lkp_schema = C_RETAIL;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.EPE_TRANSACTION_ITEM_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.EPE_TRANSACTION_ITEM`;
var lkp_tb1 =`${cnf_db}.${lkp_schema}.EPE_Transaction_Header`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.EPE_TRANSACTION_ITEM_Exceptions`;


// ************** Load for EPE_TRANSACTION_ITEM table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.




var sql_command = `CREATE OR REPLACE transient TABLE   ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT
                               --Transaction_Integration_Id
							   UPC_Nbr
							  ,Department_Nbr
							  ,Discount_Allowed_Ind
							  ,Item_Sequence_Id
							  ,Points_Apply_Item_Ind
							  ,Item_UOM_Cd
							  ,Sell_By_Weight_Cd
							  ,Department_Group_Nbr
							  ,Link_Plu_Nbr
							  ,Item_Plu_Nbr
							  ,Clipped_Offer_Start_Ts
                              ,Clipped_Offer_End_Ts
							  ,Price_Per_Item_Amt
							  ,Base_Price_Amt
                              ,Item_Price_Amt
							  ,Net_Promotion_Amt
                              ,Unit_Price_Amt
							  ,Extended_Price_Amt
							  ,Base_Price_Per_Amt
							  ,Club_Card_Savings_Amt
                              ,Average_Weight_Qty
                              ,Item_Unit_Qty
							  ,Item_Qty
							  ,TERMINALNUMBER
							  ,TRANSACTIONNUMBER
							 ,TRANSACTIONTIMESTAMP
							  ,UpdatedDate
							  ,filename
							,Row_number() OVER ( partition BY TERMINALNUMBER,TRANSACTIONNUMBER,TRANSACTIONTIMESTAMP,UPC_Nbr,Item_Sequence_Id ORDER BY
							To_timestamp_ntz(UpdatedDate ) desc) AS rn
                            from
                            (
                            SELECT DISTINCT
							   --Transaction_Integration_Id,
							   ITEMS_ITEMCODE as UPC_Nbr
							  ,ITEMS_DEPARTMENT as Department_Nbr
							  ,ITEMS_DISCOUNTALLOWED as Discount_Allowed_Ind
							  ,ITEMS_ENTRYID as Item_Sequence_Id
							  ,ITEMS_POINTSAPPLYITEM as Points_Apply_Item_Ind
							  ,Items_QuantityType as Item_UOM_Cd
							  ,ITEMS_SELLBYWEIGHT as Sell_By_Weight_Cd
							  ,Items_DepartmentGroupNumber as Department_Group_Nbr
							  ,ITEMS_LINKPLUNUMBER as Link_Plu_Nbr
							  ,ITEMS_ITEMPLUNUMBER as Item_Plu_Nbr
							--  ,try_to_timestamp(ITEMS_STRARTDATE) as Clipped_Offer_Start_Ts




							,case
when  (STRTOK( ITEMS_STRARTDATE,'+',2)<>'' and contains(STRTOK( ITEMS_STRARTDATE,'+',2),':')= false ) or
(contains(ITEMS_STRARTDATE,'T')=true and STRTOK(ITEMS_STRARTDATE,'-',4) <>'' and contains(STRTOK( ITEMS_STRARTDATE,'-',4),':')= false)
 then to_timestamp_ltz(ITEMS_STRARTDATE,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(ITEMS_STRARTDATE)
end as Clipped_Offer_Start_Ts







                            --  ,try_to_timestamp(ITEMS_ENDDATE )as Clipped_Offer_End_Ts


							 ,case
when  (STRTOK( ITEMS_ENDDATE,'+',2)<>'' and contains(STRTOK( ITEMS_ENDDATE,'+',2),':')= false ) or
(contains(ITEMS_ENDDATE,'T')=true and STRTOK(ITEMS_ENDDATE,'-',4) <>'' and contains(STRTOK( ITEMS_ENDDATE,'-',4),':')= false)
 then to_timestamp_ltz(ITEMS_ENDDATE,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(ITEMS_ENDDATE)
end as Clipped_Offer_End_Ts




							  ,ITEMS_PRICEPER as Price_Per_Item_Amt
							  ,ITEMS_PRICEPERBASEPRICE as Base_Price_Amt
                              ,ITEMS_PRICE as Item_Price_Amt
							  ,ITEMS_NETPROMOTIONAMOUNT as Net_Promotion_Amt
                              ,ITEMS_UNITPRICE as Unit_Price_Amt
							  ,ITEMS_EXTENDEDPRICE as Extended_Price_Amt
							  ,ITEMS_BASEPRICEPER as Base_Price_Per_Amt
							  ,ITEMS_CLUBCARDSAVING as Club_Card_Savings_Amt
                              ,ITEMS_AVERAGEWEIGHT as Average_Weight_Qty
                              ,Items_QuantityValue as Item_Unit_Qty
							  ,ITEMS_QTY as Item_Qty
							  ,TERMINALNUMBER
							  ,try_to_numeric(TRANSACTIONNUMBER) as TRANSACTIONNUMBER
							  --,TRANSACTIONTIMESTAMP
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
				END,STORETIMEZONE) END as TRANSACTIONTIMESTAMP
							--  ,UpdatedDate

							, case
when  (STRTOK( UpdatedDate,'+',2)<>'' and contains(STRTOK( UpdatedDate,'+',2),':')= false ) or
(contains(UpdatedDate,'T')=true and STRTOK(createddate,'-',4) <>'' and contains(STRTOK( UpdatedDate,'-',4),':')= false)
 then to_timestamp_ltz(UpdatedDate,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(UpdatedDate)
end as UpdatedDate



								   ,filename
							FROM ${src_wrk_tbl}


						UNION ALL
						SELECT DISTINCT
								UPC_Nbr
						       ,Department_Nbr
							  ,Discount_Allowed_Ind
							  ,Item_Sequence_Id
							  ,Points_Apply_Item_Ind
							  ,Item_UOM_Cd
							  ,Sell_By_Weight_Cd
							  ,Department_Group_Nbr
							  ,Link_Plu_Nbr
							  ,Item_Plu_Nbr
							  ,Clipped_Offer_Start_Ts
                              ,Clipped_Offer_End_Ts
							  ,Price_Per_Item_Amt
							  ,Base_Price_Amt
                              ,Item_Price_Amt
							  ,Net_Promotion_Amt
                              ,Unit_Price_Amt
							  ,Extended_Price_Amt
							  ,Base_Price_Per_Amt
							  ,Club_Card_Savings_Amt
                              ,Average_Weight_Qty
                              ,Item_Unit_Qty
							  ,Item_Qty
							  ,TERMINALNUMBER
							  ,TRANSACTIONNUMBER
							  ,TRANSACTIONTIMESTAMP
							  ,cast(UpdatedDate as varchar)
							  ,filename
						FROM ${tgt_exp_tbl}
						)
						)

						SELECT
						src.Transaction_Integration_Id
							  ,src.UPC_Nbr
							  ,src.Department_Nbr
							  ,src.Discount_Allowed_Ind
							  ,src.Item_Sequence_Id
							  ,src.Points_Apply_Item_Ind
							  ,src.Item_UOM_Cd
							  ,src.Sell_By_Weight_Cd
							  ,src.Department_Group_Nbr
							  ,src.Link_Plu_Nbr
							  ,src.Item_Plu_Nbr
							  ,src.Clipped_Offer_Start_Ts
                              ,src.Clipped_Offer_End_Ts
							  ,src.Price_Per_Item_Amt
							  ,src.Base_Price_Amt
                              ,src.Item_Price_Amt
							  ,src.Net_Promotion_Amt
                              ,src.Unit_Price_Amt
							  ,src.Extended_Price_Amt
							  ,src.Base_Price_Per_Amt
							  ,src.Club_Card_Savings_Amt
                              ,src.Average_Weight_Qty
                              ,src.Item_Unit_Qty
							  ,src.Item_Qty
							  ,src.TERMINALNUMBER
							  ,src.TRANSACTIONNUMBER
							  ,src.TRANSACTIONTIMESTAMP
							  ,src.DW_Logical_delete_ind
							  ,src.UpdatedDate
							   ,src.filename

						 ,CASE WHEN tgt.Transaction_Integration_Id IS NULL AND tgt.UPC_Nbr IS NULL AND tgt.Item_Sequence_Id IS NULL
						 THEN 'I' ELSE 'U' END AS DML_Type
                         ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
						FROM
					    (
						select
						 LKP_EPE_Transaction_Header.Transaction_Integration_Id  AS Transaction_Integration_Id
						 ,src1.UPC_Nbr
			            ,src1.Department_Nbr
							 -- ,src1.Discount_Allowed_Ind
							 ,cast(src1.Discount_Allowed_Ind as Boolean) as Discount_Allowed_Ind
							  ,src1.Item_Sequence_Id
							  --,src1.Points_Apply_Item_Ind
							  ,cast(src1.Points_Apply_Item_Ind as Boolean) as Points_Apply_Item_Ind
							  ,src1.Item_UOM_Cd
							  ,src1.Sell_By_Weight_Cd
							  ,src1.Department_Group_Nbr
							  ,src1.Link_Plu_Nbr
							  ,src1.Item_Plu_Nbr
							  ,src1.Clipped_Offer_Start_Ts
                              ,src1.Clipped_Offer_End_Ts
							  ,src1.Price_Per_Item_Amt
							  ,src1.Base_Price_Amt
                              ,src1.Item_Price_Amt
							  ,src1.Net_Promotion_Amt
                              ,src1.Unit_Price_Amt
							  ,src1.Extended_Price_Amt
							  ,src1.Base_Price_Per_Amt
							  ,src1.Club_Card_Savings_Amt
                              ,src1.Average_Weight_Qty
                              ,src1.Item_Unit_Qty
							  ,src1.Item_Qty
							  ,src1.TERMINALNUMBER
							  ,src1.TRANSACTIONNUMBER
							  ,src1.TRANSACTIONTIMESTAMP
							  ,src1.DW_Logical_delete_ind
							  ,src1.UpdatedDate
							   ,src1.filename
						from
						(
						SELECT

							  UPC_Nbr
							  ,Department_Nbr
							  ,Discount_Allowed_Ind
							  ,Item_Sequence_Id
							  ,Points_Apply_Item_Ind
							  ,Item_UOM_Cd
							  ,Sell_By_Weight_Cd
							  ,Department_Group_Nbr
							  ,Link_Plu_Nbr
							  ,Item_Plu_Nbr
							  ,Clipped_Offer_Start_Ts
                              ,Clipped_Offer_End_Ts
							  ,Price_Per_Item_Amt
							  ,Base_Price_Amt
                              ,Item_Price_Amt
							  ,Net_Promotion_Amt
                              ,Unit_Price_Amt
							  ,Extended_Price_Amt
							  ,Base_Price_Per_Amt
							  ,Club_Card_Savings_Amt
                              ,Average_Weight_Qty
                              ,Item_Unit_Qty
							  ,Item_Qty
							  ,TERMINALNUMBER
							  ,TRANSACTIONNUMBER
							  ,TRANSACTIONTIMESTAMP
						,UpdatedDate
						,false AS DW_Logical_delete_ind
						,FileName
						FROM   src_wrk_tbl_recs --src1
						WHERE rn = 1
						--AND Transaction_Integration_Id  IS NOT NULL
						AND UPC_Nbr  IS NOT NULL
						AND Item_Sequence_Id IS NOT NULL
					    ) src1

							LEFT JOIN
							(SELECT DISTINCT Transaction_Integration_Id,
							Terminal_Nbr,Transaction_Id,Transaction_Ts,Source_System_Cd
							 FROM ${lkp_tb1}
							 WHERE DW_CURRENT_VERSION_IND = TRUE
							 AND DW_LOGICAL_DELETE_IND = FALSE
							) LKP_EPE_Transaction_Header
							 ON src1.terminalnumber = LKP_EPE_Transaction_Header.Terminal_Nbr
							AND src1.TRANSACTIONNUMBER = LKP_EPE_Transaction_Header.Transaction_Id
							AND src1.TRANSACTIONTIMESTAMP = LKP_EPE_Transaction_Header.Transaction_Ts
							--AND src1.Facility_Integration_ID = LKP_EPE_Transaction_Header.Facility_Integration_ID
							--AND Source_System_Cd = 'STORE'
						)src

						LEFT JOIN (SELECT
						 tgt.Transaction_Integration_Id
							  ,tgt.UPC_Nbr
							  ,tgt.Department_Nbr
							  ,tgt.Discount_Allowed_Ind
							  ,tgt.Item_Sequence_Id
							  ,tgt.Points_Apply_Item_Ind
							  ,tgt.Item_UOM_Cd
							  ,tgt.Sell_By_Weight_Cd
							  ,tgt.Department_Group_Nbr
							  ,tgt.Link_Plu_Nbr
							  ,tgt.Item_Plu_Nbr
							  ,tgt.Clipped_Offer_Start_Ts
                              ,tgt.Clipped_Offer_End_Ts
							  ,tgt.Price_Per_Item_Amt
							  ,tgt.Base_Price_Amt
                              ,tgt.Item_Price_Amt
							  ,tgt.Net_Promotion_Amt
                              ,tgt.Unit_Price_Amt
							  ,tgt.Extended_Price_Amt
							  ,tgt.Base_Price_Per_Amt
							  ,tgt.Club_Card_Savings_Amt
                              ,tgt.Average_Weight_Qty
                              ,tgt.Item_Unit_Qty
							  ,tgt.Item_Qty
							   ,tgt.dw_logical_delete_ind
							   ,tgt.dw_first_effective_dt
						FROM ${tgt_tbl} tgt
						WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
						) tgt
						ON tgt.Transaction_Integration_Id = src.Transaction_Integration_Id

                       	AND tgt.UPC_Nbr = src.UPC_Nbr
						AND tgt.Item_Sequence_Id = src.Item_Sequence_Id


						  WHERE  (tgt.Transaction_Integration_Id is null and tgt.UPC_Nbr is null and tgt.Item_Sequence_Id is null)
                          or(
                          NVL(src.Department_Nbr,'-1') <> NVL(tgt.Department_Nbr,'-1')
                          OR NVL(src.Discount_Allowed_Ind,-1) <> NVL(tgt.Discount_Allowed_Ind,-1)
						  --OR NVL(src.Item_Sequence_Id,'-1') <> NVL(tgt.Item_Sequence_Id,'-1')
						  OR NVL(src.Points_Apply_Item_Ind,-1) <> NVL(tgt.Points_Apply_Item_Ind,-1)
						  OR NVL(src.Item_UOM_Cd,'-1') <> NVL(tgt.Item_UOM_Cd,'-1')
						  OR NVL(src.Sell_By_Weight_Cd,'-1') <> NVL(tgt.Sell_By_Weight_Cd,'-1')
						  OR NVL(src.Department_Group_Nbr,'-1') <> NVL(tgt.Department_Group_Nbr,'-1')
						  OR NVL(src.Link_Plu_Nbr,'-1') <> NVL(tgt.Link_Plu_Nbr,'-1')
						  OR NVL(src.Item_Plu_Nbr,'-1') <> NVL(tgt.Item_Plu_Nbr,'-1')
						  OR NVL(src.Clipped_Offer_Start_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Clipped_Offer_Start_Ts,'9999-12-31 00:00:00.000')
						  OR NVL(src.Clipped_Offer_End_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Clipped_Offer_End_Ts,'9999-12-31 00:00:00.000')
						  OR NVL(src.Price_Per_Item_Amt,'-1') <> NVL(tgt.Price_Per_Item_Amt,'-1')
						  OR NVL(src.Base_Price_Amt,'-1') <> NVL(tgt.Base_Price_Amt,'-1')
						  OR NVL(src.Item_Price_Amt,'-1') <> NVL(tgt.Item_Price_Amt,'-1')
						  OR NVL(src.Net_Promotion_Amt,'-1') <> NVL(tgt.Net_Promotion_Amt,'-1')
						  OR NVL(src.Unit_Price_Amt,'-1') <> NVL(tgt.Unit_Price_Amt,'-1')
						  OR NVL(src.Extended_Price_Amt,'-1') <> NVL(tgt.Extended_Price_Amt,'-1')
						  OR NVL(src.Base_Price_Per_Amt,'-1') <> NVL(tgt.Base_Price_Per_Amt,'-1')
						  OR NVL(src.Club_Card_Savings_Amt,'-1') <> NVL(tgt.Club_Card_Savings_Amt,'-1')
						  OR NVL(src.Average_Weight_Qty,'-1') <> NVL(tgt.Average_Weight_Qty,'-1')
						  OR NVL(src.Item_Unit_Qty,'-1') <> NVL(tgt.Item_Unit_Qty,'-1')
						  OR NVL(src.Item_Qty,'-1') <> NVL(tgt.Item_Qty,'-1')
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )
						  `;

try {


        snowflake.execute ({sqlText: sql_command});

        }
    catch (err)  {
        return "Creation of EPE_TRANSACTION_ITEM "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
                                           Transaction_Integration_Id
								          ,UPC_Nbr
										  ,Item_Sequence_Id
                                          ,filename
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U'
                             AND Sameday_chg_ind = 0
                             AND Transaction_Integration_Id is not NULL
							 AND UPC_Nbr is not null
							 AND Item_Sequence_Id is not null
                             ) src
                             WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
							 AND tgt.UPC_Nbr = src.UPC_Nbr
							 AND tgt.Item_Sequence_Id = src.Item_Sequence_Id
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET        Department_Nbr                 = src.Department_Nbr
							  ,Discount_Allowed_Ind           = src.Discount_Allowed_Ind
							  --,Item_Sequence_Id               = src.Item_Sequence_Id
							  ,Points_Apply_Item_Ind          = src.Points_Apply_Item_Ind
							  ,Item_UOM_Cd                    = src.Item_UOM_Cd
							  ,Sell_By_Weight_Cd              = src.Sell_By_Weight_Cd
							  ,Department_Group_Nbr           = src.Department_Group_Nbr
							  ,Link_Plu_Nbr                   = src.Link_Plu_Nbr
							  ,Item_Plu_Nbr                   = src.Item_Plu_Nbr
							  ,Clipped_Offer_Start_Ts         = src.Clipped_Offer_Start_Ts
                              ,Clipped_Offer_End_Ts           = src.Clipped_Offer_End_Ts
							  ,Price_Per_Item_Amt             = src.Price_Per_Item_Amt
							  ,Base_Price_Amt                 = src.Base_Price_Amt
                              ,Item_Price_Amt                 = src.Item_Price_Amt
							  ,Net_Promotion_Amt              = src.Net_Promotion_Amt
                              ,Unit_Price_Amt                 = src.Unit_Price_Amt
							  ,Extended_Price_Amt             = src.Extended_Price_Amt
							  ,Base_Price_Per_Amt             = src.Base_Price_Per_Amt
							  ,Club_Card_Savings_Amt          = src.Club_Card_Savings_Amt
                              ,Average_Weight_Qty             = src.Average_Weight_Qty
                        ,Item_Qty                          = src.Item_Qty
					   ,DW_Logical_delete_ind 			   = src.DW_Logical_delete_ind
					   ,DW_LAST_UPDATE_TS                  = CURRENT_TIMESTAMP
					   ,DW_SOURCE_UPDATE_NM                = FileName
					   FROM ( SELECT
								   Transaction_Integration_Id
							  ,UPC_Nbr
							  ,Department_Nbr
							  ,Discount_Allowed_Ind
							  ,Item_Sequence_Id
							  ,Points_Apply_Item_Ind
							  ,Item_UOM_Cd
							  ,Sell_By_Weight_Cd
							  ,Department_Group_Nbr
							  ,Link_Plu_Nbr
							  ,Item_Plu_Nbr
							  ,Clipped_Offer_Start_Ts
                              ,Clipped_Offer_End_Ts
							  ,Price_Per_Item_Amt
							  ,Base_Price_Amt
                              ,Item_Price_Amt
							  ,Net_Promotion_Amt
                              ,Unit_Price_Amt
							  ,Extended_Price_Amt
							  ,Base_Price_Per_Amt
							  ,Club_Card_Savings_Amt
                              ,Average_Weight_Qty
                              ,Item_Unit_Qty
							  ,Item_Qty
								  ,DW_Logical_delete_ind
								  --,Updated_Dt
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
							AND tgt.Item_Sequence_Id = src.Item_Sequence_Id
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                      Transaction_Integration_Id
							  ,UPC_Nbr
							  ,DW_Last_Effective_Dt
							  ,DW_First_Effective_Dt
							  ,Department_Nbr
							  ,Discount_Allowed_Ind
							  ,Item_Sequence_Id
							  ,Points_Apply_Item_Ind
							  ,Item_UOM_Cd
							  ,Sell_By_Weight_Cd
							  ,Department_Group_Nbr
							  ,Link_Plu_Nbr
							  ,Item_Plu_Nbr
							  ,Clipped_Offer_Start_Ts
                              ,Clipped_Offer_End_Ts
							  ,Price_Per_Item_Amt
							  ,Base_Price_Amt
                              ,Item_Price_Amt
							  ,Net_Promotion_Amt
                              ,Unit_Price_Amt
							  ,Extended_Price_Amt
							  ,Base_Price_Per_Amt
							  ,Club_Card_Savings_Amt
                              ,Average_Weight_Qty
                              ,Item_Unit_Qty
							  ,Item_Qty


                    ,DW_CREATE_TS
                    ,DW_LOGICAL_DELETE_IND
                    ,DW_SOURCE_CREATE_NM
                    ,DW_CURRENT_VERSION_IND
                   )
                   SELECT DISTINCT
                       Transaction_Integration_Id
							  ,UPC_Nbr
							  ,'31-DEC-9999'
						      ,CURRENT_DATE
							  ,Department_Nbr
							  ,Discount_Allowed_Ind
							  ,Item_Sequence_Id
							  ,Points_Apply_Item_Ind
							  ,Item_UOM_Cd
							  ,Sell_By_Weight_Cd
							  ,Department_Group_Nbr
							  ,Link_Plu_Nbr
							  ,Item_Plu_Nbr
							  ,Clipped_Offer_Start_Ts
                              ,Clipped_Offer_End_Ts
							  ,Price_Per_Item_Amt
							  ,Base_Price_Amt
                              ,Item_Price_Amt
							  ,Net_Promotion_Amt
                              ,Unit_Price_Amt
							  ,Extended_Price_Amt
							  ,Base_Price_Per_Amt
							  ,Club_Card_Savings_Amt
                              ,Average_Weight_Qty
                              ,Item_Unit_Qty
							  ,Item_Qty


					 ,CURRENT_TIMESTAMP
                     ,DW_Logical_delete_ind
                     ,FileName
                     ,TRUE
				FROM ${tgt_wrk_tbl}
                where Transaction_Integration_Id is not null
				and UPC_Nbr is not null
				and Item_Sequence_Id is not null
				and Sameday_chg_ind = 0`;

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

        return `Loading of EPE_TRANSACTION_ITEM table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }

	var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;

var sql_exceptions = `INSERT INTO  ${tgt_exp_tbl}
							select Distinct
								    Transaction_Integration_Id
							  ,UPC_Nbr
							  ,Department_Nbr
							  ,Discount_Allowed_Ind
							  ,Item_Sequence_Id
							  ,Points_Apply_Item_Ind
							  ,Item_UOM_Cd
							  ,Sell_By_Weight_Cd
							  ,Department_Group_Nbr
							  ,Link_Plu_Nbr
							  ,Item_Plu_Nbr
							  ,Clipped_Offer_Start_Ts
                              ,Clipped_Offer_End_Ts
							  ,Price_Per_Item_Amt
							  ,Base_Price_Amt
                              ,Item_Price_Amt
							  ,Net_Promotion_Amt
                              ,Unit_Price_Amt
							  ,Extended_Price_Amt
							  ,Base_Price_Per_Amt
							  ,Club_Card_Savings_Amt
                              ,Average_Weight_Qty
                              ,Item_Unit_Qty
							  ,Item_Qty
								,TERMINALNUMBER
									,TRANSACTIONNUMBER
									,UpdatedDate
									,FileName
									,DML_Type
									,Sameday_chg_ind
									,CASE WHEN Transaction_Integration_Id is NULL THEN 'Transaction_Integration_Id is NULL'
										 WHEN UPC_Nbr IS NULL  THEN 'UPC_Nbr IS NULL '
										 WHEN Item_Sequence_Id IS NULL THEN 'Item_Sequence_Id IS NULL'
										 END AS Exception_Reason,

								    CURRENT_TIMESTAMP AS DW_CREATE_TS,
								    TO_TIMESTAMP_NTZ (TRANSACTIONTIMESTAMP) AS TRANSACTIONTIMESTAMP
									FROM  ${tgt_wrk_tbl}
									WHERE Transaction_Integration_Id IS NULL
										or UPC_Nbr IS NULL
										or Item_Sequence_Id IS NULL

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



// ************** Load for EPE_TRANSACTION_ITEM table ENDs *****************
