
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_RETAIL;
var lkp_schema = C_RETAIL;
var src_wrk_tbl = SRC_WRK_TBL;
var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Item_Savings_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.EPE_Transaction_Item_Savings`;
var lkp_tbl = `${cnf_db}.${lkp_schema}.EPE_Transaction_Header`;
var tgt_exp_tbl = `${cnf_db}. ${wrk_schema}.EPE_Transaction_Item_Savings_Exceptions`;


// ************** Load for EPE_Transaction_Item_Savings table BEGIN*****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
   var sql_command = `Create
   or replace transient table ${tgt_wrk_tbl} as WITH src_wrk_tbl_recs as
   (
      SELECT DISTINCT
         UPC_Nbr,

         Offer_Id,
		 Item_Sequence_Id,
         Savings_Category_Nm,
         Discount_Message_Txt,
         Source_system_cd,
         Discount_Type_Txt,
         Discount_Dsc,
         Discount_Level_Txt,
         Savings_Category_Id,
         Min_Purchase_Qty,
         Discount_Amt,
         Discount_Qty,
         Cast(Non_Digital_Offer_Ind as Boolean) as Non_Digital_Offer_Ind,
		 Cast(Calculate_Usage_Ind as Boolean) as Calculate_Usage_Ind,
		 Net_Promotion_Amt,
		 Usage_Cnt ,
		Program_Cd   ,
		--Start_Dt  ,
		case
when (STRTOK( Start_Dt,'+',2)<>'' and contains(STRTOK( Start_Dt,'+',2),':')= false ) or
(contains(Start_Dt,'T')=true and STRTOK(Start_Dt,'-',4) <>'' and contains(STRTOK( Start_Dt,'-',4),':')= false)
then to_timestamp_ltz(Start_Dt,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(Start_Dt)
end as Start_Dt,
		--End_Dt,
		case
when (STRTOK( End_Dt,'+',2)<>'' and contains(STRTOK( End_Dt,'+',2),':')= false ) or
(contains(End_Dt,'T')=true and STRTOK(End_Dt,'-',4) <>'' and contains(STRTOK( End_Dt,'-',4),':')= false)
then to_timestamp_ltz(End_Dt,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(End_Dt)
end as End_Dt,
         TERMINALNUMBER,
         TRANSACTIONNUMBER,
         TRANSACTIONTIMESTAMP,
         UpdatedDate,
		 External_Offer_Id,
		 Promo_Cd,
         filename,
		 Program_Type_Cd,
         Row_number() OVER ( partition BY TERMINALNUMBER,TRANSACTIONNUMBER,TRANSACTIONTIMESTAMP,UPC_Nbr,Offer_Id,Item_Sequence_Id ORDER BY UpdatedDate desc) AS rn
      from
         (
            SELECT DISTINCT
               items_itemcode as UPC_Nbr,
              -- Savings.VALUE:External_Offer_Id::string as
               Savings.VALUE:offerId::string as Offer_Id,
			   ITEMS_ENTRYID as Item_Sequence_Id ,
               Savings.VALUE:categoryName::string as Savings_Category_Nm,
               Savings.VALUE:discountMessage::string as Discount_Message_Txt
			   --  ,Savings.VALUE:source::string  as Source_cd
,
               Transactionsource as Source_system_cd,
               Savings.VALUE:discountType::string as Discount_Type_Txt,
               Savings.VALUE:description::string as Discount_Dsc,
               Savings.VALUE:discountLevel::string as Discount_Level_Txt,
               Savings.VALUE:categoryId::string as Savings_Category_Id,
               Savings.VALUE:minPurchaseQuantity::string as Min_Purchase_Qty,
               Savings.VALUE:discountAmount::string as Discount_Amt,
               Savings.VALUE:discountQty::string as Discount_Qty,
			   Savings.VALUE:nonDigital::string as Non_Digital_Offer_Ind,
               Savings.VALUE:calculateUsage::string as Calculate_Usage_Ind,
			   Savings.VALUE:netPromotionAmount::string as Net_Promotion_Amt,
			   Savings.VALUE:usageCount::string as Usage_Cnt,
			   Savings.VALUE:programCode::string as Program_Cd,
			   Savings.VALUE:startDate::string as Start_Dt,
			   Savings.VALUE:endDate::string as End_Dt,
               TERMINALNUMBER,
               try_to_numeric(TRANSACTIONNUMBER) as TRANSACTIONNUMBER ,
			   --,TRANSACTIONTIMESTAMP,
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
				END,STORETIMEZONE) END                as TRANSACTIONTIMESTAMP , 					--,UpdatedDate
               case
                  when
                     (
                        STRTOK( UpdatedDate, '+', 2) <> ''
                        and contains(STRTOK( UpdatedDate, '+', 2), ':') = false
                     )
                     or
                     (
                        contains(UpdatedDate, 'T') = true
                        and STRTOK(UpdatedDate, '-', 4) <> ''
                        and contains(STRTOK( UpdatedDate, '-', 4), ':') = false
                     )
                  then
                     to_timestamp_ltz(UpdatedDate, 'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
                  else
                     to_timestamp_ltz(UpdatedDate)
               end
               as UpdatedDate ,
			   Savings.VALUE:externalOfferId::string as External_Offer_Id,
			   Savings.VALUE:promoCode::string as Promo_Cd,
			   Savings.VALUE:programType::string as Program_Type_Cd,
			   filename as filename
            FROM
               ${src_wrk_tbl} , LATERAL FLATTEN(input => Items_Savings, outer => TRUE) as Savings
            UNION ALL
            SELECT DISTINCT
               UPC_Nbr,

               Offer_Id,
			   Item_Sequence_Id,
               Savings_Category_Nm,
               Discount_Message_Txt,
               Source_system_cd,
               Discount_Type_Txt,
               Discount_Dsc,
               Discount_Level_Txt,
               Savings_Category_Id,
               Min_Purchase_Qty,
               Discount_Amt,
               Discount_Qty,
               Non_Digital_Offer_Ind,
			   Calculate_Usage_Ind,
		 Net_Promotion_Amt,
		 Usage_Cnt ,
		Program_Cd   ,
		cast(Start_Dt as varchar),
		cast(End_Dt as varchar),
               TERMINALNUMBER,
               TRANSACTIONNUMBER,
               TRANSACTIONTIMESTAMP,
               cast(UpdatedDate as varchar),
			   External_Offer_Id,
			   Promo_Cd,
			   Program_Type_Cd,
               filename
            FROM
               ${tgt_exp_tbl}
         )
   )
   select
      src.Transaction_Integration_Id,
      src.UPC_Nbr,
      src.Offer_Id,
	  src.Item_Sequence_Id,
      src.Savings_Category_Nm,
      src.Discount_Message_Txt,
      src.Source_system_cd,
      src.Discount_Type_Txt,
      src.Discount_Dsc,
      src.Discount_Level_Txt,
      src.Savings_Category_Id,
      src.Min_Purchase_Qty,
      src.Discount_Amt,
      src.Discount_Qty,
      src.Non_Digital_Offer_Ind,
	  src.Calculate_Usage_Ind,
		 src.Net_Promotion_Amt,
		 src.Usage_Cnt ,
		src.Program_Cd   ,
		src.Start_Dt  ,
		src.End_Dt,

      src.TERMINALNUMBER,
      src.TRANSACTIONNUMBER,
      src.TRANSACTIONTIMESTAMP,
      src.FileName,
      src.DW_Logical_delete_ind,
      src.UpdatedDate,
	  src.External_Offer_Id,
	  src.Promo_Cd,
	  src.Program_Type_Cd,
      CASE
         WHEN
            (
               tgt.Transaction_Integration_Id IS NULL
               AND tgt.UPC_Nbr IS NULL
               AND tgt.Offer_Id IS NULL
			   AND tgt.Item_Sequence_Id IS NULL
            )
         THEN
            'I'
         ELSE
            'U'
      END
      AS DML_Type ,
      CASE
         WHEN
            tgt.dw_first_effective_dt = CURRENT_DATE
         THEN
            1
         ELSE
            0
      END
      AS Sameday_chg_ind
   from
      (
         select Distinct
            LKP_EPE_Transaction_Header.Transaction_Integration_Id,
            src1.UPC_Nbr,
            src1.Offer_Id,
			src1.Item_Sequence_Id,
            src1.Savings_Category_Nm,
            src1.Discount_Message_Txt,
            src1.Source_system_cd,
            src1.Discount_Type_Txt,
            src1.Discount_Dsc,
            src1.Discount_Level_Txt,
            src1.Savings_Category_Id,
            src1.Min_Purchase_Qty,
            src1.Discount_Amt,
            src1.Discount_Qty,
            src1.Non_Digital_Offer_Ind,
			src1.Calculate_Usage_Ind,
		 src1.Net_Promotion_Amt,
		 src1.Usage_Cnt ,
		src1.Program_Cd   ,
		src1.Start_Dt  ,
		src1.End_Dt,

            src1.TERMINALNUMBER,
            src1.TRANSACTIONNUMBER,
            src1.TRANSACTIONTIMESTAMP,
            src1.FileName,
            src1.DW_Logical_delete_ind,
            src1.UpdatedDate,
			src1.External_Offer_Id,
			src1.Promo_Cd,
			src1.Program_Type_Cd
         from
            (
               select
                  UPC_Nbr,

                  Offer_Id,
				  Item_Sequence_Id,
                  Savings_Category_Nm,
                  Discount_Message_Txt,
                  Source_system_cd,
                  Discount_Type_Txt,
                  Discount_Dsc,
                  Discount_Level_Txt,
                  Savings_Category_Id,
                  Min_Purchase_Qty,
                  Discount_Amt,
                  Discount_Qty,
                  Non_Digital_Offer_Ind,
				  Calculate_Usage_Ind,
		 Net_Promotion_Amt,
		 Usage_Cnt ,
		Program_Cd   ,
		Start_Dt  ,
		End_Dt,
                  TERMINALNUMBER,
                  TRANSACTIONNUMBER,
                  TRANSACTIONTIMESTAMP,
                  false AS DW_Logical_delete_ind,
                  FileName,
                  UpdatedDate,
				  External_Offer_Id	,
				  Promo_Cd,
				  Program_Type_Cd
               FROM
                  src_wrk_tbl_recs
               where
                  UPC_Nbr is not null
                  and Offer_Id is not null
				  and Item_Sequence_Id is not null
                  AND TERMINALNUMBER is not null
                  AND TRANSACTIONNUMBER is not null
                  AND TRANSACTIONTIMESTAMP is not null
                  and rn = 1
            )
            src1
            LEFT JOIN
               (
                  SELECT DISTINCT
                     Transaction_Integration_Id,
                     Terminal_Nbr,
                     Transaction_Id,
                     Transaction_Ts,
                     Source_System_Cd
                  FROM
                     ${lkp_tbl}
                  WHERE
                     DW_CURRENT_VERSION_IND = TRUE
                     AND DW_LOGICAL_DELETE_IND = FALSE
               )
               LKP_EPE_Transaction_Header
               ON src1.terminalnumber = LKP_EPE_Transaction_Header.Terminal_Nbr
               AND src1.TRANSACTIONNUMBER = LKP_EPE_Transaction_Header.Transaction_Id
               AND src1.TRANSACTIONTIMESTAMP = LKP_EPE_Transaction_Header.Transaction_Ts 					--AND src1.Facility_Integration_ID = LKP_EPE_Transaction_Header.Facility_Integration_ID
              -- AND LKP_EPE_Transaction_Header.Source_System_Cd = 'STORE'
      )
      src
      LEFT JOIN
         (
            SELECT Distinct
               tgt.Transaction_Integration_Id,
               tgt.UPC_Nbr,

               tgt.Offer_Id,
			   tgt.Item_Sequence_Id,
               tgt.Savings_Category_Nm,
               tgt.Discount_Message_Txt,
               tgt.Source_system_cd,
               tgt.Discount_Type_Txt,
               tgt.Discount_Dsc,
               tgt.Discount_Level_Txt,
               tgt.Savings_Category_Id,
               tgt.Min_Purchase_Qty,
               tgt.Discount_Amt,
               tgt.Discount_Qty,
               tgt.Non_Digital_Offer_Ind,
			   tgt.Calculate_Usage_Ind,
		 tgt.Net_Promotion_Amt,
		 tgt.Usage_Cnt ,
		tgt.Program_Cd   ,
		tgt.Start_Dt  ,
		tgt.End_Dt,

               tgt.dw_logical_delete_ind,
               tgt.dw_first_effective_dt,
			   tgt.External_Offer_Id,
			   tgt.Promo_Cd,
			   tgt.Program_Type_Cd
            FROM
               ${tgt_tbl} tgt
            WHERE
               tgt.DW_CURRENT_VERSION_IND = TRUE
         )
         as tgt
         ON tgt.UPC_Nbr = src.UPC_Nbr
         AND tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
         AND tgt.Offer_Id = src.Offer_Id
		 AND tgt.Item_Sequence_Id = src.Item_Sequence_Id
   where
      (
         tgt.UPC_Nbr IS NULL
         AND tgt.Transaction_Integration_Id IS NULL
         AND tgt.Offer_Id IS NULL
		 AND tgt.Item_Sequence_Id IS NULL
      )
      OR
      (
         --NVL(src.Offer_Id, '-1') <> NVL(tgt.Offer_Id, '-1')
         NVL(src.Savings_Category_Nm, '-1') <> NVL(tgt.Savings_Category_Nm, '-1')
         OR NVL(src.Discount_Message_Txt, '-1') <> NVL(tgt.Discount_Message_Txt, '-1')
         OR NVL(src.Source_system_cd, '-1') <> NVL(tgt.Source_system_cd, '-1')
         OR NVL(src.Discount_Type_Txt, '-1') <> NVL(tgt.Discount_Type_Txt, '-1')
         OR NVL(src.Discount_Dsc, '-1') <> NVL(tgt.Discount_Dsc, '-1')
         OR NVL(src.Discount_Level_Txt, '-1') <> NVL(tgt.Discount_Level_Txt, '-1')
         OR NVL(src.Savings_Category_Id, '-1') <> NVL(tgt.Savings_Category_Id, '-1')
         OR NVL(src.Min_Purchase_Qty, '-1') <> NVL(tgt.Min_Purchase_Qty, '-1')
         OR NVL(src.Discount_Amt, '-1') <> NVL(tgt.Discount_Amt, '-1')
         OR NVL(src.Discount_Qty, '-1') <> NVL(tgt.Discount_Qty, '-1')
         OR NVL(src.Non_Digital_Offer_Ind, - 1) <> NVL(tgt.Non_Digital_Offer_Ind, - 1)
		 OR NVL(src.Calculate_Usage_Ind, - 1) <> NVL(tgt.Calculate_Usage_Ind, - 1)
		 OR NVL(src.Net_Promotion_Amt, '-1') <> NVL(tgt.Net_Promotion_Amt, '-1')
		 OR NVL(src.Usage_Cnt, '-1') <> NVL(tgt.Usage_Cnt, '-1')
		 OR NVL(src.Program_Cd, '-1') <> NVL(tgt.Program_Cd, '-1')
		 OR NVL(src.Start_Dt::date,'9999-12-31') <> NVL(tgt.Start_Dt,'9999-12-31')
		 OR NVL(src.End_Dt::date,'9999-12-31') <> NVL(tgt.End_Dt,'9999-12-31')
		 OR NVL(src.External_Offer_Id, '-1') <> NVL(tgt.External_Offer_Id, '-1')
		 OR NVL(src.Promo_Cd,'-1') <> NVL(tgt.Promo_Cd,'-1')
         OR src.DW_LOGICAL_DELETE_IND <> tgt.DW_LOGICAL_DELETE_IND
		 OR NVL(src.Program_Type_Cd,'-1') <> NVL(tgt.Program_Type_Cd,'-1')
      )
      `;
try { snowflake.execute ({sqlText: sql_command});
} catch (err) { throw "Creation of EPE_Transaction_Item_Savings work table Failed with error: " + err;
}
// SCD Type2 transaction begins
// Processing Updates of Type 2 SCD
var sql_begin = "BEGIN"

var sql_updates = `UPDATE
      ${tgt_tbl} as tgt
   SET
      DW_Last_Effective_dt = CURRENT_DATE - 1,
      DW_CURRENT_VERSION_IND = FALSE,
      DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
      DW_SOURCE_UPDATE_NM = FileName
   FROM
      (
         SELECT
            Transaction_Integration_Id,
            UPC_Nbr,
            Offer_Id,
			Item_Sequence_Id,
            FileName
         FROM
            ${tgt_wrk_tbl}
         WHERE
            DML_Type = 'U'
            AND Sameday_chg_ind = 0
            AND Transaction_Integration_Id IS NOT NULL
            AND UPC_Nbr IS NOT NULL
            AND Offer_Id IS NOT NULL
			AND Item_Sequence_Id IS NOT NULL
      )
      src
   WHERE
      tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
      AND tgt.UPC_Nbr = src.UPC_Nbr
      AND tgt.Offer_Id = src.Offer_Id
	  AND tgt.Item_Sequence_Id = src.Item_Sequence_Id
      AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Sameday updates
var sql_sameday = `UPDATE
   ${tgt_tbl} as tgt
SET
   Offer_Id = src.Offer_Id,
   Item_Sequence_Id = src.Item_Sequence_Id,
   Savings_Category_Nm = src.Savings_Category_Nm,
   Discount_Message_Txt = src.Discount_Message_Txt,
   Source_system_cd = src.Source_system_cd,
   Discount_Type_Txt = src.Discount_Type_Txt,
   Discount_Dsc = src.Discount_Dsc,
   Discount_Level_Txt = src.Discount_Level_Txt,
   Savings_Category_Id = src.Savings_Category_Id,
   Min_Purchase_Qty = src.Min_Purchase_Qty,
   Discount_Amt = src.Discount_Amt,
   Discount_Qty = src.Discount_Qty,
   Non_Digital_Offer_Ind = src.Non_Digital_Offer_Ind,
   DW_Logical_delete_ind = src.DW_Logical_delete_ind,
   Calculate_Usage_Ind = src.Calculate_Usage_Ind,
	Net_Promotion_Amt=src.Net_Promotion_Amt,
    Usage_Cnt =src.Usage_Cnt,
    Program_Cd =src.Program_Cd   ,
	Start_Dt=src.Start_Dt  ,
	End_Dt =src.End_Dt,
	External_Offer_Id = src.External_Offer_Id,
	Promo_Cd = src.Promo_Cd,
	Program_Type_Cd = src.Program_Type_Cd,
   DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
   DW_SOURCE_UPDATE_NM = FileName
FROM
   (
      SELECT
         Transaction_Integration_Id,
         UPC_Nbr,
         Offer_Id,
		 Item_Sequence_Id,
         Savings_Category_Nm,
         Discount_Message_Txt,
         Source_system_cd,
         Discount_Type_Txt,
         Discount_Dsc,
         Discount_Level_Txt,
         Savings_Category_Id,
         Min_Purchase_Qty,
         Discount_Amt,
         Discount_Qty,
         Non_Digital_Offer_Ind,
		 Calculate_Usage_Ind ,
	Net_Promotion_Amt,
    Usage_Cnt ,
    Program_Cd    ,
	Start_Dt,
	End_Dt ,
         UpdatedDate,
         DW_Logical_delete_ind,
         FileName,
		 External_Offer_Id,
		 Promo_Cd,
		 Program_Type_Cd
      FROM
         ${tgt_wrk_tbl}
      WHERE
         DML_Type = 'U'
         AND Sameday_chg_ind = 1
         AND Transaction_Integration_Id IS NOT NULL
         AND UPC_Nbr IS NOT NULL
         AND Offer_Id IS NOT NULL
		 AND Item_Sequence_Id IS NOT NULL
   )
   src
WHERE
   tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
   AND tgt.UPC_Nbr = src.UPC_Nbr
   AND tgt.Offer_Id = src.Offer_Id
   AND tgt.Item_Sequence_Id = src.Item_Sequence_Id
   AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts

var sql_inserts = `INSERT INTO
   ${tgt_tbl}(Transaction_Integration_Id ,
				UPC_Nbr ,
				DW_Last_Effective_Dt ,
				DW_First_Effective_Dt ,
				Offer_Id ,
				Item_Sequence_Id,
				Savings_Category_Nm ,
				Discount_Message_Txt,
				Source_system_cd,
				Discount_Type_Txt,
				Discount_Dsc,
				Discount_Level_Txt,
				Savings_Category_Id,
				Min_Purchase_Qty,
				Discount_Amt,
				Discount_Qty ,
				Non_Digital_Offer_Ind,
				DW_CREATE_TS ,
				DW_LOGICAL_DELETE_IND ,
				DW_SOURCE_CREATE_NM ,
				DW_CURRENT_VERSION_IND,
				Calculate_Usage_Ind ,
	Net_Promotion_Amt,
    Usage_Cnt ,
    Program_Cd    ,
	Start_Dt,
	End_Dt,
	External_Offer_Id,
	Promo_Cd,
    Program_Type_Cd	)
   SELECT DISTINCT
      Transaction_Integration_Id,
      UPC_Nbr,

      '31-DEC-9999',
      CURRENT_DATE,
      Offer_Id,
	  Item_Sequence_Id,
      Savings_Category_Nm,
      Discount_Message_Txt,
      Source_system_cd,
      Discount_Type_Txt,
      Discount_Dsc,
      Discount_Level_Txt,
      Savings_Category_Id,
      Min_Purchase_Qty,
      Discount_Amt,
      Discount_Qty,
      Non_Digital_Offer_Ind,
      CURRENT_TIMESTAMP,
      DW_Logical_delete_ind,
      FileName,
      TRUE,
	  Calculate_Usage_Ind ,
	Net_Promotion_Amt,
    Usage_Cnt ,
    Program_Cd    ,
	Start_Dt,
	End_Dt ,
	External_Offer_Id,
	Promo_Cd,
	Program_Type_Cd
   FROM
      ${tgt_wrk_tbl}
   WHERE
      Sameday_chg_ind = 0
      AND Transaction_Integration_Id IS NOT NULL
      AND UPC_Nbr IS NOT NULL
      AND Offer_Id IS NOT NULL
	  AND Item_Sequence_Id IS NOT NULL`;

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
var sql_exceptions = `INSERT INTO ${tgt_exp_tbl}
   select Distinct
      Transaction_Integration_Id,
      UPC_Nbr,

      Offer_Id,
	  Item_Sequence_Id,
      Savings_Category_Nm,
      Discount_Message_Txt,
      Source_system_cd,
      Discount_Type_Txt,
      Discount_Dsc,
      Discount_Level_Txt,
      Savings_Category_Id,
      Min_Purchase_Qty,
      Discount_Amt,
      Discount_Qty,
      Non_Digital_Offer_Ind,
      TERMINALNUMBER,
      TRANSACTIONNUMBER,
      UpdatedDate,
      FileName,
      DML_Type,
      Sameday_chg_ind,
      CASE
         WHEN
            Transaction_Integration_Id is NULL
         THEN
            'Transaction_Integration_Id is NULL'
      END
      AS Exception_Reason, CURRENT_TIMESTAMP AS DW_CREATE_TS,
		Calculate_Usage_Ind ,
	Net_Promotion_Amt,
    Usage_Cnt ,
    Program_Cd    ,
	Start_Dt,
	End_Dt,
	External_Offer_Id,
	      TO_TIMESTAMP_NTZ (TRANSACTIONTIMESTAMP) AS TRANSACTIONTIMESTAMP,
		  Promo_Cd,
		  Program_Type_Cd
   FROM
      ${tgt_wrk_tbl}
   WHERE
      Transaction_Integration_Id IS NULL
      or UPC_Nbr IS NULL
      or Offer_Id IS NULL
	  or Item_Sequence_Id IS NULL`;


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


// ************** Load for EPE_Transaction_Item_Savings table ENDs *****************
