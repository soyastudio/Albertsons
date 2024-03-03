
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_RETAIL;
var lkp_schema = C_RETAIL;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Header_Savings_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.EPE_Transaction_Header_Savings`;
var lkp_tbl =`${cnf_db}.${lkp_schema}.EPE_Transaction_Header`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Header_Savings_Exceptions`;



// ************** Load for EPE_Transaction_Header_Savings table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.




var sql_command = `Create or replace transient table  ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT 
                                                                 Offer_Id 
                                                                ,Saving_Dsc
                                                                ,Source_System_Cd
                                                               --,Points_Burned_Nbr 
                                                                --,Points_Earned_Nbr 
                                                                ,Redemption_Amt 
                                                                ,Redemption_Cnt
                                                                --,Updated_Dt
																,Cast(Non_Digital_Offer_Ind as Boolean) as Non_Digital_Offer_Ind
																,Cast(calculate_usage_ind as Boolean) as calculate_usage_ind
																,Discount_Level_Txt
																,Discount_Message_Txt
																,Discount_Type_Txt
																,Net_Promotion_Amt
																,Savings_Category_Id
																,Savings_Category_Nm
																,Usage_Cnt
																,Program_Cd
																,Start_Dt
																,End_Dt
																,TERMINALNUMBER
                                                                ,TRANSACTIONNUMBER
                                                                ,TRANSACTIONTIMESTAMP
                                                                ,UpdatedDate
																,External_Offer_Id
																,Promo_Cd
                                                                ,filename
																,Program_Type_Cd
                                                                ,Row_number() OVER ( partition BY TERMINALNUMBER,TRANSACTIONNUMBER,TRANSACTIONTIMESTAMP,Offer_Id ORDER BY 
                                                                 To_timestamp_ntz(UpdatedDate ) desc) AS rn
                                                                 from
                            (
                            SELECT DISTINCT 
  
                                                                         
                                                                        TxnLevel_OfferId  as Offer_Id
                                                                        ,TxnLevel_Description as Saving_Dsc
                                                                        --,TxnLevel_source as Source_Cd
																		,TRANSACTIONSOURCE AS Source_System_Cd
                                                                       -- ,Points.VALUE:burn::string as Points_Burned_Nbr
                                                                       -- ,Points.VALUE:earn::string as Points_Earned_Nbr
                                                                        ,TXNLEVEL_DISCOUNTAMOUNT as Redemption_Amt
                                                                        ,TXNLEVEL_DISCOUNTQTY as Redemption_Cnt
                                                                       -- ,CAST (TRY_TO_TIMESTAMP(updateddate) as DATE) as Updated_Dt
																		/*  case
when  (STRTOK( UpdatedDate,'+',2)<>'' and contains(STRTOK( UpdatedDate,'+',2),':')= false ) or
(contains(UpdatedDate,'T')=true and STRTOK(UpdatedDate,'-',4) <>'' and contains(STRTOK( UpdatedDate,'-',4),':')= false) 
 then to_timestamp_ltz(UpdatedDate,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(UpdatedDate)
end as Updated_Dt */
			, txnlevelsavings_nondigital as non_digital_offer_ind
			,txnlevel_calculateusage as calculate_usage_ind
																,txnlevel_discountlevel as Discount_Level_Txt
																,txnlevel_discountmessage as Discount_Message_Txt
																,txnlevel_discounttype as Discount_Type_Txt
																,txnlevel_netpromotionamount as  Net_Promotion_Amt
																,txnlevel_categoryid as Savings_Category_Id
																,txnlevel_categoryname as Savings_Category_Nm
																,txnlevel_usagecount as Usage_Cnt
																,txnlevel_programcode as Program_Cd
																--,txnlevel_startdate as Start_Dt
																,case
when  (STRTOK( txnlevel_startdate,'+',2)<>'' and contains(STRTOK( txnlevel_startdate,'+',2),':')= false ) or
(contains(txnlevel_startdate,'T')=true and STRTOK(txnlevel_startdate,'-',4) <>'' and contains(STRTOK( txnlevel_startdate,'-',4),':')= false) 
 then to_timestamp_ltz(txnlevel_startdate,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(txnlevel_startdate)
end as Start_Dt 
																
																--,txnlevel_enddate as End_Dt
																,case
when  (STRTOK( txnlevel_enddate,'+',2)<>'' and contains(STRTOK( txnlevel_enddate,'+',2),':')= false ) or
(contains(txnlevel_enddate,'T')=true and STRTOK(txnlevel_enddate,'-',4) <>'' and contains(STRTOK( txnlevel_enddate,'-',4),':')= false) 
 then to_timestamp_ltz(txnlevel_enddate,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(txnlevel_enddate)
end as End_Dt 
                                                                        ,TERMINALNUMBER
                                                                        ,try_to_numeric(TRANSACTIONNUMBER) as TRANSACTIONNUMBER
                                                                       -- ,TRANSACTIONTIMESTAMP,
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
,
                                                                       -- ,UpdatedDate
case
when  (STRTOK( UpdatedDate,'+',2)<>'' and contains(STRTOK( UpdatedDate,'+',2),':')= false ) or
(contains(UpdatedDate,'T')=true and STRTOK(UpdatedDate,'-',4) <>'' and contains(STRTOK( UpdatedDate,'-',4),':')= false)
then to_timestamp_ltz(UpdatedDate,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(UpdatedDate)
end as UpdatedDate
											
																	   
																	    ,TXNLEVEL_EXTERNALOFFERID as External_Offer_Id
																		,TxnLevel_promoCode as Promo_Cd
                                                                        ,filename 
																		,TxnLevel_ProgramType as Program_Type_Cd
FROM ${src_wrk_tbl}
Union All
Select Distinct
                                                                
                                                                Offer_Id 
                                                                ,Saving_Dsc
                                                                ,Source_System_Cd
                                                               -- ,Points_Burned_Nbr 
                                                                --,Points_Earned_Nbr 
                                                                ,Redemption_Amt 
                                                                ,Redemption_Cnt
                                                               -- ,Updated_Dt
																,non_digital_offer_ind
																,calculate_usage_ind
																,Discount_Level_Txt
																,Discount_Message_Txt
																,Discount_Type_Txt
																,Net_Promotion_Amt
																,Savings_Category_Id
																,Savings_Category_Nm
																,Usage_Cnt
																,Program_Cd
																,cast(Start_Dt as varchar)
																,cast(End_Dt as varchar)
                                                                ,TERMINALNUMBER
                                                                ,TRANSACTIONNUMBER
                                                                ,TRANSACTIONTIMESTAMP
                                                                ,cast(UpdatedDate as varchar)
																,External_Offer_Id
																,Promo_Cd
                                                                ,filename
																,Program_Type_Cd
FROM ${tgt_exp_tbl}
                            ) 
                       ) 
select 
 src.Transaction_Integration_Id
,src.Offer_Id
,src.Saving_Dsc
,src.Source_System_Cd
--,src.Points_Burned_Nbr 
--,src.Points_Earned_Nbr 
,src.Redemption_Amt
,src.Redemption_Cnt
--,src.Updated_Dt
,src.non_digital_offer_ind
,src.calculate_usage_ind
																,src.Discount_Level_Txt
																,src.Discount_Message_Txt
																,src.Discount_Type_Txt
																,src.Net_Promotion_Amt
																,src.Savings_Category_Id
																,src.Savings_Category_Nm
																,src.Usage_Cnt
																,src.Program_Cd
																,src.Start_Dt
																,src.End_Dt
,src.TERMINALNUMBER
,src.TRANSACTIONNUMBER
,src.TRANSACTIONTIMESTAMP
,src.DW_Logical_delete_ind
,src.UpdatedDate
,src.External_Offer_Id
,src.Promo_Cd
,src.filename
,src.Program_Type_Cd
,CASE WHEN tgt.Transaction_Integration_Id IS NULL  AND tgt.Offer_Id IS NULL THEN 'I' ELSE 'U' END AS DML_Type
,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
from 
(
select
 LKP_EPE_Transaction_Header.Transaction_Integration_Id AS Transaction_Integration_Id
,src1.Offer_Id
,src1.Saving_Dsc
,src1.Source_System_Cd
--,src1.Points_Burned_Nbr 
--,src1.Points_Earned_Nbr 
,src1.Redemption_Amt
,src1.Redemption_Cnt
--,src1.Updated_Dt
,src1.non_digital_offer_ind
,src1.calculate_usage_ind
																,src1.Discount_Level_Txt
																,src1.Discount_Message_Txt
																,src1.Discount_Type_Txt
																,src1.Net_Promotion_Amt
																,src1.Savings_Category_Id
																,src1.Savings_Category_Nm
																,src1.Usage_Cnt
																,src1.Program_Cd
																,src1.Start_Dt
																,src1.End_Dt
,src1.TERMINALNUMBER
,src1.TRANSACTIONNUMBER
,src1.TRANSACTIONTIMESTAMP
,src1.DW_Logical_delete_ind
,src1.UpdatedDate
,src1.External_Offer_Id
,src1.Promo_Cd
,src1.filename
,src1.Program_Type_Cd
from
(
select
Offer_Id 
,Saving_Dsc
,Source_System_Cd
--,Points_Burned_Nbr 
--,Points_Earned_Nbr 
,Redemption_Amt 
,Redemption_Cnt
--,Updated_Dt
,non_digital_offer_ind
,calculate_usage_ind
																,Discount_Level_Txt
																,Discount_Message_Txt
																,Discount_Type_Txt
																,Net_Promotion_Amt
																,Savings_Category_Id
																,Savings_Category_Nm
																,Usage_Cnt
																,Program_Cd
																,Start_Dt
																,End_Dt
,TERMINALNUMBER
,TRANSACTIONNUMBER
,TRANSACTIONTIMESTAMP
,UpdatedDate
,false AS DW_Logical_delete_ind
,External_Offer_Id
,Promo_Cd
,Program_Type_Cd
,Filename
FROM src_wrk_tbl_recs
where rn=1
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
--AND src1.Facility_Integration_ID = LKP_EPE_Transaction_Header.Facility_Integration_ID
)src
LEFT JOIN (SELECT
tgt.Transaction_Integration_Id
,tgt.Offer_Id
,tgt.Saving_Dsc
,tgt.Source_System_Cd
--,tgt.Points_Burned_Nbr 
--,tgt.Points_Earned_Nbr 
,tgt.Redemption_Amt
,tgt.Redemption_Cnt 
--,tgt.Updated_Dt
,tgt.non_digital_offer_ind
,tgt.calculate_usage_ind
																,tgt.Discount_Level_Txt
																,tgt.Discount_Message_Txt
																,tgt.Discount_Type_Txt
																,tgt.Net_Promotion_Amt
																,tgt.Savings_Category_Id
																,tgt.Savings_Category_Nm
																,tgt.Usage_Cnt
																,tgt.Program_Cd
																,tgt.Start_Dt
																,tgt.End_Dt
																,tgt.External_Offer_Id
																,tgt.Promo_Cd
,tgt.dw_logical_delete_ind
,tgt.dw_first_effective_dt
,tgt.Program_Type_Cd
FROM ${tgt_tbl} tgt
WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
) as tgt 
ON tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
AND tgt.Offer_Id = src.Offer_Id
 
where (tgt.Transaction_Integration_Id IS NULL  AND tgt.Offer_Id IS NULL)
OR ( 
NVL(src.Saving_Dsc,'-1') <> NVL(tgt.Saving_Dsc,'-1')
                                                        OR NVL(src.Source_System_Cd,'-1') <> NVL(tgt.Source_System_Cd,'-1')
                                                        --OR NVL(src.Points_Burned_Nbr,'-1') <> NVL(tgt.Points_Burned_Nbr,'-1')
                                                       -- OR NVL(src.Points_Earned_Nbr,'-1') <> NVL(tgt.Points_Earned_Nbr,'-1')
                                                        OR NVL(src.Redemption_Amt,'-1') <> NVL(tgt.Redemption_Amt,'-1')
                                                        OR NVL(src.Redemption_Cnt,'-1') <> NVL(tgt.Redemption_Cnt,'-1')
                                                        --OR NVL(src.Updated_Dt,'9999-12-31') <> NVL(tgt.Updated_Dt,'9999-12-31')
														OR NVL(src.non_digital_offer_ind,-1) <>NVL(tgt.non_digital_offer_ind,-1)
														OR NVL(src.calculate_usage_ind,-1) <>NVL(tgt.calculate_usage_ind,-1)
														OR NVL(src.Discount_Level_Txt,'-1') <> NVL(tgt.Discount_Level_Txt,'-1')
                                                        OR NVL(src.Discount_Message_Txt,'-1') <> NVL(tgt.Discount_Message_Txt,'-1')
                                                        OR NVL(src.Discount_Type_Txt,'-1') <> NVL(tgt.Discount_Type_Txt,'-1')
                                                        OR NVL(src.Net_Promotion_Amt,'-1') <> NVL(tgt.Net_Promotion_Amt,'-1')
                                                        OR NVL(src.Savings_Category_Id,'-1') <> NVL(tgt.Savings_Category_Id,'-1')
                                                        OR NVL(src.Savings_Category_Nm,'-1') <> NVL(tgt.Savings_Category_Nm,'-1')
                                                        OR NVL(src.Usage_Cnt,'-1') <> NVL(tgt.Usage_Cnt,'-1')
                                                        OR NVL(src.Start_Dt::date,'9999-12-31') <> NVL(tgt.Start_Dt,'9999-12-31')
														OR NVL(src.End_Dt::date,'9999-12-31') <> NVL(tgt.End_Dt,'9999-12-31')
														OR NVL(src.External_Offer_Id,'-1') <> NVL(tgt.External_Offer_Id,'-1')
														OR NVL(src.Promo_Cd,'-1') <> NVL(tgt.Promo_Cd,'-1')
														OR NVL(src.Program_Type_Cd,'-1') <> NVL(tgt.Program_Type_Cd,'-1')
                                                        
OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
)
`;
try {
        
        snowflake.execute ({sqlText: sql_command});
            
        }
    catch (err)  {
        throw "Creation of EPE_Transaction_Header_Savings work table Failed with error: "+ err;   // Return a error message.
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
,FileName
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 0
AND Transaction_Integration_Id  IS NOT NULL
AND Offer_Id IS NOT NULL
) src
WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id  
AND tgt.Offer_Id = src.Offer_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE
AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;
   

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
SET  Saving_Dsc = src.Saving_Dsc
    ,Source_System_Cd = src.Source_System_Cd
    --,Points_Burned_Nbr = src.Points_Burned_Nbr
    --,Points_Earned_Nbr = src.Points_Earned_Nbr 
    ,Redemption_Amt = src.Redemption_Amt
    ,Redemption_Cnt = src.Redemption_Cnt
    --,Updated_Dt = src.Updated_Dt
	,non_digital_offer_ind = src.non_digital_offer_ind
	,calculate_usage_ind  = src.calculate_usage_ind
	,Discount_Level_Txt = src.Discount_Level_Txt
																,Discount_Message_Txt =src.Discount_Message_Txt
																,Discount_Type_Txt =src.Discount_Type_Txt
																,Net_Promotion_Amt = src.Net_Promotion_Amt
																,Savings_Category_Id=src.Savings_Category_Id
																,Savings_Category_Nm = src.Savings_Category_Nm
																,Usage_Cnt =src.Usage_Cnt
																,Program_Cd =src.Program_Cd
																,Start_Dt =src.Start_Dt
																,End_Dt = src.End_Dt
																,External_Offer_Id = src.External_Offer_Id
																,Promo_Cd = src.Promo_Cd
																,Program_Type_Cd = src.Program_Type_Cd
,DW_Logical_delete_ind = src.DW_Logical_delete_ind
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = FileName
FROM ( SELECT
Transaction_Integration_Id
,Offer_Id 
,Saving_Dsc
,Source_System_Cd
--,Points_Burned_Nbr 
--,Points_Earned_Nbr 
,Redemption_Amt 
,Redemption_Cnt
--,Updated_Dt
,non_digital_offer_ind
,calculate_usage_ind
																,Discount_Level_Txt
																,Discount_Message_Txt
																,Discount_Type_Txt
																,Net_Promotion_Amt
																,Savings_Category_Id
																,Savings_Category_Nm
																,Usage_Cnt
																,Program_Cd
																,Start_Dt
																,End_Dt
,DW_Logical_delete_ind
,UpdatedDate
,External_Offer_Id
,Promo_Cd
,Program_Type_Cd
,FileName
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 1
AND Transaction_Integration_Id  IS NOT NULL
AND Offer_Id IS NOT NULL
) src
WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
AND tgt.Offer_Id = tgt.Offer_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
(Transaction_Integration_Id ,
Offer_Id,
DW_Last_Effective_Dt ,
DW_First_Effective_Dt ,
Saving_Dsc,
Source_System_Cd,
--Points_Burned_Nbr, 
--Points_Earned_Nbr ,
Redemption_Amt ,
Redemption_Cnt,
--Updated_Dt,
non_digital_offer_ind,
DW_CREATE_TS ,
DW_LOGICAL_DELETE_IND ,
DW_SOURCE_CREATE_NM ,
DW_CURRENT_VERSION_IND,
Calculate_Usage_Ind   ,  
Discount_Message_Txt    ,
Net_Promotion_Amt   ,
Usage_Cnt               ,
Discount_Type_Txt       ,
Discount_Level_Txt      ,
Savings_Category_Id     ,
Savings_Category_Nm     ,
Program_Cd              ,
Start_Dt                ,
End_Dt                  ,
External_Offer_Id ,
Promo_Cd,
Program_Type_Cd
)
   SELECT DISTINCT
Transaction_Integration_Id 
,Offer_Id
,'31-DEC-9999'
,CURRENT_DATE
,Saving_Dsc
,Source_System_Cd
--,Points_Burned_Nbr 
--,Points_Earned_Nbr 
,Redemption_Amt
,Redemption_Cnt
--,Updated_Dt
,non_digital_offer_ind
,CURRENT_TIMESTAMP
,DW_Logical_delete_ind
,FileName
,TRUE,
Calculate_Usage_Ind   ,  
Discount_Message_Txt    ,
Net_Promotion_Amt   ,
Usage_Cnt               ,
Discount_Type_Txt       ,
Discount_Level_Txt      ,
Savings_Category_Id     ,
Savings_Category_Nm     ,
Program_Cd              ,
Start_Dt                ,
End_Dt        			,
External_Offer_Id,
Promo_Cd,
Program_Type_Cd
FROM ${tgt_wrk_tbl}
WHERE Sameday_chg_ind = 0
AND Transaction_Integration_Id  IS NOT NULL
AND Offer_Id IS NOT NULL
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
        return `Loading of EPE_Transaction_Header_Savings table ${tgt_tbl} Failed with error:  ${err}`;   // Return a error message.
       
}
 var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;

var sql_exceptions = `INSERT INTO  ${tgt_exp_tbl}  
select Distinct
Transaction_Integration_Id,
Offer_Id ,
Saving_Dsc,
Source_System_Cd,
--Points_Burned_Nbr, 
--Points_Earned_Nbr,
Redemption_Amt,
Redemption_Cnt,
--Updated_Dt,
non_digital_offer_ind,
TERMINALNUMBER,
TRANSACTIONNUMBER,
UpdatedDate,
FileName,
DML_Type,
Sameday_chg_ind,
CASE WHEN Transaction_Integration_Id is NULL THEN 'Transaction_Integration_Id is NULL'
     WHEN Offer_Id IS NULL THEN 'Offer_Id IS NULL'
END AS Exception_Reason,
CURRENT_TIMESTAMP AS DW_CREATE_TS,
Calculate_Usage_Ind   ,  
Discount_Message_Txt    ,
Net_Promotion_Amt   ,
Usage_Cnt               ,
Discount_Type_Txt       ,
Discount_Level_Txt      ,
Savings_Category_Id     ,
Savings_Category_Nm     ,
Program_Cd              ,
Start_Dt                ,
End_Dt        			,
External_Offer_Id,
TO_TIMESTAMP_NTZ (TRANSACTIONTIMESTAMP) AS TRANSACTIONTIMESTAMP,
Promo_Cd,
Program_Type_Cd
FROM  ${tgt_wrk_tbl}
WHERE Transaction_Integration_Id IS NULL 
or Offer_Id IS NULL
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


// ************** Load for EPE_Transaction_Header_Savings table ENDs *****************