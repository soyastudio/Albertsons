 
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_RETAIL;
var lkp_schema = C_RETAIL;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Usage_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.EPE_Transaction_Usage`;
var lkp_tb1 =`${cnf_db}.${lkp_schema}.EPE_Transaction_Header`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.EPE_Transaction_Usage_Exceptions`;

// **************	Load for EPE_Transaction_Usage table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 




var sql_command = `Create or replace table   ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT
							row_number() over (PARTITION BY TRANSACTIONNUMBER,TERMINALNUMBER,TRANSACTIONTIMESTAMP ORDER BY (Action_Type_Cd,offer_id)) as Sequence_Nbr, 
						 Offer_Id,
						CASE WHEN Offer_Id IS NULL THEN  NULL ELSE Action_Type_Cd END as Action_Type_Cd,
						External_Offer_Id,
						TERMINALNUMBER,
						TRANSACTIONNUMBER,
						TRANSACTIONTIMESTAMP,
						UpdatedDate,
                        
                        
                        
                        
						FileName,
						Row_number() OVER ( partition BY TERMINALNUMBER,TRANSACTIONNUMBER,TRANSACTIONTIMESTAMP,offer_id ORDER BY to_timestamp_ntz(UpdatedDate) desc) AS rn
				from
                            	(
                                ((SELECT DISTINCT
								 Added_offer.VALUE:offerId::string  as Offer_Id
								 ,'Added' AS Action_Type_Cd
								 ,Added_offer.VALUE:externalOfferId::string  as External_Offer_Id
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
,
								 --,UpdatedDate
							case
when  (STRTOK( UpdatedDate,'+',2)<>'' and contains(STRTOK( UpdatedDate,'+',2),':')= false ) or
(contains(UpdatedDate,'T')=true and STRTOK(UpdatedDate,'-',4) <>'' and contains(STRTOK( UpdatedDate,'-',4),':')= false) 
 then to_timestamp_ltz(UpdatedDate,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(UpdatedDate)
end as UpdatedDate 
								 
								 
								 
								 
								 ,filename
								 
								 FROM ${src_wrk_tbl}
								 ,LATERAL FLATTEN(input => offersAdded, outer => TRUE) as Added_offer)
						  UNION ALL
								  (SELECT DISTINCT
								  Removed_Offers.VALUE:offerId::string as Offer_Id
								  ,'Removed' AS Action_Type_Cd
								  ,Removed_Offers.VALUE:externalOfferId::string as External_Offer_Id
								  ,TERMINALNUMBER
								  ,try_to_numeric(TRANSACTIONNUMBER) as TRANSACTIONNUMBER
								  --,TRANSACTIONTIMESTAMP,
								  ,case
when  (STRTOK( TransactionTimestamp,'+',2)<>'' and contains(STRTOK( TransactionTimestamp,'+',2),':')= false ) or
(contains(TransactionTimestamp,'T')=true and STRTOK(TransactionTimestamp,'-',4) <>'' and contains(STRTOK( TransactionTimestamp,'-',4),':')= false)
  then to_timestamp_ltz(TransactionTimestamp,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
--when TransactionTimestamp like '%T%' then to_timestamp_ltz(TransactionTimestamp,'YYYY-MM-DD HH24:MI:SS')
else to_timestamp_ltz(TransactionTimestamp)
end as TRANSACTIONTIMESTAMP
,
				            case
when  (STRTOK( UpdatedDate,'+',2)<>'' and contains(STRTOK( UpdatedDate,'+',2),':')= false ) or
(contains(UpdatedDate,'T')=true and STRTOK(UpdatedDate,'-',4) <>'' and contains(STRTOK( UpdatedDate,'-',4),':')= false) 
 then to_timestamp_ltz(UpdatedDate,'YYYY-MM-DDTHH24:MI:SS.FF8TZHTZM')
else to_timestamp_ltz(UpdatedDate)
end as UpdatedDate 
                                  
                                  
								  
								-- ,UpdatedDate
								 ,filename
								
								 FROM ${src_wrk_tbl}
								,LATERAL FLATTEN(input => offersRemoved, outer => TRUE) as Removed_Offers))
								
						UNION ALL
							SELECT DISTINCT
							 Offer_Id,
							 Action_Type_Cd,
							 External_Offer_Id,
							TERMINALNUMBER,
							TRANSACTIONNUMBER,
							cast(TRANSACTIONTIMESTAMP as Varchar),
							UpdatedDate,
							 FileName
							
						FROM ${tgt_exp_tbl}
                          )      
						  )                    
			            SELECT
						 src.Transaction_Integration_Id
						,src.Sequence_Nbr
						,src.Offer_Id
						,src.Action_Type_Cd
						,src.External_Offer_Id
						,src.TERMINALNUMBER
						,src.TRANSACTIONNUMBER
						,src.TRANSACTIONTIMESTAMP
						,src.DW_Logical_delete_ind
						,src.UpdatedDate
						,src.filename
						,CASE WHEN(tgt.Transaction_Integration_Id is NULL) THEN 'I' ELSE 'U' END as DML_Type
						,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
				        from
                        (
						select
						 LKP_EPE_Transaction_Header.Transaction_Integration_Id AS Transaction_Integration_Id
						,src1.Sequence_Nbr
						,src1.Offer_Id
						,src1.Action_Type_Cd
						,src1.External_Offer_Id
						,src1.TERMINALNUMBER
						,src1.TRANSACTIONNUMBER
						,src1.TRANSACTIONTIMESTAMP
						,src1.DW_Logical_delete_ind
						,src1.updateddate
						,src1.FileName
						from
					(SELECT distinct
						Sequence_Nbr
						,Offer_Id
						,Action_Type_Cd
						,External_Offer_Id
						,TERMINALNUMBER
						,TRANSACTIONNUMBER
						,TRANSACTIONTIMESTAMP
						,updateddate
						,false AS DW_Logical_delete_ind
						,FileName
						FROM   src_wrk_tbl_recs --src1
						WHERE rn = 1
													
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
							
						)src
						
						LEFT JOIN 
                          (SELECT  DISTINCT
						 tgt.Transaction_Integration_Id
						,tgt.Sequence_Nbr
						,tgt.Offer_Id
						,tgt.Action_Type_Cd
						,tgt.External_Offer_Id
						,tgt.dw_logical_delete_ind
						,tgt.dw_first_effective_dt							   
                          FROM ${tgt_tbl} tgt 
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt 
                         ON tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
						AND tgt.Sequence_Nbr = src.Sequence_Nbr
						WHERE  (tgt.Transaction_Integration_Id  IS NULL)
						OR (NVL(src.Offer_Id,'-1') <> NVL(tgt.Offer_Id,'-1')
						OR NVL(src.Action_Type_Cd,'-1') <> NVL(tgt.Action_Type_Cd,'-1')
						OR NVL(src.External_Offer_Id,'-1') <> NVL(tgt.External_Offer_Id,'-1')
						OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )  `;        

try {
        
        snowflake.execute ({sqlText: sql_command});
            
        }
    catch (err)  {
        return "Creation of EPE_Transaction_Usage work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
					         Sequence_Nbr,
					         filename
                            FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U' 
                             AND Sameday_chg_ind = 0                                      
                             AND 	Transaction_Integration_Id  IS NOT NULL
					         AND Sequence_Nbr is not null
							
				             ) src
                             WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id 
							AND tgt.Sequence_Nbr = src.Sequence_Nbr
							AND tgt.DW_CURRENT_VERSION_IND = TRUE 
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE
							`;
// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET Offer_Id= src.Offer_Id,
						Action_Type_Cd = src.Action_Type_Cd,
						External_Offer_Id = src.External_Offer_Id,
					    DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					    DW_LAST_UPDATE_TS	= CURRENT_TIMESTAMP,
					    DW_SOURCE_UPDATE_NM   = FileName
					FROM
					( SELECT Transaction_Integration_Id
						,Sequence_Nbr
						,Offer_Id
						,Action_Type_Cd
						,External_Offer_Id
						,DW_Logical_delete_ind
						 ,Updateddate
						,filename
						 FROM ${tgt_wrk_tbl}
							   WHERE DML_Type = 'U'
							   AND Sameday_chg_ind = 1
							   AND Transaction_Integration_Id  IS NOT NULL
							   AND Sequence_Nbr IS NOT NULL
							   
							) src
							WHERE tgt.Transaction_Integration_Id = src.Transaction_Integration_Id
						  AND tgt.Sequence_Nbr = src.Sequence_Nbr
					      AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                  (Transaction_Integration_Id ,
						Sequence_Nbr ,
						DW_Last_Effective_Dt ,
						DW_First_Effective_Dt ,
						Offer_Id ,
						Action_Type_Cd ,
						External_Offer_Id ,
						DW_CREATE_TS ,
						DW_LOGICAL_DELETE_IND ,
						DW_SOURCE_CREATE_NM ,
						DW_CURRENT_VERSION_IND
						)
						   SELECT DISTINCT
						Transaction_Integration_Id
						,Sequence_Nbr
						,'31-DEC-9999'
						,CURRENT_DATE
						,Offer_Id
						,Action_Type_Cd
						,External_Offer_Id
						,CURRENT_TIMESTAMP
						,DW_Logical_delete_ind
						,FileName
						,TRUE
						FROM ${tgt_wrk_tbl}
						WHERE Transaction_Integration_Id  IS NOT NULL
						AND Sameday_chg_ind = 0
						`;

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
						Sequence_Nbr,
						Offer_Id,
						Action_Type_Cd,
						External_Offer_Id,
						TERMINALNUMBER,
						TRANSACTIONNUMBER,					
						
						FileName,
                        UpdatedDate,
						DML_Type,
						Sameday_chg_ind,
						CASE WHEN Transaction_Integration_Id is NULL THEN 'Transaction_Integration_Id is NULL'
						WHEN Sequence_Nbr is NULL THEN 'Sequence_Nbr  is NULL'
						END AS Exception_Reason,
					    CURRENT_TIMESTAMP AS DW_CREATE_TS,
					    TO_TIMESTAMP_NTZ (TRANSACTIONTIMESTAMP) AS TRANSACTIONTIMESTAMP
						FROM  ${tgt_wrk_tbl}
						WHERE Transaction_Integration_Id IS NULL
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
        return `Insert into tgt Exception table ${tgt_exp_tbl} Failed with error:  ${err}`;   // Return a error message.
              }


// ************** Load for EPE_Transaction_Usage table ENDs *****************