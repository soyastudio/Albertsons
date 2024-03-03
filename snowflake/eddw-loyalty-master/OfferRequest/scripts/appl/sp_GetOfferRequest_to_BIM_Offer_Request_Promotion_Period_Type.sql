--liquibase formatted sql
--changeset SYSTEM:sp_GetOfferRequest_to_BIM_Offer_Request_Promotion_Period_Type runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETOFFERREQUEST_TO_BIM_OFFER_REQUEST_PROMOTION_PERIOD_TYPE(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  
    
	var cnf_db = CNF_DB;
	var wrk_schema = WRK_SCHEMA;
	var cnf_schema = CNF_SCHEMA;
	var src_wrk_tbl = SRC_WRK_TBL;
	
	// **************	Load for Offer_Request_Promotion_Period_Type table BEGIN *****************
	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Promotion_Period_Type_wrk";
	var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request_Promotion_Period_Type";
	var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Promotion_Period_Type_Exceptions";
	var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as 
					WITH src_wrk_tbl_recs as
					(	SELECT DISTINCT OfferRequestId AS Offer_Request_Id,    
										PromotionPeriodId AS Promotion_Period_Id,
										PromotionPeriodNm AS Promotion_Period_Nm,
										PromotionWeekId AS Promotion_Week_Id,
										PromotionStartDt AS Promotion_Start_Dt,
										PromotionEndDt as Promotion_End_Dt,
										CreationDt,
										actiontypecd,
										FileName  ,
							Row_number() OVER ( partition BY OfferRequestId, PromotionPeriodId ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
						FROM ` + src_wrk_tbl +`
					)
					SELECT 	src.Offer_Request_Id,
							src.Promotion_Period_Id,
							src.Promotion_Period_Nm,
							src.Promotion_Week_Id,
							src.Promotion_Start_Dt,
							src.Promotion_End_Dt,
							src.dw_logical_delete_ind, 
							src.FileName 
							,CASE WHEN (tgt.Offer_Request_Id IS NULL AND tgt.Promotion_Period_Id IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
							,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
					FROM   (SELECT 	Offer_Request_Id,
									Promotion_Period_Id,
									Promotion_Period_Nm,
									Promotion_Week_Id,
									Promotion_Start_Dt,
									Promotion_End_Dt,
									creationdt ,
									
									FALSE AS DW_Logical_delete_ind,
									FileName 
							FROM	src_wrk_tbl_recs
							WHERE  rn = 1
							 
							
							AND UPPER(ActionTypeCd) <> 'DELETE'
					) src 
					LEFT JOIN 	(SELECT tgt.Offer_Request_Id ,
										tgt.Promotion_Period_Id ,
										tgt.Promotion_Period_Nm,
										tgt.Promotion_Week_Id,
										tgt.Promotion_Start_Dt,
										tgt.Promotion_End_Dt,
										tgt.dw_logical_delete_ind, 
										tgt.dw_first_effective_dt 
								FROM ` + tgt_tbl + ` tgt
								WHERE DW_CURRENT_VERSION_IND = TRUE
					) tgt 
						ON tgt.Offer_Request_Id = src.Offer_Request_Id 
						AND tgt.Promotion_Period_Id = src.Promotion_Period_Id	
						WHERE  	(tgt.Offer_Request_Id IS NULL AND tgt.Promotion_Period_Id IS NULL)
						OR 	
						(      	NVL(src.Promotion_Period_Nm,'-1') <> NVL(tgt.Promotion_Period_Nm,'-1')
						OR		NVL(src.Promotion_Week_Id,'-1') <> NVL(tgt.Promotion_Week_Id,'-1')
						OR		NVL(src.Promotion_Start_Dt,'-1') <> NVL(tgt.Promotion_Start_Dt,'-1')
						OR		NVL(src.Promotion_End_Dt,'-1') <> NVL(tgt.Promotion_End_Dt,'-1')
						OR 		src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
						)
					UNION ALL
					SELECT 	tgt.Offer_Request_Id
                            ,tgt.Promotion_Period_Id
                            ,tgt.Promotion_Period_Nm
							,tgt.Promotion_Week_Id
                            ,tgt.Promotion_Start_Dt
							,tgt.Promotion_End_Dt
							,TRUE AS DW_Logical_delete_ind
							,src.Filename
							,'U' as DML_Type
                            ,CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
					FROM ` + tgt_tbl + ` tgt
					 inner join src_wrk_tbl_recs src on src.Offer_Request_Id = tgt.Offer_Request_Id
					
					WHERE	DW_CURRENT_VERSION_IND = TRUE
					and rn = 1
					AND		upper(ActionTypeCd) = 'DELETE'
					AND		DW_LOGICAL_DELETE_IND = FALSE
					AND		(tgt.Offer_Request_Id) in 
							(
								SELECT 	DISTINCT Offer_Request_Id
								FROM	src_wrk_tbl_recs src
								WHERE 	rn = 1
								AND		upper(ActionTypeCd) = 'DELETE'
								AND		Offer_Request_Id is not null
								
							)
					`;
	
	try { 
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Offer_Request_Promotion_Period_Type work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
	
	//SCD Type2 transaction begins
    var sql_begin = "BEGIN"
	var sql_updates = `// Processing Updates of Type 2 SCD
						UPDATE ` + tgt_tbl + ` as tgt
						SET		 DW_Last_Effective_dt = CURRENT_DATE-1
								,DW_CURRENT_VERSION_IND = FALSE
								,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								,DW_SOURCE_UPDATE_NM = FileName
						FROM	(	SELECT 	 Offer_Request_Id
                                            ,Promotion_Period_Id
											,FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND		Sameday_chg_ind = 0
									AND 	Offer_Request_Id is not NULL
									AND 	Promotion_Period_Id is not NULL
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id
						AND		tgt.Promotion_Period_Id = src.Promotion_Period_Id
					    AND		tgt.DW_CURRENT_VERSION_IND = TRUE`;
						
    var sql_sameday = `// Processing Sameday updates
						UPDATE ` + tgt_tbl + ` as tgt
						SET		 Promotion_Period_Nm = src.Promotion_Period_Nm
								,Promotion_Week_Id = src.Promotion_Week_Id
								,Promotion_Start_Dt= src.Promotion_Start_Dt
								,Promotion_End_Dt= src.Promotion_End_Dt
								,DW_Logical_delete_ind = src.DW_Logical_delete_ind
								,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								,DW_SOURCE_UPDATE_NM = FileName
						FROM	(	SELECT 	 Offer_Request_Id,
                                            Promotion_Period_Id,
                                            Promotion_Period_Nm,
											Promotion_Week_Id,
											Promotion_Start_Dt,
											Promotion_End_Dt,
											DW_Logical_delete_ind,
											FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND		Sameday_chg_ind = 1
									AND		Offer_Request_Id is not NULL
									AND		Promotion_Period_Id is not NULL
								
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id
						AND		tgt.Promotion_Period_Id = src.Promotion_Period_Id
					    AND		tgt.DW_CURRENT_VERSION_IND = TRUE`;

	// Processing Inserts
	
	var sql_inserts = `INSERT INTO ` + tgt_tbl + `
						(	 Offer_Request_Id
						    ,Promotion_Period_Id
							, DW_First_Effective_Dt     
							, DW_Last_Effective_Dt 
							,Promotion_Period_Nm
							,Promotion_Week_Id
							,Promotion_Start_Dt
							,Promotion_End_Dt
							, DW_CREATE_TS
							, DW_LOGICAL_DELETE_IND
							, DW_SOURCE_CREATE_NM
							, DW_CURRENT_VERSION_IND
						)
						SELECT 	Offer_Request_Id
						        ,Promotion_Period_Id
								, CURRENT_DATE
								, '31-DEC-9999'
								,Promotion_Period_Nm
								,Promotion_Week_Id
								,Promotion_Start_Dt
							    ,Promotion_End_Dt
								, CURRENT_TIMESTAMP
								, DW_Logical_delete_ind
								, FileName
								, TRUE
						FROM	`+ tgt_wrk_tbl +`	
						WHERE	Sameday_chg_ind = 0
						AND		Offer_Request_Id is not null
						AND		Promotion_Period_Id is not null`;
						
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
        return "Loading of Offer_Request_Promotion_Period_Type table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
		var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;
		
		var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl +`
		                                select Distinct
												 Offer_Request_Id,
												    Promotion_Period_Id,
													Promotion_Period_Nm,
													Promotion_Week_Id,
													Promotion_Start_Dt,
													Promotion_End_Dt,
													FileName ,
													DW_Logical_delete_ind,
												    DML_Type,
												    Sameday_chg_ind
                                                ,CASE 	WHEN Offer_Request_Id is NULL THEN 'Offer_Request_Id is NULL'
														WHEN Promotion_Period_Id is NULL THEN 'Promotion_Period_Id is NULL'
														ELSE NULL END AS Exception_Reason
												,CURRENT_TIMESTAMP AS DW_CREATE_TS
                                            FROM `+ tgt_wrk_tbl +` 
											WHERE Offer_Request_Id IS NULL
											OR Promotion_Period_Id IS NULL
										
												`;
	
    try {
	
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
        return "Insert into tgt Exception table "+ tgt_exp_tbl +" Failed with error: " + err;   // Return a error message.
        }
		
	// **************	Load for Offer_Request_Promotion_Period_Type table table ENDs *****************
	
	
$$;
