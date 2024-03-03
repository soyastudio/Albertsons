--liquibase formatted sql
--changeset SYSTEM:sp_GetOfferRequest_to_BIM_load_Offer_Request_Restriction_Type runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETOFFERREQUEST_TO_BIM_LOAD_OFFER_REQUEST_RESTRICTION_TYPE(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
     
    
	var cnf_db = CNF_DB;
	var wrk_schema = WRK_SCHEMA;
	var cnf_schema = CNF_SCHEMA;
	var src_wrk_tbl = SRC_WRK_TBL;
	
	// **************	Load for Offer_Request_Restriction_Type table BEGIN *****************
	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Restriction_Type_wrk";
	var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request_Restriction_Type";
	var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Restriction_Type_Exceptions";
	var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as 
					WITH src_wrk_tbl_recs as
					(	SELECT DISTINCT OfferRequestId As Offer_Request_Id
							,UsagelimitTypeTxt As Usage_limit_Type_Txt
							,OfferRestrictionType_LimitAmt As Limit_Amt
							,OfferRestrictionType_LimitQty As Limit_Qty
							,OfferRestrictionType_LimitVol As Limit_Vol
							,OfferRestrictionType_LimitWt As Limit_Wt
							,OfferRestrictionType_UOMCd As Unit_Of_Measure_Cd
							,OfferRestrictionType_UOMNm As Unit_Of_Measure_Nm
							,RestrictionType_Code As Restriction_Type_Cd
							,RestrictionType_Description As Restriction_Type_Dsc
							,RestrictionType_ShortDescription As Restriction_Type_Short_Dsc
					        ,UsageLimitNbr As Usage_Limit_Nbr
										,CreationDt
										,actiontypecd
										,FileName  
										,USAGELIMITPERIODNBR As USAGE_LIMIT_PERIOD_NBR
							,Row_number() OVER ( partition BY OfferRequestId, UsagelimitTypeTxt ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
						FROM ` + src_wrk_tbl +`
					)
					SELECT 	src.Offer_Request_Id
							,src.Usage_limit_Type_Txt
							,src.Limit_Amt 
							,src.Limit_Qty 
							,src.Limit_Vol 
							,src.Limit_Wt
							,src.Unit_Of_Measure_Cd
							,src.Unit_Of_Measure_Nm
							,src.Restriction_Type_Cd
							,src.Restriction_Type_Dsc
							,src.Restriction_Type_Short_Dsc
					        ,src.Usage_Limit_Nbr
							,src.dw_logical_delete_ind
							,src.FileName 
							,src.USAGE_LIMIT_PERIOD_NBR
							,CASE WHEN (tgt.Offer_Request_Id IS NULL AND tgt.Usage_limit_Type_Txt IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
							,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
					FROM   (SELECT 	Offer_Request_Id 
									,Usage_limit_Type_Txt 
									,Limit_Amt 
									,Limit_Qty 
									,Limit_Vol
									,Limit_Wt
									,Unit_Of_Measure_Cd
									,Unit_Of_Measure_Nm
									,Restriction_Type_Cd
									,Restriction_Type_Dsc
									,Restriction_Type_Short_Dsc
								    ,Usage_Limit_Nbr
									,creationdt 
									,FALSE AS DW_Logical_delete_ind
									,FileName 
									,USAGE_LIMIT_PERIOD_NBR
							FROM	src_wrk_tbl_recs
							WHERE  rn = 1
							AND UPPER(ActionTypeCd) <> 'DELETE'
					) src 
					LEFT JOIN 	(SELECT tgt.Offer_Request_Id 
										,tgt.Usage_limit_Type_Txt 
										,tgt.Limit_Amt
										,tgt.Limit_Qty 
										,tgt.Limit_Vol 
										,tgt.Limit_Wt
										,tgt.Unit_Of_Measure_Cd	
										,tgt.Unit_Of_Measure_Nm
										,tgt.Restriction_Type_Cd
										,tgt.Restriction_Type_Dsc
										,tgt.Restriction_Type_Short_Dsc
								        ,tgt.Usage_Limit_Nbr
										,tgt.dw_logical_delete_ind 
										,tgt.dw_first_effective_dt 
										,tgt.USAGE_LIMIT_PERIOD_NBR
								FROM ` + tgt_tbl + ` tgt
								WHERE DW_CURRENT_VERSION_IND = TRUE
					) tgt 
						ON tgt.Offer_Request_Id = src.Offer_Request_Id 
						AND tgt.Usage_limit_Type_Txt = src.Usage_limit_Type_Txt	
							
					WHERE  	(tgt.Offer_Request_Id IS NULL AND tgt.Usage_limit_Type_Txt IS NULL)
						OR 	
						(      NVL(src.Limit_Amt,'-1') <> NVL(tgt.Limit_Amt,'-1')
						OR 		NVL(src.Limit_Qty,'-1') <> NVL(tgt.Limit_Qty,'-1')
						OR 		NVL(src.Limit_Vol,'-1') <> NVL(tgt.Limit_Vol,'-1')
						OR 		NVL(src.Limit_Wt,'-1') <> NVL(tgt.Limit_Wt,'-1')
						OR 		NVL(src.Unit_Of_Measure_Cd,'-1') <> NVL(tgt.Unit_Of_Measure_Cd,'-1')
						OR 		NVL(src.Unit_Of_Measure_Nm,'-1') <> NVL(tgt.Unit_Of_Measure_Nm,'-1')
						OR 		NVL(src.Restriction_Type_Cd,'-1') <> NVL(tgt.Restriction_Type_Cd,'-1')
						OR 		NVL(src.Restriction_Type_Dsc,'-1') <> NVL(tgt.Restriction_Type_Dsc,'-1')
						OR 		NVL(src.Restriction_Type_Short_Dsc,'-1') <> NVL(tgt.Restriction_Type_Short_Dsc,'-1')
						OR 		NVL(src.Usage_Limit_Nbr,'-1') <> NVL(tgt.Usage_Limit_Nbr,'-1')
						OR 		NVL(src.USAGE_LIMIT_PERIOD_NBR,'-1') <> NVL(tgt.USAGE_LIMIT_PERIOD_NBR,'-1')
						OR 		src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
						)
					UNION ALL
					SELECT tgt.Offer_Request_Id
                            ,tgt.Usage_limit_Type_Txt
                            ,tgt.Limit_Amt
                            ,tgt.Limit_Qty
                            ,tgt.Limit_Vol
							,tgt.Limit_Wt
							,tgt.Unit_Of_Measure_Cd
							,tgt.Unit_Of_Measure_Nm
							,tgt.Restriction_Type_Cd
							,tgt.Restriction_Type_Dsc
							,tgt.Restriction_Type_Short_Dsc
							,tgt.Usage_Limit_Nbr
							,TRUE AS DW_Logical_delete_ind
							,src.Filename
							,tgt.USAGE_LIMIT_PERIOD_NBR
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
        return "Creation of Offer_Request_Restriction_Type work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
                                            ,Usage_limit_Type_Txt
											,FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND		Sameday_chg_ind = 0
									AND 	Offer_Request_Id is not NULL
									AND 	Usage_limit_Type_Txt is not NULL
								
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id
						AND		tgt.Usage_limit_Type_Txt = src.Usage_limit_Type_Txt
					AND		tgt.DW_CURRENT_VERSION_IND = TRUE`;
						
    var sql_sameday = `// Processing Sameday updates
						UPDATE ` + tgt_tbl + ` as tgt
						SET		Limit_Amt  = src.Limit_Amt
								,Limit_Qty = src.Limit_Qty
								,Limit_Vol = src.Limit_Vol
								,Limit_Wt = src.Limit_Wt
								,Unit_Of_Measure_Cd = src.Unit_Of_Measure_Cd
								,Unit_Of_Measure_Nm = src.Unit_Of_Measure_Nm
								,Restriction_Type_Cd = src.Restriction_Type_Cd
								,Restriction_Type_Dsc = src.Restriction_Type_Dsc
								,Restriction_Type_Short_Dsc = src.Restriction_Type_Short_Dsc
								,Usage_Limit_Nbr = src.Usage_Limit_Nbr
								,DW_Logical_delete_ind = src.DW_Logical_delete_ind
								,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								,DW_SOURCE_UPDATE_NM = FileName
								,USAGE_LIMIT_PERIOD_NBR = src.USAGE_LIMIT_PERIOD_NBR
						FROM	(	SELECT 	 Offer_Request_Id
                                            ,Usage_limit_Type_Txt
                                            ,Limit_Amt
											,Limit_Qty
                                            ,Limit_Vol
											,Limit_Wt
											,Unit_Of_Measure_Cd
											,Unit_Of_Measure_Nm
											,Restriction_Type_Cd
											,Restriction_Type_Dsc
											,Restriction_Type_Short_Dsc
											,Usage_Limit_Nbr
											,DW_Logical_delete_ind
											,FileName
											,USAGE_LIMIT_PERIOD_NBR
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND		Sameday_chg_ind = 1
									AND		Offer_Request_Id is not NULL
									AND		Usage_limit_Type_Txt is not NULL
								
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id
						AND		tgt.Usage_limit_Type_Txt = src.Usage_limit_Type_Txt
					AND		tgt.DW_CURRENT_VERSION_IND = TRUE`;

	// Processing Inserts
	
	var sql_inserts = `INSERT INTO ` + tgt_tbl + `
						(	  Offer_Request_Id
							,Usage_limit_Type_Txt 
							,Limit_Amt
							,Limit_Qty
							,Limit_Vol
							,Limit_Wt
							,Unit_Of_Measure_Cd
							,Unit_Of_Measure_Nm
							,Restriction_Type_Cd
							,Restriction_Type_Dsc
							,Restriction_Type_Short_Dsc
							,Usage_Limit_Nbr
							,DW_First_Effective_Dt     
							,DW_Last_Effective_Dt     
							,DW_CREATE_TS
							,DW_LOGICAL_DELETE_IND
							,DW_SOURCE_CREATE_NM
							,DW_CURRENT_VERSION_IND
							,USAGE_LIMIT_PERIOD_NBR
						)
						SELECT 	 Offer_Request_Id
								,Usage_limit_Type_Txt
								,Limit_Amt
								,Limit_Qty
								,Limit_Vol
								,Limit_Wt
								,Unit_Of_Measure_Cd
								,Unit_Of_Measure_Nm
								,Restriction_Type_Cd
								,Restriction_Type_Dsc
								,Restriction_Type_Short_Dsc
								,Usage_Limit_Nbr
								,CURRENT_DATE
								,'31-DEC-9999'
								,CURRENT_TIMESTAMP
								,DW_Logical_delete_ind
								,FileName
								,TRUE
								,USAGE_LIMIT_PERIOD_NBR
						FROM	`+ tgt_wrk_tbl +`	
						WHERE	Sameday_chg_ind = 0
						AND		Offer_Request_Id is not NULL
						AND		Usage_limit_Type_Txt is not NULL`;
						
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
        return "Loading of Offer_Request_Restriction_Type table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
		var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;
		
		var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl +`
		                                select Distinct
												  Offer_Request_Id 
												,Usage_limit_Type_Txt 
												,Limit_Amt 
												,Limit_Qty 
												,Limit_Vol 
												,Limit_Wt
												,Unit_Of_Measure_Cd
												,Unit_Of_Measure_Nm
												,Restriction_Type_Cd
												,Restriction_Type_Dsc
												,Restriction_Type_Short_Dsc
												,Usage_Limit_Nbr
												,FileName 
												,DW_Logical_delete_ind
												,DML_Type
												,Sameday_chg_ind
                                                ,CASE 	WHEN Offer_Request_Id is NULL THEN 'Offer_Request_Id is NULL'
														WHEN Usage_limit_Type_Txt is NULL THEN 'Usage_limit_Type_Txt is NULL'
														ELSE NULL END AS Exception_Reason
												,CURRENT_TIMESTAMP AS DW_CREATE_TS
												,USAGE_LIMIT_PERIOD_NBR
                                            FROM `+ tgt_wrk_tbl +` 
											WHERE Offer_Request_Id IS NULL
											OR Usage_limit_Type_Txt IS NULL
										
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
		
	// **************	Load for Offer_Request_Restriction_Type table table ENDs *****************
	
	
	
	
$$;
