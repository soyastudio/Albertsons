--liquibase formatted sql
--changeset SYSTEM:sp_GetOfferRequest_to_BIM_LOAD_Offer_Request_POD_Category runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETOFFERREQUEST_TO_BIM_LOAD_OFFER_REQUEST_POD_CATEGORY(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
    
	var cnf_db = CNF_DB;
	var wrk_schema = WRK_SCHEMA;
	var cnf_schema = CNF_SCHEMA;
	var src_wrk_tbl = SRC_WRK_TBL;
	
	// **************	Load for Offer_Request_POD_Category table BEGIN *****************
	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_POD_Category_wrk";
	var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request_POD_Category";
	var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_POD_Category_Exceptions";
	var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as 
					WITH src_wrk_tbl_recs as
					(	SELECT DISTINCT OfferRequestId AS Offer_Request_Id,    
										AttachedOfferTypeId AS User_Interface_Unique_Id,
										PODCategory_Code AS POD_Category_Cd,
										PODCategory_Description AS POD_Category_Dsc,
										PODCategory_ShortDescription AS POD_Category_Short_Dsc,
										CreationDt,
										actiontypecd,
										FileName  ,
							Row_number() OVER ( partition BY OfferRequestId, AttachedOfferTypeId,PODCategory_Code ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
						FROM ` + src_wrk_tbl +`
					)
					SELECT 	src.Offer_Request_Id,
							src.User_Interface_Unique_Id,
							src.POD_Category_Cd,
							src.POD_Category_Dsc,
							src.POD_Category_Short_Dsc,
							src.dw_logical_delete_ind, 
							src.FileName 
							,CASE WHEN (tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL AND tgt.POD_Category_Cd IS NULL ) THEN 'I' ELSE 'U' END AS DML_Type
							,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
					FROM   (SELECT 	Offer_Request_Id,
									User_Interface_Unique_Id,
									POD_Category_Cd,
									POD_Category_Dsc,
									POD_Category_Short_Dsc,
									creationdt ,
									
									FALSE AS DW_Logical_delete_ind,
									FileName 
							FROM	src_wrk_tbl_recs
							WHERE  rn = 1
							--AND Offer_Request_Id IS NOT NULL 
							--AND User_Interface_Unique_Id IS NOT NULL
							--AND POD_Category_Cd IS NOT NULL
							AND UPPER(ActionTypeCd) <> 'DELETE'
					) src 
					LEFT JOIN 	(SELECT tgt.Offer_Request_Id ,
										tgt.User_Interface_Unique_Id ,
										tgt.POD_Category_Cd,
										tgt.POD_Category_Dsc,
										tgt.POD_Category_Short_Dsc,
										tgt.dw_logical_delete_ind, 
										tgt.dw_first_effective_dt 
								FROM ` + tgt_tbl + ` tgt
								WHERE DW_CURRENT_VERSION_IND = TRUE
					) tgt 
						ON tgt.Offer_Request_Id = src.Offer_Request_Id 
						AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id	
						AND tgt.POD_Category_Cd = src.POD_Category_Cd	
					WHERE  	(tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL AND tgt.POD_Category_Cd IS NULL )
						OR 	
						(      	NVL(src.POD_Category_Dsc,'-1') <> NVL(tgt.POD_Category_Dsc,'-1')
						OR		NVL(src.POD_Category_Short_Dsc,'-1') <> NVL(tgt.POD_Category_Short_Dsc,'-1')
						OR 		src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
						)
					UNION ALL
					SELECT 	tgt.Offer_Request_Id
                            ,tgt.User_Interface_Unique_Id
                            ,tgt.POD_Category_Cd
							,tgt.POD_Category_Dsc
                            ,tgt.POD_Category_Short_Dsc
							,TRUE AS DW_Logical_delete_ind
							--,tgt.DW_SOURCE_CREATE_NM
							,src.filename
							,'U' as DML_Type
							
                            ,CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
					FROM ` + tgt_tbl + ` tgt
					inner join src_wrk_tbl_recs src on 
					src.Offer_Request_Id = tgt.Offer_Request_Id
					
					WHERE	DW_CURRENT_VERSION_IND = TRUE
					AND     rn = 1
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
        return "Creation of Offer_Request_POD_Category work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
                                            ,User_Interface_Unique_Id
											,POD_Category_Cd
											,FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND		Sameday_chg_ind = 0
									AND 	Offer_Request_Id is not NULL
									AND 	User_Interface_Unique_Id is not NULL
									AND 	POD_Category_Cd is not NULL
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id
						AND		tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id
						AND		tgt.POD_Category_Cd = src.POD_Category_Cd
						AND		tgt.DW_CURRENT_VERSION_IND = TRUE`;
						
    var sql_sameday = `// Processing Sameday updates
						UPDATE ` + tgt_tbl + ` as tgt
						SET		 POD_Category_Dsc = src.POD_Category_Dsc
								,POD_Category_Short_Dsc = src.POD_Category_Short_Dsc
								,DW_Logical_delete_ind = src.DW_Logical_delete_ind
								,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								,DW_SOURCE_UPDATE_NM = FileName
						FROM	(	SELECT 	 Offer_Request_Id,
                                            User_Interface_Unique_Id,
                                            POD_Category_Cd,
											POD_Category_Dsc,
											POD_Category_Short_Dsc,
											DW_Logical_delete_ind,
											FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND		Sameday_chg_ind = 1
									AND		Offer_Request_Id is not NULL
									AND		User_Interface_Unique_Id is not NULL
									AND		POD_Category_Cd is not NULL
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id
						AND		tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id
						AND		tgt.POD_Category_Cd = src.POD_Category_Cd
						AND		tgt.DW_CURRENT_VERSION_IND = TRUE`;

	// Processing Inserts
	
	var sql_inserts = `INSERT INTO ` + tgt_tbl + `
						(	 Offer_Request_Id,
							User_Interface_Unique_Id ,
							POD_Category_Cd,
							POD_Category_Dsc,
							POD_Category_Short_Dsc,
							DW_First_Effective_Dt,
							DW_Last_Effective_Dt,
							DW_CREATE_TS,
							DW_LOGICAL_DELETE_IND,
							DW_SOURCE_CREATE_NM,
							DW_CURRENT_VERSION_IND
						)
						SELECT 	 Offer_Request_Id
								,User_Interface_Unique_Id
								,POD_Category_Cd
								,POD_Category_Dsc
								,POD_Category_Short_Dsc
								,CURRENT_DATE
								,'31-DEC-9999'
								,CURRENT_TIMESTAMP
								,DW_Logical_delete_ind
								,FileName
								,TRUE
						FROM	`+ tgt_wrk_tbl +`
						WHERE	Sameday_chg_ind = 0
						AND		Offer_Request_Id is not NULL
						AND		User_Interface_Unique_Id is not NULL
						AND		POD_Category_Cd is not NULL`;
						
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
        return "Loading of Offer_Request_POD_Category table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
		var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;
		
		var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl +`
		                               select 	Offer_Request_Id 
												,User_Interface_Unique_Id 
												,POD_Category_Cd
												,POD_Category_Dsc
												,POD_Category_Short_Dsc
												,FileName 
												,DW_Logical_delete_ind
												,DML_Type
												,Sameday_chg_ind
                                                ,CASE 	WHEN Offer_Request_Id is NULL THEN 'Offer_Request_Id is NULL'
														WHEN User_Interface_Unique_Id is NULL THEN 'User_Interface_Unique_Id is NULL'
														WHEN POD_Category_Cd is NULL THEN 'POD_Category_Cd is NULL'
														ELSE NULL END AS Exception_Reason
												,CURRENT_TIMESTAMP AS DW_CREATE_TS
                                            FROM `+ tgt_wrk_tbl +` 
											WHERE Offer_Request_Id IS NULL
											OR User_Interface_Unique_Id IS NULL
											OR POD_Category_Cd IS NULL
												`;
	
    try {
        snowflake.execute (
            {sqlText: sql_exceptions  }
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
		
	// **************	Load for Offer_Request_POD_Category table table ENDs *****************
	
	
	
$$;
