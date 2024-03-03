--liquibase formatted sql
--changeset SYSTEM:sp_GetOfferRequest_to_BIM_LOAD_Offer_Request_Store_Group runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETOFFERREQUEST_TO_BIM_LOAD_OFFER_REQUEST_STORE_GROUP(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
    
	 
    
	var cnf_db = CNF_DB;
	var wrk_schema = WRK_SCHEMA;
	var cnf_schema = CNF_SCHEMA;
	var src_wrk_tbl = SRC_WRK_TBL;
	
	// **************	Load for Offer_Request_Store_Group table BEGIN *****************
	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Store_Group_wrk";
	var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request_Store_Group";
	var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Store_Group_Exceptions";
	var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as 
					WITH src_wrk_tbl_recs as
					(	SELECT DISTINCT OfferRequestId AS Offer_Request_Id,    
										AttachedOfferTypeId AS User_Interface_Unique_Id,
										StoreGroupId AS Store_Group_Id ,
										StoreGroupType_Code AS Store_Group_Type_Cd,
										StoreGroupType_Description AS Store_Group_Type_Dsc,
										StoreGroupType_ShortDescription AS Store_Group_Type_Short_Dsc,										 
										CreationDt,
										actiontypecd,
										FileName  ,
							Row_number() OVER ( partition BY OfferRequestId, AttachedOfferTypeId, StoreGroupId, StoreGroupType_Code ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
						FROM ` + src_wrk_tbl +`
					)
					SELECT 	src.Offer_Request_Id,
							src.User_Interface_Unique_Id,
							src.Store_Group_Id,
							src.Store_Group_Type_Cd,
							src.Store_Group_Type_Dsc,
							src.Store_Group_Type_Short_Dsc,
							src.dw_logical_delete_ind, 
							src.FileName 
							,CASE WHEN (tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL AND tgt.Store_Group_Id IS NULL AND tgt.Store_Group_Type_Cd IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
							,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
					FROM   (SELECT 	Offer_Request_Id,
									User_Interface_Unique_Id,
									Store_Group_Id,
									Store_Group_Type_Cd,
									Store_Group_Type_Dsc,
									Store_Group_Type_Short_Dsc,
									creationdt ,
									
									FALSE AS DW_Logical_delete_ind,
									FileName 
							FROM	src_wrk_tbl_recs
							WHERE  rn = 1
							--AND Offer_Request_Id IS NOT NULL 
							--AND User_Interface_Unique_Id IS NOT NULL
							--AND Store_Group_Id IS NOT NULL
							--AND Store_Group_Type_Cd IS NOT NULL
							AND UPPER(ActionTypeCd) <> 'DELETE'
					) src 
					LEFT JOIN 	(SELECT tgt.Offer_Request_Id ,
										tgt.User_Interface_Unique_Id ,
										tgt.Store_Group_Id,
										tgt.Store_Group_Type_Cd,
										tgt.Store_Group_Type_Dsc,
										tgt.Store_Group_Type_Short_Dsc,
										tgt.dw_logical_delete_ind, 
										tgt.dw_first_effective_dt 
								FROM ` + tgt_tbl + ` tgt
								WHERE DW_CURRENT_VERSION_IND = TRUE
					) tgt 
						ON tgt.Offer_Request_Id = src.Offer_Request_Id 
						AND tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id	
						AND tgt.Store_Group_Id = src.Store_Group_Id
						AND tgt.Store_Group_Type_Cd = src.Store_Group_Type_Cd	
					WHERE  	(tgt.Offer_Request_Id IS NULL AND tgt.User_Interface_Unique_Id IS NULL AND tgt.Store_Group_Id IS NULL AND tgt.Store_Group_Type_Cd IS NULL )
						OR 	
						(      	NVL(src.Store_Group_Type_Dsc,'-1') <> NVL(tgt.Store_Group_Type_Dsc,'-1')
						OR		NVL(src.Store_Group_Type_Short_Dsc,'-1') <> NVL(tgt.Store_Group_Type_Short_Dsc,'-1')
						OR 		src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
						)
					UNION ALL
					SELECT 	tgt.Offer_Request_Id
                            ,tgt.User_Interface_Unique_Id
                            ,tgt.Store_Group_Id
							,tgt.Store_Group_Type_Cd
							,tgt.Store_Group_Type_Dsc
                            ,tgt.Store_Group_Type_Short_Dsc
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
        return "Creation of Offer_Request_Store_Group work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
											,Store_Group_Id
											,Store_Group_Type_Cd
											,FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND		Sameday_chg_ind = 0
									AND 	Offer_Request_Id is not NULL
									AND 	User_Interface_Unique_Id is not NULL
									AND 	Store_Group_Id is not NULL
									AND 	Store_Group_Type_Cd is not NULL
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id
						AND		tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id
						AND		tgt.Store_Group_Id = src.Store_Group_Id
						AND		tgt.Store_Group_Type_Cd = src.Store_Group_Type_Cd
						AND		tgt.DW_CURRENT_VERSION_IND = TRUE`;
						
    var sql_sameday = `// Processing Sameday updates
						UPDATE ` + tgt_tbl + ` as tgt
						SET		 Store_Group_Type_Dsc = src.Store_Group_Type_Dsc
								,Store_Group_Type_Short_Dsc = src.Store_Group_Type_Short_Dsc
								,DW_Logical_delete_ind = src.DW_Logical_delete_ind
								,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								,DW_SOURCE_UPDATE_NM = FileName
						FROM	(	SELECT 	 Offer_Request_Id,
                                            User_Interface_Unique_Id,
                                            Store_Group_Id,
											Store_Group_Type_Cd,
											Store_Group_Type_Dsc,
											Store_Group_Type_Short_Dsc,
											DW_Logical_delete_ind,
											FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND		Sameday_chg_ind = 1
									AND		Offer_Request_Id is not NULL
									AND		User_Interface_Unique_Id is not NULL
									AND		Store_Group_Id is not NULL
									AND		Store_Group_Type_Cd is not NULL
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id
						AND		tgt.User_Interface_Unique_Id = src.User_Interface_Unique_Id
						AND		tgt.Store_Group_Id = src.Store_Group_Id
						AND		tgt.Store_Group_Type_Cd = src.Store_Group_Type_Cd
						AND		tgt.DW_CURRENT_VERSION_IND = TRUE`;

	// Processing Inserts
	
	var sql_inserts = `INSERT INTO ` + tgt_tbl + `
						(	 Offer_Request_Id,
							User_Interface_Unique_Id ,
							Store_Group_Id,
							Store_Group_Type_Cd,
							Store_Group_Type_Dsc,
							Store_Group_Type_Short_Dsc,
							DW_First_Effective_Dt,
							DW_Last_Effective_Dt,
							DW_CREATE_TS,
							DW_LOGICAL_DELETE_IND,
							DW_SOURCE_CREATE_NM,
							DW_CURRENT_VERSION_IND
						)
						SELECT 	 Offer_Request_Id
								,User_Interface_Unique_Id
								,Store_Group_Id
								,Store_Group_Type_Cd
								,Store_Group_Type_Dsc
								,Store_Group_Type_Short_Dsc
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
						AND		Store_Group_Id is not NULL
						AND		Store_Group_Type_Cd is not NULL`;
						
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
        return "Loading of Offer_Request_Store_Group table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
		var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;
		
		var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl +`
		                               select 	Offer_Request_Id 
												,User_Interface_Unique_Id 
												,Store_Group_Id
												,Store_Group_Type_Cd
												,Store_Group_Type_Dsc
												,Store_Group_Type_Short_Dsc
												,FileName 
												,DW_Logical_delete_ind
												,DML_Type
												,Sameday_chg_ind
                                                ,CASE 	WHEN Offer_Request_Id is NULL THEN 'Offer_Request_Id is NULL'
														WHEN User_Interface_Unique_Id is NULL THEN 'User_Interface_Unique_Id is NULL'
														WHEN Store_Group_Id is NULL THEN 'Store_Group_Id is NULL'
														WHEN Store_Group_Type_Cd is NULL THEN 'Store_Group_Type_Cd is NULL'
														ELSE NULL END AS Exception_Reason
												,CURRENT_TIMESTAMP AS DW_CREATE_TS
                                            FROM `+ tgt_wrk_tbl +` 
											WHERE Offer_Request_Id IS NULL
											OR User_Interface_Unique_Id IS NULL
											OR Store_Group_Id IS NULL
											OR Store_Group_Type_Cd IS NULL
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
		
	// **************	Load for Offer_Request_Store_Group table table ENDs *****************
	
	
	
	
$$;
