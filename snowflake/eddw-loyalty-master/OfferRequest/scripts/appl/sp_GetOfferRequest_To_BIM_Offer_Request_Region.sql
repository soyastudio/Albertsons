--liquibase formatted sql
--changeset SYSTEM:sp_GetOfferRequest_To_BIM_Offer_Request_Region runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETOFFERREQUEST_TO_BIM_OFFER_REQUEST_REGION(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
  
    
var cnf_db = CNF_DB ;
var wrk_schema = WRK_SCHEMA ;
var cnf_schema = CNF_SCHEMA;
var src_wrk_tbl = SRC_WRK_TBL;
	
// **************	Load for Offer_Request_Region table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Region_wrk";
var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request_Region";
var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Region_Exceptions";
var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as 
				    WITH src_wrk_tbl_recs as
					(	SELECT 	DISTINCT OfferRequestId AS Offer_Request_Id
											, DiscountVersion_DiscountVersionId AS Discount_Version_Id
											, OFFERORGANIZATIONREGIONID AS Region_Id
											, OFFERORGANIZATIONREGIONNM AS Region_Nm
											, creationdt
											, actiontypecd 
											, FileName
											, Row_number() OVER ( partition BY OfferRequestId, offerorganizationregionid ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
									FROM ` + src_wrk_tbl +`
						)
						SELECT 		src.Offer_Request_Id
								, src.Region_Id
								, src.Region_Nm
								, src.dw_logical_delete_ind 
								, src.FileName 
								, CASE WHEN (tgt.Offer_Request_Id IS NULL AND tgt.Region_Id IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
								, CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
								FROM   	(SELECT		Offer_Request_Id
													, Region_Id
													, Region_Nm
													, creationdt
													, FALSE AS DW_Logical_delete_ind 
													, FileName														
										 FROM src_wrk_tbl_recs
											WHERE  rn = 1
											  
											AND	UPPER(ActionTypeCd) <> 'DELETE'												
										) src
								LEFT JOIN 	(SELECT 	tgt.Offer_Request_Id
														, tgt.Region_Id
														, tgt.Region_Nm
														, tgt.dw_logical_delete_ind 
														, tgt.dw_first_effective_dt 											
											FROM ` + tgt_tbl + ` tgt
											WHERE DW_CURRENT_VERSION_IND = TRUE	
								) tgt 
								ON 	tgt.Offer_Request_Id = src.Offer_Request_Id 
								AND tgt.Region_Id = src.Region_Id 
						WHERE  (tgt.Offer_Request_Id  IS NULL AND tgt.Region_Id  IS NULL)
						OR 		(
									 NVL(src.Region_Nm,'-1') <> NVL(tgt.Region_Nm,'-1')
									 OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
								)
						UNION ALL
							SELECT 	tgt.Offer_Request_Id
									, tgt.Region_Id
									, tgt.Region_Nm				
									, TRUE AS DW_Logical_delete_ind
									--, tgt.DW_SOURCE_CREATE_NM
									,src.Filename
									, 'U' as DML_Type
									, CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
						FROM	` + tgt_tbl + ` tgt
						 inner join src_wrk_tbl_recs src on src.Offer_Request_Id = tgt.Offer_Request_Id
						 
							WHERE	DW_CURRENT_VERSION_IND = TRUE
							and rn = 1
							AND	upper(ActionTypeCd) = 'DELETE'
								AND DW_LOGICAL_DELETE_IND = FALSE
								AND (tgt.Offer_Request_Id) in 
										(
											SELECT 	DISTINCT Offer_Request_Id
														
												FROM	src_wrk_tbl_recs src
												WHERE 	rn = 1
													AND	upper(ActionTypeCd) = 'DELETE'
													AND	Offer_Request_Id  IS NOT NULL 
													 
													)`;
	try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Offer_Request_Region work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
		
		 
													
	//SCD Type2 transaction begins
    var sql_begin = "BEGIN"
	var sql_updates = `// Processing Updates of Type 2 SCD
						UPDATE ` + tgt_tbl + ` as tgt
						SET		 DW_Last_Effective_dt = CURRENT_DATE-1
								,DW_CURRENT_VERSION_IND = FALSE
								,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								,DW_SOURCE_UPDATE_NM = FileName
						FROM	(	SELECT 	 		 Offer_Request_Id
											, Region_Id
											,FileName
									FROM 	`+ tgt_wrk_tbl +` 
										WHERE	DML_Type = 'U'
											AND	Sameday_chg_ind = 0
											AND	Offer_Request_Id  IS NOT NULL 
											AND	Region_Id  IS NOT NULL  
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id 
							AND tgt.Region_Id = src.Region_Id 
							AND	tgt.DW_CURRENT_VERSION_IND = TRUE`;
    var sql_sameday = `// Processing Sameday updates
						UPDATE ` + tgt_tbl + ` as tgt
						SET		 Region_Nm = src.Region_Nm
								, DW_Logical_delete_ind = src.DW_Logical_delete_ind
								, DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								, DW_SOURCE_UPDATE_NM = FileName
						FROM	(	SELECT 	 Offer_Request_Id
											, Region_Id
											, Region_Nm
											, DW_Logical_delete_ind
											, FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND	Sameday_chg_ind = 1
									AND	Offer_Request_Id  IS NOT NULL 
									AND	Region_Id  IS NOT NULL   
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id 
							AND tgt.Region_Id = src.Region_Id 
							AND	tgt.DW_CURRENT_VERSION_IND = TRUE`;

						// Processing Inserts
	
	
	var sql_inserts = `INSERT INTO ` + tgt_tbl + `
						(	 Offer_Request_Id
							, Region_Id
							, Region_Nm
							, DW_First_Effective_Dt     
							, DW_Last_Effective_Dt     
							, DW_CREATE_TS
							, DW_LOGICAL_DELETE_IND
							, DW_SOURCE_CREATE_NM
							, DW_CURRENT_VERSION_IND
						)
						SELECT 	 Offer_Request_Id
								, Region_Id
								, Region_Nm
								, CURRENT_DATE
								, '31-DEC-9999'
								, CURRENT_TIMESTAMP
								, DW_Logical_delete_ind
								, FileName
								, TRUE
						FROM	`+ tgt_wrk_tbl +`	
						WHERE	Sameday_chg_ind = 0
						AND	Offer_Request_Id  IS NOT NULL 
						AND	Region_Id  IS NOT NULL 
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
        return "Loading of Offer_Request_Region table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
		var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl}`;
		var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl	+`  
		                               select 	    						Offer_Request_Id
													, Region_Id
													, Region_Nm
													, FileName 
													, DW_Logical_delete_ind
												    , DML_Type
												    , Sameday_chg_ind
												    ,CASE 	WHEN Offer_Request_Id is NULL THEN 'Offer_Request_Id is NULL' 
															WHEN Region_Id is NULL THEN 'Region_Id is NULL'
															ELSE NULL END AS Exception_Reason
												    ,CURRENT_TIMESTAMP AS DW_CREATE_TS
												 FROM `+ tgt_wrk_tbl +` 
													 WHERE 	Offer_Request_Id  IS NULL  
														OR	Region_Id  IS NULL 
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
        return "Insert into tgt Exception table "+ tgt_exp_tbl +" Failed with error: " + err;   // Return a error message.
        }
		
	// **************	Load for Offer_Request_Region table ENDs *****************
	
	
$$;
