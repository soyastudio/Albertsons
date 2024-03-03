--liquibase formatted sql
--changeset SYSTEM:SP_GETOFFERREQUEST_TO_BIM_Offer_Request_Excluded_Promotion runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFERREQUEST_TO_BIM_Offer_Request_Excluded_Promotion
(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS $$
 
    
var cnf_db = CNF_DB ;
var wrk_schema = WRK_SCHEMA ;
var cnf_schema = CNF_SCHEMA;
var src_wrk_tbl = SRC_WRK_TBL;
	
// **************	Load for Offer_Request_Excluded_Promotion table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Excluded_Promotion_wrk";
var tgt_tbl = cnf_db +"."+ cnf_schema +".Offer_Request_Excluded_Promotion";

var src_wrk_tmp_tbl = cnf_db + "." + wrk_schema + ".Offer_Request_Excluded_SRC_WRK";

var cr_src_wrk_tbl = ` CREATE OR REPLACE TABLE ` + src_wrk_tmp_tbl + ` AS
                      with flat_tmp as
					    (	SELECT 	DISTINCT  OfferRequestId AS Offer_Request_Id
											, EcommPromoCd as Promo_Cd
											, creationdt
											, actiontypecd 
											, FileName
											, Row_number() OVER ( partition BY OfferRequestId, Promo_Cd ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn
									FROM ` + src_wrk_tbl +`
									where Offer_Request_Id is not null 
									
					)
					SELECT DISTINCT
		            Offer_Request_Id,
					Promo_Cd,			 
					filename,
					CREATIONDT,
					actiontypecd,
					RN
					FROM (
                              SELECT
						         Offer_Request_Id,
							     Promo_Cd,			 
							     filename,
								 CREATIONDT,
								 actiontypecd,
								 rn
													 FROM flat_tmp
													 												   
							                		 ) 			
					where rn=1 and 
						Offer_Request_Id is not NULL AND
						Promo_Cd is not NULL`;				
											
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        );
    }
    catch (err)  { 
        return "Creation of Offer_request_Excluded_Promotion "+ src_wrk_tmp_tbl +" Failed with error: " + err;   // Return a error message.
        }
											
var create_tgt_wrk_table = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 		
						SELECT DISTINCT
								src.Offer_Request_Id
								, src.Promo_Cd
								, src.FileName 
								, src.dw_logical_delete_ind
								, CASE WHEN (tgt.Offer_Request_Id IS NULL AND tgt.Promo_Cd IS NULL) THEN 'I' ELSE 'U' END AS DML_Type
								, CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
								FROM   
								(
								SELECT		       Offer_Request_Id
													, Promo_Cd
													, creationdt
													, FALSE AS DW_Logical_delete_ind 
													, FileName														
										 FROM ` + src_wrk_tmp_tbl + `
											WHERE  rn = 1											
											AND	UPPER(ActionTypeCd) <> 'DELETE'												
										) src
								LEFT JOIN 	(SELECT 	tgt.Offer_Request_Id
														, tgt.Promo_Cd
														, tgt.dw_logical_delete_ind 
														, tgt.dw_first_effective_dt 											
											FROM ` + tgt_tbl + ` tgt
											WHERE DW_CURRENT_VERSION_IND = TRUE	
								) tgt 
								ON 	tgt.Offer_Request_Id = src.Offer_Request_Id 
								AND tgt.Promo_Cd = src.Promo_Cd 
						WHERE  tgt.Offer_Request_Id  IS NULL AND tgt.Promo_Cd  IS NULL
						OR 		(								 
									 src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
								)
						UNION ALL
							SELECT 	 tgt.Offer_Request_Id
									, tgt.Promo_Cd
									, TRUE AS DW_Logical_delete_ind
									--, tgt.DW_SOURCE_CREATE_NM
									,src.Filename
									, 'U' as DML_Type
									, CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
						FROM	` + tgt_tbl + ` tgt
						inner join ` + src_wrk_tmp_tbl + ` src 
						on src.Offer_Request_Id = tgt.Offer_Request_Id
						AND src.Promo_Cd = tgt.Promo_Cd
							WHERE	DW_CURRENT_VERSION_IND = TRUE
							and rn = 1
							AND	upper(ActionTypeCd) = 'DELETE'
								AND DW_LOGICAL_DELETE_IND = FALSE
								AND (tgt.Offer_Request_Id,tgt.Promo_Cd) in 
										(
											SELECT 	DISTINCT Offer_Request_Id,
													            Promo_Cd	
												FROM	` + src_wrk_tmp_tbl + ` src
												WHERE 	rn = 1
													AND	upper(ActionTypeCd) = 'DELETE'
													AND	Offer_Request_Id  IS NOT NULL
													AND Promo_Cd IS NOT NULL													  
													)`;
	try {
		snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }												
	
		
	var sql_deletes = `INSERT INTO ${tgt_wrk_tbl}
        SELECT TGT.OFFER_REQUEST_ID,
               TGT.PROMO_CD,
               SRC_WRK.FILENAME,			   
		       TRUE AS DW_Logical_delete_ind,
               'U' AS DML_TYPE,
               CASE WHEN DW_First_Effective_dt = CURRENT_DATE  THEN 1 Else 0 END as Sameday_chg_ind
             FROM ${tgt_tbl} tgt
                LEFT JOIN
            (
            SELECT distinct OFFER_REQUEST_ID
                           ,Promo_Cd
                          ,FileName
            FROM ${src_wrk_tmp_tbl}
            ) src 
              ON src.Promo_Cd = tgt.Promo_Cd
             AND src.OFFER_REQUEST_ID = tgt.OFFER_REQUEST_ID
			 LEFT JOIN
              (
               SELECT distinct OFFER_REQUEST_ID
                ,FileName
                FROM ${src_wrk_tmp_tbl}
                 ) src_wrk
                on src_wrk.OFFER_REQUEST_ID = tgt.OFFER_REQUEST_ID
            WHERE    
             (tgt.OFFER_REQUEST_ID ) in (select distinct OfferRequestId
           FROM ${src_wrk_tbl}
          )
             AND 
              dw_current_version_ind = TRUE
            AND dw_logical_delete_ind = FALSE
              and src.OFFER_REQUEST_ID is NULL
           and src.Promo_Cd is NULL
            `;
try {
        snowflake.execute (
            {sqlText: sql_deletes  }
            );
        }
    catch (err)  {
        return "Insert of Delete records for OFFER_REQUEST_EXCLUDED_PROMOTION work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
											, Promo_Cd
											,FileName
									FROM 	`+ tgt_wrk_tbl +` 
										WHERE	DML_Type = 'U'
											AND	Sameday_chg_ind = 0
											AND	Offer_Request_Id  IS NOT NULL 
											AND	Promo_Cd  IS NOT NULL  
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id 
							AND tgt.Promo_Cd = src.Promo_Cd 
							AND	tgt.DW_CURRENT_VERSION_IND = TRUE`;
    var sql_sameday = `// Processing Sameday updates
						UPDATE ` + tgt_tbl + ` as tgt
						SET		
								 DW_Logical_delete_ind = src.DW_Logical_delete_ind
								, DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
								, DW_SOURCE_UPDATE_NM = FileName
								,DW_CURRENT_VERSION_IND = FALSE
						FROM	(	SELECT 	 		Offer_Request_Id
											, Promo_Cd
											
											, DW_Logical_delete_ind
											, FileName
									FROM 	`+ tgt_wrk_tbl +` 
									WHERE	DML_Type = 'U'
									AND	Sameday_chg_ind = 1
									AND	Offer_Request_Id  IS NOT NULL 
									AND	Promo_Cd  IS NOT NULL  
								) src
						WHERE	tgt.Offer_Request_Id = src.Offer_Request_Id 
							AND tgt.Promo_Cd = src.Promo_Cd 
							AND	tgt.DW_CURRENT_VERSION_IND = TRUE`;

						// Processing Inserts
	
	
	var sql_inserts = `INSERT INTO ` + tgt_tbl + `
						(	 Offer_Request_Id
							, Promo_Cd
							
							, DW_First_Effective_Dt     
							, DW_Last_Effective_Dt     
							, DW_CREATE_TS
							, DW_LOGICAL_DELETE_IND
							, DW_SOURCE_CREATE_NM
							, DW_CURRENT_VERSION_IND
						)
						SELECT 	  	Offer_Request_Id
								, Promo_Cd
								
								, CURRENT_DATE
								, '31-DEC-9999'
								, CURRENT_TIMESTAMP
								, DW_Logical_delete_ind
								, FileName
								, TRUE
						FROM	`+ tgt_wrk_tbl +`	
						WHERE	Sameday_chg_ind = 0
						AND	Offer_Request_Id  IS NOT NULL 
						AND	Promo_Cd  IS NOT NULL 
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
        return "Loading of Offer_Request_Excluded_Promotion table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
				
	// **************	Load for Offer_Request_Requirement_Type table ENDs *****************
	$$;
