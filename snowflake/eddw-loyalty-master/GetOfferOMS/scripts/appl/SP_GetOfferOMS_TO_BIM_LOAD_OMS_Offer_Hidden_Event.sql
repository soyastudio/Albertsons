--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Hidden_Event runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD_OMS_OFFER_HIDDEN_EVENT(SRC_WRK_TBL VARCHAR(16777216), CNF_DB VARCHAR(16777216), C_PRODUCT VARCHAR(16777216), C_STAGE VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$

	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_PRODUCT;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.OMS_Offer_Hidden_Event_WRK`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.OMS_Offer_Hidden_Event`;
	var src_wrk_tmp_tbl = `${CNF_DB}.${C_STAGE}.OMS_Offer_Hidden_Event_src_wrk`;

                       
    // **************        Load for OMS_Offer_Hidden_Event table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	
	// Empty the source work table
		var sql_empty_src_wrk_tmp_tbl = `TRUNCATE TABLE `+ src_wrk_tmp_tbl +` `;
		try {
			snowflake.execute ({sqlText: sql_empty_src_wrk_tmp_tbl });
			}
		catch (err) { 
			throw "Truncation of table "+ src_wrk_tmp_tbl +" Failed with error: " + err;   // Return a error message.
			}
			
	// Empty the target work table
		var sql_empty_tgt_wrk_tbl = `TRUNCATE TABLE `+ tgt_wrk_tbl +` `;
		try {
			snowflake.execute ({sqlText: sql_empty_tgt_wrk_tbl });
			}
		catch (err) { 
			throw "Truncation of table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
			}
			
	    var cr_src_wrk_tbl = `	INSERT INTO `+ src_wrk_tmp_tbl +`
                          with flat_tmp as
                              (		
								
										SELECT  
										payload_id AS OMS_Offer_Id,
										payload_hiddenevents, 
                                                                                filename,
										row_number() over ( PARTITION BY OMS_Offer_Id
                                        ORDER BY to_timestamp_ltz(LastUpdateTs) desc) as rn
										FROM ` + src_wrk_tbl +`
										WHERE OMS_Offer_Id IS NOT NULL AND payload_hiddenevents IS NOT NULL
									
                                  )
                            
                                            SELECT DISTINCT
						                    OMS_Offer_Id,
							            OMS_Offer_Event_Description_Txt,	
								    OMS_Offer_Hidden_Event_Cd,
                                                                    filename
                                            FROM (
                                                      SELECT
						                                   OMS_Offer_Id,
							                           EventIdmap.Key::string as OMS_Offer_Hidden_Event_Cd,
                                                                                   EventIdmap.Value::string as OMS_Offer_Event_Description_Txt,
                                                                                   filename,
									           rn
										   FROM flat_tmp
										   ,LATERAL FLATTEN(input => payload_hiddenevents, outer => TRUE) as EventIdMap													   
							                		 ) 				
                                                  WHERE rn = 1 AND
											      OMS_Offer_Id is not NULL AND
							                	  OMS_Offer_Hidden_Event_Cd is not NULL`;
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        );
    }
    catch (err)  { 
        return "Creation of  "+ src_wrk_tmp_tbl +" Failed with error: " + err;   // Return a error message.
        }

	var Insert_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl} 
	SELECT DISTINCT
		src.OMS_Offer_Id,
		src.OMS_Offer_Hidden_Event_Cd,
		src.OMS_Offer_Event_Description_Txt,
		src.filename,
		src.DW_Logical_delete_ind,
		CASE 
			WHEN 
			tgt.OMS_Offer_Id is NULL AND
			tgt.OMS_Offer_Hidden_Event_Cd is NULL
			THEN 'I' 
			ELSE 'U' 
		END as DML_Type,
		CASE 
			WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
			THEN 1 
			Else 0 
		END as Sameday_chg_ind
	FROM (   
	
			SELECT
				OMS_Offer_Id,
				OMS_Offer_Hidden_Event_Cd,
				OMS_Offer_Event_Description_Txt,
				filename,
				FALSE AS DW_Logical_delete_ind
				FROM ${src_wrk_tmp_tbl}
	) src  
	LEFT JOIN (
		SELECT
			OMS_Offer_Id,
			OMS_Offer_Hidden_Event_Cd,
			OMS_Offer_Event_Description_Txt,
			DW_Logical_delete_ind,
			DW_First_Effective_dt
        FROM ${tgt_tbl}
		WHERE DW_CURRENT_VERSION_IND = TRUE
	) as tgt on
	src.OMS_Offer_Id = tgt.OMS_Offer_Id AND
	src.OMS_Offer_Hidden_Event_Cd = tgt.OMS_Offer_Hidden_Event_Cd
	WHERE
		tgt.OMS_Offer_Id is NULL AND
		tgt.OMS_Offer_Hidden_Event_Cd is NULL 
		  OR (
		 NVL(src.OMS_Offer_Event_Description_Txt,'-1') <> NVL(tgt.OMS_Offer_Event_Description_Txt,'-1') 
        OR (SRC.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND)
		 )
		 `;  	
	
	try {
		snowflake.execute ({ sqlText: Insert_tgt_wrk_table });
	}
    catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
		var sql_deletes = `INSERT INTO ${tgt_wrk_tbl}
         select  tgt.OMS_Offer_Id
		,tgt.OMS_Offer_Hidden_Event_Cd
		,tgt.OMS_Offer_Event_Description_Txt
        ,src_wrk.FileName
        ,TRUE AS DW_Logical_delete_ind  
        ,'U' AS DML_Type  
        ,CASE WHEN DW_First_Effective_dt = CURRENT_DATE  THEN 1 Else 0 END as Sameday_chg_ind
             FROM ${tgt_tbl} tgt
            LEFT JOIN
            (
            SELECT distinct OMS_Offer_Id
                           ,OMS_Offer_Hidden_Event_Cd
                          ,FileName
            FROM ${src_wrk_tmp_tbl}
            ) src 
              ON src.OMS_Offer_Hidden_Event_Cd = tgt.OMS_Offer_Hidden_Event_Cd
             AND src.OMS_Offer_Id = tgt.OMS_Offer_Id
			 LEFT JOIN
              (
               SELECT distinct OMS_Offer_Id
                ,FileName
                FROM ${src_wrk_tmp_tbl}
                 ) src_wrk
                on src_wrk.OMS_Offer_Id = tgt.OMS_Offer_Id

            WHERE    
             (tgt.OMS_Offer_Id ) in (select distinct OMS_Offer_Id
           FROM ${src_wrk_tmp_tbl}
          )
             AND 
              dw_current_version_ind = TRUE
            AND dw_logical_delete_ind = FALSE
              and src.OMS_Offer_Id is NULL
              and src.OMS_Offer_Hidden_Event_Cd is NULL
            `;
try {
        snowflake.execute (
            {sqlText: sql_deletes  }
            );
        }
    catch (err)  {
        return "Insert of Delete records for Conditional_Stored_Value work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
            
    
	// Transaction for Updates, Insert begins           
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
			OMS_Offer_Id,
			OMS_Offer_Hidden_Event_Cd,
			filename
		FROM ${tgt_wrk_tbl}
		WHERE 
			DML_Type = 'U' AND 
			Sameday_chg_ind = 0 AND
			OMS_Offer_Id is not NULL AND
			OMS_Offer_Hidden_Event_Cd is not NULL             
		) src
		WHERE 
			tgt.OMS_Offer_Id = src.OMS_Offer_Id AND
			tgt.OMS_Offer_Hidden_Event_Cd = src.OMS_Offer_Hidden_Event_Cd AND
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
		
     	// SCD Type1 - Processing Sameday updates
	var sql_sameday = `
	UPDATE ${tgt_tbl} as tgt
	SET      
    	OMS_Offer_Event_Description_Txt=src.OMS_Offer_Event_Description_Txt,	
		DW_Logical_delete_ind = src.DW_Logical_delete_ind,
		DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
		DW_SOURCE_UPDATE_NM = filename
		FROM ( 
			SELECT
				OMS_Offer_Id,
				OMS_Offer_Hidden_Event_Cd,
				OMS_Offer_Event_Description_Txt,
			    DW_Logical_delete_ind,
				filename
			FROM ${tgt_wrk_tbl}
			WHERE 
				DML_Type = 'U' AND 
				Sameday_chg_ind = 1 AND
				OMS_Offer_Id is not NULL AND
				OMS_Offer_Hidden_Event_Cd is not NULL
		) src
		WHERE
			tgt.OMS_Offer_Id = src.OMS_Offer_Id AND
			tgt.OMS_Offer_Hidden_Event_Cd = src.OMS_Offer_Hidden_Event_Cd AND  
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
                                
                            
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} (
	OMS_Offer_Id,
	OMS_Offer_Hidden_Event_Cd,
	OMS_Offer_Event_Description_Txt,
	DW_First_Effective_dt, 
	DW_Last_Effective_dt, 
	DW_CREATE_TS,          
	DW_LOGICAL_DELETE_IND,  
	DW_SOURCE_CREATE_NM,   
	DW_CURRENT_VERSION_IND  
	)
	SELECT
		OMS_Offer_Id,
		OMS_Offer_Hidden_Event_Cd,
	    OMS_Offer_Event_Description_Txt,
		CURRENT_DATE as DW_First_Effective_dt,
		'31-DEC-9999',
		CURRENT_TIMESTAMP,
		DW_Logical_delete_ind,
		filename,
		TRUE as DW_CURRENT_VERSION_IND
	FROM ${tgt_wrk_tbl}
	WHERE 
		Sameday_chg_ind = 0 AND
		OMS_Offer_Id is not NULL AND
		OMS_Offer_Hidden_Event_Cd is not NULL
	`;
    
	var sql_commit = "COMMIT";
    var sql_rollback = "ROLLBACK";
    
	try {
        snowflake.execute ({ sqlText: sql_begin });
        snowflake.execute ({ sqlText: sql_updates });
		snowflake.execute ({ sqlText: sql_sameday });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
       return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
	}
                // **************        Load for OMS_Offer_Hidden_Event table ENDs *****************

$$;
