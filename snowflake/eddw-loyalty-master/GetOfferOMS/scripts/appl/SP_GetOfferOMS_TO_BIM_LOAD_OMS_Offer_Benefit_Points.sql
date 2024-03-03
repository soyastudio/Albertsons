--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferOMS_TO_BIM_LOAD_OMS_Offer_Benefit_Points runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD_OMS_OFFER_BENEFIT_POINTS("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_PRODUCT" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_PRODUCT;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.OMS_Offer_Benefit_Points_WRK`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.OMS_Offer_Benefit_Points`;

                       
    // **************        Load for OMS_Offer_Benefit_Points table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	
	// Empty the target work table
		var sql_empty_tgt_wrk_tbl = `TRUNCATE TABLE `+ tgt_wrk_tbl +` `;
		try {
			snowflake.execute ({sqlText: sql_empty_tgt_wrk_tbl });
			}
		catch (err) { 
			throw "Truncation of table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
		}
		
	    var cr_src_wrk_tbl = `INSERT INTO `+ tgt_wrk_tbl +`
		
							with flat_tmp as
                            ( select 
                             payload_id as  OMS_Offer_Id
							 ,PAYLOAD_BENEFIT_POINTS
                            ,filename
                            ,row_number() over ( PARTITION BY OMS_Offer_Id
                                                  ORDER BY to_timestamp_ltz(LastUpdateTs) desc) as rn
                             from ` + src_wrk_tbl +` 
                             WHERE OMS_Offer_Id IS NOT NULL AND PAYLOAD_BENEFIT_POINTS  IS NOT NULL	
                             )
							 	
								SELECT DISTINCT
								OMS_Offer_Id,
								Points_Program_Id,
                                                                Points_Program_Nm,
                                                                Scorecard_Nm,
                                                                Scorecard_Txt,
								filename
								FROM (
										SELECT  
										OMS_Offer_Id,
										PAYLOAD_BENEFITPOINTS.value:pointsProgramId::string AS Points_Program_Id,
										PAYLOAD_BENEFITPOINTS.value:pointsProgram::string AS Points_Program_Nm,
										PAYLOAD_BENEFITPOINTS.value:scoreCard::string AS Scorecard_Nm,
                                                                                PAYLOAD_BENEFITPOINTS.value:scoreCardText::string AS Scorecard_Txt,
										filename,
										rn
										FROM flat_tmp
										,LATERAL FLATTEN(input => PAYLOAD_BENEFIT_POINTS, outer => TRUE ) as PAYLOAD_BENEFITPOINTS
									)
    
                                WHERE rn=1 AND
							    OMS_Offer_Id is not NULL AND
							    Points_Program_Id is not NULL `;
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        );
    }
    catch (err)  { 
        return "Creation of OMS_Offer_Benefit_Points tgt_wrk_tbl table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

	sql_deletes = `DELETE FROM ` + tgt_tbl + `
                   WHERE (OMS_Offer_Id) in
                   (SELECT OMS_Offer_Id 
                   FROM `+ tgt_wrk_tbl +`) 
                   `;

 try {
        snowflake.execute (
            {sqlText: sql_deletes  }
            );
        }   catch (err)  {
        return "Delete records for OMS_Offer_Benefit_Points table " + tgt_tbl +" Failed with error: " + err;   // Return a error message.
        }
    
	// Transaction for Updates, Insert begins           
    var sql_begin = "BEGIN"
                                
                            
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} (
        OMS_Offer_Id,
	Points_Program_Id,
        DW_First_Effective_dt, 
	DW_Last_Effective_dt, 
        Points_Program_Nm,
        Scorecard_Nm,
        Scorecard_Txt, 
	DW_CREATE_TS,          
	DW_LOGICAL_DELETE_IND,  
	DW_SOURCE_CREATE_NM,   
	DW_CURRENT_VERSION_IND  
	)
	SELECT
		OMS_Offer_Id,
		Points_Program_Id,
		CURRENT_DATE as DW_First_Effective_dt,
		'31-DEC-9999',
		Points_Program_Nm,
                Scorecard_Nm,
                Scorecard_Txt,    
		CURRENT_TIMESTAMP,
		FALSE AS DW_Logical_delete_ind,
		filename,
		TRUE as DW_CURRENT_VERSION_IND
	FROM ${tgt_wrk_tbl}
	WHERE 
		OMS_Offer_Id is not NULL AND
		Points_Program_Id is not NULL
	`;
    
	var sql_commit = "COMMIT";
    var sql_rollback = "ROLLBACK";
    
	try {
        snowflake.execute ({ sqlText: sql_begin });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
	} catch (err) {
        snowflake.execute ({ sqlText: sql_rollback });
       return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
	}
                // **************        Load for OMS_Offer_Benefit_Points table ENDs *****************
$$;
