--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferOMS_TO_BIM_LOAD_Oms_Offer_Qualification_Tender_Type runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;


CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD_OMS_OFFER_QUALIFICATION_TENDER_TYPE
(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_PRODUCT VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_PRODUCT;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Oms_Offer_Qualification_Tender_Type_WRK`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.Oms_Offer_Qualification_Tender_Type`;
	var src_wrk_tmp_tbl = `${CNF_DB}.${C_STAGE}.Oms_Offer_Qualification_Tender_Type_src_wrk`;

                       
    // **************        Load for Oms_Offer_Qualification_Tender_Type table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
		
		var sql_truncate_src_wrk_tmp_tbl = `Truncate table  ${src_wrk_tmp_tbl}`;
	    var cr_src_wrk_tbl = `	INSERT INTO `+ src_wrk_tmp_tbl +` 
                          with flat_tmp as
                              (		
								
							SELECT DISTINCT 
							payload_id as OMS_Offer_Id
						   ,payload_qualificationTenderTypes
						   ,lastUpdateTS
						   ,Filename
						   ,Row_number() OVER ( partition BY OMS_Offer_Id ORDER BY To_timestamp_ntz(lastUpdateTS) DESC) AS rn                            
						   FROM ${src_wrk_tbl}
						    WHERE 
							OMS_Offer_Id is not NULL and 
							payload_qualificationTenderTypes is not null	
									
                                  )
                            
                               
							SELECT DISTINCT 
                             OMS_Offer_Id
							,Tender_Type_Dsc  							 
							,Conjunction_Type_Txt  
							,Display_Order_Nbr
							,lastUpdateTS
							,FileName
							from
                            (
							
						  
						  SELECT  
                            OMS_Offer_Id
						    ,qualification_TenderTypes.VALUE:tenderType::string as Tender_Type_Dsc 
							,qualification_TenderTypes.VALUE:conjunction::string as Conjunction_Type_Txt,qualification_TenderTypes.VALUE:displayOrder as Display_Order_Nbr
						   ,lastUpdateTS
						   ,Filename
						   ,rn
						   FROM flat_tmp
						  ,LATERAL FLATTEN(input => payload_qualificationTenderTypes, outer => TRUE ) as qualification_TenderTypes
                          
                          )
			          WHERE 
							            OMS_Offer_Id is not NULL AND
							            Tender_Type_Dsc   is not NULL AND
										
								    
										rn = 1 
                          `;
    try {
		snowflake.execute ({ sqlText: sql_truncate_src_wrk_tmp_tbl});
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        );
    }
    catch (err)  { 
        return "Creation of  "+ src_wrk_tmp_tbl +" Failed with error: " + err;   // Return a error message.
        }
	
	var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;
	var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}  
	SELECT DISTINCT
		src.OMS_Offer_Id
		,src.Tender_Type_Dsc
        ,SRC.Conjunction_Type_Txt     
		,src.Display_Order_Nbr
		
		,src.filename
		,src.DW_Logical_delete_ind,
		CASE 
			WHEN 
			tgt.OMS_Offer_Id is NULL AND
			tgt.Tender_Type_Dsc is NULL AND
			tgt.Conjunction_Type_Txt is NULL AND
			tgt.Display_Order_Nbr is NULL
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
				 OMS_Offer_Id
				,Tender_Type_Dsc   
                ,Conjunction_Type_Txt     
				,Display_Order_Nbr 
				,filename
				,FALSE AS DW_Logical_delete_ind
				FROM ${src_wrk_tmp_tbl}
	) src  
	LEFT JOIN (
		SELECT
			 OMS_Offer_Id
				,Tender_Type_Dsc   
                ,Conjunction_Type_Txt     
				,Display_Order_Nbr 
			,DW_Logical_delete_ind
			,DW_First_Effective_dt
        FROM ${tgt_tbl}
		WHERE DW_CURRENT_VERSION_IND = TRUE
	)  as tgt on
	src.OMS_Offer_Id = tgt.OMS_Offer_Id AND
	src.Tender_Type_Dsc = tgt.Tender_Type_Dsc 
	
	WHERE
		tgt.OMS_Offer_Id is NULL AND
		tgt.Tender_Type_Dsc is NULL 
		
		  OR (
		 NVL(src.Conjunction_Type_Txt ,'-1') <> NVL(tgt.Conjunction_Type_Txt ,'-1') 
		OR NVL(src.Display_Order_Nbr ,'-1') <> NVL(tgt.Display_Order_Nbr ,'-1') 
        OR (SRC.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND)
		 )
		 `;  	
	
	try {
		snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
		var sql_deletes = `INSERT INTO ${tgt_wrk_tbl}
         select  tgt.OMS_Offer_Id
		,tgt.Tender_Type_Dsc
		,tgt.Conjunction_Type_Txt
		,tgt.Display_Order_Nbr

        ,src_wrk.FileName
        ,TRUE AS DW_Logical_delete_ind  
        ,'U' AS DML_Type  
        ,CASE WHEN DW_First_Effective_dt = CURRENT_DATE  THEN 1 Else 0 END as Sameday_chg_ind
             FROM ${tgt_tbl} tgt
            LEFT JOIN
            (
            SELECT distinct OMS_Offer_Id
                           ,Tender_Type_Dsc
						   ,FileName
            FROM ${src_wrk_tmp_tbl}
            ) src 
              ON  src.OMS_Offer_Id = tgt.OMS_Offer_Id
			 AND SRC.Tender_Type_Dsc = TGT.Tender_Type_Dsc
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
              and src.Tender_Type_Dsc is NULL
            `;
try {
		snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
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
			Tender_Type_Dsc,
			filename
		FROM ${tgt_wrk_tbl}
		WHERE 
			DML_Type = 'U' AND 
			Sameday_chg_ind = 0 AND
			OMS_Offer_Id is not NULL AND
			Tender_Type_Dsc is not NULL 
			
		) src
		WHERE 
			tgt.OMS_Offer_Id = src.OMS_Offer_Id AND
			tgt.Tender_Type_Dsc = src.Tender_Type_Dsc AND
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
		
     	// SCD Type1 - Processing Sameday updates
	var sql_sameday = `
	UPDATE ${tgt_tbl} as tgt
	SET      
    	OMS_Offer_Id = src.OMS_Offer_Id,
		Tender_Type_Dsc = src.Tender_Type_Dsc,
        Conjunction_Type_Txt = src.Conjunction_Type_Txt,
        Display_Order_Nbr = src.Display_Order_Nbr,		
		DW_Logical_delete_ind = src.DW_Logical_delete_ind,
		DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
		DW_SOURCE_UPDATE_NM = filename
		FROM ( 
			SELECT
				OMS_Offer_Id,
				Tender_Type_Dsc,
				Conjunction_Type_Txt,
				Display_Order_Nbr,
				
			    DW_Logical_delete_ind,
				
				filename
			FROM ${tgt_wrk_tbl}
			WHERE 
				DML_Type = 'U' AND 
				Sameday_chg_ind = 1 AND
				OMS_Offer_Id is not NULL AND
				Tender_Type_Dsc is not null 
				
		) src
		WHERE
			tgt.OMS_Offer_Id = src.OMS_Offer_Id AND
			tgt.Tender_Type_Dsc = src.Tender_Type_Dsc AND
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
                                
                            
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
					 OMS_Offer_Id
					,Tender_Type_Dsc  							 
					,DW_First_Effective_Dt      
					,DW_Last_Effective_Dt
					,Conjunction_Type_Txt  
					,Display_Order_Nbr   
					,DW_CREATE_TS                                                                                             
                    ,DW_LOGICAL_DELETE_IND                                                                                           
                    ,DW_SOURCE_CREATE_NM                                                                                         
                    ,DW_CURRENT_VERSION_IND      
                   )
                   SELECT distinct
                     OMS_Offer_Id 
					,Tender_Type_Dsc 
					,CURRENT_TIMESTAMP 
                    ,'31-DEC-9999'     
                    ,Conjunction_Type_Txt  
					,Display_Order_Nbr  
					,CURRENT_TIMESTAMP
                    ,False
                    ,FileName
                    ,TRUE                                                                                                      
				FROM ${tgt_wrk_tbl}
                where OMS_Offer_Id is not null
				and Tender_Type_Dsc is not null
				and Sameday_chg_ind = 0
				
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
               $$;
