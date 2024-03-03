--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferOMS_TO_BIM_LOAD_Oms_Offer_Fulfillment_Channel_Type runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;



CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD_OMS_OFFER_FULFILLMENT_CHANNEL_TYPE(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, 
                                                                                          C_PRODUCT VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_PRODUCT;
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Oms_Offer_Fulfillment_Channel_Type_WRK`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.Oms_Offer_Fulfillment_Channel_Type`;
	var src_wrk_tmp_tbl = `${CNF_DB}.${C_STAGE}.Oms_Offer_Fulfillment_Channel_Type_src_wrk`;

                       
    // **************        Load for Oms_Offer_Fulfillment_Channel_Type table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	
	
	    var sql_truncate_src_wrk_tmp_tbl = `Truncate table  ${src_wrk_tmp_tbl}`;
	
	    var cr_src_wrk_tbl = `	INSERT INTO `+ src_wrk_tmp_tbl +`
                          with flat_tmp as
                              (		
								
							SELECT DISTINCT 
							payload_id as OMS_Offer_Id
						   ,payload_fulfillmentChannel
						   ,lastUpdateTS
						   ,Filename
						   
						   ,Row_number() OVER ( partition BY OMS_Offer_Id ORDER BY To_timestamp_ntz(lastUpdateTS) DESC) AS rn                            
						   FROM ${src_wrk_tbl}
						    WHERE 
							OMS_Offer_Id is not NULL and 
							payload_fulfillmentChannel is not null	
									
                                  )
                            
                                            SELECT DISTINCT 
                             
							 Fulfillment_Channel_Type_Cd   
                            ,Fulfillment_Channel_Ind     
							,Fulfillment_Channel_Dsc
                            ,OMS_Offer_Id							
							
							,lastUpdateTS
							,FileName
							from
                            (
							SELECT  
                            
						    fulfillment_Channel.key as Fulfillment_Channel_Type_Cd 
							,fulfillment_Channel.value as Fulfillment_Channel_Ind
							,null as Fulfillment_Channel_Dsc
							,OMS_Offer_Id
						   ,lastUpdateTS
						   ,Filename
						   ,rn
						   FROM  flat_tmp
						  ,LATERAL FLATTEN(input => payload_fulfillmentChannel, outer => TRUE ) as fulfillment_Channel
                          
                          )
			          WHERE 
							            OMS_Offer_Id is not NULL AND
							            Fulfillment_Channel_Type_Cd   is not NULL AND
																    
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
		
		src.Fulfillment_Channel_Type_Cd
        ,SRC.Fulfillment_Channel_Ind     
		,src.Fulfillment_Channel_Dsc
		,src.OMS_Offer_Id		
		,src.filename
		,src.DW_Logical_delete_ind,
		CASE 
			WHEN 
			tgt.OMS_Offer_Id is NULL AND
			tgt.Fulfillment_Channel_Type_Cd is NULL 
			
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
				 
				Fulfillment_Channel_Type_Cd   
                ,Fulfillment_Channel_Ind     
				,Fulfillment_Channel_Dsc
                 ,OMS_Offer_Id				
				,filename
				,FALSE AS DW_Logical_delete_ind
				FROM ${src_wrk_tmp_tbl}
	) src  
	LEFT JOIN (
		SELECT
			 
				Fulfillment_Channel_Type_Cd   
                ,Fulfillment_Channel_Ind     
				,Fulfillment_Channel_Dsc 
				,OMS_Offer_Id
			,DW_Logical_delete_ind
			,DW_First_Effective_dt
        FROM ${tgt_tbl}
		WHERE DW_CURRENT_VERSION_IND = TRUE
	)  as tgt on
	src.OMS_Offer_Id = tgt.OMS_Offer_Id AND
	src.Fulfillment_Channel_Type_Cd = tgt.Fulfillment_Channel_Type_Cd 	
	
	WHERE
		
		tgt.Fulfillment_Channel_Type_Cd is NULL AND
		tgt.OMS_Offer_Id is NULL
		
		
		  OR (
		  NVL(to_boolean(src.Fulfillment_Channel_Ind ) ,-1) <> NVL(tgt.Fulfillment_Channel_Ind ,-1)
		or NVL(src.Fulfillment_Channel_Dsc ,'-1') <> NVL(tgt.Fulfillment_Channel_Dsc ,'-1') 
        OR (SRC.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND)
		 )
		 `;  	
	
	try {
		snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
		snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
		var sql_deletes = `INSERT INTO ${tgt_wrk_tbl}
         select  
		tgt.Fulfillment_Channel_Type_Cd
		,tgt.Fulfillment_Channel_Ind
		,tgt.Fulfillment_Channel_Dsc
        ,tgt.OMS_Offer_Id
        ,src_wrk.FileName
        ,TRUE AS DW_Logical_delete_ind  
        ,'U' AS DML_Type  
        ,CASE WHEN DW_First_Effective_dt = CURRENT_DATE  THEN 1 Else 0 END as Sameday_chg_ind
             FROM ${tgt_tbl} tgt
            LEFT JOIN
            (
            SELECT distinct 
                           Fulfillment_Channel_Type_Cd
						   ,OMS_Offer_Id
                           ,FileName
            FROM ${src_wrk_tmp_tbl}
            ) src 
              ON 
			  SRC.Fulfillment_Channel_Type_Cd = TGT.Fulfillment_Channel_Type_Cd
               and src.OMS_Offer_Id = tgt.OMS_Offer_Id
			 
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
              
              and src.Fulfillment_Channel_Type_Cd is NULL
					  and src.OMS_Offer_Id is NULL
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
			
			Fulfillment_Channel_Type_Cd,
		
			OMS_Offer_Id,
			
			filename
		FROM ${tgt_wrk_tbl}
		WHERE 
			DML_Type = 'U' AND 
			Sameday_chg_ind = 0 AND
			OMS_Offer_Id is not NULL AND
			Fulfillment_Channel_Type_Cd is not NULL 
			
			
		) src
		WHERE 
			
			tgt.Fulfillment_Channel_Type_Cd = src.Fulfillment_Channel_Type_Cd AND
			tgt.OMS_Offer_Id = src.OMS_Offer_Id AND
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
		
     	// SCD Type1 - Processing Sameday updates
	var sql_sameday = `
	UPDATE ${tgt_tbl} as tgt
	SET 
	    
		Fulfillment_Channel_Type_Cd = src.Fulfillment_Channel_Type_Cd,
	    Fulfillment_Channel_Ind = src.Fulfillment_Channel_Ind,
    	Fulfillment_Channel_Dsc=src.Fulfillment_Channel_Dsc,	
		DW_Logical_delete_ind = src.DW_Logical_delete_ind,
		OMS_Offer_Id = src.OMS_Offer_Id,
		DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
		DW_SOURCE_UPDATE_NM = filename
		FROM ( 
			SELECT
				
				Fulfillment_Channel_Type_Cd,
				Fulfillment_Channel_Ind,
				Fulfillment_Channel_Dsc,
				OMS_Offer_Id,
			    DW_Logical_delete_ind,
				filename
			FROM ${tgt_wrk_tbl}
			WHERE 
				DML_Type = 'U' AND 
				Sameday_chg_ind = 1 AND
				OMS_Offer_Id is not NULL AND
				Fulfillment_Channel_Type_Cd is not null
				
				
		) src
		WHERE
			
			tgt.Fulfillment_Channel_Type_Cd = src.Fulfillment_Channel_Type_Cd AND
			tgt.OMS_Offer_Id = src.OMS_Offer_Id AND
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
                                
                            
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
					 
					 Fulfillment_Channel_Type_Cd 
                    ,Fulfillment_Channel_Ind					
					,DW_First_Effective_Dt      
					,DW_Last_Effective_Dt
					,Fulfillment_Channel_Dsc          
					,OMS_Offer_Id
					,DW_CREATE_TS                                                                                             
                    ,DW_LOGICAL_DELETE_IND                                                                                           
                    ,DW_SOURCE_CREATE_NM                                                                                         
                    ,DW_CURRENT_VERSION_IND      
                   )
                   SELECT distinct
                      
					 Fulfillment_Channel_Type_Cd 
					,Fulfillment_Channel_Ind
					,CURRENT_TIMESTAMP 
                    ,'31-DEC-9999'     
                    ,Fulfillment_Channel_Dsc
                    ,OMS_Offer_Id					
					  
					,CURRENT_TIMESTAMP
                    ,False
                    ,FileName
                    ,TRUE                                                                                                      
				FROM ${tgt_wrk_tbl}
                where OMS_Offer_Id is not null
				and Fulfillment_Channel_Type_Cd is not null
				
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
