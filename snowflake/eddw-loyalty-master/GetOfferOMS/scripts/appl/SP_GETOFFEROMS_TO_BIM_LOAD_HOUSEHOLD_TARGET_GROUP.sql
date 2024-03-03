CREATE OR REPLACE PROCEDURE SP_GETOFFEROMS_TO_BIM_LOAD_HOUSEHOLD_TARGET_GROUP
("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_PRODUCT" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_PRODUCT;
	var ref_schema = "DW_R_PRODUCT";
	var ref_db = "EDM_REFINED_PRD";
	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Oms_Offer_Household_Target_Group_wrk`;
    var tgt_tbl = `${CNF_DB}.${cnf_schema}.Oms_Offer_Household_Target_Group`;
	var flat_tbl = `${ref_db}.${ref_schema}.OFFEROMS_FLAT`;

// ************** Load for OMS_Offer table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_truncate_wrk_tbl = `Truncate table  ${tgt_wrk_tbl}`;

var sql_command = `INSERT INTO ${tgt_wrk_tbl}  
                            WITH src_wrk_tbl_recs as 
                            (					
						       SELECT DISTINCT 
								FLT.payload_id as OMS_Offer_Id
								,FLT.payload_externalOfferId as External_Offer_Id
								,FLT.payload_offerRequestId as Offer_Request_Id
								,FLT.lastUpdateTS
								,FLT.Filename
								,FLT.PAYLOAD_HOUSEHOLDTARGETING 
							    ,ROW_NUMBER() OVER (PARTITION BY FLT.payload_id  ORDER BY FLT.lastUpdateTS DESC) AS RN
                          FROM ${src_wrk_tbl} SRC
						  INNER JOIN ${flat_tbl} FLT ON SRC.payload_id = FLT.payload_id
						  )
							     
                          SELECT
                          src.OMS_Offer_Id
                         ,src.Target_group_id
						 ,src.Target_Group_Element_Id
                         ,src.Element_type_nm
                         ,src.Element_option_nm
                         ,TO_TIMESTAMP(dateadd('s',src.Element_Start_Ts,'1970-01-01')) as Element_Start_Ts
						 ,TO_TIMESTAMP(dateadd('s',src.Element_End_Ts,'1970-01-01')) as Element_End_Ts
						 ,src.Instore_Channel_Type_Ind
						 ,src.Ecommerce_Channel_Type_Ind
                         ,CASE WHEN (tgt.OMS_Offer_Id IS NULL ) THEN 'I' ELSE 'U' END AS DML_Type
                         ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
						 ,src.DW_LOGICAL_DELETE_IND
                         ,src.DW_CURRENT_VERSION_IND
                         ,CURRENT_DATE as DW_First_Effective_dt
                         ,'31-DEC-9999' as DW_Last_Effective_dt
                         ,Filename as DW_SOURCE_CREATE_NM
                          from
                          (
						  SELECT DISTINCT OMS_OFFER_ID, Target_group_id,			Target_Group_Element_Id, 
							CASE WHEN  Channel_detais  like '%Instore%' THEN  TRUE ELSE FALSE END AS  Instore_Channel_Type_Ind,CASE WHEN  Channel_detais Like '%Ecommerce%' THEN TRUE ELSE FALSE END AS  Ecommerce_Channel_Type_Ind,
							Element_Start_Ts, Element_End_Ts,Element_option_nm,Element_type_nm,FALSE as DW_LOGICAL_DELETE_IND, TRUE as DW_CURRENT_VERSION_IND , Filename
							FROM (
							select  DISTINCT   OMS_OFFER_ID, f1.value as "targeting_group",  Filename,  
							REPLACE(f1.value:id,'"','') as Target_group_id, 
							MD5(CONCAT(REPLACE(f1.value:type ,'"',''),REPLACE(f1.value:option,'"',''),REPLACE(NVL(f1.value:time_frame_from,'1999-01-01'),'"',''),REPLACE( NVL(f1.value:time_frame_end,'9999-12-31'),'"','')))  Target_Group_Element_Id,
							REPLACE(f1.value:type ,'"','') as Element_type_nm, 
							REPLACE(f1.value:option,'"','') as Element_option_nm, 
							REPLACE( f1.value:time_frame_end,'"','') as Element_End_Ts, 
							REPLACE(f1.value:time_frame_from,'"','') as Element_Start_Ts, 
							REPLACE( f1.value:channel,'"','') as Channel_detais , 
							REPLACE(channel.value ,'"','') as Channel
							from src_wrk_tbl_recs  p
							,lateral flatten(input => parse_json(p.PAYLOAD_HOUSEHOLDTARGETING) , path => 'household_targeting') f
							,lateral flatten(input => f.value:targeting_group) f1
							,Table(Flatten( f1.value:channel,outer => true)) channel
							,Table(Flatten( f1.value:item_details,outer => true)) item_details WHERE RN = 1
							 )
						  						  
                          ) src 
                          LEFT JOIN 
                          (SELECT  DISTINCT
                           tgt.OMS_Offer_Id 
						  ,tgt.Target_group_id
						 ,tgt.Target_Group_Element_Id
						 ,tgt.Instore_Channel_Type_Ind
						 ,tgt.Ecommerce_Channel_Type_Ind
						 ,tgt.Element_Start_Ts
						 ,tgt.Element_End_Ts
						 ,tgt.Element_option_nm
						 ,tgt.Element_type_nm                                                                                                   
                          ,tgt.DW_LOGICAL_DELETE_IND
                          ,tgt.dw_first_effective_dt
                          FROM ${tgt_tbl} tgt 
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt 
                          ON tgt.OMS_Offer_Id = src.OMS_Offer_Id  and   tgt.Target_group_id = src.Target_group_id and   tgt.Target_Group_Element_Id = src.Target_Group_Element_Id						  
                          WHERE  (tgt.OMS_Offer_Id is null or tgt.Target_group_id is null or tgt.Target_Group_Element_Id is null  )   
                          or(
                          src.Instore_Channel_Type_Ind <> tgt.Instore_Channel_Type_Ind                                                                                                                  
                          OR src.Ecommerce_Channel_Type_Ind <> tgt.Ecommerce_Channel_Type_Ind                                                                                               
                          OR NVL(src.Element_Start_Ts,'-1') <> NVL(tgt.Element_Start_Ts,'-1')   
                          OR NVL(src.Element_End_Ts,'-1') <> NVL(tgt.Element_End_Ts,'-1')  
                          OR NVL(src.Element_option_nm,'-1') <> NVL(tgt.Element_option_nm,'-1')              
                          OR NVL(src.Element_type_nm,'-1') <> NVL(tgt.Element_type_nm,'-1')
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )
						  `;
                          
						 

try {
        snowflake.execute ({ sqlText: sql_truncate_wrk_tbl});
        snowflake.execute ({sqlText: sql_command  });
        }
    catch (err)  {
        return "Creation of OMS_Offer work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

var sql_begin = "BEGIN"

// SCD Type2 - Processing Different day updates
              var sql_updates = `UPDATE ${tgt_tbl} as tgt
              SET 
                         
                             DW_CURRENT_VERSION_IND = FALSE,
                             DW_LOGICAL_DELETE_IND = TRUE,
                             DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP ,
                             DW_Last_Effective_dt = CURRENT_DATE - 1,
                             DW_SOURCE_UPDATE_NM = filename
              FROM ( 
                             SELECT 
                                           OMS_Offer_Id,                              
                                           Target_group_id,
										   Target_Group_Element_Id,
                                           DW_SOURCE_CREATE_NM as Filename
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U' 
                             AND Sameday_chg_ind = 0                                      
                             AND OMS_Offer_Id is not NULL                              
                             ) src
                             WHERE tgt.OMS_Offer_Id = src.OMS_Offer_Id and   tgt.Target_group_id = src.Target_group_id and   tgt.Target_Group_Element_Id = src.Target_Group_Element_Id
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE 
							 AND  tgt.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET  Instore_Channel_Type_Ind = src.Instore_Channel_Type_Ind                                                
						,Ecommerce_Channel_Type_Ind = src.Ecommerce_Channel_Type_Ind                                                               
						,Element_Start_Ts = src.Element_Start_Ts  
						,Element_End_Ts = src.Element_End_Ts						
						,Element_option_nm = src.Element_option_nm                                                                
						,Element_type_nm = src.Element_type_nm  
						,DW_Logical_delete_ind = src.DW_Logical_delete_ind
						,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
						FROM ( SELECT 
						OMS_Offer_Id  ,Target_group_id  ,Target_Group_Element_Id     ,Instore_Channel_Type_Ind    ,Ecommerce_Channel_Type_Ind                                                                                               
									,Element_Start_Ts,Element_End_Ts
									,Element_option_nm
									,Element_type_nm
									,DW_Logical_delete_ind
									FROM ${tgt_wrk_tbl}
									WHERE DML_Type = 'U'
									AND Sameday_chg_ind = 1
									AND OMS_Offer_Id IS NOT NULL									
									) src
							WHERE tgt.OMS_Offer_Id = src.OMS_Offer_Id  and   tgt.Target_group_id = src.Target_group_id and   tgt.Target_Group_Element_Id = src.Target_Group_Element_Id
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
					OMS_Offer_Id
					,Target_group_id
					,Target_Group_Element_Id
					,Instore_Channel_Type_Ind
					,Ecommerce_Channel_Type_Ind
					,Element_Start_Ts
					,Element_End_Ts
					,Element_option_nm
					,Element_type_nm              
                    ,DW_CREATE_TS
                    ,DW_LOGICAL_DELETE_IND
                    ,DW_CURRENT_VERSION_IND
                    ,DW_First_Effective_Dt 
                    ,DW_Last_Effective_dt 
                    ,DW_SOURCE_CREATE_NM 
                   )
                   SELECT distinct
                      OMS_Offer_Id
					,Target_group_id
					,Target_Group_Element_Id
					,Instore_Channel_Type_Ind
					,Ecommerce_Channel_Type_Ind
					,Element_Start_Ts
					,Element_End_Ts
					,Element_option_nm
					,Element_type_nm                    
					 ,CURRENT_TIMESTAMP
                     ,DW_Logical_delete_ind
                     ,TRUE
	                 ,DW_First_Effective_Dt 
                    ,DW_Last_Effective_dt 
                    ,DW_SOURCE_CREATE_NM
			
				FROM ${tgt_wrk_tbl}
                where OMS_Offer_Id is not null
			  and Sameday_chg_ind = 0
               
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

        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }

// ************** Load for OMS_Offer table ENDs *****************

$$;
