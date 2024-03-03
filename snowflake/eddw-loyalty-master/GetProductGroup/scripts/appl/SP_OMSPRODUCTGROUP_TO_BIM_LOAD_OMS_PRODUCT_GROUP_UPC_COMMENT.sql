--liquibase formatted sql
--changeset SYSTEM:SP_OMSPRODUCTGROUP_TO_BIM_LOAD_OMS_PRODUCT_GROUP_UPC_COMMENT runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_OMSPRODUCTGROUP_TO_BIM_LOAD_OMS_PRODUCT_GROUP_UPC_COMMENT(SRC_WRK_TBL VARCHAR, 
CNF_DB VARCHAR, C_PROD VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
   var src_wrk_tbl = SRC_WRK_TBL;
   var cnf_db = CNF_DB;
   var cnf_schema = C_PROD;
   var wrk_schema = C_STAGE;
   var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.OMS_Product_Group_UPC_Comment_WRK`;
   var tgt_tbl = `${cnf_db}.${cnf_schema}.OMS_Product_Group_UPC_Comment`;
   var src_wrk_tmp_tbl = `${cnf_db}.${wrk_schema}.OMS_Product_Group_UPC_Comment_SRC_WRK`;

var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ src_wrk_tmp_tbl +` AS
              with flat_tmp as
              (
SELECT
                            PAYLOAD_ID as Product_Group_Id
                            ,F.value::string  as UPC_Cd
							,Payload_Productgroupinfo_Comments_commentTs as Comment_Ts
							,Payload_Productgroupinfo_Comments_comment as comment_dsc
							,Payload_Productgroupinfo_Comments_commentBy as comment_by_user_id
                            ,sourceaction
                            ,filename
                            ,lastupdatets
                            ,row_number() over ( PARTITION BY Product_Group_Id, UPC_Cd,Comment_Ts
                        ORDER BY to_timestamp_ntz(LASTUPDATETS) desc) as rn
                            FROM  ${src_wrk_tbl},
                            Table(Flatten(${src_wrk_tbl}.PAYLOAD_PRODUCTGROUPIDS_UPCIDS)) F
                            WHERE
                                Product_Group_Id is not NULL  AND
                                UPC_Cd is not NULL AND Comment_Ts is not null )
                        (   SELECT
                                  Product_Group_Id
                                  ,UPC_Cd
								  ,comment_ts
								  ,comment_dsc
								  ,comment_by_user_id
                                  ,sourceaction
                                  ,filename
                                  ,lastupdatets
                                  FROM
                                  (
                                  SELECT distinct
                                  Product_Group_Id
                                  ,UPC_Cd
								  ,comment_ts
								  ,comment_dsc
								  ,comment_by_user_id
                                  ,sourceaction
                                  ,filename
                                  ,lastupdatets
                            FROM flat_tmp
where rn = 1
                            )
                            )  `;                        

     try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        );
    }
    catch (err)  {
       return "Creation of OMS_Product_Group_UPC_Comment src_wrk_tmp_tbl table  Failed with error: " + err;   // Return a error message.
        }
                      

    // **************        Load for OMS_Product_Group_UPC_Comment table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.
        var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
                SELECT distinct Product_Group_Id,
				UPC_Cd,
				comment_ts
								  ,comment_dsc
								  ,comment_by_user_id,
				filename,
				DW_Logical_delete_ind ,
				lastupdatets,
				sourceaction
                ,CASE WHEN   Product_Group_Id is NULL  AND  UPC_Cd is NULL THEN 'I' ELSE 'U' END as DML_Type
                ,CASE WHEN to_date( DW_First_Effective_ts) = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
                   FROM (     
                              SELECT   tgt.Product_Group_Id,
									   tgt.UPC_Cd
							          ,tgt.comment_ts
								  ,tgt.comment_dsc
								  ,tgt.comment_by_user_id,
							  tgt.DW_Logical_delete_ind,
							  tgt.dw_first_effective_ts,
							  src.filename,
							  src.lastupdatets,
							  src.sourceaction
                              FROM ${tgt_tbl} tgt
                              JOIN 
                                (SELECT Product_Group_Id ,
								UPC_Cd 
								,comment_ts
								  ,comment_dsc
								  ,comment_by_user_id,
								filename ,FALSE as DW_Logical_delete_ind ,lastupdatets,sourceaction
                                    FROM  `+ src_wrk_tmp_tbl +`) as src 
                                    on   src.Product_Group_Id = tgt.Product_Group_Id 
                              LEFT JOIN (SELECT Product_Group_Id ,
							                  UPC_Cd ,
							                comment_ts
								  ,comment_dsc
								  ,comment_by_user_id,
							  filename 
							  ,FALSE as DW_Logical_delete_ind ,
							  lastupdatets,
							  sourceaction
                                    FROM  `+ src_wrk_tmp_tbl +`) as src1 
                                   on   src1.Product_Group_Id = tgt.Product_Group_Id  and src1.UPC_CD =tgt.UPC_CD and src1.comment_ts= tgt.comment_ts
                                  AND src1.Product_Group_Id IS NULL  AND tgt.DW_CURRENT_VERSION_IND=TRUE AND tgt.DW_LOGICAL_DELETE_IND=FALSE 
								  AND(
								  NVL(tgt.comment_dsc,'-1') <> NVL(src.comment_dsc,'-1') OR
										NVL(tgt.comment_by_user_id,'-1') <> NVL(src.comment_by_user_id,'-1')
										))
                             `;           
          try {
                  snowflake.execute ({ sqlText: sql_command });
              } 
             catch (err) {
             return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
            }
           
              var sql_undeletes = `UPDATE `+ tgt_tbl +` as tgt
              SET             dw_last_effective_ts = timestampadd(minute, -1, current_timestamp)
                             ,DW_CURRENT_VERSION_IND = FALSE 
                             ,DW_SOURCE_UPDATE_NM = filename
                             ,DW_LAST_UPDATE_TS =current_timestamp
              FROM (
                             SELECT distinct  Product_Group_Id
                                           ,UPC_Cd
										   ,comment_ts
										     ,comment_dsc
								            ,comment_by_user_id
							  
                                           ,filename
                                           
                             FROM ` + tgt_wrk_tbl +` tgt1 
               
                             ) src
                             --WHERE Sameday_chg_ind = 1
                             WHERE src.Product_Group_Id=tgt.Product_Group_Id and src.UPC_CD = tgt.UPC_CD and src.comment_ts=tgt.comment_ts
                                
                           
                            `;
         
         var sql_begin = "BEGIN"     
              // Processing Inserts
                               var sql_inserts = `INSERT INTO ${tgt_tbl}
                                (
                                           Product_Group_Id
											,UPC_Cd
											,comment_ts
											  ,comment_dsc
											,comment_by_user_id
                                           ,dw_first_effective_ts
                                           ,dw_last_effective_ts
                                           ,DW_CREATE_TS          
                                           ,DW_LOGICAL_DELETE_IND 
                                           ,DW_SOURCE_CREATE_NM  
                                           ,DW_CURRENT_VERSION_IND 
                                           )
                             SELECT
                                           src.Product_Group_Id
                                           ,src.UPC_Cd
										   ,src.comment_ts
										     ,src.comment_dsc
								  ,src.comment_by_user_id
							               ,CURRENT_TIMESTAMP
                                           ,'9999-12-31 00:00:00.000'
                                           ,CURRENT_TIMESTAMP
                                           ,FALSE as DW_Logical_delete_ind
                                           ,src.filename
                                           ,TRUE
                                FROM ${src_wrk_tmp_tbl} src 
                                LEFT JOIN ${tgt_tbl}  tgt on  src.Product_Group_Id =tgt.Product_Group_Id and src.UPC_Cd=tgt.UPC_Cd and src.comment_ts=tgt.comment_ts AND tgt.DW_CURRENT_VERSION_IND=TRUE AND tgt.DW_LOGICAL_DELETE_IND=FALSE
                                 AND
                                tgt.Product_Group_Id is  NULL AND
                               tgt.UPC_Cd is  null AND
							   tgt.comment_ts is null
                               UNION ALL
                           SELECT
                                     src.Product_Group_Id
                                     ,src.UPC_Cd
									 ,src.comment_ts
									   ,src.comment_dsc
								  ,src.comment_by_user_id
							  
                                     ,CURRENT_TIMESTAMP
                                     ,'9999-12-31 00:00:00.000'
                                     ,CURRENT_TIMESTAMP
                                     ,TRUE as DW_Logical_delete_ind
                                     ,src.filename
                                     ,TRUE
                                FROM ${tgt_wrk_tbl} src 
                           
                
                
                                           `;
    var sql_commit = "COMMIT";
    var sql_rollback = "ROLLBACK";
    try {
        snowflake.execute (
           {sqlText: sql_begin  }
            );
   
        /* snowflake.execute (
            {sqlText: sql_deletes  }
            );  */                                        

        snowflake.execute (
            {sqlText: sql_undeletes  }
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
         
                // **************        Load for OMS_Product_Group_UPC_Comment table ENDs *****************
    $$;
