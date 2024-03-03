--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferRequest_To_Analytical_LOAD_DIM_Store_Group runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_ANALYTICS_PRD;
use schema DW_RETAIL_EXP;

CREATE OR REPLACE PROCEDURE EDM_ANALYTICS_PRD.DW_RETAIL_EXP.SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_STORE_GROUP(SRC_WRK_TBL VARCHAR, ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
    // **************        Load for DIM_Store_Group table BEGIN *****************
    var src_wrk_tbl = SRC_WRK_TBL;
    var anl_db = ANL_DB;
    var anl_schema = ANL_SCHEMA;
    var wrk_schema = WRK_SCHEMA; 
    
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
    
    var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".DIM_Store_Group_WRK";
    var tgt_tbl = anl_db + "." + anl_schema + ".DIM_Store_Group";
    
    
    var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS
                            SELECT           src.Store_Group_Id
                                            ,src.Store_Group_Nm   
                                            ,src.Store_Group_Category_Cd  
                                            ,src.Store_Group_Dsc                   
                                            ,src.DW_LOGICAL_DELETE_IND
                                            ,CASE WHEN tgt.Store_Group_Id is NULL then 'I' ELSE 'U' END as DML_Type
                                    FROM (
                                        SELECT   Store_Group_Id
                                                ,Store_Group_Nm
                                                ,Store_Group_Category_Cd   
                                                ,Store_Group_Dsc
                                                ,FALSE AS DW_Logical_delete_ind
                                        FROM (
                                                SELECT   storegroupid as Store_Group_Id                                               
                                                        ,storegroupnm as Store_Group_Nm  
                                                        ,StoreGroupType_Code as  Store_Group_Category_Cd 
                                                        ,storegroupdsc as Store_Group_Dsc                              
                                                        ,ActionTypeCd  
                                                        ,row_number() over ( PARTITION BY Store_Group_Id, StoreGroupType_Code ORDER BY to_timestamp_ntz(updatets) desc) as rn
                                                FROM   ` + src_wrk_tbl +`
                                                WHERE   Store_Group_Id is not null
                                                
                                            )
                                        WHERE rn = 1
                                    ) src
                                LEFT JOIN
                                        (
                                         SELECT 
                                                Store_Group_Id
                                               ,Store_Group_Nm
                                               ,Store_Group_Category_Cd 
                                               ,Store_Group_Dsc
                                               ,DW_Logical_delete_ind
                                         FROM   ` + tgt_tbl + `
                                         ) tgt  on  src.Store_Group_Id = tgt.Store_Group_Id
                                               and src.Store_Group_Category_Cd = tgt.Store_Group_Category_Cd
                                 where tgt.Store_Group_Id is null 
                                  OR (
                                  nvl(tgt.Store_Group_Nm, '-1') <> nvl(src.Store_Group_Nm, '-1') OR
                                  nvl(tgt.Store_Group_Dsc , '-1') <> nvl(src.Store_Group_Dsc, '-1') OR
                                  src.DW_Logical_delete_ind <> tgt.DW_Logical_delete_ind
                                     )
                                  `;
                            
    
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Creation of DIM_Store_Group tgt_wrk_tbl table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

var sql_updates = // Processing Updates of Type 2 SCD
                  ` UPDATE ` + tgt_tbl + ` as tgt
                    SET  Store_Group_Nm = src.Store_Group_Nm
                        ,Store_Group_Dsc = src.Store_Group_Dsc
                        ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
                        ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
                    FROM (  SELECT   Store_Group_Id
                            ,Store_Group_Nm  
                            ,Store_Group_Category_Cd    
                            ,Store_Group_Dsc 
                            ,DW_Logical_delete_ind
                            FROM    `+ tgt_wrk_tbl +` 
                            WHERE   DML_Type = 'U'
                          ) src
                    WHERE   src.Store_Group_Id = tgt.Store_Group_Id 
                    and     src.Store_Group_Category_Cd = tgt.Store_Group_Category_Cd `;  
         
var sql_begin = "BEGIN"

    // Processing Inserts
    var sql_inserts = `INSERT INTO ` + tgt_tbl + `
        ( 
         Store_Group_Id 
        ,Store_Group_Nm
        ,Store_Group_Category_Cd
        ,Store_Group_Dsc
        ,DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    )
    SELECT
         Store_Group_Id 
        ,Store_Group_Nm
        ,Store_Group_Category_Cd
        ,Store_Group_Dsc
        ,current_timestamp() AS DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    FROM ` + tgt_wrk_tbl + `
    WHERE   DML_Type = 'I'
   `; 

    var sql_commit = "COMMIT"
    var sql_rollback = "ROLLBACK"
    try {
        snowflake.execute (
            {sqlText: sql_begin}
        );
        snowflake.execute (
            {sqlText: sql_updates}
        );
        snowflake.execute (
            {sqlText: sql_inserts}
        );
        snowflake.execute (
            {sqlText: sql_commit}
        );    
    }
    catch (err) {
        snowflake.execute (
            {sqlText: sql_rollback}
        );
        return "Loading of DIM_Store_Group " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
    }
            // **************        Load for DIM_Store_Group ENDs *****************
            
    return "Done"


$$;