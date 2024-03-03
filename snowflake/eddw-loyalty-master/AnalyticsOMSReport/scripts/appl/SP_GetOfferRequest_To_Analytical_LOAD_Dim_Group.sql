--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferRequest_To_Analytical_LOAD_Dim_Group runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_ANALYTICS_PRD;
use schema DW_RETAIL_EXP;

CREATE OR REPLACE PROCEDURE EDM_ANALYTICS_PRD.DW_RETAIL_EXP.SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_GROUP(SRC_WRK_TBL VARCHAR, ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
    // **************        Load for Dim_Group table BEGIN *****************
    var src_wrk_tbl = SRC_WRK_TBL;
    var anl_db = ANL_DB;
    var anl_schema = ANL_SCHEMA;
    var wrk_schema = WRK_SCHEMA; 
    
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
    
    var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".Dim_Group_WRK";
    var tgt_tbl = anl_db + "." + anl_schema + ".Dim_Group";
    

    var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS
                            SELECT           src.Group_Cd
                                            ,src.Group_Dsc                     
                                            ,src.DW_LOGICAL_DELETE_IND
                                            ,CASE WHEN tgt.Group_Cd is NULL then 'I' ELSE 'U' END as DML_Type
                                    FROM (
                                        SELECT   Group_Cd
                                                ,Group_Dsc   
                                                ,FALSE AS DW_Logical_delete_ind
                                        FROM (
                                                SELECT   GroupCd as Group_Cd  
                                                        ,Groupnm as  Group_Dsc                               
                                                        ,ActionTypeCd  
                                                        ,row_number() over ( PARTITION BY Group_Cd ORDER BY to_timestamp_ntz(updatets) desc) as rn
                                                FROM   ` + src_wrk_tbl +`
                                                WHERE   Group_Cd is not null
                                                
                                            )
                                        WHERE rn = 1
                                    ) src
                                LEFT JOIN
                                        (
                                         SELECT 
                                                Group_Cd
                                               ,Group_Dsc 
                                               ,DW_Logical_delete_ind
                                         FROM   ` + tgt_tbl + `
                                         ) tgt  on  src.Group_Cd = tgt.Group_Cd
                                 where tgt.Group_Cd is null 
                                  OR (
                                  nvl(tgt.Group_Dsc , '-1') <> nvl(src.Group_Dsc, '-1') OR
                                  src.DW_Logical_delete_ind <> tgt.DW_Logical_delete_ind
                                     )
                                  `;
                            
    
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Creation of Dim_Group tgt_wrk_tbl table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

var sql_updates = // Processing Updates of Type 2 SCD
                  ` UPDATE ` + tgt_tbl + ` as tgt
                    SET  Group_Dsc = src.Group_Dsc 
                        ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
                        ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
                    FROM (  SELECT   Group_Cd
                            ,Group_Dsc 
                            ,DW_Logical_delete_ind   
                            FROM    `+ tgt_wrk_tbl +` 
                            WHERE   DML_Type = 'U'
                          ) src
                    WHERE   src.Group_Cd = tgt.Group_Cd `;  
         
var sql_begin = "BEGIN"

    // Processing Inserts
    var sql_inserts = `INSERT INTO ` + tgt_tbl + `
        ( 
         Group_Cd 
        ,Group_Dsc
        ,DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    )
    SELECT
         Group_Cd 
        ,Group_Dsc
        ,current_timestamp() AS DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    FROM ` + tgt_wrk_tbl + `
    WHERE DML_Type = 'I'
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
        return "Loading of Dim_Group " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
    }
            // **************        Load for Dim_Group ENDs *****************
            
    return "Done"


$$;