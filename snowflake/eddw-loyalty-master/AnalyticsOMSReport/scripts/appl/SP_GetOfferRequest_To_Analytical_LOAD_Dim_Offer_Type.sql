--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferRequest_To_Analytical_LOAD_Dim_Offer_Type runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_ANALYTICS_PRD;
use schema DW_RETAIL_EXP;

CREATE OR REPLACE PROCEDURE EDM_ANALYTICS_PRD.DW_RETAIL_EXP.SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_OFFER_TYPE(SRC_WRK_TBL VARCHAR, ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
    // **************        Load for Dim_Offer_Type table BEGIN *****************
    var src_wrk_tbl = SRC_WRK_TBL;
    var anl_db = ANL_DB;
    var anl_schema = ANL_SCHEMA;
    var wrk_schema = WRK_SCHEMA; 
    
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
    
    var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".Dim_Offer_Type_WRK";
    var tgt_tbl = anl_db + "." + anl_schema + ".Dim_Offer_Type";
    

    var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS
                            SELECT          src.Offer_Type_Cd
                                            ,src.Offer_Type_Dsc                     
                                            ,src.DW_LOGICAL_DELETE_IND
                                            ,CASE WHEN tgt.Offer_Type_Cd is NULL then 'I' ELSE 'U' END as DML_Type
                                    FROM (
                                        SELECT   Offer_Type_Cd
                                                ,Offer_Type_Dsc
                                                ,FALSE AS DW_Logical_delete_ind
                                        FROM (
                                                SELECT   Prototype_code as Offer_Type_Cd                                               
                                                        ,Prototype_description as Offer_Type_Dsc                                                      
                                                        ,ActionTypeCd  
                                                        ,row_number() over ( PARTITION BY Offer_Type_Cd ORDER BY to_timestamp_ntz(updatets) desc) as rn
                                                FROM   ` + src_wrk_tbl +`
                                                WHERE   Offer_Type_Cd is not null
                                                
                                            )
                                        WHERE rn = 1
                                    ) src
                                LEFT JOIN
                                        (
                                         SELECT 
                                                Offer_Type_Cd
                                               ,Offer_Type_Dsc
                                               ,DW_Logical_delete_ind
                                         FROM   ` + tgt_tbl + `
                                         ) tgt  on  src.Offer_Type_Cd = tgt.Offer_Type_Cd
                                 where tgt.Offer_Type_Cd is null 
                                  OR (
                                  nvl(tgt.Offer_Type_Dsc, '-1') <> nvl(src.Offer_Type_Dsc, '-1') OR
                                  src.DW_Logical_delete_ind <> tgt.DW_Logical_delete_ind
                                     )
                                  `;
                            
    
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Creation of Dim_Offer_Type tgt_wrk_tbl table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

var sql_updates = // Processing Updates of Type 2 SCD
                  ` UPDATE ` + tgt_tbl + ` as tgt
                    SET  Offer_Type_Dsc = src.Offer_Type_Dsc
                        ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
                        ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
                    FROM (  SELECT   Offer_Type_Cd
                            ,Offer_Type_Dsc  
                            ,DW_Logical_delete_ind       
                            FROM    `+ tgt_wrk_tbl +` 
                            WHERE   DML_Type = 'U'
                          ) src
                    WHERE   src.Offer_Type_Cd = tgt.Offer_Type_Cd `;  
         
var sql_begin = "BEGIN"

    // Processing Inserts
    var sql_inserts = `INSERT INTO ` + tgt_tbl + `

        ( 
         Offer_Type_Cd 
        ,Offer_Type_Dsc
        ,DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    )
    SELECT
         Offer_Type_Cd 
        ,Offer_Type_Dsc
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
        return "Loading of Dim_Offer_Type " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
    }
            // **************        Load for Dim_Offer_Type ENDs *****************
            
    return "Done"


$$;