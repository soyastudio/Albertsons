--liquibase formatted sql
--changeset SYSTEM:SP_GetOfferRequest_To_Analytical_LOAD_DIM_Store runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_ANALYTICS_PRD;
use schema DW_RETAIL_EXP;

CREATE OR REPLACE PROCEDURE EDM_ANALYTICS_PRD.DW_RETAIL_EXP.SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_STORE(SRC_WRK_TBL VARCHAR, ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, CNF_DB VARCHAR, CNF_LOC_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
 
    // **************        Load for Dim_Store table BEGIN *****************
    var src_wrk_tbl = SRC_WRK_TBL;
    var anl_db = ANL_DB;
    var anl_schema = ANL_SCHEMA;
    var wrk_schema = WRK_SCHEMA;
    var cnf_db = CNF_DB ;
    var cnf_loc_schema = CNF_LOC_SCHEMA;
   
    
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
    
    var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".Dim_Store_WRK";
    var tgt_tbl = anl_db + "." + anl_schema + ".Dim_Store";
    var fac_lkp_tbl = cnf_db + "." + cnf_loc_schema + ".facility";
    

    var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS
                            SELECT src.Store_Id
                                                ,src.Store_Dsc
                                                ,src.DW_Logical_delete_ind
                                                ,CASE WHEN tgt.Store_Id is NULL then 'I' ELSE 'U' END as DML_Type
                                                FROM 
                                                (select f.facility_Nbr as Store_Id 
                                                        ,NULL as Store_Dsc 
                                                        ,DW_Logical_delete_ind
                                                        FROM 
                                                        (select Store_Id
                                                        from 
                                                        (SELECT   payload_stores as Store_Id 
                                                                 ,row_number() over ( PARTITION BY payload_stores ORDER BY to_timestamp_ntz(LASTUPDATETS) desc) as rn
                                                                 FROM   ` + src_wrk_tbl +`
                                                                 WHERE payload_stores is NOT NULL
                                                         ) sg
                                                         where rn = 1) sg
                                                        JOIN
                                                        (select facility_nbr 
                                                        ,dw_Logical_delete_ind
                                                        from ` + fac_lkp_tbl + `
                                                        where dw_current_version_ind = TRUE
                                                        ) f
                                                        ON sg.Store_Id = f.facility_nbr
                                                    ) src
                                LEFT JOIN
                                        (
                                         SELECT 
                                                Store_Id
                                               ,Store_Dsc
                                               ,DW_Logical_delete_ind
                                         FROM   dim_store
                                         ) tgt  on  src.Store_Id = tgt.Store_Id
                                 where tgt.Store_Id is null 
                                  OR (
                                  nvl(tgt.Store_Dsc, '-1') <> nvl(src.Store_Dsc, '-1') OR
                                  src.DW_Logical_delete_ind <> tgt.DW_Logical_delete_ind
                                     )`;
                                  
                            
    
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Creation of Dim_Store tgt_wrk_tbl table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

var sql_updates = // Processing Updates of Type 2 SCD
                  ` UPDATE ` + tgt_tbl + ` as tgt
                    SET  DW_Logical_delete_ind = src.DW_Logical_delete_ind
                        ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
                    FROM (  SELECT   Store_Id
                            ,Store_Dsc  
                            ,DW_Logical_delete_ind       
                            FROM    `+ tgt_wrk_tbl +` 
                            WHERE   DML_Type = 'U'
                          ) src
                    WHERE   src.Store_Id = tgt.Store_Id `;  
         
var sql_begin = "BEGIN"

    // Processing Inserts
    var sql_inserts = `INSERT INTO ` + tgt_tbl + `

        ( 
         Store_Id 
        ,Store_Dsc
        ,DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    )
    SELECT
         Store_Id 
        ,Store_Dsc
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
        return "Loading of Dim_Store " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
    }
            // **************        Load for Dim_Store ENDs *****************
            
    return "Done"


$$;