--liquibase formatted sql
--changeset SYSTEM:SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_ROG runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_ANALYTICS_PRD;
use schema DW_RETAIL_EXP;

CREATE OR REPLACE PROCEDURE EDM_ANALYTICS_PRD.DW_RETAIL_EXP.SP_GETOFFERREQUEST_TO_ANALYTICAL_LOAD_DIM_ROG(ANL_DB VARCHAR, ANL_SCHEMA VARCHAR, CNF_DB VARCHAR, CNF_SCHEMA VARCHAR, WRK_SCHEMA VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

    // **************        Load for Dim_Rog table BEGIN *****************
   
    var anl_db = ANL_DB;
    var anl_schema = ANL_SCHEMA;
    var wrk_schema = WRK_SCHEMA;
    var cnf_db = CNF_DB ;
    var cnf_schema = CNF_SCHEMA;
    
    
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
    
    var tgt_wrk_tbl =  anl_db + "." + wrk_schema + ".Dim_Rog_WRK";
    var tgt_tbl = anl_db + "." + anl_schema + ".Dim_Rog";
    var store_tbl = anl_db + "." + anl_schema + ".Dim_Store";
    var fac_lkp_tbl = cnf_db + "." + cnf_schema + ".facility";
    var rs_lkp_tbl = cnf_db + "." + cnf_schema + ".RETAIL_STORE";
    var rog_div_lkp_tbl = cnf_db + "." + cnf_schema + ".retail_order_group_division";
    var rog_lkp_tbl = cnf_db + "." + cnf_schema + ".retail_order_group";
    var div_lkp_tbl = cnf_db + "." + cnf_schema + ".division";
    

    var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE `+ tgt_wrk_tbl +` AS
                                             SELECT src.Rog_Id
                                                ,src.Rog_Dsc
                                                ,src.division_id
                                                ,src.division_nm
                                                ,src.DW_Logical_delete_ind
                                                ,CASE WHEN tgt.Rog_Id is NULL then 'I' ELSE 'U' END as DML_Type
                                                FROM 
                                                (select distinct rd.rog_id
                                                        ,rog.Rog_Nm as Rog_Dsc
                                                        ,rd.division_id
                                                        ,CASE WHEN d.division_nm = 'US RETAIL' THEN 'SOUTHWEST'
                                                           ELSE d.division_nm END AS division_nm
                                                        ,rog.DW_Logical_delete_ind
                                                        FROM
                                                        (SELECT store_id 
                                                        FROM   ` + store_tbl +`
                                                        WHERE dw_logical_Delete_ind = FALSE) s
                                                        JOIN
                                                        (SELECT facility_integration_id
                                                        ,facility_nbr
                                                        FROM  ` + fac_lkp_tbl +`
                                                         WHERE dw_logical_delete_ind = FALSE 
                                                        AND DW_current_version_ind = TRUE
                                                        ) f
                                                        ON s.store_id = f.facility_nbr
                                                        JOIN  ` + rs_lkp_tbl +` rs
                                                        ON rs.facility_integration_id = f.facility_integration_id 
														AND rs.dw_current_version_ind = TRUE
														AND rs.rog_id in  ('SDEN','SHGN','AIMT','AJWL','ACME','AKBA','SWMA','SNCA','SHAW',
														'APHO','VLAS','SPRT','SSEA','SSPK','SACG','ASHA','AVMT','PSOC','VSOC','ADAL','RDAL',
														'RHOU','SPHO','UNTD')
                                                        JOIN (select division_id, rog_id  from ` + rog_div_lkp_tbl +` where dw_current_version_ind = TRUE )  rd
                                                        ON rd.rog_id = rs.rog_id
                                                        JOIN ` + rog_lkp_tbl +` rog
                                                        ON rd.rog_id = rog.rog_id
														AND rog.dw_current_version_ind = TRUE
                                                        JOIN ` + div_lkp_tbl +` d
                                                        ON rd.division_id = d.division_id
														and d.corporation_id = '001'
														and d.dw_current_version_ind = TRUE ) src
                                                        LEFT JOIN
                                                                (
                                                                 SELECT 
                                                                        Rog_Id
                                                                       ,Rog_Dsc
                                                                       ,Division_Id
                                                                       ,Division_Nm
                                                                       ,DW_Logical_delete_ind
                                                                 FROM   ` + tgt_tbl +` 
                                                                 ) tgt  on  src.Rog_Id = tgt.Rog_Id
                                 where tgt.Rog_Id is null 
                                  OR (
                                  nvl(tgt.Rog_Dsc, '-1') <> nvl(src.Rog_Dsc, '-1') OR
                                  nvl(tgt.Division_Id, '-1') <> nvl(src.Division_Id, '-1') OR
                                  nvl(tgt.Division_Nm, '-1') <> nvl(src.Division_Nm, '-1') OR
                                  src.DW_Logical_delete_ind <> tgt.DW_Logical_delete_ind
                                     )`;
                                  
                            
    
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        )
    }
    catch (err)  {
        return "Creation of Dim_Rog tgt_wrk_tbl table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

var sql_updates = // Processing Updates of Type 2 SCD
                  ` UPDATE ` + tgt_tbl + ` as tgt
                    SET  Rog_Dsc = src.Rog_Dsc
                        ,Division_Id = src.Division_Id
                        ,Division_Nm = src.Division_Nm
                        ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
                        ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
                    FROM (  SELECT   Rog_Id
                            ,Rog_Dsc  
                            ,Division_Id
                            ,Division_Nm
                            ,DW_Logical_delete_ind       
                            FROM    `+ tgt_wrk_tbl +` 
                            WHERE   DML_Type = 'U'
                          ) src
                    WHERE   src.Rog_Id = tgt.Rog_Id `;  
         
var sql_begin = "BEGIN"

    // Processing Inserts
    var sql_inserts = `INSERT INTO ` + tgt_tbl + `

        ( 
         Rog_Id 
        ,Rog_Dsc
        ,Division_Id
        ,Division_Nm
        ,DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    )
    SELECT
         Rog_Id 
        ,Rog_Dsc
        ,Division_Id
        ,Division_Nm
        ,current_timestamp() AS DW_CREATE_TS 
        ,DW_LOGICAL_DELETE_IND
    FROM ` + tgt_wrk_tbl + `
    WHERE DML_TYPE = 'I'
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
        return "Loading of Dim_Rog " + tgt_tbl + " Failed with error: " + err;   // Return a error message.
    }
            // **************        Load for Dim_Rog ENDs *****************
            
    return "Done"


$$;