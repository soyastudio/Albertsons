--liquibase formatted sql
--changeset SYSTEM:SP_OMSStoreGroup_TO_BIM_LOAD_OMS_Store_Group_Store runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_OMSSTOREGROUP_TO_BIM_LOAD_OMS_STORE_GROUP_STORE(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_PROD VARCHAR, C_STAGE VARCHAR, C_LOC VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$



    var src_wrk_tbl = SRC_WRK_TBL;
    var cnf_db = CNF_DB;
    var cnf_schema = C_PROD ;
    var wrk_schema = C_STAGE ;
    var loc_schema = C_LOC ;
    var tgt_wrk_tbl =  cnf_db + "." + wrk_schema +  ".OMS_Store_Group_Store_WRK";
    var tgt_tbl = cnf_db + "." + cnf_schema + ".OMS_Store_Group_Store";
    var lkp_tbl1 = cnf_db + "." + loc_schema  + ".FACILITY";
    var lkp_tbl2 = cnf_db + "." + loc_schema  + ".Retail_Store";
    var src_wrk_tmp_tbl = cnf_db + "." + wrk_schema + ".OMS_Store_Group_Store_SRC_WRK";
    var tgt_exp_tbl = cnf_db + "." + wrk_schema + ".OMS_Store_Group_Store_Exceptions";

    var cr_src_wrk_tbl =`CREATE OR REPLACE TABLE `+ src_wrk_tmp_tbl +` AS

WITH tmp_flat AS
 (
        
        select    
        payload_id AS Store_Group_ID
        ,payload_stores
        ,sourceAction
        ,LastUpdateTs
        ,filename
        from ` + src_wrk_tbl +`
        where ( Store_Group_ID,LastUpdateTs,filename )
        in (
            select Store_Group_ID,LastUpdateTs,filename 
            from(
                  select    payload_id AS Store_Group_ID
                  ,payload_stores
                  ,sourceAction
                  ,LastUpdateTs
                  ,filename
                  ,row_number() over ( PARTITION BY Store_Group_Id ORDER BY to_timestamp_ntz(LastUpdateTs) desc) as rn       
              from ` + src_wrk_tbl +`
                  WHERE Store_Group_ID  is not null
                ) where rn =1
          )  
)

select distinct Store_Group_ID,
       facility_integration_id,
       sourceAction,
       LastUpdateTs ,
       filename
        FROM
        (
            SELECT  Store_Group_ID
            ,f.facility_integration_id
            ,sourceAction
            ,LastUpdateTs
            ,FileName
        FROM
        ( SELECT distinct Store_Group_ID
                        ,payload_stores
                        ,sourceAction
                        ,LastUpdateTs
                        ,FileName
                FROM  tmp_flat
                --where rn=1
        ) src  

left join
        (
                    SELECT ${lkp_tbl1}.Facility_Integration_Id, ${lkp_tbl1}.Facility_Nbr FROM ${lkp_tbl1}
                    INNER JOIN ${lkp_tbl2}
                    ON ${lkp_tbl1}.Facility_Integration_Id = ${lkp_tbl2}.Facility_Integration_Id
                    WHERE ${lkp_tbl1}.DW_CURRENT_VERSION_IND = TRUE
                    AND ${lkp_tbl2}.DW_CURRENT_VERSION_IND = TRUE
                    AND ${lkp_tbl1}.DW_LOGICAL_DELETE_IND = FALSE
           
        ) f
       
        ON LPAD(src.payload_stores,4,'0') = f.facility_nbr
          )

        `;

   try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        );
    }
    catch (err)  {
        //  return cr_src_wrk_tbl;
        return "Creation of OMS_Store_Group_Store src_wrk_tmp_tbl table "+ src_wrk_tmp_tbl +" Failed with error: " + err;   // Return a error message.
        }

var sql_command = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as
                            SELECT src.Store_Group_ID
                            ,src.FACILITY_INTEGRATION_ID
                            ,src.filename
                            ,src.DW_Logical_delete_ind
              		    ,src.LastUpdateTs
                           ,src.SourceAction
                            ,CASE WHEN tgt1.Store_Group_ID is NULL  then 'I' ELSE 'U' END as DML_Type
                            ,CASE WHEN to_date(DW_FIRST_EFFECTIVE_TS) = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
                        FROM (
                             SELECT Store_Group_ID
                            ,Facility_Integration_ID
                            ,filename
                            ,SourceAction
                            ,LastUpdateTs
                            ,FALSE as DW_Logical_delete_ind
                            FROM ` + src_wrk_tmp_tbl + `
                            )  src
                        LEFT JOIN (
                            Select
                             Store_Group_ID
                            ,FACILITY_INTEGRATION_ID
                            ,DW_Logical_delete_ind
                            ,DW_FIRST_EFFECTIVE_TS
                            from ` + tgt_tbl + `
                            where   DW_CURRENT_VERSION_IND = TRUE
                            )tgt1 on src.Store_Group_ID = tgt1.Store_Group_ID AND
                               tgt1.Facility_Integration_ID = src.Facility_Integration_ID
                                where tgt1.Store_Group_ID is NULL
                                AND tgt1.Facility_Integration_ID is NULL
                            OR
                          (  
                              tgt1.DW_Logical_delete_ind <> src.DW_Logical_delete_ind
                          )
                           `;              
                try {
                      snowflake.execute (
                      {sqlText: sql_command  }
                     );
                    }
             catch (err)  {
             return sql_command;
             //return "Creation of OMS_Store_Group_Store work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
                     }

var sql_deletes = `INSERT INTO `+ tgt_wrk_tbl +`
                               select
                                tgt.Store_Group_ID
                               ,tgt.FACILITY_INTEGRATION_ID
                              ,tgt.DW_SOURCE_CREATE_NM
                               ,TRUE AS DW_Logical_delete_ind  
                               ,src.lastupdatets
                               ,src.sourceaction
                               ,'U' AS DML_Type  
                               ,CASE WHEN to_date(DW_FIRST_EFFECTIVE_TS) = CURRENT_DATE  THEN 1 Else 0 END as Sameday_chg_ind
                        FROM    ` + tgt_tbl +` tgt
                        LEFT JOIN
                            (
                                SELECT distinct
                                         Store_Group_ID
                                        ,FACILITY_INTEGRATION_ID
                                        ,sourceaction
                   ,lastupdatets
                                        ,FileName
                                FROM  `+ src_wrk_tmp_tbl +`
                            ) src   ON  tgt.Store_Group_ID = src.Store_Group_ID
                                    AND tgt.FACILITY_INTEGRATION_ID = src.FACILITY_INTEGRATION_ID
                                   
                        WHERE (tgt.Store_Group_ID) in (select distinct Store_Group_ID
                                                    FROM `+ src_wrk_tmp_tbl +`
                                                    )
                        AND dw_current_version_ind = TRUE
                        AND dw_logical_delete_ind=FALSE
                        AND src.Store_Group_ID is NULL
                        AND src.FACILITY_INTEGRATION_ID is NULL
                        `;
    try {
        snowflake.execute (
            {sqlText: sql_deletes  }
            );
        }
    catch (err)  {

        return "Insert of Delete records for OMS_Store_Group_Store work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

// Insert into exception table
    var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl + `
                            SELECT Store_Group_ID
                            ,Facility_Integration_ID
                            ,SourceAction
                            ,FileName
			    ,LastUpdateTs
                            ,DW_LOGICAL_DELETE_IND
                            ,DML_TYPE
                            ,SAMEDAY_CHG_IND
                            ,CASE WHEN Facility_Integration_ID is NULL THEN 'Facility Integeration is NULL' END AS EXCEPTION_REASON
                            ,CURRENT_TIMESTAMP AS DW_CREATE_TS
                            FROM ` + tgt_wrk_tbl + `
                            WHERE Facility_Integration_ID IS NULL `;
    try {
    snowflake.execute (
        {sqlText: sql_exceptions  }
        );
    }

catch (err)  {
        return "Insert into tgt Exception table "+ tgt_exp_tbl +" Failed with error: " + err;   // Return a error message.
    }  

//SCD Type2 transaction begins
                var sql_begin = "BEGIN"
                var sql_updates = `// Processing Updates of Type 2 SCD
                UPDATE ` + tgt_tbl + ` as tgt
                SET  DW_Last_Effective_ts = timestampadd(millisecond, -1, current_timestamp)
               ,DW_CURRENT_VERSION_IND = FALSE
               ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
               ,DW_SOURCE_UPDATE_NM = filename
                FROM ( SELECT Store_Group_ID
			,Facility_Integration_ID
                  	,filename
                        FROM    `+ tgt_wrk_tbl +`
                        WHERE DML_Type = 'U'
                        AND Sameday_chg_ind = 0
AND FACILITY_INTEGRATION_ID is not NULL
) src
WHERE tgt.Store_Group_ID = src.Store_Group_ID AND
tgt.Facility_Integration_ID = src.Facility_Integration_ID
                AND  tgt.DW_CURRENT_VERSION_IND = TRUE`;

var sql_sameday = `// Processing Sameday updates
UPDATE ` + tgt_tbl + ` as tgt
 SET      
   
  DW_Logical_delete_ind = src.DW_Logical_delete_ind
  ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
  ,DW_SOURCE_UPDATE_NM = filename
FROM  
( SELECT Store_Group_ID
  ,Facility_Integration_ID
    ,DW_Logical_delete_ind
    ,filename
 FROM    `+ tgt_wrk_tbl +`
 WHERE DML_Type = 'U'
 AND Sameday_chg_ind = 1
 AND FACILITY_INTEGRATION_ID is not NULL
 ) src
 WHERE tgt.Store_Group_ID = src.Store_Group_ID AND
tgt.Facility_Integration_ID = src.Facility_Integration_ID
              AND  tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
 var sql_inserts = `INSERT INTO ` + tgt_tbl + `
  (    Store_Group_Id
  ,Facility_Integration_ID
                ,DW_First_Effective_TS
                ,DW_Last_Effective_TS
                ,DW_CREATE_TS          
                ,DW_LOGICAL_DELETE_IND  
                ,DW_SOURCE_CREATE_NM  
                ,DW_CURRENT_VERSION_IND  
)
SELECT Store_Group_Id
    ,Facility_Integration_ID      
  ,CURRENT_TIMESTAMP
  ,'31-DEC-9999'
  ,CURRENT_TIMESTAMP
  ,DW_Logical_delete_ind
  ,filename
  ,TRUE
  FROM   `+ tgt_wrk_tbl +`
  WHERE Sameday_chg_ind = 0
    and Facility_Integration_ID is not null
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
       return "Loading of Store_Group_Store table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }
                // **************        Load for OMS_Store_Group_Store table ENDs *****************
    
$$;
