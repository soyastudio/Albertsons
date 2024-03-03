--liquibase formatted sql
--changeset SYSTEM:SP_ONETAG_OTHER_TO_BIM_LOAD_PAGE runOnChange:true splitStatements:false OBJECT_TYPE:SP

Use database EDM_CONFIRMED_<<ENV>>;
Use schema DW_APPL;

CREATE OR REPLACE PROCEDURE "SP_ONETAG_OTHER_TO_BIM_LOAD_PAGE"("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_USER_ACT" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS 
$$


    var src_wrk_tbl = SRC_WRK_TBL;
    var cnf_db = CNF_DB; //change this to CNF_DB while deploying, changed it to CORE_TECH for testing purpose  
    var cnf_schema = C_USER_ACT; //change this to C_CUST while deploying, changed it to CORE_TECH for testing purpose  
    var wrk_schema = C_STAGE; //change this to C_STAGE while deploying, changed it to CORE_TECH for testing purpose  
   
               
// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

    var tgt_wrk_tbl = cnf_db + "." + wrk_schema + ".CUSTOMER_SESSION_PAGE_WRK";
    var tgt_tbl 	= cnf_db + "." + cnf_schema + ".CUSTOMER_SESSION_PAGE";
               
// ** Load for ONE TAG Operating_System table BEGIN ***

// identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

	var sql_truncate_tgt_wrk_tbl = `TRUNCATE TABLE `+ tgt_wrk_tbl ;

	try {
		snowflake.execute (
		{sqlText: sql_truncate_tgt_wrk_tbl  }
		);
	}
	catch (err)  {
		return "TRUNCATE of work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
	}

	var sql_crt_tgt_wrk_tbl = `INSERT INTO `+ tgt_wrk_tbl +` (PAGE_NM
			,DW_CREATE_TS
			,DW_LAST_UPDATE_TS
			,DW_LOGICAL_DELETE_IND
			,DW_CURRENT_VERSION_IND
			,DW_SOURCE_CREATE_NM
			,DW_SOURCE_UPDATE_NM ) 
	(
		SELECT distinct
			PAGE_NM
			,CURRENT_TIMESTAMP() as DW_CREATE_TS
			,CURRENT_TIMESTAMP() as DW_LAST_UPDATE_TS
			,FALSE as DW_LOGICAL_DELETE_IND
			,TRUE as DW_CURRENT_VERSION_IND
			,'OneTag' as DW_SOURCE_CREATE_NM
			,'OneTag' as DW_SOURCE_UPDATE_NM
		FROM 
		( 
			SELECT distinct
				tgt.PAGE_INTEGRATION_ID as PAGE_INTEGRATION_ID
				,src.PAGE_PGNAME as PAGE_NM
			FROM
				(
					SELECT distinct
						PAGE_PGNAME as PAGE_PGNAME
					FROM ` + src_wrk_tbl  + `  WHERE PAGE_PGNAME IS NOT NULL
				)src  
			LEFT JOIN 
				(
					SELECT 
						PAGE_INTEGRATION_ID
						,PAGE_NM
						,PAGE_TYPE_CD
						,PAGE_SUBSECTION1_DSC
						,PAGE_SUBSECTION2_DSC
						,PAGE_SUBSECTION3_DSC
						,PAGE_SUBSECTION4_DSC
					FROM ${tgt_tbl} 
					WHERE DW_CURRENT_VERSION_IND = TRUE
				) tgt on
				src.PAGE_PGNAME = tgt.PAGE_NM
		) WHERE PAGE_INTEGRATION_ID IS NULL
	)`;
              
   try {
			snowflake.execute (
						  {sqlText: sql_crt_tgt_wrk_tbl  }
						  );
			}
  catch (err)  {
			return "Insert of work table "+ sql_crt_tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
			}
                                                          


// Processing Inserts
	  var sql_inserts = `INSERT INTO ${tgt_tbl} (
			PAGE_INTEGRATION_ID
			,PAGE_NM
			,DW_CREATE_TS
			,DW_LAST_UPDATE_TS
			,DW_LOGICAL_DELETE_IND
			,DW_CURRENT_VERSION_IND
			,DW_SOURCE_CREATE_NM
			,DW_SOURCE_UPDATE_NM 
			)
			SELECT
				ONETAG_OTHER_SEQ.NEXTVAL as PAGE_INTEGRATION_ID
				,PAGE_NM
				,DW_CREATE_TS
				,DW_LAST_UPDATE_TS
				,DW_LOGICAL_DELETE_IND
				,DW_CURRENT_VERSION_IND
				,DW_SOURCE_CREATE_NM
				,DW_SOURCE_UPDATE_NM 
			FROM ${tgt_wrk_tbl}
			`;

    var sql_commit = "COMMIT"
    var sql_rollback = "ROLLBACK"
    var sql_begin = "BEGIN"	

try {
        snowflake.execute (
            {sqlText: sql_begin  }
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
        return "Loading of ONE TAG BIM table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }


// *** Loading of ONE TAG Operating_System table ENDs ****

$$;
