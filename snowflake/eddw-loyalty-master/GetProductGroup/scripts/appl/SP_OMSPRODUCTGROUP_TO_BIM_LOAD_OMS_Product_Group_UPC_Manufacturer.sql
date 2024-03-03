--liquibase formatted sql
--changeset SYSTEM:SP_OMSPRODUCTGROUP_TO_BIM_LOAD_OMS_Product_Group_UPC_Manufacturer runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_OMSPRODUCTGROUP_TO_BIM_LOAD_OMS_PRODUCT_GROUP_UPC_MANUFACTURER(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_PROD VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

   
   var src_wrk_tbl = SRC_WRK_TBL;
   var cnf_db = CNF_DB;
   var cnf_schema = C_PROD;
   var wrk_schema = C_STAGE;
   var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.OMS_Product_Group_UPC_Manufacturer_WRK`;
   var tgt_tbl = `${cnf_db}.${cnf_schema}.OMS_Product_Group_UPC_Manufacturer`;
   var src_wrk_tmp_tbl = `${cnf_db}.${wrk_schema}.OMS_Product_Group_UPC_Manufacturer_SRC_WRK`;

                       
    // **************        Load for OMS_Product_Group_UPC_Manufacturer table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	
	    var cr_src_wrk_tbl = `	CREATE OR REPLACE TABLE `+ src_wrk_tmp_tbl +` AS
                          with flat_tmp as
                              (		
								
							SELECT
                            PAYLOAD_ID as Product_Group_Id
                            ,F.value::string  as UPC_Manufacturer_Id
                            ,sourceaction
                            ,filename
                            ,lastupdatets
                            ,row_number() over ( PARTITION BY Product_Group_Id, UPC_Manufacturer_Id
							ORDER BY to_timestamp_ntz(LASTUPDATETS) desc) as rn
                            FROM  ${src_wrk_tbl}
                            ,LATERAL FLATTEN(input => PAYLOAD_PRODUCTGROUPIDS_MANUFACTUREIDS, outer => TRUE) F                           
									
                                  )
                            
                            SELECT distinct
                                  Product_Group_Id
                                  ,UPC_Manufacturer_Id
                                  ,sourceaction
                                  ,filename
                                  ,lastupdatets
                            FROM flat_tmp
							where rn = 1
							AND Product_Group_Id is not NULL  AND
                                UPC_Manufacturer_Id is not NULL
                            `;
    try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
        );
    }
    catch (err)  { 
        return "Creation of OMS_Product_Group_UPC_Manufacturer "+ src_wrk_tmp_tbl +" Failed with error: " + err;   // Return a error message.
        }

	var create_tgt_wrk_table = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
	SELECT DISTINCT
		src.Product_Group_Id,
		src.UPC_Manufacturer_Id,
		src.filename,
		src.DW_Logical_delete_ind,
		CASE 
			WHEN 
				tgt.Product_Group_Id is NULL AND
			tgt.UPC_Manufacturer_Id is NULL
			THEN 'I' 
			ELSE 'U' 
		END as DML_Type,
		CASE 
			WHEN tgt.DW_First_Effective_TS = CURRENT_DATE 
			THEN 1 
			Else 0 
		END as Sameday_chg_ind
	FROM (   
	
			SELECT
				Product_Group_Id,
				UPC_Manufacturer_Id,			 
				filename,
				FALSE AS DW_Logical_delete_ind
				FROM ${src_wrk_tmp_tbl}
	) src  
	LEFT JOIN (
		SELECT
			Product_Group_Id,
			UPC_Manufacturer_Id,
			DW_Logical_delete_ind,
			DW_First_Effective_TS
        FROM ${tgt_tbl}
		WHERE DW_CURRENT_VERSION_IND = TRUE
	) as tgt on
	src.Product_Group_Id = tgt.Product_Group_Id AND
	src.UPC_Manufacturer_Id = tgt.UPC_Manufacturer_Id
	WHERE
		tgt.Product_Group_Id is NULL AND
				tgt.UPC_Manufacturer_Id is NULL 
		 //OR (tgt.dw_logical_delete_ind  <>  src.dw_logical_delete_ind )
		 `;  	
	
	try {
		snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
		var sql_deletes = `INSERT INTO ${tgt_wrk_tbl}
         select  tgt.Product_Group_Id
		,tgt.UPC_Manufacturer_Id
        ,tgt.DW_SOURCE_CREATE_NM
        ,TRUE AS DW_Logical_delete_ind  
        ,'U' AS DML_Type  
        ,CASE WHEN DW_First_Effective_TS = CURRENT_DATE  THEN 1 Else 0 END as Sameday_chg_ind
             FROM ${tgt_tbl} tgt
            LEFT JOIN
            (
            SELECT distinct Product_Group_Id
                           ,UPC_Manufacturer_Id
                          ,FileName
            FROM ${src_wrk_tmp_tbl}
            ) src 
              ON src.UPC_Manufacturer_Id = tgt.UPC_Manufacturer_Id
             AND src.Product_Group_Id = tgt.Product_Group_Id
            WHERE    
             (tgt.Product_Group_Id ) in (select distinct Product_Group_Id
           FROM ${src_wrk_tmp_tbl}
          )
             AND 
              dw_current_version_ind = TRUE
            AND dw_logical_delete_ind = FALSE
              and src.Product_Group_Id is NULL
              and src.UPC_Manufacturer_Id is NULL
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
		DW_Last_Effective_TS = timestampadd(minute, -1, current_timestamp),
		DW_CURRENT_VERSION_IND = FALSE,
		DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
		DW_SOURCE_UPDATE_NM = filename
	FROM ( 
		SELECT 
			Product_Group_Id,
			UPC_Manufacturer_Id,
			filename
		FROM ${tgt_wrk_tbl}
		WHERE 
			DML_Type = 'U' AND 
			Sameday_chg_ind = 0 AND
			Product_Group_Id is not NULL AND
			UPC_Manufacturer_Id is not NULL             
		) src
		WHERE 
			tgt.Product_Group_Id = src.Product_Group_Id AND
			tgt.UPC_Manufacturer_Id = src.UPC_Manufacturer_Id AND
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
		
     	// SCD Type1 - Processing Sameday updates
	var sql_sameday = `
	UPDATE ${tgt_tbl} as tgt
	SET       
		DW_Logical_delete_ind = src.DW_Logical_delete_ind,
		DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
		DW_SOURCE_UPDATE_NM = filename
		FROM ( 
			SELECT
				Product_Group_Id,
				UPC_Manufacturer_Id,
				DW_Logical_delete_ind,
				filename
			FROM ${tgt_wrk_tbl}
			WHERE 
				DML_Type = 'U' AND 
				Sameday_chg_ind = 1 AND
				Product_Group_Id is not NULL AND
				UPC_Manufacturer_Id is not NULL
		) src
		WHERE
			tgt.Product_Group_Id = src.Product_Group_Id AND
			tgt.UPC_Manufacturer_Id = src.UPC_Manufacturer_Id AND  
			tgt.DW_CURRENT_VERSION_IND = TRUE AND
			tgt.DW_LOGICAL_DELETE_IND = FALSE`;
                                
                            
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl} (
	Product_Group_Id,
	UPC_Manufacturer_Id,
	DW_First_Effective_TS, 
	DW_Last_Effective_TS, 
	DW_CREATE_TS,          
	DW_LOGICAL_DELETE_IND,  
	DW_SOURCE_CREATE_NM,   
	DW_CURRENT_VERSION_IND  
	)
	SELECT
		Product_Group_Id,
		UPC_Manufacturer_Id,
		CURRENT_DATE as DW_First_Effective_TS,
		'31-DEC-9999',
		CURRENT_TIMESTAMP,
		DW_Logical_delete_ind,
		filename,
		TRUE as DW_CURRENT_VERSION_IND
	FROM ${tgt_wrk_tbl}
	WHERE 
		Sameday_chg_ind = 0 AND
		Product_Group_Id is not NULL AND
		UPC_Manufacturer_Id is not NULL
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
	
	var sql_parent_deletes = `UPDATE ${cnf_db}.${cnf_schema}.OMS_Product_Group as tgt
	SET DW_LAST_EFFECTIVE_TS = timestampadd(minute, -1, current_timestamp)
		,DW_CURRENT_VERSION_IND = TRUE
		,DW_LOGICAL_DELETE_IND = TRUE
		//,DW_SOURCE_UPDATE_NM = 'Parent Delete from Child Tables'
	FROM (
		SELECT tgt0.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group tgt0
		WHERE DW_LOGICAL_DELETE_IND = FALSE AND 
			DW_CURRENT_VERSION_IND = TRUE
	) Master
	INNER JOIN (
		SELECT tgt1.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_Department_Section tgt1
		WHERE tgt1.DW_LOGICAL_DELETE_IND = TRUE AND 
			tgt1.DW_CURRENT_VERSION_IND = TRUE
		MINUS
		SELECT tgt2.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_Department_Section tgt2
		WHERE tgt2.DW_LOGICAL_DELETE_IND = FALSE AND 
			tgt2.DW_CURRENT_VERSION_IND = TRUE
	) DS on DS.Product_Group_Id = Master.Product_Group_Id
	INNER JOIN (
		SELECT tgt1.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_UPC tgt1
		WHERE tgt1.DW_LOGICAL_DELETE_IND = TRUE AND 
			tgt1.DW_CURRENT_VERSION_IND = TRUE
		MINUS
		SELECT tgt2.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_UPC tgt2
		WHERE tgt2.DW_LOGICAL_DELETE_IND = FALSE AND 
			tgt2.DW_CURRENT_VERSION_IND = TRUE
	) UPC on UPC.Product_Group_Id = DS.Product_Group_Id
	INNER JOIN (
		SELECT tgt1.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_UPC_Manufacturer tgt1
		WHERE tgt1.DW_LOGICAL_DELETE_IND = TRUE AND 
			tgt1.DW_CURRENT_VERSION_IND = TRUE
		MINUS
		SELECT tgt2.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_UPC_Manufacturer tgt2
		WHERE tgt2.DW_LOGICAL_DELETE_IND = FALSE AND 
			tgt2.DW_CURRENT_VERSION_IND = TRUE
	) UPC_Manu on UPC_Manu.Product_Group_Id = UPC.Product_Group_Id
	Where Master.Product_Group_Id = tgt.Product_Group_Id;
	`;
	
	var sql_parent_undeletes = `UPDATE ${cnf_db}.${cnf_schema}.OMS_Product_Group as tgt
	SET DW_LAST_EFFECTIVE_TS = timestampadd(minute, -1, current_timestamp)
		,DW_CURRENT_VERSION_IND = TRUE
		,DW_LOGICAL_DELETE_IND = FALSE
		//,DW_SOURCE_UPDATE_NM = 'Parent Delete from Child Tables'
	FROM (
		SELECT tgt0.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group tgt0
		WHERE DW_LOGICAL_DELETE_IND = TRUE AND 
			DW_CURRENT_VERSION_IND = TRUE
	) Master
	INNER JOIN (
		SELECT tgt1.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_Department_Section tgt1
		WHERE tgt1.DW_LOGICAL_DELETE_IND = FALSE AND 
			tgt1.DW_CURRENT_VERSION_IND = TRUE
		MINUS
		SELECT tgt2.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_Department_Section tgt2
		WHERE tgt2.DW_LOGICAL_DELETE_IND = TRUE AND 
			tgt2.DW_CURRENT_VERSION_IND = TRUE
	) DS on DS.Product_Group_Id = Master.Product_Group_Id
	INNER JOIN (
		SELECT tgt1.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_UPC tgt1
		WHERE tgt1.DW_LOGICAL_DELETE_IND = FALSE AND 
			tgt1.DW_CURRENT_VERSION_IND = TRUE
		MINUS
		SELECT tgt2.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_UPC tgt2
		WHERE tgt2.DW_LOGICAL_DELETE_IND = TRUE AND 
			tgt2.DW_CURRENT_VERSION_IND = TRUE
	) UPC on UPC.Product_Group_Id = DS.Product_Group_Id
	INNER JOIN (
		SELECT tgt1.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_UPC_Manufacturer tgt1
		WHERE tgt1.DW_LOGICAL_DELETE_IND = FALSE AND 
			tgt1.DW_CURRENT_VERSION_IND = TRUE
		MINUS
		SELECT tgt2.Product_Group_Id
		FROM ${cnf_db}.${cnf_schema}.OMS_Product_Group_UPC_Manufacturer tgt2
		WHERE tgt2.DW_LOGICAL_DELETE_IND = TRUE AND 
			tgt2.DW_CURRENT_VERSION_IND = TRUE
	) UPC_Manu on UPC_Manu.Product_Group_Id = UPC.Product_Group_Id
	Where Master.Product_Group_Id = tgt.Product_Group_Id;
	`;

	try {
		snowflake.execute ({ sqlText: sql_parent_deletes });
		snowflake.execute ({ sqlText: sql_parent_undeletes });
	} catch (err) {
        return `Parent Delete in ${cnf_db}.${cnf_schema}.OMS_Product_Group Failed with error: ${err}`;   // Return a error message.
    }
                // **************        Load for OMS_Product_Group_UPC_Manufacturer table ENDs *****************
                // **************        Load for OMS_Product_Group_UPC_Manufacturer table ENDs *****************

$$;
