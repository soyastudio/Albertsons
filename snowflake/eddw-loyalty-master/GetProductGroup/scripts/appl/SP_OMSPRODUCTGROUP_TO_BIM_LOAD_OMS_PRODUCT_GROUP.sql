--liquibase formatted sql
--changeset SYSTEM:SP_OMSPRODUCTGROUP_TO_BIM_LOAD_OMS_PRODUCT_GROUP runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_OMSPRODUCTGROUP_TO_BIM_LOAD_OMS_PRODUCT_GROUP(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_PROD VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_db = CNF_DB;
	var cnf_schema = C_PROD;
	var wrk_schema = C_STAGE;
	var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.OMS_Product_Group_WRK`;
	var tgt_tbl = `${cnf_db}.${cnf_schema}.OMS_Product_Group`;
	var src_wrk_tmp_tbl = `${cnf_db}.${wrk_schema}.OMS_Product_Group_SRC_WRK`;

    // **************        Load for OMS_Product_Group table BEGIN *****************

	var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE ${src_wrk_tmp_tbl} AS
	with flat_tmp as (
		SELECT 
			PAYLOAD_ID as Product_Group_Id
			,PAYLOAD_NAME as Product_Group_Nm
			,PAYLOAD_DESCRIPTION as Product_Group_Dsc
			,to_timestamp_ltz(PAYLOAD_CREATETS) as Create_Ts
			,to_timestamp_ltz(PAYLOAD_UPDATETS) as Update_Ts
			,PAYLOAD_CREATEDUSER_USERID as Create_User_Id
			,PAYLOAD_CREATEDUSER_FIRSTNAME as Create_First_Nm
			,PAYLOAD_CREATEDUSER_LASTNAME as Create_Last_Nm
			,PAYLOAD_UPDATEDUSER_USERID as Update_User_Id
			,PAYLOAD_UPDATEDUSER_FIRSTNAME as Update_First_Nm 
			,payload_version as Product_Group_Version_NBR
			,payload_productGroupType as Product_Group_Type_Dsc
			,payload_productgroupinfo_mobid	as Mob_Id
			,sourceaction
			,filename 
			,to_timestamp_ltz(lastupdatets) as lastupdatets
			,row_number() over ( 
				PARTITION BY Product_Group_Id
				ORDER BY to_timestamp_ntz(LASTUPDATETS) desc
			) as rn
		FROM  ${src_wrk_tbl}
		WHERE
			Product_Group_Id is not NULL
	)
	SELECT 	
		Product_Group_Id
		,Product_Group_Nm
		,Product_Group_Dsc
		,Create_Ts
		,Update_Ts
		,Create_User_Id
		,Create_First_Nm
		,Create_Last_Nm
		,Update_User_Id
		,Update_First_Nm
		,Product_Group_Version_NBR
		,Product_Group_Type_Dsc
		,Mob_Id
		,sourceaction
		,filename 
		,lastupdatets
	FROM (
		SELECT 	distinct
			Product_Group_Id
			,Product_Group_Nm
			,Product_Group_Dsc
			,Create_Ts
			,Update_Ts
			,Create_User_Id
			,Create_First_Nm
			,Create_Last_Nm
			,Update_User_Id
			,Update_First_Nm
			,Product_Group_Version_NBR
			,Product_Group_Type_Dsc
			,Mob_Id
			,sourceaction
			,filename 
			,lastupdatets
		FROM flat_tmp 
		where rn = 1
	) 
	`;
                              
    try {
        snowflake.execute ({ sqlText: cr_src_wrk_tbl });
    } catch (err) {
        return "Creation of OMS_Product_Group_Department_Section src_wrk_tmp_tbl table  Failed with error: " + err;   // Return a error message.
    }


    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_tbl = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
	SELECT distinct src.Product_Group_Id
		,src.Product_Group_Nm
		,src.Product_Group_Dsc
		,src.Create_Ts
		,src.Update_Ts
		,src.Create_User_Id
		,src.Create_First_Nm
		,src.Create_Last_Nm
		,src.Update_User_Id
		,src.Update_First_Nm
		,src.Product_Group_Version_NBR
		,src.Product_Group_Type_Dsc
		,src.Mob_Id
		,src.filename
		,src.DW_Logical_delete_ind
		,src.lastupdatets
		,src.sourceaction
		,CASE 
			WHEN 
				tgt.Product_Group_Id is NULL  
			THEN 'I' 
			ELSE 'U' 
		END as DML_Type
	FROM (     
		SELECT 
			Product_Group_Id
			,Product_Group_Nm
			,Product_Group_Dsc
			,Create_Ts
			,Update_Ts
			,Create_User_Id
			,Create_First_Nm
			,Create_Last_Nm
			,Update_User_Id
			,Update_First_Nm 
			,Product_Group_Version_NBR
			,Product_Group_Type_Dsc
			,Mob_Id
			,filename
			,FALSE as DW_Logical_delete_ind
			,lastupdatets
			,sourceaction
		FROM ${src_wrk_tmp_tbl}
	) src  
	LEFT JOIN (
		SELECT  
			Product_Group_Id
			,Product_Group_Nm
			,Product_Group_Dsc
			,Create_Ts
			,Update_Ts
			,Create_User_Id
			,Create_First_Nm
			,Create_Last_Nm
			,Update_User_Id
			,Update_First_Nm
			,Product_Group_Version_NBR
			,Product_Group_Type_Dsc
			,Mob_Id
			,DW_Logical_delete_ind
			,DW_First_Effective_ts
		FROM ${tgt_tbl} tgt
		WHERE tgt.DW_CURRENT_VERSION_IND = TRUE 
	) as tgt on 	
	src.Product_Group_Id = tgt.Product_Group_Id
	WHERE 
		tgt.Product_Group_Id is NULL 
		OR (
			NVL(tgt.Product_Group_Nm,'-1') <> NVL(src.Product_Group_Nm,'-1') OR
			NVL(tgt.Product_Group_Dsc,'-1') <> NVL(src.Product_Group_Dsc,'-1') OR
			NVL(tgt.Create_Ts,'9999-12-31 00:00:00.000') <> NVL(to_timestamp(src.Create_Ts),'9999-12-31 00:00:00.000') OR
			NVL(tgt.Update_Ts,'9999-12-31 00:00:00.000') <> NVL(to_timestamp(src.Update_Ts),'9999-12-31 00:00:00.000') OR
			NVL(tgt.Create_User_Id,'-1') <> NVL(src.Create_User_Id,'-1') OR
			NVL(tgt.Create_First_Nm,'-1') <> NVL(src.Create_First_Nm,'-1') OR
			NVL(tgt.Create_Last_Nm,'-1') <> NVL(src.Create_Last_Nm,'-1') OR
			NVL(tgt.Update_User_Id,'-1') <> NVL(src.Update_User_Id,'-1') OR
			NVL(tgt.Update_First_Nm,'-1') <> NVL(src.Update_First_Nm,'-1') OR
			NVL(tgt.Product_Group_Version_NBR,'-1') <> NVL(src.Product_Group_Version_NBR,'-1') OR
			NVL(tgt.Product_Group_Type_Dsc,'-1') <> NVL(src.Product_Group_Type_Dsc,'-1') OR
			NVL(tgt.Mob_Id,'-1') <> NVL(src.Mob_Id,'-1')
			
		)
	`;  	
	try {
		snowflake.execute ({ sqlText: create_tgt_wrk_tbl });
	} catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
            
               
    // SCD Type2 transaction begins
	
	var sql_begin = "BEGIN"
	
	var sql_updates = `// Processing Updates of Type 2 SCD
	UPDATE ${tgt_tbl} as tgt
	SET  DW_Last_Effective_Ts = timestampadd(minute, -1, current_timestamp)
	,DW_CURRENT_VERSION_IND = FALSE
	,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
	,DW_SOURCE_UPDATE_NM = filename
	FROM ( 
		SELECT 
			Product_Group_Id
			,filename
		FROM ${tgt_wrk_tbl}
		WHERE DML_Type = 'U' and
			Product_Group_Id is not null
	) src
	WHERE 
	tgt.Product_Group_Id = src.Product_Group_Id
	AND  tgt.DW_CURRENT_VERSION_IND = TRUE`;
                            
	// Processing Inserts
	var sql_inserts = `INSERT INTO ${tgt_tbl}( 
		Product_Group_Id
		,Product_Group_Nm
		,Product_Group_Dsc
		,Create_Ts
		,Update_Ts
		,Create_User_Id
		,Create_First_Nm
		,Create_Last_Nm
		,Update_User_Id
		,Update_First_Nm
		,DW_First_Effective_Ts 
		,DW_Last_Effective_Ts
		,DW_CREATE_TS          
		,DW_LOGICAL_DELETE_IND  
		,DW_SOURCE_CREATE_NM   
		,DW_CURRENT_VERSION_IND
		,Product_Group_Version_NBR
		,Product_Group_Type_Dsc
		,Mob_Id
	)
	SELECT 
		Product_Group_Id
		,Product_Group_Nm
		,Product_Group_Dsc
		,Create_Ts
		,Update_Ts
		,Create_User_Id
		,Create_First_Nm
		,Create_Last_Nm
		,Update_User_Id
		,Update_First_Nm
		,CURRENT_TIMESTAMP
		,'31-DEC-9999'
		,CURRENT_TIMESTAMP
		,DW_Logical_delete_ind
		,filename
		,TRUE
		,Product_Group_Version_NBR
		,Product_Group_Type_Dsc
		,Mob_Id
	FROM ${tgt_wrk_tbl}
	WHERE Product_Group_Id is not null
	`;

    var sql_commit = "COMMIT";
    var sql_rollback = "ROLLBACK";

    try {
        snowflake.execute ({ sqlText: sql_begin });
        snowflake.execute ({ sqlText: sql_updates });
        snowflake.execute ({ sqlText: sql_inserts });
        snowflake.execute ({ sqlText: sql_commit });
    }
    catch (err)  {
        snowflake.execute ({ sqlText: sql_rollback });
		return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
	}
	
                // **************        Load for OMS_Product_Group table ENDs *****************
    
$$;
