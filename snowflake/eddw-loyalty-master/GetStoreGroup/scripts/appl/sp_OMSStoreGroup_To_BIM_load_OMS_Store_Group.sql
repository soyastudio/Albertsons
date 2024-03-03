--liquibase formatted sql
--changeset SYSTEM:sp_OMSStoreGroup_To_BIM_load_OMS_Store_Group runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.sp_OMSStoreGroup_To_BIM_load_OMS_Store_Group
(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_PROD VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


   var src_wrk_tbl = SRC_WRK_TBL;
   var cnf_schema = 'C_PROD';
   var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.OMS_Store_Group_WRK`;
   var tgt_tbl = `${CNF_DB}.${C_PROD}.OMS_STORE_GROUP`;
                       
    // **************        Load for OMS_Store_Group table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
        var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
				SELECT DISTINCT src.Store_Group_Id
				,src.Store_Group_Nm
				,src.Store_Group_Dsc
				,src.Create_Ts
				,src.Update_Ts
				,src.Create_User_Id
				,src.Create_First_Nm
				,src.Create_Last_Nm
				,src.Update_User_Id
				,src.Update_First_Nm 
				,src.filename
				,src.DW_Logical_delete_ind
                ,src.LastUpdateTs
				,src.SourceAction
				,CASE 
					WHEN 
						tgt.Store_Group_Id is NULL  
					THEN 'I' 
					ELSE 'U' 
				END as DML_Type 
				FROM (     
					SELECT 
					Store_Group_Id
				,Store_Group_Nm
				,Store_Group_Dsc
				,Create_Ts
				,Update_Ts
				,Create_User_Id
				,Create_First_Nm
				,Create_Last_Nm
				,Update_User_Id
				,Update_First_Nm 
						,filename
						,DW_Logical_delete_ind
						,LastUpdateTs
						,SourceAction
						FROM (
							SELECT 
							Store_Group_Id
				,Store_Group_Nm
				,Store_Group_Dsc
				,Create_Ts
				,Update_Ts
				,Create_User_Id
				,Create_First_Nm
				,Create_Last_Nm
				,Update_User_Id
				,Update_First_Nm 
						,filename
						,FALSE as DW_Logical_delete_ind
						,LastUpdateTs
						,SourceAction
						,row_number() over ( PARTITION BY Store_Group_Id 
						ORDER BY to_timestamp_ntz(LastUpdateTs) desc) as rn
						FROM (
							SELECT 	
							Payload_Id AS Store_Group_Id
                            ,Payload_Name AS Store_Group_Nm
                            ,Payload_Description AS Store_Group_Dsc
                            ,Payload_CreateTs AS Create_Ts
                            ,Payload_UpdateTs AS Update_Ts
                            ,Payload_CreatedUser_UserId AS Create_User_Id
                            ,Payload_CreatedUser_FirstName AS Create_First_Nm
                            ,Payload_CreatedUser_LastName AS Create_Last_Nm
                            ,Payload_UpdatedUser_UserId AS Update_user_Id
                            ,Payload_UpdatedUser_FirstName AS Update_First_Nm
							,SourceAction
							,filename 
							,LastUpdateTs
							FROM ${src_wrk_tbl}
							WHERE 
								Store_Group_Id is not NULL 
						) 
					) Where rn = 1 
				) src  
				LEFT JOIN (
					SELECT  
						Store_Group_Id
				,Store_Group_Nm
				,Store_Group_Dsc
				,Create_Ts
				,Update_Ts
				,Create_User_Id
				,Create_First_Nm
				,Create_Last_Nm
				,Update_User_Id
				,Update_First_Nm
						,tgt.DW_Logical_delete_ind
						,tgt.DW_First_Effective_Ts
						FROM ${tgt_tbl} tgt
						WHERE tgt.DW_CURRENT_VERSION_IND = TRUE 
					) as tgt on 	
					src.Store_Group_Id = tgt.Store_Group_Id
					where 
					tgt.Store_Group_Id is NULL 
					OR
					(NVL(tgt.Store_Group_Nm,'-1') <> NVL(src.Store_Group_Nm,'-1') OR
		NVL(tgt.Store_Group_Dsc,'-1') <> NVL(src.Store_Group_Dsc,'-1') OR
		NVL(tgt.Create_Ts,'9999-12-31 00:00:00.000') <> NVL(to_timestamp(src.Create_Ts),'9999-12-31 00:00:00.000') OR
		NVL(tgt.Update_Ts,'9999-12-31 00:00:00.000') <> NVL(to_timestamp(src.Update_Ts),'9999-12-31 00:00:00.000') OR
		NVL(tgt.Create_User_Id,'-1') <> NVL(src.Create_User_Id,'-1') OR
		NVL(tgt.Create_First_Nm,'-1') <> NVL(src.Create_First_Nm,'-1') OR
		NVL(tgt.Create_Last_Nm,'-1') <> NVL(src.Create_Last_Nm,'-1') OR
		NVL(tgt.Update_User_Id,'-1') <> NVL(src.Update_User_Id,'-1') OR
		NVL(tgt.Update_First_Nm,'-1') <> NVL(src.Update_First_Nm,'-1')
					OR  tgt.dw_logical_delete_ind  <>  src.dw_logical_delete_ind 
					)
				`;  	
			try 
			{
				snowflake.execute (
				{sqlText: sql_command  }
				);
			}
            catch (err)  
			{
             	return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
            }
            
               
    //SCD Type2 transaction begins
     var sql_begin = "BEGIN"
     var sql_updates = `// Processing Updates of Type 2 SCD
        UPDATE ${tgt_tbl} as tgt
	    SET  DW_Last_Effective_Ts = timestampadd(millisecond, -1, current_timestamp)
	    ,DW_CURRENT_VERSION_IND = FALSE
	    ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
	    ,DW_SOURCE_UPDATE_NM = filename
	    FROM ( 
		    SELECT 
			    Store_Group_Id
			    ,filename
		FROM ${tgt_wrk_tbl}
		WHERE DML_Type = 'U' and
			Store_Group_Id is not null
	) src
	WHERE 
	tgt.Store_Group_Id = src.Store_Group_Id
	AND  tgt.DW_CURRENT_VERSION_IND = TRUE`;
        
                            
	// Processing Inserts
		  var sql_inserts = `INSERT INTO ${tgt_tbl}
		   ( 
			Store_Group_Id
		,Store_Group_Nm
		,Store_Group_Dsc
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
			)
		SELECT 
			Store_Group_Id
		,Store_Group_Nm
		,Store_Group_Dsc
		,Create_Ts
		,Update_Ts
		,Create_User_Id
		,Create_First_Nm
		,Create_Last_Nm
		,Update_User_Id
		,Update_First_Nm
			,CURRENT_TIMESTAMP
			,'9999-12-31 00:00:00.000'
			,CURRENT_TIMESTAMP
			,DW_Logical_delete_ind
			,filename
			,TRUE
		   FROM ${tgt_wrk_tbl}
		   WHERE Store_Group_Id is not NULL
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
    
