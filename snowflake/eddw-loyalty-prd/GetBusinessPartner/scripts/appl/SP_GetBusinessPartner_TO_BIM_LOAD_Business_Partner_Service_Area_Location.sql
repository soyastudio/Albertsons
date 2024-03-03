		     
 
var cnf_db = CNF_DB;
var wrk_schema = C_STAGE;
var cnf_schema = C_LOYAL;
var lkp_schema = C_LOYAL;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Service_Area_Location_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Business_Partner_Service_Area_Location`;
var lkp_tbl =`${cnf_db}.${lkp_schema}.Business_Partner_Profile`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Service_Area_Location_Exceptions`;

// ************** Load for Business_Partner_Service_Area_Location table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
				WITH src_wrk_tbl_recs as
						(SELECT DISTINCT
							Partner_Nm,
							Service_Area_Location_Type_Cd,
							Service_Area_Location_Value_Txt,
							FileName,
							creationdt,
							row_number() over (PARTITION BY Partner_Nm , Service_Area_Location_Type_Cd order by To_timestamp_ntz(CREATIONDT) DESC)as rn
						FROM
						(
							SELECT DISTINCT
							PartnerProfile_PartnerNm as Partner_Nm,
							AreaTypeCd as Service_Area_Location_Type_Cd,
							AreaTypeValueTxt as Service_Area_Location_Value_Txt,
							FileName,
							creationdt
							FROM  ${src_wrk_tbl}	
														
						)
						
						),
							src_wrk_tbl_recs_TEMP as
							(
							select 
							LKP.Business_Partner_Integration_Id as Business_Partner_Integration_Id, 
							src.Partner_Nm,
							src.Service_Area_Location_Type_Cd,
							src.Service_Area_Location_Value_Txt,
							src.DW_Logical_delete_ind,
							src.FileName,
							src.creationdt
							from
							(
							select
							Partner_Nm,
							Service_Area_Location_Type_Cd,
							Service_Area_Location_Value_Txt,
							false AS DW_Logical_delete_ind,
							FileName,
							creationdt
							FROM src_wrk_tbl_recs
							where 
							rn=1
							) src
							
							LEFT JOIN
							(SELECT DISTINCT Business_Partner_Integration_Id, Partner_Nm
							FROM ${lkp_tbl}
							WHERE DW_CURRENT_VERSION_IND = TRUE
							AND DW_LOGICAL_DELETE_IND = FALSE
							)LKP
							ON src.Partner_Nm = LKP.Partner_Nm
							
							)
							select
							src.Business_Partner_Integration_Id,
							src.Partner_Nm,
							src.Service_Area_Location_Type_Cd,
							src.Service_Area_Location_Value_Txt,
							src.FileName,
							src.DW_Logical_delete_ind,
							src.creationdt,
							CASE WHEN (tgt.Business_Partner_Integration_Id IS NULL AND tgt.Partner_Nm IS NULL AND tgt.Service_Area_Location_Type_Cd IS NULL) THEN 'I' ELSE 'U' END AS DML_Type,
							CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
							from src_wrk_tbl_recs_TEMP src
							
							LEFT JOIN (SELECT
							tgt.Business_Partner_Integration_Id,
							tgt.Partner_Nm,
							tgt.Service_Area_Location_Type_Cd,
							tgt.Service_Area_Location_Value_Txt,
							tgt.dw_logical_delete_ind,
							tgt.dw_first_effective_dt
							FROM ${tgt_tbl} tgt
							WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
							) as tgt
							ON tgt.Partner_Nm = src.Partner_Nm
							AND tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
							AND tgt.Service_Area_Location_Type_Cd = src.Service_Area_Location_Type_Cd
							where (tgt.Business_Partner_Integration_Id IS NULL AND tgt.Partner_Nm IS NULL AND tgt.Service_Area_Location_Type_Cd IS NULL)
							OR (NVL(src.Service_Area_Location_Value_Txt,'-1') <> NVL(tgt.Service_Area_Location_Value_Txt,'-1')
							OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
							)`;
try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        throw "Creation of Business_Partner_Service_Area_Location work table Failed with error: "+ err;   // Return a error message.
        }
		
//SCD Type2 transaction begins 
// Processing Updates of Type 2 SCD
var sql_begin = `BEGIN`
                    var sql_updates =`UPDATE ${tgt_tbl} as tgt
					SET DW_Last_Effective_dt = CURRENT_DATE-1,
						DW_CURRENT_VERSION_IND = FALSE,
						DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
						DW_SOURCE_UPDATE_NM = FileName
					FROM ( SELECT
						Business_Partner_Integration_Id,
						Partner_Nm,
						Service_Area_Location_Type_Cd,
						FileName
				FROM ${tgt_wrk_tbl}
					WHERE DML_Type = 'U'
						AND Sameday_chg_ind = 0
						AND Business_Partner_Integration_Id IS NOT NULL 
						AND Partner_Nm IS NOT NULL 
						AND Service_Area_Location_Type_Cd IS NOT NULL
						) src
					WHERE tgt.Business_Partner_Integration_Id  = src.Business_Partner_Integration_Id 
					AND tgt.Partner_Nm = src.Partner_Nm
					AND tgt.Service_Area_Location_Type_Cd = src.Service_Area_Location_Type_Cd
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                              
 //Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
						SET Service_Area_Location_Value_Txt = src.Service_Area_Location_Value_Txt
						FROM ( SELECT 
							Business_Partner_Integration_Id,
							Partner_Nm,
                             Service_Area_Location_Type_Cd,
							Service_Area_Location_Value_Txt,
							FileName,
							DW_Logical_delete_ind
							FROM ${tgt_wrk_tbl}
						WHERE DML_Type = 'U'
							AND Sameday_chg_ind = 1
							AND Business_Partner_Integration_Id IS NOT NULL 
							AND Partner_Nm IS NOT NULL 
							AND Service_Area_Location_Type_Cd IS NOT NULL
							) src
						WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id 
						AND    tgt.Partner_Nm = src.Partner_Nm
						AND    tgt.Service_Area_Location_Type_Cd = src.Service_Area_Location_Type_Cd 
						AND    tgt.DW_CURRENT_VERSION_IND = TRUE`;
						
						
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                        (Business_Partner_Integration_Id,
						Partner_Nm,            
 						DW_First_Effective_Dt,  
 						DW_Last_Effective_Dt,  
 						Service_Area_Location_Type_Cd,
                        Service_Area_Location_Value_Txt,
 						DW_CREATE_TS,        
 						DW_LOGICAL_DELETE_IND, 
 						DW_SOURCE_CREATE_NM,   
 						DW_CURRENT_VERSION_IND  
						)
						SELECT DISTINCT
						Business_Partner_Integration_Id,
                        Partner_Nm,
						CURRENT_DATE,
						'31-DEC-9999',
						Service_Area_Location_Type_Cd,
                        Service_Area_Location_Value_Txt,
                        CURRENT_TIMESTAMP,
                        DW_Logical_delete_ind,
						FileName,
						TRUE
						FROM ${tgt_wrk_tbl}
						WHERE Sameday_chg_ind = 0
						AND Business_Partner_Integration_Id IS NOT NULL 
						AND Partner_Nm IS NOT NULL 
						AND Service_Area_Location_Type_Cd IS NOT NULL`;
                          
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
    catch (err){
        snowflake.execute (
            {sqlText: sql_rollback  }
            );
        return "Loading of Business_Partner_Service_Area_Location table Failed with error: "+ err;   // Return a error message.
        
       }
	   
	   
var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl}`;
 
var sql_exceptions =`INSERT INTO  ${tgt_exp_tbl} 
						SELECT DISTINCT 
						Business_Partner_Integration_Id,
						Partner_Nm,
						Service_Area_Location_Type_Cd,
						Service_Area_Location_Value_Txt,
						FileName,
						DW_Logical_delete_ind,
						CREATIONDT,
						DML_Type,
						Sameday_chg_ind,
						CASE WHEN Business_Partner_Integration_Id is NULL THEN 'Business_Partner_Integration_Id is NULL'
						     WHEN Partner_Nm is NULL THEN 'Partner_Nm is NULL'	
						     WHEN Service_Area_Location_Type_Cd is NULL THEN 'Service_Area_Location_Type_Cd is NULL'
						     ELSE NULL END AS Exception_Reason,
						CURRENT_TIMESTAMP AS DW_CREATE_TS								 
							FROM  ${tgt_wrk_tbl}
						    WHERE Business_Partner_Integration_Id is NULL
							or Partner_Nm is NULL 
							or Service_Area_Location_Type_Cd is NULL 
							`;
		
try 
{
	 snowflake.execute (
{sqlText: sql_begin }
);
	 snowflake.execute(
	 {sqlText: truncate_exceptions}
	 );  
	 snowflake.execute (
	 {sqlText: sql_exceptions  }
	 );
	 snowflake.execute (
{sqlText: sql_commit  }
);
}
catch (err)  {
	 snowflake.execute (
{sqlText: sql_rollback  }
);
return `Insert into tgt Exception table  ${tgt_exp_tbl} Failed with error:  ${err}`;   // Return a error message.
}
	   
// ************** Load for Business_Partner_Service_Area_Location table ENDs *****************