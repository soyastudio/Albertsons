--liquibase formatted sql
--changeset SYSTEM:SP_GetBusinessPartner_To_BIM_Business_Partner_Service_Override runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETBUSINESSPARTNER_TO_BIM_BUSINESS_PARTNER_SERVICE_OVERRIDE(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

 
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_LOYAL;
var lkp_schema = C_LOYAL;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Service_Override_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Business_Partner_Service_Override`;
var lkp_tb1 =`${cnf_db}.${lkp_schema}.Business_Partner_Profile`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Service_Override_Exceptions`;


// ************** Load for Business_Partner_Service_Override table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
				WITH src_wrk_tbl_recs as
						(SELECT DISTINCT
							    Partner_Nm,
							    Override_Partner_Id,
								Override_Partner_Nm,
								Override_Type_Cd,
								Override_Type_Dsc,
								Override_Type_Short_Dsc,
								Override_Ind,
								Override_Reason_Type_Cd,
								Override_Reason_Type_Dsc,
								Override_Reason_Type_Short_Dsc,
								CreationDt,
								FileName,
							    Row_number() OVER ( partition BY Partner_Nm, Override_Type_Cd  ORDER BY To_timestamp_ntz(CreationDt) DESC) AS rn
							
						FROM
						(
						SELECT DISTINCT
						         PartnerProfile_PartnerNm AS Partner_Nm,
							     Overrides_PartnerId AS Override_Partner_Id,
								 Overrides_PartnerNm AS Override_Partner_Nm,
								 OverrideType_Code AS Override_Type_Cd,
								 OverrideType_Description AS Override_Type_Dsc,
								 OverrideType_ShortDescription AS Override_Type_Short_Dsc,
								 OverrideInd AS Override_Ind,
								 OverrideReasonType_Code AS Override_Reason_Type_Cd,
								 OverrideReasonType_Description AS Override_Reason_Type_Dsc,
								 OverrideReasonType_ShortDescription AS Override_Reason_Type_Short_Dsc,
								 CreationDt,
								 FileName  	
                            	 FROM  ${src_wrk_tbl}

						)
			            )
						
						SELECT
						src.Business_Partner_Integration_Id
						,src.Partner_Nm
						,src.Override_Partner_Id
						,src.Override_Partner_Nm
						,src.Override_Type_Cd
						,src.Override_Type_Dsc
						,src.Override_Type_Short_Dsc
						,src.Override_Ind
						,src.Override_Reason_Type_Cd
						,src.Override_Reason_Type_Dsc
						,src.Override_Reason_Type_Short_Dsc
						,src.CreationDt
						,src.DW_Logical_delete_ind
						,src.filename
						,CASE WHEN(tgt.Business_Partner_Integration_Id is NULL AND tgt.Partner_Nm is NULL AND tgt.Override_Type_Cd is NULL) THEN 'I' ELSE 'U' END as DML_Type
						,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind 
						FROM
					    (
						select
						 LKP_Business_Partner_Profile.Business_Partner_Integration_Id AS Business_Partner_Integration_Id
			            ,src1.Partner_Nm
						,src1.Override_Partner_Id
						,src1.Override_Partner_Nm
						,src1.Override_Type_Cd
						,src1.Override_Type_Dsc
						,src1.Override_Type_Short_Dsc
						,src1.Override_Ind
						,src1.Override_Reason_Type_Cd
						,src1.Override_Reason_Type_Dsc
						,src1.Override_Reason_Type_Short_Dsc
						,src1.CreationDt
						,src1.DW_Logical_delete_ind
						,src1.FileName												
						from
						(
						SELECT
						 Partner_Nm
						,Override_Partner_Id
						,Override_Partner_Nm
						,Override_Type_Cd
						,Override_Type_Dsc
						,Override_Type_Short_Dsc
						,Override_Ind
						,Override_Reason_Type_Cd
						,Override_Reason_Type_Dsc
						,Override_Reason_Type_Short_Dsc
						,CreationDt
						,false AS DW_Logical_delete_ind
						,FileName
						FROM   src_wrk_tbl_recs --src1
						WHERE rn = 1.
						AND Override_Type_Cd  IS NOT NULL	
						AND Partner_Nm  IS NOT NULL	
					    ) src1								
							
							LEFT JOIN
							(SELECT DISTINCT Business_Partner_Integration_Id,Partner_Nm
							 FROM ${lkp_tb1}
							 WHERE DW_CURRENT_VERSION_IND = TRUE
							 AND DW_LOGICAL_DELETE_IND = FALSE
							) LKP_Business_Partner_Profile
							 ON src1.Partner_Nm = LKP_Business_Partner_Profile.Partner_Nm
							
						)src
						
						LEFT JOIN (SELECT
						 tgt.Business_Partner_Integration_Id
						,tgt.Partner_Nm
						,tgt.Override_Partner_Id
						,tgt.Override_Partner_Nm
						,tgt.Override_Type_Cd
						,tgt.Override_Type_Dsc
						,tgt.Override_Type_Short_Dsc
						,tgt.Override_Ind
						,tgt.Override_Reason_Type_Cd
						,tgt.Override_Reason_Type_Dsc
						,tgt.Override_Reason_Type_Short_Dsc
						,tgt.dw_logical_delete_ind
						,tgt.dw_first_effective_dt
						FROM ${tgt_tbl} tgt
						WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
						) tgt
						ON tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
						AND tgt.Partner_Nm = src.Partner_Nm
                        AND tgt.Override_Type_Cd = src.Override_Type_Cd						
						
						WHERE  (tgt.Business_Partner_Integration_Id  IS NULL AND tgt.Partner_Nm IS NULL AND tgt.Override_Type_Cd IS NULL)
						OR (NVL(src.Override_Partner_Id,'-1') <> NVL(tgt.Override_Partner_Id,'-1')
						OR  NVL(src.Override_Partner_Nm,'-1') <> NVL(tgt.Override_Partner_Nm,'-1')
						OR  NVL(src.Override_Type_Dsc,'-1') <> NVL(tgt.Override_Type_Dsc,'-1')
						OR  NVL(src.Override_Type_Short_Dsc,'-1') <> NVL(tgt.Override_Type_Short_Dsc,'-1')
						OR  NVL(src.Override_Ind, -1) <> NVL(tgt.Override_Ind, -1)
						OR  NVL(src.Override_Reason_Type_Cd,'-1') <> NVL(tgt.Override_Reason_Type_Cd,'-1')
						OR  NVL(src.Override_Reason_Type_Dsc,'-1') <> NVL(tgt.Override_Reason_Type_Dsc,'-1')
						OR  NVL(src.Override_Reason_Type_Short_Dsc,'-1') <> NVL(tgt.Override_Reason_Type_Short_Dsc,'-1')
						OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
						)
						`;


try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return `Creation of Business_Partner_Service_Override work table ${tgt_wrk_tbl} Failed with error:  ${err}`;   // Return a error message.
        }


//SCD Type2 transaction begins 
// Processing Updates of Type 2 SCD
var sql_begin = "BEGIN"
var sql_updates =
				`UPDATE ${tgt_tbl} as tgt
					SET DW_Last_Effective_dt = CURRENT_DATE-1
						,DW_CURRENT_VERSION_IND = FALSE
						,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
						,DW_SOURCE_UPDATE_NM = FileName
					FROM ( SELECT
						 Business_Partner_Integration_Id
						,Partner_Nm
						,Override_Type_Cd
						,FileName
				FROM ${tgt_wrk_tbl}
					WHERE DML_Type = 'U'
						AND Sameday_chg_ind = 0
						AND Business_Partner_Integration_Id  IS NOT NULL
						AND Partner_Nm  IS NOT NULL
						AND Override_Type_Cd  IS NOT NULL
					) src
					WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id  
						AND tgt.Partner_Nm = src.Partner_Nm
                        AND tgt.Override_Type_Cd = src.Override_Type_Cd						
						AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
   

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
						SET  Override_Partner_Id= src.Override_Partner_Id
						    ,Override_Partner_Nm=src.Override_Partner_Nm
							,Override_Type_Cd= src.Override_Type_Cd
							,Override_Type_Dsc= src.Override_Type_Dsc
							,Override_Type_Short_Dsc= src.Override_Type_Short_Dsc
							,Override_Ind = src.Override_Ind
							,Override_Reason_Type_Cd = src.Override_Reason_Type_Cd
							,Override_Reason_Type_Dsc = src.Override_Reason_Type_Dsc
							,Override_Reason_Type_Short_Dsc = src.Override_Reason_Type_Short_Dsc
							,DW_Logical_delete_ind = src.DW_Logical_delete_ind
							,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
							,DW_SOURCE_UPDATE_NM = FileName
					FROM ( SELECT
							Business_Partner_Integration_Id
							,Partner_Nm
							,Override_Partner_Id
							,Override_Partner_Nm
							,Override_Type_Cd
							,Override_Type_Dsc
							,Override_Type_Short_Dsc
							,Override_Ind
							,Override_Reason_Type_Cd
							,Override_Reason_Type_Dsc
							,Override_Reason_Type_Short_Dsc
							,CreationDt
							,DW_Logical_delete_ind
							,FileName
					FROM ${tgt_wrk_tbl}
					WHERE DML_Type = 'U'
							AND Sameday_chg_ind = 1
							AND Business_Partner_Integration_Id  IS NOT NULL
							AND Partner_Nm  IS NOT NULL
							AND Override_Type_Cd  IS NOT NULL
							 ) src
					WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id  
							AND tgt.Partner_Nm = src.Partner_Nm
                            AND tgt.Override_Type_Cd = src.Override_Type_Cd							
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
								(Business_Partner_Integration_Id,
								Partner_Nm,
								Override_Type_Cd,
								DW_First_Effective_Dt,
								DW_Last_Effective_Dt,
								Override_Partner_Id,
							    Override_Partner_Nm,
								Override_Ind,
								Override_Type_Dsc,
								Override_Type_Short_Dsc,
								Override_Reason_Type_Cd,
								Override_Reason_Type_Dsc,
								Override_Reason_Type_Short_Dsc,
								DW_CREATE_TS,
								DW_LOGICAL_DELETE_IND,
								DW_SOURCE_CREATE_NM,
								DW_CURRENT_VERSION_IND 
								)
							    SELECT DISTINCT
								Business_Partner_Integration_Id
								,Partner_Nm
								,Override_Type_Cd
								,CURRENT_DATE
								,'31-DEC-9999'
								,Override_Partner_Id
								,Override_Partner_Nm
								,Override_Ind
								,Override_Type_Dsc
								,Override_Type_Short_Dsc
								,Override_Reason_Type_Cd
								,Override_Reason_Type_Dsc
								,Override_Reason_Type_Short_Dsc
								,CURRENT_TIMESTAMP								
								,DW_Logical_delete_ind
								,FileName
								,TRUE
							FROM ${tgt_wrk_tbl}
							WHERE Sameday_chg_ind = 0
							AND Business_Partner_Integration_Id  IS NOT NULL
							AND Partner_Nm  IS NOT NULL	
							AND Override_Type_Cd  IS NOT NULL	
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
        return `Loading of Business_Partner_Service_Override table ${tgt_tbl} Failed with error:  ${err}`;   // Return a error message.
        
}
 var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;
                    
					

var sql_exceptions = `INSERT INTO  ${tgt_exp_tbl}  
							select Distinct
								    Business_Partner_Integration_Id,
								    Partner_Nm,
								    Override_Partner_Id,
									Override_Partner_Nm,
									Override_Type_Cd,
									Override_Type_Dsc,
									Override_Type_Short_Dsc,
									Override_Ind,
									Override_Reason_Type_Cd,
									Override_Reason_Type_Dsc,
									Override_Reason_Type_Short_Dsc,
									CreationDt,
									FileName,								 
									DML_Type,
									Sameday_chg_ind,
									CASE WHEN Business_Partner_Integration_Id is NULL THEN 'Business_Partner_Integration_Id is NULL'
									 END AS Exception_Reason,
								    CURRENT_TIMESTAMP AS DW_CREATE_TS								 
									FROM  ${tgt_wrk_tbl}
									WHERE Business_Partner_Integration_Id IS NULL
									or Partner_Nm IS NULL  	
									or Override_Type_Cd IS NULL 
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
        return `Insert into tgt Exception table ${tgt_exp_tbl} Failed with error:  ${err}`;   // Return a error message.
              }


// ************** Load for Business_Partner_Service_Override table ENDs *****************

$$;
