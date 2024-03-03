

var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_LOYAL;
var lkp_schema = C_LOYAL;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Service_Fee_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Business_Partner_Service_Fee`;
var lkp_tb1 =`${cnf_db}.${lkp_schema}.Business_Partner_Profile`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Service_Fee_Exceptions`;


// ************** Load for Business_Partner_Service_Fee table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
				WITH src_wrk_tbl_recs as
						(SELECT DISTINCT
							    Partner_Nm,
								Service_Fee_Type_Cd,
								Service_Fee_Category_Cd,
								Service_Fee_Amt,
								Service_Fee_Item_Id,
								CreationDt,
								FileName,
								Row_number() OVER ( partition BY Partner_Nm, Service_Fee_Type_Cd  ORDER BY To_timestamp_ntz(CreationDt) DESC) AS rn

						FROM
						(
						SELECT DISTINCT
                                 PartnerProfile_PartnerNm AS Partner_Nm,
							     ServiceFeeTypeCd AS Service_Fee_Type_Cd,
								 ServiceFeeCategoryCd AS Service_Fee_Category_Cd,
								 ServiceFeeAmt AS Service_Fee_Amt,
								 ServiceFeeItemId AS Service_Fee_Item_Id,
								 CreationDt,
								 FileName
                            	FROM  ${src_wrk_tbl}

						)
			            )
						SELECT
						src.Business_Partner_Integration_Id
						,src.Partner_Nm
						,src.Service_Fee_Type_Cd
						,src.Service_Fee_Category_Cd
						,src.Service_Fee_Amt
						,src.Service_Fee_Item_Id
						,src.CreationDt
						,src.DW_Logical_delete_ind
						,src.filename
						,CASE WHEN(tgt.Business_Partner_Integration_Id is NULL AND tgt.Partner_Nm is NULL AND tgt.Service_Fee_Type_Cd is NULL) THEN 'I' ELSE 'U' END as DML_Type
						,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1 Else 0 END as Sameday_chg_ind
						FROM
					    (
						select
						 LKP_Business_Partner_Profile.Business_Partner_Integration_Id AS Business_Partner_Integration_Id
			            ,src1.Partner_Nm
						,src1.Service_Fee_Type_Cd
						,src1.Service_Fee_Category_Cd
						,src1.Service_Fee_Amt
						,src1.Service_Fee_Item_Id
						,src1.CreationDt
						,src1.DW_Logical_delete_ind
						,src1.FileName
						from
						(
						SELECT
						 Partner_Nm
						,Service_Fee_Type_Cd
						,Service_Fee_Category_Cd
						,Service_Fee_Amt
						,Service_Fee_Item_Id
						,CreationDt
						,false AS DW_Logical_delete_ind
						,FileName
						FROM   src_wrk_tbl_recs --src1
						WHERE rn = 1
						--AND Service_Fee_Type_Cd  IS NOT NULL
						--AND Partner_Nm  IS NOT NULL
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
						,tgt.Service_Fee_Type_Cd
						,tgt.Service_Fee_Category_Cd
						,tgt.Service_Fee_Amt
						,tgt.Service_Fee_Item_Id
						,tgt.dw_logical_delete_ind
						,tgt.dw_first_effective_dt
						FROM ${tgt_tbl} tgt
						WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
						) tgt
						ON tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
						AND tgt.Partner_Nm = src.Partner_Nm
                        AND tgt.Service_Fee_Type_Cd = src.Service_Fee_Type_Cd

						WHERE  (tgt.Business_Partner_Integration_Id  IS NULL AND tgt.Partner_Nm IS NULL AND tgt.Service_Fee_Type_Cd IS NULL)
						OR (NVL(src.Service_Fee_Category_Cd,'-1') <> NVL(tgt.Service_Fee_Category_Cd,'-1')
						OR  NVL(src.Service_Fee_Amt,'-1') <> NVL(tgt.Service_Fee_Amt,'-1')
						OR  NVL(src.Service_Fee_Item_Id,'-1') <> NVL(tgt.Service_Fee_Item_Id,'-1')
						OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
						)
						`;


try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return `Creation of Business_Partner_Service_Fee work table ${tgt_wrk_tbl} Failed with error:  ${err}`;   // Return a error message.
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
						,Service_Fee_Type_Cd
						,FileName
				FROM ${tgt_wrk_tbl}
					WHERE DML_Type = 'U'
						AND Sameday_chg_ind = 0
						AND Business_Partner_Integration_Id  IS NOT NULL
						AND Partner_Nm  IS NOT NULL
						AND Service_Fee_Type_Cd  IS NOT NULL
					) src
					WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
						AND tgt.Partner_Nm = src.Partner_Nm
                        AND tgt.Service_Fee_Type_Cd = src.Service_Fee_Type_Cd
						AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
						SET  Service_Fee_Category_Cd= src.Service_Fee_Category_Cd
						    ,Service_Fee_Amt=src.Service_Fee_Amt
							,Service_Fee_Item_Id= src.Service_Fee_Item_Id
							,DW_Logical_delete_ind = src.DW_Logical_delete_ind
							,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
							,DW_SOURCE_UPDATE_NM = FileName
					FROM ( SELECT
							Business_Partner_Integration_Id
							,Partner_Nm
							,Service_Fee_Type_Cd
							,Service_Fee_Category_Cd
							,Service_Fee_Amt
							,Service_Fee_Item_Id
							,CreationDt
							,DW_Logical_delete_ind
							,FileName
					FROM ${tgt_wrk_tbl}
					WHERE DML_Type = 'U'
							AND Sameday_chg_ind = 1
							AND Business_Partner_Integration_Id  IS NOT NULL
							AND Partner_Nm  IS NOT NULL
							AND Service_Fee_Type_Cd  IS NOT NULL
							 ) src
					WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
							AND tgt.Partner_Nm = src.Partner_Nm
                            AND tgt.Service_Fee_Type_Cd = src.Service_Fee_Type_Cd
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
								(Business_Partner_Integration_Id,
								 Partner_Nm,
								 Service_Fee_Type_Cd,
								 DW_First_Effective_Dt,
								 DW_Last_Effective_Dt,
								 Service_Fee_Category_Cd,
								 Service_Fee_Amt,
								 Service_Fee_Item_Id,
								 DW_CREATE_TS,
								 DW_LOGICAL_DELETE_IND,
								 DW_SOURCE_CREATE_NM,
								 DW_CURRENT_VERSION_IND
								)
							    SELECT DISTINCT
								Business_Partner_Integration_Id
								,Partner_Nm
								,Service_Fee_Type_Cd
								,CURRENT_DATE
								,'31-DEC-9999'
								,Service_Fee_Category_Cd
								,Service_Fee_Amt
								,Service_Fee_Item_Id
								,CURRENT_TIMESTAMP
								,DW_Logical_delete_ind
								,FileName
								,TRUE
							FROM ${tgt_wrk_tbl}
							WHERE Sameday_chg_ind = 0
							AND Business_Partner_Integration_Id  IS NOT NULL
							AND Partner_Nm  IS NOT NULL
							AND Service_Fee_Type_Cd  IS NOT NULL
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
        return `Loading of Business_Partner_Service_Fee table ${tgt_tbl} Failed with error:  ${err}`;   // Return a error message.

}
 var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl};`;


var sql_exceptions = `INSERT INTO  ${tgt_exp_tbl}
							select Distinct
								    Business_Partner_Integration_Id,
								    Partner_Nm,
								    Service_Fee_Type_Cd,
								    Service_Fee_Category_Cd,
									Service_Fee_Amt,
									Service_Fee_Item_Id,
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
									or Service_Fee_Type_Cd IS NULL
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


// ************** Load for Business_Partner_Service_Fee table ENDs *****************