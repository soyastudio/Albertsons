
	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_LOYAL;
	var wrk_schema = C_STAGE;

	var tgt_wrk_tbl = `${CNF_DB}.${wrk_schema}.Business_Partner_Organization_wrk`;
    var tgt_tbl = `${CNF_DB}.${cnf_schema}.Business_Partner_Organization`;
	var lkp_tbl = `${CNF_DB}.${cnf_schema}.Business_Partner_Profile`;
	var tgt_exp_tbl = `${CNF_DB}.${wrk_schema}.Business_Partner_Organization_EXCEPTIONS`;

// ************** Load for Business_Partner_Organization table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT
                                                         Partner_Nm
							,Organization_Type_Cd
							,Organization_Value_Txt
							,CreationDt
							,filename
							,Row_number() OVER ( partition BY Partner_Nm, Organization_Type_Cd ORDER BY To_timestamp_ntz(CreationDt) DESC) AS rn
                            from
                            (
                            SELECT DISTINCT
									 PartnerProfile_PartnerNm as Partner_Nm
									,PartnerProfile_OrganizationTypeCd as Organization_Type_Cd
									,PartnerProfile_OrganizationValueTxt as Organization_Value_Txt
									,CreationDt
									,filename
							FROM ${src_wrk_tbl}

						  -- UNION ALL

							--SELECT DISTINCT
							      -- Partner_Nm
								--  ,Organization_Type_Cd
								--  ,Organization_Value_Txt
								 -- ,CreationDt
								 -- ,filename
							--FROM ${tgt_exp_tbl}
						  )
                          )

                          SELECT
                            src.Business_Partner_Integration_Id
						   ,src.Partner_Nm
						   ,src.Organization_Type_Cd
						   ,src.Organization_Value_Txt
						   ,src.CreationDt
						   ,src.DW_Logical_delete_ind
						   ,src.filename
                           ,CASE WHEN tgt.Business_Partner_Integration_Id IS NULL AND tgt.Partner_Nm IS NULL AND tgt.Organization_Type_Cd IS NULL THEN 'I' ELSE 'U' END AS DML_Type
                           ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
                           from
                           (SELECT
								   B.Business_Partner_Integration_Id
								  ,s.Partner_Nm
								  ,s.Organization_Type_Cd
								  ,s.Organization_Value_Txt
								  ,s.CreationDt
								  ,s.DW_Logical_delete_ind
								  ,s.filename
							FROM
							(
							select
								   Partner_Nm
								  ,Organization_Type_Cd
								  ,Organization_Value_Txt
								  ,CreationDt
								  ,FALSE AS DW_Logical_delete_ind
								  ,filename
							from src_wrk_tbl_recs
							WHERE Partner_Nm is not null
							AND Organization_Type_Cd is not null
							) s
						   LEFT JOIN
							(	SELECT Business_Partner_Integration_Id
									  ,Partner_Nm
								FROM ${lkp_tbl}
								WHERE DW_CURRENT_VERSION_IND = TRUE
								AND DW_LOGICAL_DELETE_IND = FALSE
							) B ON S.Partner_Nm = B.Partner_Nm
						)src

                        LEFT JOIN
                          (SELECT  DISTINCT
								 tgt.Business_Partner_Integration_Id
								,tgt.Partner_Nm
								,tgt.Organization_Type_Cd
								,tgt.Organization_Value_Txt
								,tgt.dw_logical_delete_ind
								,tgt.dw_first_effective_dt
                          FROM ${tgt_tbl} tgt
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt
                          ON tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
						  AND tgt.Partner_Nm = src.Partner_Nm
						  AND tgt.Organization_Type_Cd = src.Organization_Type_Cd
                          WHERE  (tgt.Partner_Nm is null and tgt.Business_Partner_Integration_Id is null and tgt.Organization_Type_Cd is null)
                          or(
                          NVL(src.Organization_Value_Txt,'-1') <> NVL(tgt.Organization_Value_Txt,'-1')
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )
						  `;

try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Business_Partner_Organization work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }

var sql_begin = "BEGIN"

// SCD Type2 - Processing Different day updates
              var sql_updates = `UPDATE ${tgt_tbl} as tgt
              SET
                             DW_Last_Effective_dt = CURRENT_DATE - 1,
                             DW_CURRENT_VERSION_IND = FALSE,
                             DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
                             DW_SOURCE_UPDATE_NM = filename
              FROM (
                             SELECT
                                           Business_Partner_Integration_Id,
                                           filename,
										   Partner_Nm,
										   Organization_Type_Cd
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U'
                             AND Sameday_chg_ind = 0
                             AND Partner_Nm is not NULL
							 AND Business_Partner_Integration_Id is not null
							 AND Organization_Type_Cd is not null
                             ) src
                             WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
							 AND tgt.Partner_Nm = src.Partner_Nm
							 AND tgt.Organization_Type_Cd = src.Organization_Type_Cd
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET Organization_Value_Txt = src.Organization_Value_Txt
					   ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
					   ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
					   ,DW_SOURCE_UPDATE_NM = FileName
						FROM ( SELECT
								     Business_Partner_Integration_Id
									,Partner_Nm
									,Organization_Type_Cd
									,Organization_Value_Txt
									,CreationDt
									,DW_Logical_delete_ind
									,filename
							   FROM ${tgt_wrk_tbl}
							   WHERE DML_Type = 'U'
							   AND Sameday_chg_ind = 1
							   AND Business_Partner_Integration_Id IS NOT NULL
							   AND Partner_Nm IS NOT NULL
							   AND Organization_Type_Cd IS NOT NULL
									) src
							WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
							AND tgt.Partner_Nm = src.Partner_Nm
							AND tgt.Organization_Type_Cd = src.Organization_Type_Cd
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                     Business_Partner_Integration_Id
					,Partner_Nm
					,Organization_Type_Cd
					,Organization_Value_Txt
                    ,DW_First_Effective_Dt
                    ,DW_Last_Effective_Dt
                    ,DW_CREATE_TS
                    ,DW_LOGICAL_DELETE_IND
                    ,DW_SOURCE_CREATE_NM
                    ,DW_CURRENT_VERSION_IND
                   )
                   SELECT DISTINCT
                      Business_Partner_Integration_Id
					 ,Partner_Nm
					 ,Organization_Type_Cd
					,Organization_Value_Txt
                     ,CURRENT_DATE as DW_First_Effective_dt
					 ,'31-DEC-9999'
					 ,CURRENT_TIMESTAMP
                     ,DW_Logical_delete_ind
                     ,FileName
                     ,TRUE
				FROM ${tgt_wrk_tbl}
                where Business_Partner_Integration_Id is not null
				and Partner_Nm is not null
				and Organization_Type_Cd is not null
				and Sameday_chg_ind = 0`;

var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl}`;

	var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl  + `
		SELECT  Business_Partner_Integration_Id
		       ,Partner_Nm
			   ,Organization_Type_Cd
			   ,Organization_Value_Txt
			   ,CreationDt
			   ,filename
			   ,CASE WHEN Business_Partner_Integration_Id IS NULL THEN 'Business_Partner_Integration_Id is NULL'
			         END AS Exception_Reason
			   ,CURRENT_TIMESTAMP AS dw_create_ts
		FROM `+ tgt_wrk_tbl +`
		WHERE  Business_Partner_Integration_Id is NULL
		OR Partner_Nm is NULL
		or Organization_Type_Cd is null
	`;

var sql_commit = "COMMIT"
var sql_rollback = "ROLLBACK"
try {
        snowflake.execute({sqlText: sql_begin});
		snowflake.execute({sqlText: sql_updates});
        snowflake.execute({sqlText: sql_sameday});
        snowflake.execute({sqlText: sql_inserts});
        snowflake.execute({sqlText: sql_commit});
		snowflake.execute({sqlText: truncate_exceptions});
		snowflake.execute({sqlText: sql_exceptions});
	}

    catch (err)  {
        snowflake.execute (
            {sqlText: sql_rollback  }
            );

        return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
        }

// ************** Load for Business_Partner_Organization table ENDs *****************
