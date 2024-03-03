
	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_LOYAL;
	var wrk_schema = C_STAGE;

	var tgt_wrk_tbl = `${CNF_DB}.${wrk_schema}.Business_Partner_Site_Organization_wrk`;
        var tgt_tbl = `${CNF_DB}.${cnf_schema}.Business_Partner_Site_Organization`;
	var lkp_tbl = `${CNF_DB}.${cnf_schema}.Business_Partner`;
	var tgt_exp_tbl = `${CNF_DB}.${wrk_schema}.Business_Partner_Site_Organization_EXCEPTIONS`;

// ************** Load for Business_Partner_Site_Organization table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
                            WITH src_wrk_tbl_recs as
                            (SELECT DISTINCT
                             Partner_Participant_Id
							,Partner_Site_Id
							,Partner_Id
							,Partner_Site_Organization_Type_Cd
							,Partner_Site_Organization_Value_Txt
							,CreationDt
							,filename
							,Row_number() OVER ( partition BY  Partner_Participant_Id, Partner_Site_Id, Partner_Id, Partner_Site_Organization_Type_Cd ORDER BY To_timestamp_ntz(CreationDt) DESC) AS rn
                            from
                            (
                            SELECT DISTINCT
							          PartnerParticipantId as Partner_Participant_Id
							         ,PartnerSiteId as Partner_Site_Id
									 ,BusinessPartnerData_PartnerId as Partner_Id
									 ,PARTNERSITEDATA_ORGANIZATIONTYPECD as Partner_Site_Organization_Type_Cd
									 ,PARTNERSITEDATA_ORGANIZATIONVALUETXT as Partner_Site_Organization_Value_Txt
									 ,CreationDt
									 ,Filename
							FROM ${src_wrk_tbl}

						  -- UNION ALL

							--SELECT DISTINCT
								   --Partner_Participant_Id
								  --,Partner_Site_Id
								 -- ,Partner_Id
							     -- ,Partner_Site_Organization_Type_Cd
							--,Partner_Site_Organization_Value_Txt
								  --,CreationDt
								  --,filename
							--FROM ${tgt_exp_tbl}
						  )
                          )

                          SELECT
                                src.Business_Partner_Integration_Id
							   ,src.Partner_Site_Organization_Type_Cd
							   ,src.Partner_Site_Organization_Value_Txt
							   ,src.Partner_Participant_Id
							   ,src.Partner_Site_Id
							   ,src.Partner_Id
							   ,src.CreationDt
							   ,src.DW_Logical_delete_ind
							   ,src.filename
                               ,CASE WHEN tgt.Business_Partner_Integration_Id IS NULL  AND tgt.Partner_Site_Organization_Type_Cd IS   NULL THEN 'I' ELSE 'U' END AS DML_Type
                               ,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind
                          from
                          (SELECT
								   B.Business_Partner_Integration_Id
								  ,s.Partner_Site_Organization_Type_Cd
							      ,s.Partner_Site_Organization_Value_Txt
								  ,s.Partner_Participant_Id
								  ,s.Partner_Site_Id
								  ,s.Partner_Id
								  ,s.CreationDt
								  ,s.DW_Logical_delete_ind
								  ,s.filename
							FROM
							(
							select
								   Partner_Participant_Id
								  ,Partner_Site_Id
								  ,Partner_Id
								  ,Partner_Site_Organization_Type_Cd
							          ,Partner_Site_Organization_Value_Txt
								  ,CreationDt
								  ,FALSE AS DW_Logical_delete_ind
								  ,filename
							from src_wrk_tbl_recs
							WHERE rn =1 AND Partner_Site_Organization_Type_Cd is not null
							--AND ( Partner_Participant_Id is not null
							--    OR Partner_Site_Id is not null
							--	OR Partner_Id is not null )
							AND Partner_Participant_Id is not null
							AND Partner_Site_Id is not null
							AND Partner_Id is not null
							) s
						   LEFT JOIN
							(	SELECT Business_Partner_Integration_Id
									  ,Partner_Participant_Id
								      ,Partner_Site_Id
								      ,Partner_Id
								FROM ${lkp_tbl}
								WHERE DW_CURRENT_VERSION_IND = TRUE
								AND DW_LOGICAL_DELETE_IND = FALSE
							) B

							ON 		((	NVL(s.Partner_Participant_Id,'-1') = NVL(B.Partner_Participant_Id,'-1')
												AND NVL(s.Partner_Site_Id,'-1') = NVL(B.Partner_Site_Id,'-1')
												AND NVL(s.Partner_Id,'-1') = NVL(B.Partner_Id,'-1')
												)
										OR  	(	NVL(s.Partner_Participant_Id,'-1') = NVL(B.Partner_Participant_Id,'-1')
												AND NVL(s.Partner_Site_Id,'-1') = NVL(B.Partner_Site_Id,'-1')
												)
									)
						)src

                        LEFT JOIN
                          (SELECT  DISTINCT
								 tgt.Business_Partner_Integration_Id
								,tgt.Partner_Site_Organization_Type_Cd
								,tgt.Partner_Site_Organization_Value_Txt
								,tgt.dw_logical_delete_ind
								,tgt.dw_first_effective_dt
                          FROM ${tgt_tbl} tgt
                          WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
                          ) tgt
                          ON tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
						  AND tgt.Partner_Site_Organization_Type_Cd = src.Partner_Site_Organization_Type_Cd
						  WHERE  (tgt.Business_Partner_Integration_Id is null and tgt.Partner_Site_Organization_Type_Cd is null)
                          or(
                          NVL(src.Partner_Site_Organization_Value_Txt,'-1') <> NVL(tgt.Partner_Site_Organization_Value_Txt,'-1')
						  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
                          )
						  `;

try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return "Creation of Business_Partner_Site_Organization work table "+ tgt_wrk_tbl +" Failed with error: " + err;   // Return a error message.
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
										   Partner_Site_Organization_Type_Cd
                             FROM ${tgt_wrk_tbl}
                             WHERE DML_Type = 'U'
                             AND Sameday_chg_ind = 0
                             AND Business_Partner_Integration_Id is not NULL
							 AND Partner_Site_Organization_Type_Cd is not null
                             ) src
                             WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
							 AND tgt.Partner_Site_Organization_Type_Cd = src.Partner_Site_Organization_Type_Cd
							 AND tgt.DW_CURRENT_VERSION_IND = TRUE
							 AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
					SET Partner_Site_Organization_Value_Txt = src.Partner_Site_Organization_Value_Txt
					   ,DW_Logical_delete_ind = src.DW_Logical_delete_ind
					   ,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
					   ,DW_SOURCE_UPDATE_NM = FileName
						FROM ( SELECT
								     Business_Partner_Integration_Id
							        ,Partner_Site_Organization_Type_Cd
							        ,Partner_Site_Organization_Value_Txt
									,CreationDt
									,DW_Logical_delete_ind
									,filename
							   FROM ${tgt_wrk_tbl}
							   WHERE DML_Type = 'U'
							   AND Sameday_chg_ind = 1
							   AND Business_Partner_Integration_Id IS NOT NULL
							   AND Partner_Site_Organization_Type_Cd IS NOT NULL
									) src
							WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
							AND tgt.Partner_Site_Organization_Type_Cd = src.Partner_Site_Organization_Type_Cd
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
                   (
                     Business_Partner_Integration_Id
					,Partner_Site_Organization_Type_Cd
					,Partner_Site_Organization_Value_Txt
                    ,DW_First_Effective_Dt
                    ,DW_Last_Effective_Dt
                    ,DW_CREATE_TS
                    ,DW_LOGICAL_DELETE_IND
                    ,DW_SOURCE_CREATE_NM
                    ,DW_CURRENT_VERSION_IND
                   )
                   SELECT DISTINCT
                      Business_Partner_Integration_Id
					 ,Partner_Site_Organization_Type_Cd
					 ,Partner_Site_Organization_Value_Txt
                     ,CURRENT_DATE as DW_First_Effective_dt
					 ,'31-DEC-9999'
					 ,CURRENT_TIMESTAMP
                     ,DW_Logical_delete_ind
                     ,FileName
                     ,TRUE
				FROM ${tgt_wrk_tbl}
                where Business_Partner_Integration_Id is not null
				and Partner_Site_Organization_Type_Cd is not null
				and Sameday_chg_ind = 0`;

var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl}`;

	var sql_exceptions = `INSERT INTO ` + tgt_exp_tbl  + `
		SELECT  Business_Partner_Integration_Id
		       ,Partner_Participant_Id
			   ,Partner_Site_Id
			   ,Partner_Id
			   ,Partner_Site_Organization_Type_Cd
			   ,Partner_Site_Organization_Value_Txt
			   ,CreationDt
			   ,filename
			   ,CASE WHEN Business_Partner_Integration_Id IS NULL THEN 'Business_Partner_Integration_Id is NULL'
			         END AS Exception_Reason
			   ,CURRENT_TIMESTAMP AS dw_create_ts
		FROM `+ tgt_wrk_tbl +`
		WHERE  Business_Partner_Integration_Id is NULL
		OR Partner_Site_Organization_Type_Cd is NULL
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

// ************** Load for Business_Partner_Site_Oranization table ENDs *****************
