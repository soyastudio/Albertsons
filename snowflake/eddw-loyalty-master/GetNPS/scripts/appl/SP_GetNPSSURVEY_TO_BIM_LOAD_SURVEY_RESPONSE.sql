--liquibase formatted sql
--changeset SYSTEM:SP_GetNPSSURVEY_TO_BIM_LOAD_SURVEY_RESPONSE runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETNPSSURVEY_TO_BIM_LOAD_SURVEY_RESPONSE(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var src_wrk_tbl = SRC_WRK_TBL;
		var cnf_schema = C_LOYAL;
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.SURVEY_RESPONSE_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.SURVEY_RESPONSE`;
                       
    // **************        Load for Survey Response table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
								with src_data_recs as
								(SELECT * FROM
								( 
								SELECT  *, Row_number() OVER (PARTITION BY surveyid order by(DW_CREATETS) DESC) as rn
								FROM(
									 SELECT * FROM 
									  ${src_wrk_tbl} S  
										WHERE S.unitid NOT IN('(I) Ad Hoc','(I) *** Unit Pending')									  
									)
								) 
								where rn=1
								)
								select src.SURVEY_ID,
										src.SURVEY_QUESTION_SEQUENCE_NBR,
										src.survey_response_txt,
										src.Survey_Response_Score_Nbr,
										src.question_txt,
										src.filename,
										src.DW_LOGICAL_DELETE_IND,
										 CASE 
										WHEN (tgt.survey_id IS NULL AND tgt.SURVEY_QUESTION_SEQUENCE_NBR IS NULL) 
										THEN 'I' 
										ELSE 'U' 
										END AS DML_Type,
										CASE   
										WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
										THEN 1 
										Else 0 
										END as Sameday_chg_ind
								from
								(select SURVEYID as SURVEY_ID,
								SURVEY_QUESTION_SEQUENCE_NBR,
								(case when surveyresponsetxt='ABS_PHARM_LTR_CMT' then ABSPHARMLTRCMT
								ELSE NULL END) survey_response_txt,
								(case when surveyresponsetxt='ABS_PHARM_LTR_CMT' then NULL
								ELSE SurveyResponseScoreNbr END) Survey_Response_Score_Nbr,
								question_txt,
								filename,
								 false as DW_LOGICAL_DELETE_IND
								from
								--Survey Type Pharmacy
								(select a.SURVEYID, a.abs_survey_type, srt.survey_response_txt surveyresponsetxt, Survey_Response_Score_Nbr SurveyResponseScoreNbr,
										ABS_PHARM_LTR_CMT ABSPHARMLTRCMT,a.filename
								from (select 'ABS_PHARM_PRESCRIPTION_READINESS_SCALE11' as survey_response_txt
									  union all select 'ABS_PHARM_ASSOCIATE_FRIENDLINESS_SCALE11' 
									  union all select 'ABS_PHARM_ASSOCIATE_KNOWLEDGE_SCALE11'
									  union all select 'ABS_PHARM_SAFETY_PRECAUTIONS_SCALE11'
									  union all select 'ABS_PHARM_SERVICES_OFFERED_SCALE11'
									  union all select 'ABS_PHARM_ITEM_AVAILABILITY_SCALE11'
									  union all select 'ABS_PHARM_LTR_SCALE11' 
									  union all select 'ABS_PHARM_LTR_CMT'
									 ) srt
								cross join (select SURVEYID, abs_survey_type ,ABS_PHARM_LTR_CMT,filename
								from  src_data_recs
								where  lower(abs_survey_type)='pharmacy') as a
								left join
								src_data_recs pt 
								unpivot
								(
								 Survey_Response_Score_Nbr
								 for survey_response_txt in ( ABS_PHARM_PRESCRIPTION_READINESS_SCALE11,
															  ABS_PHARM_ASSOCIATE_FRIENDLINESS_SCALE11,
															  ABS_PHARM_ASSOCIATE_KNOWLEDGE_SCALE11,
															  ABS_PHARM_SAFETY_PRECAUTIONS_SCALE11,
															  ABS_PHARM_SERVICES_OFFERED_SCALE11,
															  ABS_PHARM_ITEM_AVAILABILITY_SCALE11,
															  ABS_PHARM_LTR_SCALE11,
															  ABS_PHARM_LTR_CMT
															 )
								) score
								on srt.survey_response_txt = score.survey_response_txt and
								   a.SURVEYID = score.SURVEYID 
								   ) r,
								  ${CNF_DB}.${cnf_schema}.SURVEY_QUESTION q
								  WHERE r.surveyresponsetxt = upper(substr(q.QUESTION_FIELD_NM,3))
								  and lower(r.abs_survey_type) = lower(q.survey_type_nm)
								 union all
								 --Survey Type DUG
								 select SURVEYID,
								SURVEY_QUESTION_SEQUENCE_NBR,
								(case when surveyresponsetxt='ABS_DUG_GIVE_MORE_FEEDBACK_YN' then feedbackyn
									  when surveyresponsetxt='ABS_DUG_LTR_CMT' then ltrcmt
									  when surveyresponsetxt='ABS_DUG_SUBSTITUTION_DISSATISFACTION_REASON_MVALUE' then reasonmvalue
									  when surveyresponsetxt='ABS_DUG_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT' then reasonother	  
								ELSE NULL END) survey_response_txt,
								(case when surveyresponsetxt in('ABS_DUG_GIVE_MORE_FEEDBACK_YN','ABS_DUG_LTR_CMT',
																'ABS_DUG_SUBSTITUTION_DISSATISFACTION_REASON_MVALUE','ABS_DUG_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT') then NULL
								ELSE SurveyResponseScoreNbr END) Survey_Response_Score_Nbr,
								question_txt,
								filename,
								 false as DW_LOGICAL_DELETE_IND
								from
								(select a.SURVEYID, a.abs_survey_type,srt.survey_response_txt surveyresponsetxt, Survey_Response_Score_Nbr SurveyResponseScoreNbr,
								 feedbackyn,ltrcmt,reasonmvalue,reasonother,a.filename
								from (select 'ABS_DUG_ASSOCIATE_FRIENDLINESS_SCALE11' as survey_response_txt
									  union all select 'ABS_DUG_LTR_SCALE11' 
									  union all select 'ABS_DUG_ORDER_PROCESS_EASE_SCALE11'
									  union all select 'ABS_DUG_ORDER_PICKUP_EASE_SCALE11'
									  union all select 'ABS_DUG_GIVE_MORE_FEEDBACK_YN'
									  union all select 'ABS_DUG_ITEM_AVAILABILITY_SCALE11'
									  union all select 'ABS_DUG_ORDER_ACCURACY_SCALE11' 
									  union all select 'ABS_DUG_ORDER_TIMELINESS_SCALE11'
									  union all select 'ABS_DUG_FRESHNESS_SCALE11'
									  union all select 'ABS_DUG_QUALITY_SCALE11' 
									  union all select 'ABS_DUG_LTR_CMT'
									  union all select 'ABS_DUG_SAFETY_PRECAUTIONS_SCALE11'
									  union all select 'ABS_DUG_SUBSTITUTION_DISSATISFACTION_REASON_MVALUE'
									  union all select 'ABS_DUG_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT'
									  union all select 'ABS_DUG_SUBSTITUTION_OSAT_SCALE11' 
									 ) srt
								cross join (select SURVEYID, abs_survey_type ,ABS_DUG_GIVE_MORE_FEEDBACK_YN feedbackyn, ABS_DUG_LTR_CMT ltrcmt,
										ABS_DUG_SUBSTITUTION_DISSATISFACTION_REASON_MVALUE  reasonmvalue,ABS_DUG_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT reasonother,filename
								from  src_data_recs 
								where  lower(abs_survey_type)='dug') as a
								left join
								src_data_recs pt 
								unpivot
								(
								 Survey_Response_Score_Nbr
								 for survey_response_txt in ( ABS_DUG_ASSOCIATE_FRIENDLINESS_SCALE11	,
																ABS_DUG_LTR_SCALE11	,
																ABS_DUG_ORDER_PROCESS_EASE_SCALE11	,
																ABS_DUG_ORDER_PICKUP_EASE_SCALE11	,
																ABS_DUG_GIVE_MORE_FEEDBACK_YN	,
																ABS_DUG_ITEM_AVAILABILITY_SCALE11	,
																ABS_DUG_ORDER_ACCURACY_SCALE11	,
																ABS_DUG_ORDER_TIMELINESS_SCALE11	,
																ABS_DUG_FRESHNESS_SCALE11	,
																ABS_DUG_QUALITY_SCALE11	,
																ABS_DUG_LTR_CMT	,
																ABS_DUG_SAFETY_PRECAUTIONS_SCALE11	,
																ABS_DUG_SUBSTITUTION_DISSATISFACTION_REASON_MVALUE	,
																ABS_DUG_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT	,
																ABS_DUG_SUBSTITUTION_OSAT_SCALE11	
															)
								) score
								on srt.survey_response_txt = score.survey_response_txt and
								   a.SURVEYID = score.SURVEYID 
								   ) r,
								  ${CNF_DB}.${cnf_schema}.SURVEY_QUESTION q
								  WHERE r.surveyresponsetxt = upper(substr(q.QUESTION_FIELD_NM,3))
								    and lower(r.abs_survey_type) = lower(q.survey_type_nm)
								 union all
								 --Survey Type Delivery
								select SURVEYID,
								SURVEY_QUESTION_SEQUENCE_NBR,
								(case when surveyresponsetxt='ABS_DELIVERY_LTR_CMT' then ABSDELIVERYLTRCMT
									  when surveyresponsetxt='ABS_DELIVERY_GIVE_MORE_FEEDBACK_YN' then ABSDELIVERYFEEDBACK_YN
									  when surveyresponsetxt='ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_MVALUE' then ABSDELIVERYREASONMVALUE
									  when surveyresponsetxt='ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT' then ABSDELIVERYREASONOHTER
								ELSE NULL END) survey_response_txt,
								(case when surveyresponsetxt in('ABS_DELIVERY_LTR_CMT','ABS_DELIVERY_GIVE_MORE_FEEDBACK_YN',
																'ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_MVALUE','ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT')
								then NULL
								ELSE SurveyResponseScoreNbr END) Survey_Response_Score_Nbr,
								question_txt,
								filename,
								 false as DW_LOGICAL_DELETE_IND
								from
								(select a.SURVEYID, a.abs_survey_type,srt.survey_response_txt surveyresponsetxt, Survey_Response_Score_Nbr SurveyResponseScoreNbr,
								ABS_DELIVERY_LTR_CMT ABSDELIVERYLTRCMT,
								ABS_DELIVERY_GIVE_MORE_FEEDBACK_YN 			ABSDELIVERYFEEDBACK_YN,
								ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_MVALUE			ABSDELIVERYREASONMVALUE,
								ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT		ABSDELIVERYREASONOHTER,
								a.filename
								from (select 'ABS_DELIVERY_ASSOCIATE_FRIENDLINESS_SCALE11' as survey_response_txt
									  union all select 'ABS_DELIVERY_LTR_SCALE11' 
									  union all select 'ABS_DELIVERY_ORDER_PROCESS_EASE_SCALE11'
									  union all select 'ABS_DELIVERY_ITEM_AVAILABILITY_SCALE11'
									  union all select 'ABS_DELIVERY_ORDER_ACCURACY_SCALE11'
									  union all select 'ABS_DELIVERY_ORDER_TIMELINESS_SCALE11'
									  union all select 'ABS_DELIVERY_FRESHNESS_SCALE11' 
									  union all select 'ABS_DELIVERY_QUALITY_SCALE11'
									  union all select 'ABS_DELIVERY_LTR_CMT'
									  union all select 'ABS_DELIVERY_SAFETY_PRECAUTIONS_SCALE11'
									   union all select 'ABS_DELIVERY_SUBSTITUTION_OSAT_SCALE11'
									   union all select 'ABS_DELIVERY_GIVE_MORE_FEEDBACK_YN'
									   union all select 'ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_MVALUE'
									   union all select 'ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT'
									   
									 ) srt
								cross join (select SURVEYID, abs_survey_type ,ABS_DELIVERY_LTR_CMT,ABS_DELIVERY_GIVE_MORE_FEEDBACK_YN,
													ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_MVALUE, ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT, filename
								from  src_data_recs 
								where  lower(abs_survey_type)='delivery') as a
								left join
								src_data_recs pt 
								unpivot
								(
								 Survey_Response_Score_Nbr
								 for survey_response_txt in ( ABS_DELIVERY_ASSOCIATE_FRIENDLINESS_SCALE11	,
																ABS_DELIVERY_LTR_SCALE11	,
																ABS_DELIVERY_ORDER_PROCESS_EASE_SCALE11	,
																ABS_DELIVERY_ITEM_AVAILABILITY_SCALE11	,
																ABS_DELIVERY_ORDER_ACCURACY_SCALE11	,
																ABS_DELIVERY_ORDER_TIMELINESS_SCALE11	,
																ABS_DELIVERY_FRESHNESS_SCALE11	,
																ABS_DELIVERY_QUALITY_SCALE11	,
																ABS_DELIVERY_LTR_CMT	,
																ABS_DELIVERY_SAFETY_PRECAUTIONS_SCALE11	,
																ABS_DELIVERY_SUBSTITUTION_OSAT_SCALE11,
																ABS_DELIVERY_GIVE_MORE_FEEDBACK_YN,
																ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_MVALUE,
																ABS_DELIVERY_SUBSTITUTION_DISSATISFACTION_REASON_OTHER_TXT
															 )
								) score
								on srt.survey_response_txt = score.survey_response_txt and
								   a.SURVEYID = score.SURVEYID 
								   ) r,
								  ${CNF_DB}.${cnf_schema}.SURVEY_QUESTION q
								  WHERE r.surveyresponsetxt = upper(substr(q.QUESTION_FIELD_NM,3))
								   and lower(r.abs_survey_type) = lower(q.survey_type_nm)
								   
								   union all
								 --Survey Type In-store
								 
								  select SURVEYID,SURVEY_QUESTION_SEQUENCE_NBR,
									(case when surveyresponsetxt='ABS_STORE_LTR_CMT' then ABSSTORELTRCMT
										ELSE NULL END) survey_response_txt,
									(case when surveyresponsetxt='ABS_STORE_LTR_CMT' then NULL
									ELSE SurveyResponseScoreNbr END) Survey_Response_Score_Nbr,
									question_txt,
									filename,
									false as DW_LOGICAL_DELETE_IND
									from
									(select a.SURVEYID, a.abs_survey_type, srt.survey_response_txt surveyresponsetxt, Survey_Response_Score_Nbr SurveyResponseScoreNbr,
									ABS_STORE_LTR_CMT ABSSTORELTRCMT,a.filename
									from (select 'ABS_STORE_ASSOCIATE_FRIENDLINESS_SCALE11' as survey_response_txt
											union all select 'ABS_STORE_EASY_TO_SHOP_SCALE11' 
											union all select 'ABS_STORE_FRESHNESS_SCALE11'
											union all select 'ABS_STORE_GOOD_VALUE_SCALE11'
											union all select 'ABS_STORE_ITEM_AVAILABILITY_SCALE11'
											union all select 'ABS_STORE_LTR_CMT'
											union all select 'ABS_STORE_LTR_SCALE11' 
											union all select 'ABS_STORE_QUALITY_SCALE11'
											union all select 'ABS_STORE_QUICK_CHECKOUT_SCALE11'
											union all select 'ABS_STORE_SAFETY_PRECAUTIONS_SCALE11'
											union all select 'ABS_STORE_SELECTION_FIT_NEEDS_SCALE11'
											) srt
									cross join (select SURVEYID, abs_survey_type ,ABS_STORE_LTR_CMT,filename
										from  src_data_recs 
										where  lower(abs_survey_type)='in-store') as a
										left join
										src_data_recs pt 
										unpivot
										(
										Survey_Response_Score_Nbr
										for survey_response_txt in ( ABS_STORE_ASSOCIATE_FRIENDLINESS_SCALE11,
																	ABS_STORE_EASY_TO_SHOP_SCALE11,
																	ABS_STORE_FRESHNESS_SCALE11,
																	ABS_STORE_GOOD_VALUE_SCALE11,
																	ABS_STORE_ITEM_AVAILABILITY_SCALE11,
																	ABS_STORE_LTR_CMT,
																	ABS_STORE_LTR_SCALE11,
																	ABS_STORE_QUALITY_SCALE11,
																	ABS_STORE_QUICK_CHECKOUT_SCALE11,
																	ABS_STORE_SAFETY_PRECAUTIONS_SCALE11,
																	ABS_STORE_SELECTION_FIT_NEEDS_SCALE11
																	)
										) score
										on srt.survey_response_txt = score.survey_response_txt and
										a.SURVEYID = score.SURVEYID 
									) r,
										${CNF_DB}.${cnf_schema}.SURVEY_QUESTION q
										WHERE r.surveyresponsetxt = upper(substr(q.QUESTION_FIELD_NM,3))
										and lower(r.abs_survey_type) = lower(q.survey_type_nm)	
										
								 ) src
								 LEFT JOIN
								  ( 
								  SELECT  DISTINCT
								  SURVEY_ID
								  ,SURVEY_QUESTION_SEQUENCE_NBR
								  ,SURVEY_RESPONSE_SCORE_NBR
								  ,SURVEY_RESPONSE_TXT
								  ,DW_LOGICAL_DELETE_IND
								  ,DW_First_Effective_dt
								  FROM
								    ${tgt_tbl}
								  WHERE DW_CURRENT_VERSION_IND = TRUE
								  )as tgt 
								  ON
								  nvl(src.SURVEY_ID,'-1') = nvl(tgt.SURVEY_ID,'-1')
								  AND nvl(src.SURVEY_QUESTION_SEQUENCE_NBR,'-1') = nvl(tgt.SURVEY_QUESTION_SEQUENCE_NBR,'-1')
								  WHERE  (
								  tgt.SURVEY_ID IS  NULL
								  AND tgt.SURVEY_QUESTION_SEQUENCE_NBR IS NULL
								   )
								  OR
								  (
								  NVL(src.SURVEY_ID,'-1') <> NVL(tgt.SURVEY_ID,'-1')
								  OR  NVL(src.SURVEY_QUESTION_SEQUENCE_NBR,'-1') <> NVL(tgt.SURVEY_QUESTION_SEQUENCE_NBR,'-1')
								  OR  NVL(src.SURVEY_RESPONSE_TXT,'-1') <> NVL(tgt.SURVEY_RESPONSE_TXT,'-1')
								  OR  NVL(src.SURVEY_RESPONSE_SCORE_NBR,'-1') <> NVL(tgt.SURVEY_RESPONSE_SCORE_NBR,'-1')
								  OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND  
								  )`;   

try {
snowflake.execute ({ sqlText: create_tgt_wrk_table });
	}
    catch (err) {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}          
            
 
// Transaction for Updates, Insert begins           
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
							SURVEY_ID,
							SURVEY_QUESTION_SEQUENCE_NBR,
							filename
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 0
					) src
					WHERE 
					nvl(src.SURVEY_ID,'-1') = nvl(tgt.SURVEY_ID,'-1')
					AND nvl(src.SURVEY_QUESTION_SEQUENCE_NBR,'-1') = nvl(tgt.SURVEY_QUESTION_SEQUENCE_NBR,'-1')  
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                   
 // SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
					SET SURVEY_RESPONSE_SCORE_NBR = src.SURVEY_RESPONSE_SCORE_NBR,
					SURVEY_RESPONSE_TXT = src.SURVEY_RESPONSE_TXT,
					DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename
					FROM ( 
							SELECT
							SURVEY_ID,
						    SURVEY_QUESTION_SEQUENCE_NBR,
						    SURVEY_RESPONSE_SCORE_NBR,
						    SURVEY_RESPONSE_TXT,
							DW_Logical_delete_ind,
							filename
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src WHERE 
						nvl(src.SURVEY_ID,'-1') = nvl(tgt.SURVEY_ID,'-1')
						AND nvl(src.SURVEY_QUESTION_SEQUENCE_NBR,'-1') = nvl(tgt.SURVEY_QUESTION_SEQUENCE_NBR,'-1') 
						AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
					SURVEY_ID,
					SURVEY_QUESTION_SEQUENCE_NBR,
					DW_First_Effective_Dt,
                    DW_Last_Effective_Dt,
					SURVEY_RESPONSE_SCORE_NBR,
					SURVEY_RESPONSE_TXT,
                    DW_CREATE_TS,
                    DW_LOGICAL_DELETE_IND,
                    DW_SOURCE_CREATE_NM,
                    DW_CURRENT_VERSION_IND  
					)
					SELECT
					SURVEY_ID,
					SURVEY_QUESTION_SEQUENCE_NBR,
					CURRENT_DATE,
					'31-DEC-9999',
					SURVEY_RESPONSE_SCORE_NBR,
					SURVEY_RESPONSE_TXT,
					CURRENT_TIMESTAMP,
					DW_Logical_delete_ind,
					filename,
					TRUE 
					FROM ${tgt_wrk_tbl}
					WHERE 
					Sameday_chg_ind = 0`;
    
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
                               
                // **************        Load for Survey Response table ENDs *****************

$$;