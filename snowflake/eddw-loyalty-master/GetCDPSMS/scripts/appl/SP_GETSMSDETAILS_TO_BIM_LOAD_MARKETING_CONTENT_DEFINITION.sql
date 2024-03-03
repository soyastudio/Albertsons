--liquibase formatted sql
--changeset SYSTEM:SP_GETSMSDETAILS_TO_BIM_LOAD_MARKETING_CONTENT_DEFINITION runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETSMSDETAILS_TO_BIM_LOAD_MARKETING_CONTENT_DEFINITION(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		var src_wrk_tbl = SRC_WRK_TBL;
		var cnf_schema = C_LOYAL;
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.Marketing_Content_Definition_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.Marketing_Content_Definition`;
                       
    // **************        Load for Marketing_Content_Definition table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as 
								SELECT DISTINCT
								 src.Campaign_Id
								,src.Calendar_Year_Nbr 
								,src.Calendar_Week_Nbr
								,src.Channel_Cd
								,src.Banner_Nm
								,src.Theme_Nm
								,src.Message_Header_Txt
								,src.Message_Content_Txt
								,src.Message_URL_Txt
								,src.Message_Footer_Txt
								,src.filename
								,src.DW_LOGICAL_DELETE_IND
                                ,CASE 
								    WHEN (
										     tgt.Campaign_Id IS NULL 
										and  tgt.Calendar_Year_Nbr is NULL 
										and  tgt.Calendar_Week_Nbr is NULL 
										and  tgt.Channel_Cd is NULL
								         ) 
									THEN 'I' 
									ELSE 'U' 
								END AS DML_Type
								,CASE   
									WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
									THEN 1 
									Else 0 
								END as Sameday_chg_ind
								FROM (   SELECT
											Campaign_Id 
											,Calendar_Year_Nbr 
											,Calendar_Week_Nbr
											,Channel_Cd
											,Banner_Nm 
											,Theme_Nm
											,Message_Header_Txt
											,Message_Content_Txt
											,Message_URL_Txt
											,Message_Footer_Txt
											,filename
											,DW_LOGICAL_DELETE_IND
										
										FROM ( 
											   SELECT
												Campaign_Id 
												,Calendar_Year_Nbr 
												,Calendar_Week_Nbr
												,Channel_Cd
												,Banner_Nm 
												,Theme_Nm
												,Message_Header_Txt
												,Message_Content_Txt
												,Message_URL_Txt
												,Message_Footer_Txt
												,filename
												,DW_CREATETS
												,false as  DW_LOGICAL_DELETE_IND
											,Row_number() OVER (
											 PARTITION BY Campaign_Id,Calendar_Year_Nbr,Calendar_Week_Nbr, Channel_Cd
											  order by(DW_CREATETS) DESC) as rn
											  FROM(
                                                    SELECT
													Campaign_Id 
													,Calendar_Year_Nbr 
													,Calendar_Week_Nbr
													,Channel_Cd
													,Banner_Nm 
													,Theme_Nm
													,Message_Header_Txt
													,Message_Content_Txt
													,Message_URL_Txt
													,Message_Footer_Txt 
													,filename
													,DW_CREATETS
													FROM
													  (
													  SELECT  
													   Campaign_Id
													  ,Calendar_Yr_Nbr as Calendar_Year_Nbr
													  ,Calendar_Week_Nbr
													  ,Channel_Cd
													  ,BANNER				as  Banner_Nm
													  ,THEME   			 	as  Theme_Nm
													  ,Message_Header_Txt
													  ,Message_Content_Txt
													  ,Message_URL_Txt
													  ,Message_Footer_Txt
													  ,filename
													  ,DW_CREATETS
													  FROM 
													   ${src_wrk_tbl} S
													  )
                                                )
											)  where rn=1	AND Campaign_Id is NOT NULL
															AND Calendar_Year_Nbr is NOT NULL
															AND Calendar_Week_Nbr is NOT NULL
															AND Channel_Cd is NOT NULL
									) src
									LEFT JOIN
									( 
									SELECT  DISTINCT
											Campaign_Id 
											,Calendar_Year_Nbr 
											,Calendar_Week_Nbr
											,Channel_Cd
											,Banner_Nm 
											,Theme_Nm
											,Message_Header_Txt
											,Message_Content_Txt
											,Message_URL_Txt
											,Message_Footer_Txt
											,DW_First_Effective_dt
											,DW_LOGICAL_DELETE_IND
									FROM
									${tgt_tbl} tgt
									WHERE DW_CURRENT_VERSION_IND = TRUE
									)as tgt 
									ON
										 nvl(src.Campaign_Id ,'-1') = nvl(tgt.Campaign_Id ,'-1')
									and  nvl(src.Calendar_Year_Nbr,'-1') = nvl(tgt.Calendar_Year_Nbr ,'-1')
									and  nvl(src.Calendar_Week_Nbr,'-1') = nvl(tgt.Calendar_Week_Nbr,'-1')
									and  nvl(src.Channel_Cd,'-1') = nvl(tgt.Channel_Cd,'-1')
									WHERE  (
									tgt.Campaign_Id IS  NULL
									AND tgt.Calendar_Year_Nbr is  NULL
									AND tgt.Calendar_Week_Nbr is NULL
									AND tgt.Channel_Cd is NULL
									 )
									OR
									(
									 NVL(src.Campaign_Id,'-1') <> NVL(tgt.Campaign_Id,'-1')  
									 OR  NVL(src.Calendar_Year_Nbr ,'-1') <> NVL(tgt.Calendar_Year_Nbr ,'-1')
									 OR  NVL(src.Calendar_Week_Nbr,'-1') <> NVL(tgt.Calendar_Week_Nbr,'-1')  
									 OR NVL(src.Channel_Cd,'-1') <> NVL(tgt.Channel_Cd,'-1')
									 OR NVL(src.Banner_Nm,'-1') <> NVL(tgt.Banner_Nm,'-1')
									 OR NVL(src.Theme_Nm,'-1') <> NVL(tgt.Theme_Nm,'-1')
									 OR NVL(src.Message_Header_txt,'-1') <>NVL(tgt.Message_Header_txt,'-1')
									 OR NVL(src.Message_Content_Txt,'-1') <> NVL(tgt.Message_Content_Txt,'-1')
									 OR NVL(src.Message_URL_Txt,'-1') <> NVL(tgt.Message_URL_Txt,'-1')
									 OR NVL(src.Message_Footer_Txt,'-1') <> NVL(tgt.Message_Footer_Txt,'-1')
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
					DW_Logical_delete_ind=TRUE,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename
				
					FROM ( 
							SELECT 
								 Campaign_Id
								,Calendar_Year_Nbr
								,Calendar_Week_Nbr 
								,Channel_Cd
								,filename
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 0
					) src
					WHERE
					nvl(src.Campaign_Id,'-1') = nvl(tgt.Campaign_Id,'-1')
					AND nvl(src.Calendar_Year_Nbr,'-1') = nvl(tgt.Calendar_Year_Nbr,'-1')
					AND nvl(src.Calendar_Week_Nbr,'-1')= nvl(tgt.Calendar_Week_Nbr,'-1')
					AND nvl(src.Channel_Cd,'-1') = nvl(tgt.Channel_Cd,'-1')					
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                   
 // SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
					SET Campaign_Id = src.Campaign_Id,
					Calendar_Year_Nbr = src.Calendar_Year_Nbr,
					Calendar_Week_Nbr = src.Calendar_Week_Nbr,
					Channel_Cd=src.Channel_Cd,
					Banner_Nm = src.Banner_Nm,
					Theme_Nm = src.Theme_Nm,
					Message_Header_Txt=src.Message_Header_Txt,
					Message_Content_Txt = src.Message_Content_Txt,
					Message_URL_Txt = src.Message_URL_Txt,
					Message_Footer_Txt = src.Message_Footer_Txt,
					DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename
					FROM ( 
							SELECT
								Campaign_Id
								,Calendar_Year_Nbr
								,Calendar_Week_Nbr 
								,Channel_Cd
								,Banner_Nm
								,Theme_Nm
								,Message_Header_txt
								,Message_Content_Txt
								,Message_URL_Txt
								,Message_Footer_Txt
								,filename
								,DW_Logical_delete_ind
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src WHERE
							nvl(src.Campaign_Id,'-1') = nvl(tgt.Campaign_Id,'-1')
							AND nvl(src.Calendar_Year_Nbr,'-1') = nvl(tgt.Calendar_Year_Nbr,'-1')
							AND nvl(src.Calendar_Week_Nbr,'-1')= nvl(tgt.Calendar_Week_Nbr,'-1')
							AND nvl(src.Channel_Cd,'-1') = nvl(tgt.Channel_Cd,'-1')	
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
					Campaign_Id
					,Calendar_Year_Nbr
					,Calendar_Week_Nbr 
					,Channel_Cd
					,DW_First_Effective_Dt
					,DW_Last_Effective_Dt
					,Theme_Nm
					,Banner_Nm
					,Message_URL_Txt
					,Message_Header_txt
					,Message_Content_Txt
					,Message_Footer_Txt
					,DW_CREATE_TS
					,DW_LOGICAL_DELETE_IND
					,DW_SOURCE_CREATE_NM
					,DW_CURRENT_VERSION_IND
					)
					SELECT
					Campaign_Id 
					,Calendar_Year_Nbr 
					,Calendar_Week_Nbr
					,Channel_Cd
					,CURRENT_DATE
					,'31-DEC-9999'
					,Theme_Nm
					,Banner_Nm
					,Message_URL_Txt
					,Message_Header_txt
					,Message_Content_Txt
					,Message_Footer_Txt 
					,CURRENT_TIMESTAMP
					,DW_LOGICAL_DELETE_IND
					,filename
					,TRUE 
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
                               
                // **************        Load for Marketing_Content_Definition Table ENDs *****************
				

$$;
