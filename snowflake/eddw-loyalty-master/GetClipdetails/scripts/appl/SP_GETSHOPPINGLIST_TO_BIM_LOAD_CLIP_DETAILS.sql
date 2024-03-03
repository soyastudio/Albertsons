--liquibase formatted sql
--changeset SYSTEM:SP_GETSHOPPINGLIST_TO_BIM_LOAD_CLIP_DETAILS runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETSHOPPINGLIST_TO_BIM_LOAD_CLIP_DETAILS(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


		var src_wrk_tbl = SRC_WRK_TBL;
		var cnf_schema = 'DW_C_LOYALTY';
      	var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.CLIP_DETAILS_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.CLIP_DETAILS`;
		
		 // **************        Truncate and Reload the work table *****************

    var truncate_tgt_wrk_table = `truncate table  ${tgt_wrk_tbl}`;
try {
snowflake.execute ({ sqlText: truncate_tgt_wrk_table });
	}
    catch (err) {
        return `Deletion of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}
                       
    // **************        Load for Clip Details table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `INSERT INTO ${tgt_wrk_tbl}  
								SELECT DISTINCT
								src.Clip_Sequence_Id
								,src.Customer_GUID
								,src.Club_Card_Nbr
								--,src.Facility_Integration_ID
								,src.RETAIL_CUSTOMER_UUID
								,src.Clip_Id
								,src.Event_Ts
								,src.Offer_Id
								,src.Clip_Source_Application_Id
								,src.Clip_Type_Cd
								,src.Clip_Dt
							    ,src.Clip_Tm
								,src.Clip_Source_Cd
							    ,src.Vendor_Banner_Cd
								,src.DW_LOGICAL_DELETE_IND
								,src.FileName
                                ,CASE 
								    WHEN (
										     tgt.Event_Ts IS NULL 
										and  tgt.clip_id is NULL 
										and  tgt.Clip_Sequence_Id is NULL 
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
										  Customer_GUID
										 ,Club_Card_Nbr
										 ,Household_Id
										 ,OFFER_ID
										 ,External_Offer_Id
										 ,Program_Cd
										 ,Event_Ts
										 ,Clip_Dt
										 ,Clip_Tm
										 ,Clip_Id
										 ,Retail_Store_Id
										 ,Clip_Type_Cd
										 ,Banner_Nm
										 ,Postal_Cd
										 ,offer_type
										 ,Service_Provider_Nm
										 ,Clip_Source_Application_Id
										 ,Clip_Source_Cd
										 ,Vendor_Banner_Cd
										 ,RETAIL_CUSTOMER_UUID
										-- ,Facility_Integration_ID
										 ,Clip_Sequence_Id
										 ,FileName
										 ,DW_CREATETS
										 ,DW_LOGICAL_DELETE_IND      
										FROM ( 
											   SELECT
											   Customer_GUID
											  ,Club_Card_Nbr
											  ,Household_Id
											  ,OFFER_ID
											  ,External_Offer_Id
											  ,Program_Cd
											  ,Event_Ts
											  ,Clip_Dt
											  ,Clip_Tm
											  ,Clip_Id
											  ,Retail_Store_Id
											  ,Clip_Type_Cd
											  ,Banner_Nm
											  ,Postal_Cd
											  ,offer_type
											  ,Service_Provider_Nm
											  ,Clip_Source_Application_Id
											  ,Clip_Source_Cd
											  ,Vendor_Banner_Cd
											  ,FileName
											  ,DW_CREATETS
                                              --,Facility_Integration_ID
                                              ,RETAIL_CUSTOMER_UUID
											  ,Clip_Sequence_Id
											  ,false as  DW_LOGICAL_DELETE_IND
											  ,Row_number() OVER (
											  PARTITION BY Clip_Id, Clip_Sequence_Id
											  order by(EVENT_TS) DESC) as rn
											  FROM(
                                                    SELECT 
													  S.Customer_GUID
													  ,S.Club_Card_Nbr
													  ,S.Household_Id
													  ,OFFER_ID
													  ,External_Offer_Id
													  ,Program_Cd
													  ,Event_Ts
													  ,Clip_Dt
													  ,Clip_Tm
													  ,Clip_Id
													  ,S.Retail_Store_Id
													  ,Clip_Type_Cd
													  ,Banner_Nm
													  ,Postal_Cd
													  ,offer_type
													  ,Service_Provider_Nm
													  ,Clip_Source_Application_Id
													  ,Clip_Source_Cd
													  ,Vendor_Banner_Cd
													  ,FileName
													  ,DW_CREATETS
													  --,FACILITY_INTEGRATION_ID
													  ,RETAIL_CUSTOMER_UUID
													  ,Clip_Sequence_Id
													  FROM
													  (
													  (
													  SELECT      
													  USERID     as     Customer_GUID
													  ,CARD       as     Club_Card_Nbr
													  ,HHID       as     Household_Id
													  ,OFFERID    as     OFFER_ID
													  ,EXTOFFERID as     External_Offer_Id
													  ,PROGRAM    as     Program_Cd
													  ,coalesce(to_timestamp_ltz(EVENTTS,'YYYY-MM-DD"T"HH24:MI:SS.FF9TZHTZM'),to_timestamp(CLIPTS,'YYYY-MM-DD"T"HH24:MI:SS.FF9TZHTZM') )    as     Event_Ts
													  ,to_date(CLIPTS,'YYYY-MM-DD"T"HH24:MI:SS.FF9TZHTZM')    as     Clip_Dt
													  ,to_time(CLIPTS,'YYYY-MM-DD"T"HH24:MI:SS.FF9TZHTZM')    as     Clip_Tm
													  ,CLIPID   as Clip_Id
													  ,STOREID  as Retail_Store_Id
													  ,CLIPTYPE as Clip_Type_Cd
													  ,BANNER   as Banner_Nm
													  ,postalCd as Postal_Cd
													  ,OFFERTYPE as Offer_type
													  ,PROVIDER as Service_Provider_Nm
													  ,SRCAPPID as Clip_Source_Application_Id
													  ,clipSrc  as Clip_Source_Cd
													  ,vndrBannerCd as Vendor_Banner_Cd
													  ,FileName
													  ,DW_CREATETS
													  FROM 
													  ${src_wrk_tbl} S
													  where Offertype <> 'WS' 										 
													  )  S
													/*  LEFT JOIN 
													  ( SELECT DISTINCT fac.FACILITY_INTEGRATION_ID,fac.FACILITY_NBR
													  FROM  ${CNF_DB}.DW_C_LOCATION."FACILITY"fac
													  WHERE fac.CORPORATION_ID ='001' 
													  AND fac.FACILITY_TYPE_CD='RT'
													  AND fac.DW_CURRENT_VERSION_IND=TRUE
													  )F ON S.Retail_Store_Id = F.FACILITY_NBR */
                                                        LEFT JOIN 
													  ( SELECT DISTINCT header.Customer_GUID, header.Club_Card_Nbr,header.RETAIL_CUSTOMER_UUID, header.Retail_Store_Id, header.Clip_Sequence_Id,header.HOUSEHOLD_ID
													  FROM ${CNF_DB}.${cnf_schema}.CLIP_HEADER header
													  WHERE header.DW_CURRENT_VERSION_IND=TRUE
													  )C ON NVL(S.Customer_GUID,'-1')   = NVL(C.Customer_GUID,'-1')
													  AND NVL(S.Club_Card_Nbr,'-1')  = NVL(C.Club_Card_Nbr,'-1')
                                                      AND  NVL(TRY_TO_NUMBER(S.Retail_Store_Id),'-1') = NVL(TRY_TO_NUMBER(C.Retail_Store_Id),'-1')
													  AND NVL(S.HOUSEHOLD_ID,'-1')  = NVL(C.HOUSEHOLD_ID,'-1')
									                -- AND NVL(S.RETAIL_CUSTOMER_UUID,'-1') = NVL(C.RETAIL_CUSTOMER_UUID,'-1')
													  )
                                                )
											)  where rn=1	
									) src
									LEFT JOIN
									( 
									SELECT  DISTINCT
									Clip_Sequence_Id
									,Clip_Id
									,Event_Ts
									,Offer_Id
									,Clip_Source_Application_Id
									,Clip_Type_Cd
									,Clip_Dt
									,Clip_Tm
									,Clip_Source_Cd
									,Vendor_Banner_Cd
									,DW_LOGICAL_DELETE_IND
									,DW_First_Effective_dt
									FROM
									${tgt_tbl} tgt
									WHERE DW_CURRENT_VERSION_IND = TRUE
									)as tgt 
									ON
									nvl(src.Clip_Id,'-1') = nvl(tgt.Clip_Id,'-1')
									and  nvl(src.Event_Ts,'9999-12-31 00:00:00.000') =  nvl(tgt.Event_Ts,'9999-12-31 00:00:00.000')
									and nvl(src.Clip_Sequence_Id,'-1') =  nvl(tgt.Clip_Sequence_Id,'-1')
									WHERE  (
									tgt.Clip_Id IS  NULL
									AND tgt.Event_Ts is  NULL
									AND tgt.Clip_Sequence_Id is NULL
									 )
									OR
									(
									 NVL(src.Clip_Id,'-1') <> NVL(tgt.Clip_Id,'-1')  
									 OR  NVL(src.Event_Ts ,'9999-12-31 00:00:00.000') <> NVL(tgt.Event_Ts ,'9999-12-31 00:00:00.000')
									 OR  NVL(src.Clip_sequence_id,'-1') <> NVL(tgt.Clip_sequence_id,'-1')  
									 OR NVL(src.Offer_Id,'-1') <> NVL(tgt.Offer_Id,'-1')
									 OR NVL(src.Clip_Source_Application_Id,'-1') <> NVL(tgt.Clip_Source_Application_Id,'-1')
									 OR NVL(src.Clip_Type_Cd,'-1') <> NVL(tgt.Clip_Type_Cd,'-1')
									 OR NVL(src.Clip_Source_Cd,'-1') <>NVL(tgt.Clip_Source_Cd,'-1')
									 OR NVL(src.Vendor_Banner_Cd,'-1') <> NVL(tgt.Vendor_Banner_Cd,'-1')
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
								Clip_Sequence_Id
								,Clip_Id
								,Event_Ts
								,Offer_Id
								,Clip_Source_Application_Id
								,Clip_Type_Cd
								,Clip_Dt
								,Clip_Tm
								,Clip_Source_Cd
								,Vendor_Banner_Cd
								,filename
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 0
					) src
					WHERE 
					nvl(src.Clip_Id,'-1') = nvl(tgt.Clip_Id,'-1')
					AND nvl(src.Event_Ts,'9999-12-31 00:00:00.000') = nvl(tgt.Event_Ts,'9999-12-31 00:00:00.000')
					AND nvl(src.Clip_Sequence_Id,'-1')= nvl(tgt.Clip_Sequence_Id,'-1') 
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                   
 // SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
					SET Offer_Id = src.Offer_Id,
					Clip_Source_Application_Id = src.Clip_Source_Application_Id,
					Clip_Type_Cd = src.Clip_Type_Cd,
					Clip_Dt = src.Clip_Dt,
					Clip_Tm = src.Clip_Tm,
					Clip_Source_Cd=src.Clip_Source_Cd,
					Vendor_Banner_Cd = src.Vendor_Banner_Cd,
					DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename
					FROM ( 
							SELECT
							 Clip_Sequence_Id
							,Clip_Id
							,Event_Ts
							,Offer_Id
							,Clip_Source_Application_Id
							,Clip_Type_Cd
							,Clip_Dt
							,Clip_Tm
							,Clip_Source_Cd
							,Vendor_Banner_Cd
							,DW_LOGICAL_DELETE_IND
							,filename
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src WHERE 
							nvl(src.Clip_Id,'-1') = nvl(tgt.Clip_Id,'-1')
							AND nvl(src.Event_Ts,'9999-12-31 00:00:00.000') = nvl(tgt.Event_Ts,'9999-12-31 00:00:00.000')
							AND nvl(src.Clip_Sequence_Id,'-1')= nvl(tgt.Clip_Sequence_Id,'-1') 
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
					Clip_Sequence_Id,
					Clip_Id,
					Event_Ts,
                    DW_First_Effective_Dt,
                    DW_Last_Effective_Dt,
                    Offer_Id,
                    Clip_Source_Application_Id,
                    Clip_Type_Cd,
                    Clip_Dt,
                    Clip_Tm,
                    Clip_Source_Cd,
                    Vendor_Banner_Cd,
                    DW_CREATE_TS,
                    DW_LOGICAL_DELETE_IND,
                    DW_SOURCE_CREATE_NM,
                    DW_CURRENT_VERSION_IND  
					)
					SELECT
					Clip_Sequence_Id,
					Clip_Id,
					Event_Ts,
					CURRENT_DATE,
					'31-DEC-9999',
					Offer_Id,
					Clip_Source_Application_Id,
					Clip_Type_Cd,
					Clip_Dt,
					Clip_Tm,
					Clip_Source_Cd,
					Vendor_Banner_Cd,
					CURRENT_TIMESTAMP,
					DW_Logical_delete_ind,
					filename,
					TRUE 
					FROM ${tgt_wrk_tbl}
					WHERE 
					Sameday_chg_ind = 0   
					AND CLIP_SEQUENCE_ID IS NOT NULL
					AND CLIP_ID IS NOT NULL
					AND Event_Ts IS NOT NULL
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
                               
                // **************        Load for Clip Details table ENDs *****************

$$;
