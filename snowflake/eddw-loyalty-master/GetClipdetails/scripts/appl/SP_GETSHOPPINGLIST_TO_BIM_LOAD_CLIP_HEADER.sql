--liquibase formatted sql
--changeset SYSTEM:SP_GETSHOPPINGLIST_TO_BIM_LOAD_CLIP_HEADER runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database EDM_CONFIRMED_PRD;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE EDM_CONFIRMED_PRD.DW_APPL.SP_GETSHOPPINGLIST_TO_BIM_LOAD_CLIP_HEADER(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

		
		var src_wrk_tbl = SRC_WRK_TBL;
		var cnf_schema = 'DW_C_LOYALTY';
		var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.CLIP_HEADER_WRK`;
		var tgt_tbl = `${CNF_DB}.${cnf_schema}.CLIP_HEADER`;
    
// Deletes clips data in work table for the current transaction
	var sql_crt_src_wrk_tbl = `truncate table  ${tgt_wrk_tbl}`;

try {
       
  snowflake.execute({ sqlText: sql_crt_src_wrk_tbl });
       
    } catch (err)  {	    
        return `Deletion of Source Work table ${tgt_wrk_tbl} Failed with error: ${err}`;   
    }
 
                    
    // **************        Load for Clip Header table BEGIN *****************
    // identify  if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var create_tgt_wrk_table = `insert into ${tgt_wrk_tbl}  
								SELECT DISTINCT
								tgt.clip_Sequence_Id
								,src.Customer_GUID
								,src.Club_Card_Nbr
								,src.Household_Id
								,src.Facility_Integration_ID
								,src.Retail_Customer_UUID
								,src.Retail_Store_Id
								,src.Banner_Nm
							    ,src.Postal_Cd
								,src.DW_LOGICAL_DELETE_IND
								,src.FileName
								,CASE 
								    WHEN (
										     tgt.Customer_GUID IS NULL 
										and  tgt.Club_Card_Nbr is NULL 
										and  tgt.Facility_Integration_ID is NULL 
										and  tgt.Retail_Customer_UUID IS NULL 
								         ) 
									THEN 'I' 
									ELSE 'U' 
								END AS DML_Type
								,CASE   
									WHEN tgt.DW_First_Effective_dt = CURRENT_DATE 
									THEN 1 
									Else 0 
								END as Sameday_chg_ind
								FROM (   
										SELECT
										  Customer_GUID
										 ,Club_Card_Nbr
										 ,Household_Id
										 ,J4U_Offer_Id
										 ,External_Offer_Id
										 ,Program_Cd
										 ,Clip_Ts
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
										 ,RETAIL_CUSTOMER_UUID
										 ,Facility_Integration_ID
										 ,FileName
										 ,DW_CREATETS
										 ,DW_LOGICAL_DELETE_IND      
										FROM ( 
											   SELECT
											   Customer_GUID
											  ,Club_Card_Nbr
											  ,Household_Id
											  ,J4U_Offer_Id
											  ,External_Offer_Id
											  ,Program_Cd
											  ,Clip_Ts
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
											  ,FileName
											  ,DW_CREATETS
                                              ,Facility_Integration_ID
                                              ,RETAIL_CUSTOMER_UUID
											  ,false as  DW_LOGICAL_DELETE_IND
											  ,Row_number() OVER (
											  PARTITION BY Household_Id,Customer_GUID, Club_Card_Nbr,Retail_Store_Id
											  order by(EVENT_TS) DESC) as rn
											  FROM(
                                                     SELECT
													  Customer_GUID
													  ,Club_Card_Nbr
													  ,Household_Id
													  ,J4U_Offer_Id
													  ,External_Offer_Id
													  ,Program_Cd
													  ,Clip_Ts
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
													  ,FileName
													  ,DW_CREATETS
                                                      ,coalesce(FACILITY_INTEGRATION_ID,-1) as FACILITY_INTEGRATION_ID
                                                      ,NULL RETAIL_CUSTOMER_UUID
													  ,EVENT_TS
													FROM
													(
													 (
													 SELECT      
													  USERID     as     Customer_GUID
													 ,CARD       as     Club_Card_Nbr
													 ,HHID       as     Household_Id
													 ,OFFERID    as     J4U_Offer_Id
													 ,EXTOFFERID as     External_Offer_Id
													 ,PROGRAM    as     Program_Cd
													 ,to_timestamp_ltz(CLIPTS,'YYYY-MM-DD"T"HH24:MI:SS.FF9TZHTZM')    as     Clip_Ts
													 ,to_date(CLIPTS,'YYYY-MM-DD"T"HH24:MI:SS.FF9TZHTZM')    as     Clip_Dt
													 ,to_time(CLIPTS,'YYYY-MM-DD"T"HH24:MI:SS.FF9TZHTZM')    as     Clip_Tm
													 ,CLIPID   as Clip_Id
													 ,TRY_TO_NUMBER(STOREID)  as Retail_Store_Id
													 ,CLIPTYPE as Clip_Type_Cd
													 ,BANNER   as Banner_Nm
													 ,postalCd as Postal_Cd
													 ,OFFERTYPE as Offer_type
													 ,PROVIDER as Service_Provider_Nm
													 ,SRCAPPID as Clip_Source_Application_Id
													 ,FileName
													 ,DW_CREATETS
													 ,COALESCE(to_timestamp_ltz(EVENTTS,'YYYY-MM-DD"T"HH24:MI:SS.FF9TZHTZM'),to_timestamp_ltz(CLIPTS,'YYYY-MM-DD"T"HH24:MI:SS.FF9TZHTZM'))    as EVENT_TS
													 FROM 
                                                      ${src_wrk_tbl} S
													 where Offer_type <> 'WS'
                                                      ) S
													LEFT JOIN 
													( SELECT DISTINCT FACILITY_INTEGRATION_ID,FACILITY_NBR 
														FROM ${CNF_DB}.DW_C_LOCATION.FACILITY
														WHERE CORPORATION_ID ='001' 
														AND DW_CURRENT_VERSION_IND=TRUE
														AND FACILITY_TYPE_CD='RT'
													)C ON TRY_TO_NUMBER(S.Retail_Store_Id) = TRY_TO_NUMBER(C.FACILITY_NBR)                                                     
                                                 )
												)
											)  where rn=1	
									) src
									LEFT JOIN
									( 
									SELECT  DISTINCT clip_Sequence_Id,
									Customer_GUID
									,Club_Card_Nbr
									,Household_Id
									,Facility_Integration_ID
									,Retail_Customer_UUID
									,Postal_Cd
									,Banner_Nm
									,Retail_Store_Id
									,DW_LOGICAL_DELETE_IND
									,DW_First_Effective_dt
									FROM
									${tgt_tbl} tgt
									WHERE DW_CURRENT_VERSION_IND = TRUE
									)as tgt 
									ON
									nvl(src.Customer_GUID,'-1') = nvl(tgt.Customer_GUID,'-1')
									and  nvl(src.Club_Card_Nbr,'-1') =  nvl(tgt.Club_Card_Nbr,'-1')
									and nvl(TRY_TO_NUMBER(src.Retail_Store_Id),-1)=nvl(TRY_TO_NUMBER(tgt.Retail_Store_Id),-1)
									and nvl(src.Household_Id,'-1') =  nvl(tgt.Household_Id,'-1')
									--and nvl(src.Retail_Customer_UUID,'-1') =  nvl(tgt.Retail_Customer_UUID,'-1')
									WHERE  (
									tgt.Customer_GUID IS  NULL
									AND tgt.Club_Card_Nbr is  NULL
									AND tgt.Retail_Store_Id is NULL
									AND tgt.Household_Id is NULL
									 )
									OR
									(
									 NVL(src.Customer_GUID,'-1') <> NVL(tgt.Customer_GUID,'-1')  
									 OR NVL(src.Club_Card_Nbr,'-1') <> NVL(tgt.Club_Card_Nbr,'-1')
									 OR NVL(src.Facility_Integration_ID,'-1') <> NVL(tgt.Facility_Integration_ID,'-1')
									 --OR NVL(src.Retail_Customer_UUID,'-1') <> NVL(tgt.Retail_Customer_UUID,'-1')
									 OR NVL(src.Household_Id,'-1') <> NVL(tgt.Household_Id,'-1')
									 OR NVL(src.Postal_Cd,'-1') <>NVL(tgt.Postal_Cd,'-1')
									 OR NVL(upper(src.Banner_Nm),'-1') <> NVL(upper(tgt.Banner_Nm),'-1')
									 OR NVL(src.Retail_Store_Id,'-1') <> NVL(tgt.Retail_Store_Id,'-1')
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
							Club_Card_Nbr,
							Customer_GUID,
							Household_Id,
							Facility_Integration_ID,
							Retail_Customer_UUID,
							Retail_Store_Id,
							filename,
							Banner_Nm,
							Postal_Cd
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 0
					) src
					WHERE 
					nvl(src.Customer_GUID,'-1') = nvl(tgt.Customer_GUID,'-1')
					AND nvl(src.Club_Card_Nbr,'-1') = nvl(tgt.Club_Card_Nbr,'-1')  
					and TRY_TO_NUMBER(src.Retail_Store_Id)=TRY_TO_NUMBER(tgt.Retail_Store_Id)
                                        and TRY_TO_NUMBER(src.Household_Id)=TRY_TO_NUMBER(tgt.Household_Id)
                                        --AND nvl(src.Facility_Integration_ID,'-1')= nvl(tgt.Facility_Integration_ID,'-1') 
					--AND nvl(src.Retail_Customer_UUID,'-1') = nvl(tgt.Retail_Customer_UUID,'-1') 
					AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
                   
 // SCD Type1 - Processing Sameday updates
var sql_sameday = `UPDATE ${tgt_tbl} as tgt
					SET Customer_GUID = src.Customer_GUID,
					club_Card_Nbr = src.club_Card_Nbr,
					Household_Id = src.Household_Id,
					Facility_Integration_ID = src.Facility_Integration_ID,
					Retail_Customer_UUID = src.Retail_Customer_UUID,
					Retail_Store_Id=src.Retail_Store_Id,
					Postal_Cd = src.Postal_Cd,
					Banner_Nm = UPPER(src.Banner_Nm),
					DW_Logical_delete_ind = src.DW_Logical_delete_ind,
					DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
					DW_SOURCE_UPDATE_NM = filename
					FROM ( 
							SELECT
							Customer_GUID,
							Club_Card_Nbr,
							Household_Id,
							Facility_Integration_ID,
							Retail_Customer_UUID,
							Retail_Store_Id,
							Postal_Cd,
							Banner_Nm,
							DW_Logical_delete_ind,
							filename
							FROM ${tgt_wrk_tbl}
							WHERE 
							DML_Type = 'U' 
							AND Sameday_chg_ind = 1
						) src WHERE 
						nvl(src.Customer_GUID,'-1') = nvl(tgt.Customer_GUID,'-1')
						AND nvl(src.Club_Card_Nbr,'-1') = nvl(tgt.Club_Card_Nbr,'-1')  
						and TRY_TO_NUMBER(src.Retail_Store_Id)=TRY_TO_NUMBER(tgt.Retail_Store_Id)
                                                and TRY_TO_NUMBER(src.Household_Id)=TRY_TO_NUMBER(tgt.Household_Id)						
                                                --AND nvl(src.Facility_Integration_ID,'-1') = nvl(tgt.Facility_Integration_ID,'-1') 
						--AND nvl(src.Retail_Customer_UUID,'-1')= nvl(tgt.Retail_Customer_UUID,'-1') 
						AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl} 
					(
					clip_Sequence_Id,
                    DW_First_Effective_Dt,
                    DW_Last_Effective_Dt,
                    Customer_GUID,
                    Club_Card_Nbr,
                    Household_Id,
                    Facility_Integration_ID,
                    Retail_Customer_UUID,
                    Retail_Store_Id,
                    Postal_Cd,
                    Banner_Nm,
                    DW_CREATE_TS,
                    DW_LOGICAL_DELETE_IND,
                    DW_SOURCE_CREATE_NM,
                    DW_CURRENT_VERSION_IND  
					)
					SELECT
					coalesce(Clip_Sequence_Id,(SELECT nvl(MAX(Clip_Sequence_Id),0) FROM ${tgt_tbl}) +
					ROW_NUMBER() OVER (ORDER BY Customer_GUID,Club_Card_Nbr,Facility_Integration_ID,Retail_Customer_UUID ASC)) AS Clip_Sequence_Id,
					CURRENT_DATE,
					'31-DEC-9999',
					Customer_GUID,
					Club_Card_Nbr,
					Household_Id,
					Facility_Integration_ID,
					Retail_Customer_UUID,
					Retail_Store_Id,
					Postal_Cd,
					Upper(Banner_Nm) as Banner_Nm ,
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
                               
                // **************        Load for Clip Header table ENDs *****************

$$;
