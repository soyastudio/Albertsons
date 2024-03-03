--liquibase formatted sql
--changeset SYSTEM:SP_GetBusinessPartner_TO_BIM_LOAD_Business_Partner_Profile runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETBUSINESSPARTNER_TO_BIM_LOAD_BUSINESS_PARTNER_PROFILE(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

   
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_LOYAL;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Profile_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Business_Partner_Profile`;
var tgt_exp_tbl = `${cnf_db}.${wrk_schema}.Business_Partner_Profile_Exceptions`;

var tbl_lkp = `${cnf_db}.${cnf_schema}.Business_Partner`;

// ************** Load for Business_Partner_Profile table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.


var sql_command = `CREATE OR REPLACE TABLE ${tgt_wrk_tbl} as
					            WITH src_wrk_tbl_recs as
							    (
								SELECT 
								 Partner_Nm  ,
								 Partner_Type_Cd         ,
								 Partner_Type_Dsc        ,
								 Partner_Type_Short_Dsc    ,
								 Partner_Address_Usage_Type_Cd    ,
								 Partner_Address_Line1_Txt    ,
								 Partner_Address_Line2_Txt    ,
								 Partner_Address_Line3_Txt    ,
								 Partner_Address_Line4_Txt    ,
								 Partner_Address_Line5_Txt    ,
								 Partner_Contact_City_Nm    ,
								 Partner_Contact_County_Nm    ,
								 Partner_Contact_County_Cd    ,
								 Partner_Contact_Postal_Zone_Cd    ,
								 Partner_Contact_State_Cd    ,
								 Partner_Contact_State_Nm    ,
								 Partner_Contact_Country_Cd    ,
								 Partner_Contact_Country_Nm    ,
								 Partner_Contact_Latitude_Dgr    ,
								 Partner_Contact_Longitude_Dgr    ,
								 Partner_Contact_TimeZone_Cd    ,
								 Partner_Contact_Phone1_Nbr,
								 Partner_Contact_Phone2_Nbr,
								 Partner_Contact_Phone3_Nbr,																	
								 Partner_Contact_Fax_Nbr    ,
								 Partner_Status_Type_Cd    ,
								 Partner_Status_Dsc      ,
								 Partner_Status_Effective_Ts    ,
								 Service_Level_Cd        ,
								 Service_Level_Dsc       ,
								 Service_Level_Short_Dsc    ,
								 Service_Level_Activity_Cd    ,
								 Service_Level_Activity_Dsc    ,
								 Service_Level_Activity_Short_Dsc    ,
								 Business_Contract_Id    ,
								 Business_Contract_Nm    ,
								 Business_Contract_Dsc    ,
								 Business_Contract_Start_Dt  ,
								 Business_Contract_End_Dt  ,
								 Contract_By_User_Id     ,
								 Contract_By_First_Nm    ,
								 Contract_By_Last_Nm     ,
								 Reason_Cd               ,
								 Reason_Dsc              ,
								 Reason_Short_Dsc        ,
								 Contract_By_Create_Ts    ,
								 Contract_Threshold_Order_Limit_Cnt    ,
								 Contract_Threshold_Maximum_Item_Cnt    ,
								 Contract_Threshold_Minimum_Item_Cnt    ,
								 Contract_Threshold_Minimum_Tote_Cnt    ,
								 Contract_Threshold_Maximum_Tote_Cnt    ,
								 Contract_Threshold_Order_Allocation_Pct    ,
								 Contract_Threshold_Mileage_Nbr    ,
								 Partner_Profile_Effective_Time_Period_Type_Cd    ,
								 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
								 Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,   
								 Partner_Id,
								 Partner_Site_Id,
								 Partner_Participant_Id,
								 creationdt,
								 actiontypecd,
								 FileName,  
								Row_number() OVER ( partition BY Partner_Nm,Partner_Id,Partner_Site_Id,Partner_Participant_Id ORDER BY To_timestamp_ntz(creationdt) DESC) AS rn1																									
																			
								FROM
								(
								SELECT
								 Partner_Nm  ,
								 Partner_Type_Cd         ,
								 Partner_Type_Dsc        ,
								 Partner_Type_Short_Dsc    ,
								 Partner_Address_Usage_Type_Cd    ,
								 Partner_Address_Line1_Txt    ,
								 Partner_Address_Line2_Txt    ,
								 Partner_Address_Line3_Txt    ,
								 Partner_Address_Line4_Txt    ,
								 Partner_Address_Line5_Txt    ,
								 Partner_Contact_City_Nm    ,
								 Partner_Contact_County_Nm    ,
								 Partner_Contact_County_Cd    ,
								 Partner_Contact_Postal_Zone_Cd    ,
								 Partner_Contact_State_Cd    ,
								 Partner_Contact_State_Nm    ,
								 Partner_Contact_Country_Cd    ,
								 Partner_Contact_Country_Nm    ,
								 Partner_Contact_Latitude_Dgr    ,
								 Partner_Contact_Longitude_Dgr    ,
								 Partner_Contact_TimeZone_Cd    ,
								 MAX(Partner_Contact_Phone1_Nbr) AS Partner_Contact_Phone1_Nbr,
								 MAX(Partner_Contact_Phone2_Nbr) AS Partner_Contact_Phone2_Nbr,
								 MAX(Partner_Contact_Phone3_Nbr) AS Partner_Contact_Phone3_Nbr,																	
								 Partner_Contact_Fax_Nbr    ,
								 Partner_Status_Type_Cd    ,
								 Partner_Status_Dsc      ,
								 Partner_Status_Effective_Ts    ,
								 Service_Level_Cd        ,
								 Service_Level_Dsc       ,
								 Service_Level_Short_Dsc    ,
								 Service_Level_Activity_Cd    ,
								 Service_Level_Activity_Dsc    ,
								 Service_Level_Activity_Short_Dsc    ,
								 Business_Contract_Id    ,
								 Business_Contract_Nm    ,
								 Business_Contract_Dsc    ,
								 Business_Contract_Start_Dt  ,
								 Business_Contract_End_Dt  ,
								 Contract_By_User_Id     ,
								 Contract_By_First_Nm    ,
								 Contract_By_Last_Nm     ,
								 Reason_Cd               ,
								 Reason_Dsc              ,
								 Reason_Short_Dsc        ,
								 Contract_By_Create_Ts    ,
								 Contract_Threshold_Order_Limit_Cnt    ,
								 Contract_Threshold_Maximum_Item_Cnt    ,
								 Contract_Threshold_Minimum_Item_Cnt    ,
								 Contract_Threshold_Minimum_Tote_Cnt    ,
								 Contract_Threshold_Maximum_Tote_Cnt    ,
								 Contract_Threshold_Order_Allocation_Pct    ,
								 Contract_Threshold_Mileage_Nbr    ,
								 Partner_Profile_Effective_Time_Period_Type_Cd    ,
								 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
								 Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,   
								 Partner_Id,
								 Partner_Site_Id,
								 Partner_Participant_Id,							
								 creationdt,
								 actiontypecd,
								 FileName
								FROM
								(
								SELECT 
								 Partner_Nm  ,
								 Partner_Type_Cd         ,
								 Partner_Type_Dsc        ,
								 Partner_Type_Short_Dsc    ,
								 Partner_Address_Usage_Type_Cd    ,
								 Partner_Address_Line1_Txt    ,
								 Partner_Address_Line2_Txt    ,
								 Partner_Address_Line3_Txt    ,
								 Partner_Address_Line4_Txt    ,
								 Partner_Address_Line5_Txt    ,
								 Partner_Contact_City_Nm    ,
								 Partner_Contact_County_Nm    ,
								 Partner_Contact_County_Cd    ,
								 Partner_Contact_Postal_Zone_Cd    ,
								 Partner_Contact_State_Cd    ,
								 Partner_Contact_State_Nm    ,
								 Partner_Contact_Country_Cd    ,
								 Partner_Contact_Country_Nm    ,
								 Partner_Contact_Latitude_Dgr    ,
								 Partner_Contact_Longitude_Dgr    ,
								 Partner_Contact_TimeZone_Cd    ,
								 MAX(CASE WHEN RN = 1 THEN Address_PhoneNbr ELSE NULL END ) AS Partner_Contact_Phone1_Nbr,
								 MAX(CASE WHEN RN = 2 THEN Address_PhoneNbr ELSE NULL END ) AS Partner_Contact_Phone2_Nbr,
								 MAX(CASE WHEN RN = 3 THEN Address_PhoneNbr ELSE NULL END ) AS Partner_Contact_Phone3_Nbr,
								 Partner_Contact_Fax_Nbr    ,
								 Partner_Status_Type_Cd    ,
								 Partner_Status_Dsc      ,
								 Partner_Status_Effective_Ts    ,
								 Service_Level_Cd        ,
								 Service_Level_Dsc       ,
								 Service_Level_Short_Dsc    ,
								 Service_Level_Activity_Cd    ,
								 Service_Level_Activity_Dsc    ,
								 Service_Level_Activity_Short_Dsc    ,
								 Business_Contract_Id    ,
								 Business_Contract_Nm    ,
								 Business_Contract_Dsc    ,
								 Business_Contract_Start_Dt  ,
								 Business_Contract_End_Dt  ,
								 Contract_By_User_Id     ,
								 Contract_By_First_Nm    ,
								 Contract_By_Last_Nm     ,
								 Reason_Cd               ,
								 Reason_Dsc              ,
								 Reason_Short_Dsc        ,
								 Contract_By_Create_Ts    ,
								 Contract_Threshold_Order_Limit_Cnt    ,
								 Contract_Threshold_Maximum_Item_Cnt    ,
								 Contract_Threshold_Minimum_Item_Cnt    ,
								 Contract_Threshold_Minimum_Tote_Cnt    ,
								 Contract_Threshold_Maximum_Tote_Cnt    ,
								 Contract_Threshold_Order_Allocation_Pct    ,
								 Contract_Threshold_Mileage_Nbr    ,
								 Partner_Profile_Effective_Time_Period_Type_Cd    ,
								 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
								 Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,   
								 Partner_Id,
								 Partner_Site_Id,
								 Partner_Participant_Id,
								 creationdt,
								 actiontypecd,
								 FileName  
								From (
								SELECT 
								S.Partner_Nm  ,
								S.Partner_Type_Cd         ,
								S.Partner_Type_Dsc        ,
								S.Partner_Type_Short_Dsc    ,
								S.Partner_Address_Usage_Type_Cd    ,
								S.Partner_Address_Line1_Txt    ,
								S.Partner_Address_Line2_Txt    ,
								S.Partner_Address_Line3_Txt    ,
								S.Partner_Address_Line4_Txt    ,
								S.Partner_Address_Line5_Txt    ,
								S.Partner_Contact_City_Nm    ,
								S.Partner_Contact_County_Nm    ,
								S.Partner_Contact_County_Cd    ,
								S.Partner_Contact_Postal_Zone_Cd    ,
								S.Partner_Contact_State_Cd    ,
								S.Partner_Contact_State_Nm    ,
								S.Partner_Contact_Country_Cd    ,
								S.Partner_Contact_Country_Nm    ,
								S.Partner_Contact_Latitude_Dgr    ,
								S.Partner_Contact_Longitude_Dgr    ,
								S.Partner_Contact_TimeZone_Cd    ,
								S.Address_PhoneNbr,
								S.Partner_Contact_Fax_Nbr    ,
								S.Partner_Status_Type_Cd    ,
								S.Partner_Status_Dsc      ,
								S.Partner_Status_Effective_Ts    ,
								S.Service_Level_Cd        ,
								S.Service_Level_Dsc       ,
								S.Service_Level_Short_Dsc    ,
								S.Service_Level_Activity_Cd    ,
								S.Service_Level_Activity_Dsc    ,
								S.Service_Level_Activity_Short_Dsc    ,
								S.Business_Contract_Id    ,
								S.Business_Contract_Nm    ,
								S.Business_Contract_Dsc    ,
								S.Business_Contract_Start_Dt  ,
								S.Business_Contract_End_Dt  ,
								S.Contract_By_User_Id     ,
								S.Contract_By_First_Nm    ,
								S.Contract_By_Last_Nm     ,
								S.Reason_Cd               ,
								S.Reason_Dsc              ,
								S.Reason_Short_Dsc        ,
								S.Contract_By_Create_Ts    ,
								S.Contract_Threshold_Order_Limit_Cnt    ,
								S.Contract_Threshold_Maximum_Item_Cnt    ,
								S.Contract_Threshold_Minimum_Item_Cnt    ,
								S.Contract_Threshold_Minimum_Tote_Cnt    ,
								S.Contract_Threshold_Maximum_Tote_Cnt    ,
								S.Contract_Threshold_Order_Allocation_Pct    ,
								S.Contract_Threshold_Mileage_Nbr    ,
								S.Partner_Profile_Effective_Time_Period_Type_Cd    ,
								S.Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
								S.Partner_Profile_Effective_Time_Period_Last_Effective_Ts,
								S.Partner_Id,
								S.Partner_Site_Id,
								S.Partner_Participant_Id,
								S.creationdt,
								S.actiontypecd,
								S.FileName,
								Row_number() OVER ( partition BY
													 Partner_Nm  ,
													 Partner_Type_Cd         ,
													 Partner_Type_Dsc        ,
													 Partner_Type_Short_Dsc    ,
													 Partner_Address_Usage_Type_Cd    ,
													 Partner_Address_Line1_Txt    ,
													 Partner_Address_Line2_Txt    ,
													 Partner_Address_Line3_Txt    ,
													 Partner_Address_Line4_Txt    ,
													 Partner_Address_Line5_Txt    ,
													 Partner_Contact_City_Nm    ,
													 Partner_Contact_County_Nm    ,
													 Partner_Contact_County_Cd    ,
													 Partner_Contact_Postal_Zone_Cd    ,
													 Partner_Contact_State_Cd    ,
													 Partner_Contact_State_Nm    ,
													 Partner_Contact_Country_Cd    ,
													 Partner_Contact_Country_Nm    ,
													 Partner_Contact_Latitude_Dgr    ,
													 Partner_Contact_Longitude_Dgr    ,
													 Partner_Contact_TimeZone_Cd    ,
													 Partner_Contact_Fax_Nbr    ,
													 Partner_Status_Type_Cd    ,
													 Partner_Status_Dsc      ,
													 Partner_Status_Effective_Ts    ,
													 Service_Level_Cd        ,
													 Service_Level_Dsc       ,
													 Service_Level_Short_Dsc    ,
													 Service_Level_Activity_Cd    ,
													 Service_Level_Activity_Dsc    ,
													 Service_Level_Activity_Short_Dsc    ,
													 Business_Contract_Id    ,
													 Business_Contract_Nm    ,
													 Business_Contract_Dsc    ,
													 Business_Contract_Start_Dt  ,
													 Business_Contract_End_Dt  ,
													 Contract_By_User_Id     ,
													 Contract_By_First_Nm    ,
													 Contract_By_Last_Nm     ,
													 Reason_Cd               ,
													 Reason_Dsc              ,
													 Reason_Short_Dsc        ,
													 Contract_By_Create_Ts    ,
													 Contract_Threshold_Order_Limit_Cnt    ,
													 Contract_Threshold_Maximum_Item_Cnt    ,
													 Contract_Threshold_Minimum_Item_Cnt    ,
													 Contract_Threshold_Minimum_Tote_Cnt    ,
													 Contract_Threshold_Maximum_Tote_Cnt    ,
													 Contract_Threshold_Order_Allocation_Pct    ,
													 Contract_Threshold_Mileage_Nbr    ,
													 Partner_Profile_Effective_Time_Period_Type_Cd    ,
													 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
													 Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,   
													 Partner_Id,
													 Partner_Site_Id,
													 Partner_Participant_Id
													ORDER BY Address_PhoneNbr
													desc ) AS RN
											    from	
												(SELECT DISTINCT 
												PartnerProfile_PartnerNm as Partner_Nm  ,
												PartnerTypeCd_Code as Partner_Type_Cd         ,
												PartnerTypeCd_Description as Partner_Type_Dsc        ,
												PartnerTypeCd_ShortDescription as Partner_Type_Short_Dsc    ,
												Address_AddressUsageTypeCd as Partner_Address_Usage_Type_Cd    ,
												Address_AddressLine1txt as Partner_Address_Line1_Txt    ,
												Address_AddressLine2txt as Partner_Address_Line2_Txt    ,
												Address_AddressLine3txt as Partner_Address_Line3_Txt    ,
												Address_AddressLine4txt as Partner_Address_Line4_Txt    ,
												Address_AddressLine5txt as Partner_Address_Line5_Txt    ,
												Address_CityNm as Partner_Contact_City_Nm    ,
												Address_CountyNm as Partner_Contact_County_Nm    ,
												Address_CountyCd as Partner_Contact_County_Cd    ,
												Address_PostalZoneCd as Partner_Contact_Postal_Zone_Cd    ,
												Address_StateCd as Partner_Contact_State_Cd    ,
												Address_StateNm as Partner_Contact_State_Nm    ,
												Address_CountryCd as Partner_Contact_Country_Cd    ,
												Address_CountryNm as Partner_Contact_Country_Nm    ,
												Address_LatitudeDegree as Partner_Contact_Latitude_Dgr    ,
												Address_LongitudeDegree as Partner_Contact_Longitude_Dgr    ,
												Address_TimeZoneCd as Partner_Contact_TimeZone_Cd    ,
												Address_PhoneNbr ,
												Address_FaxNbr as Partner_Contact_Fax_Nbr    ,
												Status_StatusTypeCd_Type as Partner_Status_Type_Cd    ,
												Status_Description as Partner_Status_Dsc      ,
												Status_EffectiveDtTm as Partner_Status_Effective_Ts    ,
												ServiceLevelType_Code as Service_Level_Cd        ,
												ServiceLevelType_Description as Service_Level_Dsc       ,
												ServiceLevelType_ShortDescription as Service_Level_Short_Dsc    ,
												ActivityType_Code as Service_Level_Activity_Cd    ,
												ActivityType_Description as Service_Level_Activity_Dsc    ,
												ActivityType_ShortDescription as Service_Level_Activity_Short_Dsc    ,
												ContractId as Business_Contract_Id    ,
												ContractNm as Business_Contract_Nm    ,
												ContractDsc as Business_Contract_Dsc    ,
												ContractStartDt as Business_Contract_Start_Dt  ,
												ContractEndDt as Business_Contract_End_Dt  ,
												UserId as Contract_By_User_Id     ,
												FirstNm as Contract_By_First_Nm    ,
												LastNm as Contract_By_Last_Nm     ,
												ReasonType_Code as Reason_Cd               ,
												ReasonType_Description as Reason_Dsc              ,
												ReasonType_ShortDescription as Reason_Short_Dsc        ,
												ContractByType_CreateDtTm as Contract_By_Create_Ts    ,
												OrderLimitCnt as Contract_Threshold_Order_Limit_Cnt    ,
												MaximumItemCnt as Contract_Threshold_Maximum_Item_Cnt    ,
												MinimumItemCnt as Contract_Threshold_Minimum_Item_Cnt    ,
												MinimumToteCnt as Contract_Threshold_Minimum_Tote_Cnt    ,
												MaximumToteCnt as Contract_Threshold_Maximum_Tote_Cnt    ,
												OrderAllocationPct as Contract_Threshold_Order_Allocation_Pct    ,
												MileageNbr as Contract_Threshold_Mileage_Nbr    ,
												PartnerProfileEffectiveTimePeriod_typeCode as Partner_Profile_Effective_Time_Period_Type_Cd    ,
												TIMESTAMP_NTZ_FROM_PARTS(try_to_date(PartnerProfileEffectiveTimePeriod_FirstEffectiveDt),try_to_time(PartnerProfileEffectiveTimePeriod_FirstEffectiveTm)) AS Partner_Profile_Effective_Time_Period_First_Effective_Ts,
												TIMESTAMP_NTZ_FROM_PARTS(try_to_date(PartnerProfileEffectiveTimePeriod_LastEffectiveDt),try_to_time(PartnerProfileEffectiveTimePeriod_LastEffectiveTm)) AS Partner_Profile_Effective_Time_Period_Last_Effective_Ts,
												BusinessPartnerData_PartnerId as Partner_Id, 
												PartnerSiteId as  Partner_Site_Id,
												PartnerParticipantId as  Partner_Participant_Id,							
												creationdt,
												actiontypecd,
												FileName
												FROM ${src_wrk_tbl}
												where PartnerProfile_PartnerNm IS NOT NULL
												AND (BusinessPartnerData_PartnerId is not null or PartnerSiteId is not null or  PartnerParticipantId is not null)
																			 
												) S
										) 
									  GROUP BY   Partner_Nm  ,
												 Partner_Type_Cd         ,
												 Partner_Type_Dsc        ,
												 Partner_Type_Short_Dsc    ,
												 Partner_Address_Usage_Type_Cd    ,
												 Partner_Address_Line1_Txt    ,
												 Partner_Address_Line2_Txt    ,
												 Partner_Address_Line3_Txt    ,
												 Partner_Address_Line4_Txt    ,
												 Partner_Address_Line5_Txt    ,
												 Partner_Contact_City_Nm    ,
												 Partner_Contact_County_Nm    ,
												 Partner_Contact_County_Cd    ,
												 Partner_Contact_Postal_Zone_Cd    ,
												 Partner_Contact_State_Cd    ,
												 Partner_Contact_State_Nm    ,
												 Partner_Contact_Country_Cd    ,
												 Partner_Contact_Country_Nm    ,
												 Partner_Contact_Latitude_Dgr    ,
												 Partner_Contact_Longitude_Dgr    ,
												 Partner_Contact_TimeZone_Cd    ,
												 Address_PhoneNbr,
												 Partner_Contact_Fax_Nbr    ,
												 Partner_Status_Type_Cd    ,
												 Partner_Status_Dsc      ,
												 Partner_Status_Effective_Ts    ,
												 Service_Level_Cd        ,
												 Service_Level_Dsc       ,
												 Service_Level_Short_Dsc    ,
												 Service_Level_Activity_Cd    ,
												 Service_Level_Activity_Dsc    ,
												 Service_Level_Activity_Short_Dsc    ,
												 Business_Contract_Id    ,
												 Business_Contract_Nm    ,
												 Business_Contract_Dsc    ,
												 Business_Contract_Start_Dt  ,
												 Business_Contract_End_Dt  ,
												 Contract_By_User_Id     ,
												 Contract_By_First_Nm    ,
												 Contract_By_Last_Nm     ,
												 Reason_Cd               ,
												 Reason_Dsc              ,
												 Reason_Short_Dsc        ,
												 Contract_By_Create_Ts    ,
												 Contract_Threshold_Order_Limit_Cnt    ,
												 Contract_Threshold_Maximum_Item_Cnt    ,
												 Contract_Threshold_Minimum_Item_Cnt    ,
												 Contract_Threshold_Minimum_Tote_Cnt    ,
												 Contract_Threshold_Maximum_Tote_Cnt    ,
												 Contract_Threshold_Order_Allocation_Pct    ,
												 Contract_Threshold_Mileage_Nbr    ,
												 Partner_Profile_Effective_Time_Period_Type_Cd    ,
												 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
												 Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,   
												 Partner_Id,
												 Partner_Site_Id,
												 Partner_Participant_Id,
												creationdt,												
												actiontypecd,
												FileName
										--)
										)
										GROUP BY
												 Partner_Nm  ,
												 Partner_Type_Cd         ,
												 Partner_Type_Dsc        ,
												 Partner_Type_Short_Dsc    ,
												 Partner_Address_Usage_Type_Cd    ,
												 Partner_Address_Line1_Txt    ,
												 Partner_Address_Line2_Txt    ,
												 Partner_Address_Line3_Txt    ,
												 Partner_Address_Line4_Txt    ,
												 Partner_Address_Line5_Txt    ,
												 Partner_Contact_City_Nm    ,
												 Partner_Contact_County_Nm    ,
												 Partner_Contact_County_Cd    ,
												 Partner_Contact_Postal_Zone_Cd    ,
												 Partner_Contact_State_Cd    ,
												 Partner_Contact_State_Nm    ,
												 Partner_Contact_Country_Cd    ,
												 Partner_Contact_Country_Nm    ,
												 Partner_Contact_Latitude_Dgr    ,
												 Partner_Contact_Longitude_Dgr    ,
												 Partner_Contact_TimeZone_Cd    ,
												 Partner_Contact_Fax_Nbr    ,
												 Partner_Status_Type_Cd    ,
												 Partner_Status_Dsc      ,
												 Partner_Status_Effective_Ts    ,
												 Service_Level_Cd        ,
												 Service_Level_Dsc       ,
												 Service_Level_Short_Dsc    ,
												 Service_Level_Activity_Cd    ,
												 Service_Level_Activity_Dsc    ,
												 Service_Level_Activity_Short_Dsc    ,
												 Business_Contract_Id    ,
												 Business_Contract_Nm    ,
												 Business_Contract_Dsc    ,
												 Business_Contract_Start_Dt  ,
												 Business_Contract_End_Dt  ,
												 Contract_By_User_Id     ,
												 Contract_By_First_Nm    ,
												 Contract_By_Last_Nm     ,
												 Reason_Cd               ,
												 Reason_Dsc              ,
												 Reason_Short_Dsc        ,
												 Contract_By_Create_Ts    ,
												 Contract_Threshold_Order_Limit_Cnt    ,
												 Contract_Threshold_Maximum_Item_Cnt    ,
												 Contract_Threshold_Minimum_Item_Cnt    ,
												 Contract_Threshold_Minimum_Tote_Cnt    ,
												 Contract_Threshold_Maximum_Tote_Cnt    ,
												 Contract_Threshold_Order_Allocation_Pct    ,
												 Contract_Threshold_Mileage_Nbr    ,
												 Partner_Profile_Effective_Time_Period_Type_Cd    ,
												 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
												 Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,   
												 Partner_Id,
												 Partner_Site_Id,
												 Partner_Participant_Id,
												 creationdt,
												 actiontypecd,
												 FileName  
												)
											)
											select 
											 src.Business_Partner_Integration_Id,
											 src.Partner_Nm  ,
											 src.Partner_Type_Cd         ,
											 src.Partner_Type_Dsc        ,
											 src.Partner_Type_Short_Dsc    ,
											 src.Partner_Address_Usage_Type_Cd    ,
											 src.Partner_Address_Line1_Txt    ,
											 src.Partner_Address_Line2_Txt    ,
											 src.Partner_Address_Line3_Txt    ,
											 src.Partner_Address_Line4_Txt    ,
											 src.Partner_Address_Line5_Txt    ,
											 src.Partner_Contact_City_Nm    ,
											 src.Partner_Contact_County_Nm    ,
											 src.Partner_Contact_County_Cd    ,
											 src.Partner_Contact_Postal_Zone_Cd    ,
											 src.Partner_Contact_State_Cd    ,
											 src.Partner_Contact_State_Nm    ,
											 src.Partner_Contact_Country_Cd    ,
											 src.Partner_Contact_Country_Nm    ,
											 src.Partner_Contact_Latitude_Dgr    ,
											 src.Partner_Contact_Longitude_Dgr    ,
											 src.Partner_Contact_TimeZone_Cd    ,
											 src.Partner_Contact_Phone1_Nbr,
											 src.Partner_Contact_Phone2_Nbr,
											 src.Partner_Contact_Phone3_Nbr,																	
											 src.Partner_Contact_Fax_Nbr    ,
											 src.Partner_Status_Type_Cd    ,
											 src.Partner_Status_Dsc      ,
											 src.Partner_Status_Effective_Ts    ,
											 src.Service_Level_Cd        ,
											 src.Service_Level_Dsc       ,
											 src.Service_Level_Short_Dsc    ,
											 src.Service_Level_Activity_Cd    ,
											 src.Service_Level_Activity_Dsc    ,
											 src.Service_Level_Activity_Short_Dsc    ,
											 src.Business_Contract_Id    ,
											 src.Business_Contract_Nm    ,
											 src.Business_Contract_Dsc    ,
											 src.Business_Contract_Start_Dt  ,
											 src.Business_Contract_End_Dt  ,
											 src.Contract_By_User_Id     ,
											 src.Contract_By_First_Nm    ,
											 src.Contract_By_Last_Nm     ,
											 src.Reason_Cd               ,
											 src.Reason_Dsc              ,
											 src.Reason_Short_Dsc        ,
											 src.Contract_By_Create_Ts    ,
											 src.Contract_Threshold_Order_Limit_Cnt    ,
											 src.Contract_Threshold_Maximum_Item_Cnt    ,
											 src.Contract_Threshold_Minimum_Item_Cnt    ,
											 src.Contract_Threshold_Minimum_Tote_Cnt    ,
											 src.Contract_Threshold_Maximum_Tote_Cnt    ,
											 src.Contract_Threshold_Order_Allocation_Pct    ,
											 src.Contract_Threshold_Mileage_Nbr    ,
											 src.Partner_Profile_Effective_Time_Period_Type_Cd    ,
											 src.Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
											 src.Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,
											 src.Partner_Id,
											 src.Partner_Site_Id,
											 src.Partner_Participant_Id,
											 src.dw_logical_delete_ind,
											 src.creationdt,
											 src.FileName,
											 CASE WHEN (tgt.Business_Partner_Integration_Id IS NULL AND tgt.Partner_Nm IS NULL) THEN 'I' ELSE 'U' END AS DML_Type,
											 CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind,
											 src.ActionTypeCd
											from 
											(SELECT 
											 B.Business_Partner_Integration_Id as Business_Partner_Integration_Id,
											 src1.Partner_Nm  ,
											 src1.Partner_Type_Cd         ,
											 src1.Partner_Type_Dsc        ,
											 src1.Partner_Type_Short_Dsc    ,
											 src1.Partner_Address_Usage_Type_Cd    ,
											 src1.Partner_Address_Line1_Txt    ,
											 src1.Partner_Address_Line2_Txt    ,
											 src1.Partner_Address_Line3_Txt    ,
											 src1.Partner_Address_Line4_Txt    ,
											 src1.Partner_Address_Line5_Txt    ,
											 src1.Partner_Contact_City_Nm    ,
											 src1.Partner_Contact_County_Nm    ,
											 src1.Partner_Contact_County_Cd    ,
											 src1.Partner_Contact_Postal_Zone_Cd    ,
											 src1.Partner_Contact_State_Cd    ,
											 src1.Partner_Contact_State_Nm    ,
											 src1.Partner_Contact_Country_Cd    ,
											 src1.Partner_Contact_Country_Nm    ,
											 src1.Partner_Contact_Latitude_Dgr    ,
											 src1.Partner_Contact_Longitude_Dgr    ,
											 src1.Partner_Contact_TimeZone_Cd    ,
											 src1.Partner_Contact_Phone1_Nbr,
											 src1.Partner_Contact_Phone2_Nbr,
											 src1.Partner_Contact_Phone3_Nbr,																	
											 src1.Partner_Contact_Fax_Nbr    ,
											 src1.Partner_Status_Type_Cd    ,
											 src1.Partner_Status_Dsc      ,
											 src1.Partner_Status_Effective_Ts    ,
											 src1.Service_Level_Cd        ,
											 src1.Service_Level_Dsc       ,
											 src1.Service_Level_Short_Dsc    ,
											 src1.Service_Level_Activity_Cd    ,
											 src1.Service_Level_Activity_Dsc    ,
											 src1.Service_Level_Activity_Short_Dsc    ,
											 src1.Business_Contract_Id    ,
											 src1.Business_Contract_Nm    ,
											 src1.Business_Contract_Dsc    ,
											 src1.Business_Contract_Start_Dt  ,
											 src1.Business_Contract_End_Dt  ,
											 src1.Contract_By_User_Id     ,
											 src1.Contract_By_First_Nm    ,
											 src1.Contract_By_Last_Nm     ,
											 src1.Reason_Cd               ,
											 src1.Reason_Dsc              ,
											 src1.Reason_Short_Dsc        ,
											 src1.Contract_By_Create_Ts    ,
											 src1.Contract_Threshold_Order_Limit_Cnt    ,
											 src1.Contract_Threshold_Maximum_Item_Cnt    ,
											 src1.Contract_Threshold_Minimum_Item_Cnt    ,
											 src1.Contract_Threshold_Minimum_Tote_Cnt    ,
											 src1.Contract_Threshold_Maximum_Tote_Cnt    ,
											 src1.Contract_Threshold_Order_Allocation_Pct    ,
											 src1.Contract_Threshold_Mileage_Nbr    ,
											 src1.Partner_Profile_Effective_Time_Period_Type_Cd    ,
											 src1.Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
											 src1.Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,
											 src1.Partner_Id,
											 src1.Partner_Site_Id,
											 src1.Partner_Participant_Id,												 
											 src1.dw_logical_delete_ind,
											 src1.creationdt,
											 src1.FileName,
											 src1.ACTIONTYPECD
											FROM 												
											(SELECT
											 Partner_Nm  ,
											 Partner_Type_Cd         ,
											 Partner_Type_Dsc        ,
											 Partner_Type_Short_Dsc    ,
											 Partner_Address_Usage_Type_Cd    ,
											 Partner_Address_Line1_Txt    ,
											 Partner_Address_Line2_Txt    ,
											 Partner_Address_Line3_Txt    ,
											 Partner_Address_Line4_Txt    ,
											 Partner_Address_Line5_Txt    ,
											 Partner_Contact_City_Nm    ,
											 Partner_Contact_County_Nm    ,
											 Partner_Contact_County_Cd    ,
											 Partner_Contact_Postal_Zone_Cd    ,
											 Partner_Contact_State_Cd    ,
											 Partner_Contact_State_Nm    ,
											 Partner_Contact_Country_Cd    ,
											 Partner_Contact_Country_Nm    ,
											 Partner_Contact_Latitude_Dgr    ,
											 Partner_Contact_Longitude_Dgr    ,
											 Partner_Contact_TimeZone_Cd    ,
											 Partner_Contact_Phone1_Nbr,
											 Partner_Contact_Phone2_Nbr,
											 Partner_Contact_Phone3_Nbr,																	
											 Partner_Contact_Fax_Nbr    ,
											 Partner_Status_Type_Cd    ,
											 Partner_Status_Dsc      ,
											 Partner_Status_Effective_Ts    ,
											 Service_Level_Cd        ,
											 Service_Level_Dsc       ,
											 Service_Level_Short_Dsc    ,
											 Service_Level_Activity_Cd    ,
											 Service_Level_Activity_Dsc    ,
											 Service_Level_Activity_Short_Dsc    ,
											 Business_Contract_Id    ,
											 Business_Contract_Nm    ,
											 Business_Contract_Dsc    ,
											 Business_Contract_Start_Dt  ,
											 Business_Contract_End_Dt  ,
											 Contract_By_User_Id     ,
											 Contract_By_First_Nm    ,
											 Contract_By_Last_Nm     ,
											 Reason_Cd               ,
											 Reason_Dsc              ,
											 Reason_Short_Dsc        ,
											 Contract_By_Create_Ts    ,
											 Contract_Threshold_Order_Limit_Cnt    ,
											 Contract_Threshold_Maximum_Item_Cnt    ,
											 Contract_Threshold_Minimum_Item_Cnt    ,
											 Contract_Threshold_Minimum_Tote_Cnt    ,
											 Contract_Threshold_Maximum_Tote_Cnt    ,
											 Contract_Threshold_Order_Allocation_Pct    ,
											 Contract_Threshold_Mileage_Nbr    ,
											 Partner_Profile_Effective_Time_Period_Type_Cd    ,
											 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
											 Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,   
											 Partner_Id,
											 Partner_Site_Id,
											 Partner_Participant_Id,
											 creationdt,
											 false AS DW_Logical_delete_ind,
											 actiontypecd,
											 FileName
											FROM   src_wrk_tbl_recs SRC1
											WHERE   rn1 = 1 
											and Partner_Nm IS NOT NULL
											AND (Partner_Id is not null and  Partner_Site_Id is not null and   Partner_Participant_Id is not null)
											) src1
											
											LEFT JOIN 
											(	SELECT Business_Partner_Integration_Id
											  ,Partner_Participant_Id
											  ,Partner_Site_Id
											  ,Partner_Id 
											FROM ${tbl_lkp} 
											WHERE DW_CURRENT_VERSION_IND = TRUE 
											AND DW_LOGICAL_DELETE_IND = FALSE 
											) B ON 							
											((NVL(SRC1.Partner_Participant_Id,'-1') = NVL(B.Partner_Participant_Id,'-1')
											  AND NVL(SRC1.Partner_Site_Id,'-1') = NVL(B.Partner_Site_Id,'-1')
											  AND NVL(SRC1.Partner_Id,'-1') = NVL(B.Partner_Id,'-1'))
											OR (NVL(SRC1.Partner_Participant_Id,'-1') = NVL(B.Partner_Participant_Id,'-1')
												AND NVL(SRC1.Partner_Site_Id,'-1') = NVL(B.Partner_Site_Id,'-1'))
											)
			
											) src
									
											LEFT JOIN 
											(SELECT  DISTINCT 
											 tgt.Business_Partner_Integration_Id,
											 tgt.Partner_Nm  ,
											 tgt.Partner_Type_Cd         ,
											 tgt.Partner_Type_Dsc        ,
											 tgt.Partner_Type_Short_Dsc    ,
											 tgt.Partner_Address_Usage_Type_Cd    ,
											 tgt.Partner_Address_Line1_Txt    ,
											 tgt.Partner_Address_Line2_Txt    ,
											 tgt.Partner_Address_Line3_Txt    ,
											 tgt.Partner_Address_Line4_Txt    ,
											 tgt.Partner_Address_Line5_Txt    ,
											 tgt.Partner_Contact_City_Nm    ,
											 tgt.Partner_Contact_County_Nm    ,
											 tgt.Partner_Contact_County_Cd    ,
											 tgt.Partner_Contact_Postal_Zone_Cd    ,
											 tgt.Partner_Contact_State_Cd    ,
											 tgt.Partner_Contact_State_Nm    ,
											 tgt.Partner_Contact_Country_Cd    ,
											 tgt.Partner_Contact_Country_Nm    ,
											 tgt.Partner_Contact_Latitude_Dgr    ,
											 tgt.Partner_Contact_Longitude_Dgr    ,
											 tgt.Partner_Contact_TimeZone_Cd    ,
											 tgt.Partner_Contact_Phone1_Nbr,
											 tgt.Partner_Contact_Phone2_Nbr,
											 tgt.Partner_Contact_Phone3_Nbr,																	
											 tgt.Partner_Contact_Fax_Nbr    ,
											 tgt.Partner_Status_Type_Cd    ,
											 tgt.Partner_Status_Dsc      ,
											 tgt.Partner_Status_Effective_Ts    ,
											 tgt.Service_Level_Cd        ,
											 tgt.Service_Level_Dsc       ,
											 tgt.Service_Level_Short_Dsc    ,
											 tgt.Service_Level_Activity_Cd    ,
											 tgt.Service_Level_Activity_Dsc    ,
											 tgt.Service_Level_Activity_Short_Dsc    ,
											 tgt.Business_Contract_Id    ,
											 tgt.Business_Contract_Nm    ,
											 tgt.Business_Contract_Dsc    ,
											 tgt.Business_Contract_Start_Dt  ,
											 tgt.Business_Contract_End_Dt  ,
											 tgt.Contract_By_User_Id     ,
											 tgt.Contract_By_First_Nm    ,
											 tgt.Contract_By_Last_Nm     ,
											 tgt.Reason_Cd               ,
											 tgt.Reason_Dsc              ,
											 tgt.Reason_Short_Dsc        ,
											 tgt.Contract_By_Create_Ts    ,
											 tgt.Contract_Threshold_Order_Limit_Cnt    ,
											 tgt.Contract_Threshold_Maximum_Item_Cnt    ,
											 tgt.Contract_Threshold_Minimum_Item_Cnt    ,
											 tgt.Contract_Threshold_Minimum_Tote_Cnt    ,
											 tgt.Contract_Threshold_Maximum_Tote_Cnt    ,
											 tgt.Contract_Threshold_Order_Allocation_Pct    ,
											 tgt.Contract_Threshold_Mileage_Nbr    ,
											 tgt.Partner_Profile_Effective_Time_Period_Type_Cd    ,
											 tgt.Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
											 tgt.Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,
											 tgt.dw_logical_delete_ind,
											 tgt.dw_first_effective_dt
											FROM ${tgt_tbl} tgt 
											WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
											) tgt 
											ON tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id  
											AND tgt.Partner_Nm = src.Partner_Nm  
											WHERE  (tgt.Business_Partner_Integration_Id IS NULL AND tgt.Partner_Nm IS NULL)  
											or(
											NVL(src.Partner_Type_Cd, '-1') <> NVL(tgt.Partner_Type_Cd,'-1')
											OR NVL(src.Partner_Type_Dsc, '-1') <> NVL(tgt.Partner_Type_Dsc,'-1')
											OR NVL(src.Partner_Type_Short_Dsc, '-1') <> NVL(tgt.Partner_Type_Short_Dsc,'-1')
											OR NVL(src.Partner_Address_Usage_Type_Cd, '-1') <> NVL(tgt.Partner_Address_Usage_Type_Cd,'-1')
											OR NVL(src.Partner_Address_Line1_Txt, '-1') <> NVL(tgt.Partner_Address_Line1_Txt,'-1')
											OR NVL(src.Partner_Address_Line2_Txt, '-1') <> NVL(tgt.Partner_Address_Line2_Txt,'-1')
											OR NVL(src.Partner_Address_Line3_Txt, '-1') <> NVL(tgt.Partner_Address_Line3_Txt,'-1')
											OR NVL(src.Partner_Address_Line4_Txt, '-1') <> NVL(tgt.Partner_Address_Line4_Txt,'-1')
											OR NVL(src.Partner_Address_Line5_Txt, '-1') <> NVL(tgt.Partner_Address_Line5_Txt,'-1')
											OR NVL(src.Partner_Contact_City_Nm, '-1') <> NVL(tgt.Partner_Contact_City_Nm,'-1')
											OR NVL(src.Partner_Contact_County_Nm, '-1') <> NVL(tgt.Partner_Contact_County_Nm,'-1')
											OR NVL(src.Partner_Contact_County_Cd, '-1') <> NVL(tgt.Partner_Contact_County_Cd,'-1')
											OR NVL(src.Partner_Contact_Postal_Zone_Cd, '-1') <> NVL(tgt.Partner_Contact_Postal_Zone_Cd,'-1')
											OR NVL(src.Partner_Contact_State_Cd, '-1') <> NVL(tgt.Partner_Contact_State_Cd,'-1')
											OR NVL(src.Partner_Contact_State_Nm, '-1') <> NVL(tgt.Partner_Contact_State_Nm,'-1')
											OR NVL(src.Partner_Contact_Country_Cd, '-1') <> NVL(tgt.Partner_Contact_Country_Cd,'-1')
											OR NVL(src.Partner_Contact_Country_Nm, '-1') <> NVL(tgt.Partner_Contact_Country_Nm,'-1')
											OR NVL(src.Partner_Contact_Latitude_Dgr, '-1') <> NVL(tgt.Partner_Contact_Latitude_Dgr,'-1')
											OR NVL(src.Partner_Contact_Longitude_Dgr, '-1') <> NVL(tgt.Partner_Contact_Longitude_Dgr,'-1')
											OR NVL(src.Partner_Contact_TimeZone_Cd, '-1') <> NVL(tgt.Partner_Contact_TimeZone_Cd,'-1')
											OR NVL(src.Partner_Contact_Phone1_Nbr, '-1') <> NVL(tgt.Partner_Contact_Phone1_Nbr,'-1')											 
											OR NVL(src.Partner_Contact_Phone2_Nbr, '-1') <> NVL(tgt.Partner_Contact_Phone2_Nbr,'-1')
											OR NVL(src.Partner_Contact_Phone3_Nbr, '-1') <> NVL(tgt.Partner_Contact_Phone3_Nbr,'-1')
											OR NVL(src.Partner_Contact_Fax_Nbr, '-1') <> NVL(tgt.Partner_Contact_Fax_Nbr,'-1')
											OR NVL(src.Partner_Status_Type_Cd, '-1') <> NVL(tgt.Partner_Status_Type_Cd,'-1')
											OR NVL(src.Partner_Status_Dsc, '-1') <> NVL(tgt.Partner_Status_Dsc,'-1')
											OR NVL(src.Partner_Status_Effective_Ts, '9999-12-31 00:00:00.000') <> NVL(tgt.Partner_Status_Effective_Ts,'9999-12-31 00:00:00.000')
											OR NVL(src.Service_Level_Cd, '-1') <> NVL(tgt.Service_Level_Cd,'-1')
											OR NVL(src.Service_Level_Dsc, '-1') <> NVL(tgt.Service_Level_Dsc,'-1')
											OR NVL(src.Service_Level_Short_Dsc, '-1') <> NVL(tgt.Service_Level_Short_Dsc,'-1')
											OR NVL(src.Service_Level_Activity_Cd, '-1') <> NVL(tgt.Service_Level_Activity_Cd,'-1')
											OR NVL(src.Service_Level_Activity_Dsc, '-1') <> NVL(tgt.Service_Level_Activity_Dsc,'-1')
											OR NVL(src.Service_Level_Activity_Short_Dsc, '-1') <> NVL(tgt.Service_Level_Activity_Short_Dsc,'-1')
											OR NVL(src.Business_Contract_Id, '-1') <> NVL(tgt.Business_Contract_Id,'-1')
											OR NVL(src.Business_Contract_Nm, '-1') <> NVL(tgt.Business_Contract_Nm,'-1')
											OR NVL(src.Business_Contract_Dsc, '-1') <> NVL(tgt.Business_Contract_Dsc,'-1')
											OR NVL(src.Business_Contract_Start_Dt, '9999-12-31') <> NVL(tgt.Business_Contract_Start_Dt,'9999-12-31')
											OR NVL(src.Business_Contract_End_Dt, '9999-12-31') <> NVL(tgt.Business_Contract_End_Dt,'9999-12-31')
											OR NVL(src.Contract_By_User_Id, '-1') <> NVL(tgt.Contract_By_User_Id,'-1')
											OR NVL(src.Contract_By_First_Nm, '-1') <> NVL(tgt.Contract_By_First_Nm,'-1')
											OR NVL(src.Contract_By_Last_Nm, '-1') <> NVL(tgt.Contract_By_Last_Nm,'-1')
											OR NVL(src.Reason_Cd, '-1') <> NVL(tgt.Reason_Cd,'-1')
											OR NVL(src.Reason_Dsc, '-1') <> NVL(tgt.Reason_Dsc,'-1')
											OR NVL(src.Reason_Short_Dsc, '-1') <> NVL(tgt.Reason_Short_Dsc,'-1')
											OR NVL(src.Contract_By_Create_Ts, '9999-12-31 00:00:00.000') <> NVL(tgt.Contract_By_Create_Ts,'9999-12-31 00:00:00.000')
											OR NVL(src.Contract_Threshold_Order_Limit_Cnt, '-1') <> NVL(tgt.Contract_Threshold_Order_Limit_Cnt,'-1')
											OR NVL(src.Contract_Threshold_Maximum_Item_Cnt, '-1') <> NVL(tgt.Contract_Threshold_Maximum_Item_Cnt,'-1')
											OR NVL(src.Contract_Threshold_Minimum_Item_Cnt, '-1') <> NVL(tgt.Contract_Threshold_Minimum_Item_Cnt,'-1')
											OR NVL(src.Contract_Threshold_Minimum_Tote_Cnt, '-1') <> NVL(tgt.Contract_Threshold_Minimum_Tote_Cnt,'-1')
											OR NVL(src.Contract_Threshold_Maximum_Tote_Cnt, '-1') <> NVL(tgt.Contract_Threshold_Maximum_Tote_Cnt,'-1')
											OR NVL(src.Contract_Threshold_Order_Allocation_Pct, '-1') <> NVL(tgt.Contract_Threshold_Order_Allocation_Pct,'-1')
											OR NVL(src.Contract_Threshold_Mileage_Nbr, '-1') <> NVL(tgt.Contract_Threshold_Mileage_Nbr,'-1')
											OR NVL(src.Partner_Profile_Effective_Time_Period_Type_Cd, '-1') <> NVL(tgt.Partner_Profile_Effective_Time_Period_Type_Cd,'-1')
											OR NVL(src.Partner_Profile_Effective_Time_Period_First_Effective_Ts, '9999-12-31 00:00:00.000') <> NVL(tgt.Partner_Profile_Effective_Time_Period_First_Effective_Ts,'9999-12-31 00:00:00.000')
											OR NVL(src.Partner_Profile_Effective_Time_Period_Last_Effective_Ts, '9999-12-31 00:00:00.000') <> NVL(tgt.Partner_Profile_Effective_Time_Period_Last_Effective_Ts,'9999-12-31 00:00:00.000')
											OR src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
											)
											`;


try {
        snowflake.execute (
            {sqlText: sql_command  }
            );
        }
    catch (err)  {
        return `Creation of Business_Partner_Profile work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
        }



//SCD Type2 transaction begins
    var sql_begin = "BEGIN"
// Processing Updates of Type 2 SCD
var sql_updates = ` UPDATE  ${tgt_tbl} as tgt
					SET DW_Last_Effective_dt = CURRENT_DATE-1
						,DW_CURRENT_VERSION_IND = FALSE
						,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
						,DW_SOURCE_UPDATE_NM = FileName
					FROM ( SELECT Business_Partner_Integration_Id,
								  Partner_Nm,
								  FileName
					FROM ${tgt_wrk_tbl}
					WHERE DML_Type = 'U'
							AND Sameday_chg_ind = 0
							AND Business_Partner_Integration_Id IS NOT NULL
							AND Partner_Nm IS NOT NULL							
              				) src
					        WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id  
							AND tgt.Partner_Nm = src.Partner_Nm 							
							AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
   

// Processing Sameday updates

var sql_sameday = `
			        UPDATE ${tgt_tbl} as tgt
				    SET    Partner_Type_Cd = src.Partner_Type_Cd         ,
					 Partner_Type_Dsc = src.Partner_Type_Dsc        ,
					 Partner_Type_Short_Dsc = src.Partner_Type_Short_Dsc    ,
					 Partner_Address_Usage_Type_Cd = src.Partner_Address_Usage_Type_Cd    ,
					 Partner_Address_Line1_Txt = src.Partner_Address_Line1_Txt    ,
					 Partner_Address_Line2_Txt = src.Partner_Address_Line2_Txt    ,
					 Partner_Address_Line3_Txt = src.Partner_Address_Line3_Txt    ,
					 Partner_Address_Line4_Txt = src.Partner_Address_Line4_Txt    ,
					 Partner_Address_Line5_Txt = src.Partner_Address_Line5_Txt    ,
					 Partner_Contact_City_Nm = src.Partner_Contact_City_Nm    ,
					 Partner_Contact_County_Nm = src.Partner_Contact_County_Nm    ,
					 Partner_Contact_County_Cd = src.Partner_Contact_County_Cd    ,
					 Partner_Contact_Postal_Zone_Cd = src.Partner_Contact_Postal_Zone_Cd    ,
					 Partner_Contact_State_Cd = src.Partner_Contact_State_Cd    ,
					 Partner_Contact_State_Nm = src.Partner_Contact_State_Nm    ,
					 Partner_Contact_Country_Cd = src.Partner_Contact_Country_Cd    ,
					 Partner_Contact_Country_Nm = src.Partner_Contact_Country_Nm    ,
					 Partner_Contact_Latitude_Dgr = src.Partner_Contact_Latitude_Dgr    ,
					 Partner_Contact_Longitude_Dgr = src.Partner_Contact_Longitude_Dgr    ,
					 Partner_Contact_TimeZone_Cd = src.Partner_Contact_TimeZone_Cd    ,
					 Partner_Contact_Phone1_Nbr = src.Partner_Contact_Phone1_Nbr    ,
					 Partner_Contact_Phone2_Nbr = src.Partner_Contact_Phone2_Nbr    ,
					 Partner_Contact_Phone3_Nbr = src.Partner_Contact_Phone3_Nbr    ,
					 Partner_Contact_Fax_Nbr = src.Partner_Contact_Fax_Nbr    ,
					 Partner_Status_Type_Cd = src.Partner_Status_Type_Cd    ,
					 Partner_Status_Dsc = src.Partner_Status_Dsc      ,
					 Partner_Status_Effective_Ts = src.Partner_Status_Effective_Ts    ,
					 Service_Level_Cd = src.Service_Level_Cd        ,
					 Service_Level_Dsc = src.Service_Level_Dsc       ,
					 Service_Level_Short_Dsc = src.Service_Level_Short_Dsc    ,
					 Service_Level_Activity_Cd = src.Service_Level_Activity_Cd    ,
					 Service_Level_Activity_Dsc = src.Service_Level_Activity_Dsc    ,
					 Service_Level_Activity_Short_Dsc = src.Service_Level_Activity_Short_Dsc    ,
					 Business_Contract_Id = src.Business_Contract_Id    ,
					 Business_Contract_Nm = src.Business_Contract_Nm    ,
					 Business_Contract_Dsc = src.Business_Contract_Dsc    ,
					 Business_Contract_Start_Dt = src.Business_Contract_Start_Dt  ,
					 Business_Contract_End_Dt = src.Business_Contract_End_Dt  ,
					 Contract_By_User_Id = src.Contract_By_User_Id     ,
					 Contract_By_First_Nm = src.Contract_By_First_Nm    ,
					 Contract_By_Last_Nm = src.Contract_By_Last_Nm     ,
					 Reason_Cd  = src.Reason_Cd               ,
					 Reason_Dsc = src.Reason_Dsc              ,
					 Reason_Short_Dsc = src.Reason_Short_Dsc        ,
					 Contract_By_Create_Ts = src.Contract_By_Create_Ts    ,
					 Contract_Threshold_Order_Limit_Cnt = src.Contract_Threshold_Order_Limit_Cnt    ,
					 Contract_Threshold_Maximum_Item_Cnt = src.Contract_Threshold_Maximum_Item_Cnt    ,
					 Contract_Threshold_Minimum_Item_Cnt = src.Contract_Threshold_Minimum_Item_Cnt    ,
					 Contract_Threshold_Minimum_Tote_Cnt = src.Contract_Threshold_Minimum_Tote_Cnt    ,
					 Contract_Threshold_Maximum_Tote_Cnt = src.Contract_Threshold_Maximum_Tote_Cnt    ,
					 Contract_Threshold_Order_Allocation_Pct = src.Contract_Threshold_Order_Allocation_Pct    ,
					 Contract_Threshold_Mileage_Nbr = src.Contract_Threshold_Mileage_Nbr    ,
					 Partner_Profile_Effective_Time_Period_Type_Cd = src.Partner_Profile_Effective_Time_Period_Type_Cd    ,
					 Partner_Profile_Effective_Time_Period_First_Effective_Ts = src.Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
					 Partner_Profile_Effective_Time_Period_Last_Effective_Ts = src.Partner_Profile_Effective_Time_Period_Last_Effective_Ts    ,
					DW_Logical_delete_ind = src.DW_Logical_delete_ind,
						  DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,
						  DW_SOURCE_UPDATE_NM = FileName
						FROM ( SELECT  Business_Partner_Integration_Id, 
										Partner_Nm  ,
										 Partner_Type_Cd         ,
										 Partner_Type_Dsc        ,
										 Partner_Type_Short_Dsc    ,
										 Partner_Address_Usage_Type_Cd    ,
										 Partner_Address_Line1_Txt    ,
										 Partner_Address_Line2_Txt    ,
										 Partner_Address_Line3_Txt    ,
										 Partner_Address_Line4_Txt    ,
										 Partner_Address_Line5_Txt    ,
										 Partner_Contact_City_Nm    ,
										 Partner_Contact_County_Nm    ,
										 Partner_Contact_County_Cd    ,
										 Partner_Contact_Postal_Zone_Cd    ,
										 Partner_Contact_State_Cd    ,
										 Partner_Contact_State_Nm    ,
										 Partner_Contact_Country_Cd    ,
										 Partner_Contact_Country_Nm    ,
										 Partner_Contact_Latitude_Dgr    ,
										 Partner_Contact_Longitude_Dgr    ,
										 Partner_Contact_TimeZone_Cd    ,
										 Partner_Contact_Phone1_Nbr,
										 Partner_Contact_Phone2_Nbr,
										 Partner_Contact_Phone3_Nbr,																	
										 Partner_Contact_Fax_Nbr    ,
										 Partner_Status_Type_Cd    ,
										 Partner_Status_Dsc      ,
										 Partner_Status_Effective_Ts    ,
										 Service_Level_Cd        ,
										 Service_Level_Dsc       ,
										 Service_Level_Short_Dsc    ,
										 Service_Level_Activity_Cd    ,
										 Service_Level_Activity_Dsc    ,
										 Service_Level_Activity_Short_Dsc    ,
										 Business_Contract_Id    ,
										 Business_Contract_Nm    ,
										 Business_Contract_Dsc    ,
										 Business_Contract_Start_Dt  ,
										 Business_Contract_End_Dt  ,
										 Contract_By_User_Id     ,
										 Contract_By_First_Nm    ,
										 Contract_By_Last_Nm     ,
										 Reason_Cd               ,
										 Reason_Dsc              ,
										 Reason_Short_Dsc        ,
										 Contract_By_Create_Ts    ,
										 Contract_Threshold_Order_Limit_Cnt    ,
										 Contract_Threshold_Maximum_Item_Cnt    ,
										 Contract_Threshold_Minimum_Item_Cnt    ,
										 Contract_Threshold_Minimum_Tote_Cnt    ,
										 Contract_Threshold_Maximum_Tote_Cnt    ,
										 Contract_Threshold_Order_Allocation_Pct    ,
										 Contract_Threshold_Mileage_Nbr    ,
										 Partner_Profile_Effective_Time_Period_Type_Cd    ,
										 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
										 Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,
										DW_Logical_delete_ind,
										FileName
										FROM ${tgt_wrk_tbl}
										WHERE DML_Type = 'U'
										AND Sameday_chg_ind = 1
										AND Business_Partner_Integration_Id IS NOT NULL
										AND Partner_Nm IS NOT NULL							        
										) src
										WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id  
										AND tgt.Partner_Nm = src.Partner_Nm 							        
										AND tgt.DW_CURRENT_VERSION_IND = TRUE`;



// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
										(Business_Partner_Integration_Id, 
										 Partner_Nm  ,
										 DW_First_Effective_Dt ,
										 DW_Last_Effective_Dt,
										 Partner_Type_Cd         ,
										 Partner_Type_Dsc        ,
										 Partner_Type_Short_Dsc    ,
										 Partner_Address_Usage_Type_Cd    ,
										 Partner_Address_Line1_Txt    ,
										 Partner_Address_Line2_Txt    ,
										 Partner_Address_Line3_Txt    ,
										 Partner_Address_Line4_Txt    ,
										 Partner_Address_Line5_Txt    ,
										 Partner_Contact_City_Nm    ,
										 Partner_Contact_County_Nm    ,
										 Partner_Contact_County_Cd    ,
										 Partner_Contact_Postal_Zone_Cd    ,
										 Partner_Contact_State_Cd    ,
										 Partner_Contact_State_Nm    ,
										 Partner_Contact_Country_Cd    ,
										 Partner_Contact_Country_Nm    ,
										 Partner_Contact_Latitude_Dgr    ,
										 Partner_Contact_Longitude_Dgr    ,
										 Partner_Contact_TimeZone_Cd    ,
										 Partner_Contact_Phone1_Nbr,
										 Partner_Contact_Phone2_Nbr,
										 Partner_Contact_Phone3_Nbr,																	
										 Partner_Contact_Fax_Nbr    ,
										 Partner_Status_Type_Cd    ,
										 Partner_Status_Dsc      ,
										 Partner_Status_Effective_Ts    ,
										 Service_Level_Cd        ,
										 Service_Level_Dsc       ,
										 Service_Level_Short_Dsc    ,
										 Service_Level_Activity_Cd    ,
										 Service_Level_Activity_Dsc    ,
										 Service_Level_Activity_Short_Dsc    ,
										 Business_Contract_Id    ,
										 Business_Contract_Nm    ,
										 Business_Contract_Dsc    ,
										 Business_Contract_Start_Dt  ,
										 Business_Contract_End_Dt  ,
										 Contract_By_User_Id     ,
										 Contract_By_First_Nm    ,
										 Contract_By_Last_Nm     ,
										 Reason_Cd               ,
										 Reason_Dsc              ,
										 Reason_Short_Dsc        ,
										 Contract_By_Create_Ts    ,
										 Contract_Threshold_Order_Limit_Cnt    ,
										 Contract_Threshold_Maximum_Item_Cnt    ,
										 Contract_Threshold_Minimum_Item_Cnt    ,
										 Contract_Threshold_Minimum_Tote_Cnt    ,
										 Contract_Threshold_Maximum_Tote_Cnt    ,
										 Contract_Threshold_Order_Allocation_Pct    ,
										 Contract_Threshold_Mileage_Nbr    ,
										 Partner_Profile_Effective_Time_Period_Type_Cd    ,
										 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
										 Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,
										 DW_CREATE_TS,
										DW_LOGICAL_DELETE_IND,
										DW_SOURCE_CREATE_NM,
										DW_CURRENT_VERSION_IND
										)
										SELECT 
										Business_Partner_Integration_Id,
										Partner_Nm,
										CURRENT_DATE,
										'31-DEC-9999',										
										 Partner_Type_Cd         ,
										 Partner_Type_Dsc        ,
										 Partner_Type_Short_Dsc    ,
										 Partner_Address_Usage_Type_Cd    ,
										 Partner_Address_Line1_Txt    ,
										 Partner_Address_Line2_Txt    ,
										 Partner_Address_Line3_Txt    ,
										 Partner_Address_Line4_Txt    ,
										 Partner_Address_Line5_Txt    ,
										 Partner_Contact_City_Nm    ,
										 Partner_Contact_County_Nm    ,
										 Partner_Contact_County_Cd    ,
										 Partner_Contact_Postal_Zone_Cd    ,
										 Partner_Contact_State_Cd    ,
										 Partner_Contact_State_Nm    ,
										 Partner_Contact_Country_Cd    ,
										 Partner_Contact_Country_Nm    ,
										 Partner_Contact_Latitude_Dgr    ,
										 Partner_Contact_Longitude_Dgr    ,
										 Partner_Contact_TimeZone_Cd    ,
										 Partner_Contact_Phone1_Nbr,
										 Partner_Contact_Phone2_Nbr,
										 Partner_Contact_Phone3_Nbr,																	
										 Partner_Contact_Fax_Nbr    ,
										 Partner_Status_Type_Cd    ,
										 Partner_Status_Dsc      ,
										 Partner_Status_Effective_Ts    ,
										 Service_Level_Cd        ,
										 Service_Level_Dsc       ,
										 Service_Level_Short_Dsc    ,
										 Service_Level_Activity_Cd    ,
										 Service_Level_Activity_Dsc    ,
										 Service_Level_Activity_Short_Dsc    ,
										 Business_Contract_Id    ,
										 Business_Contract_Nm    ,
										 Business_Contract_Dsc    ,
										 Business_Contract_Start_Dt  ,
										 Business_Contract_End_Dt  ,
										 Contract_By_User_Id     ,
										 Contract_By_First_Nm    ,
										 Contract_By_Last_Nm     ,
										 Reason_Cd               ,
										 Reason_Dsc              ,
										 Reason_Short_Dsc        ,
										 Contract_By_Create_Ts    ,
										 Contract_Threshold_Order_Limit_Cnt    ,
										 Contract_Threshold_Maximum_Item_Cnt    ,
										 Contract_Threshold_Minimum_Item_Cnt    ,
										 Contract_Threshold_Minimum_Tote_Cnt    ,
										 Contract_Threshold_Maximum_Tote_Cnt    ,
										 Contract_Threshold_Order_Allocation_Pct    ,
										 Contract_Threshold_Mileage_Nbr    ,
										 Partner_Profile_Effective_Time_Period_Type_Cd    ,
										 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
										 Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,
										CURRENT_TIMESTAMP,
										DW_Logical_delete_ind,
										FileName,
										TRUE
										FROM ${tgt_wrk_tbl}
										WHERE Sameday_chg_ind = 0
										AND Business_Partner_Integration_Id IS NOT NULL
										AND Partner_Nm IS NOT NULL
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
	return `Loading of table ${tgt_tbl} Failed with error: ${err}` ;   // Return a error message.
	}
	
var truncate_exceptions =`DELETE FROM ${tgt_exp_tbl}`;

 
var sql_exceptions = `INSERT INTO ${tgt_exp_tbl} 
					            select   Business_Partner_Integration_Id,
										 Partner_Nm  ,
										 Partner_Type_Cd         ,
										 Partner_Type_Dsc        ,
										 Partner_Type_Short_Dsc    ,
										 Partner_Address_Usage_Type_Cd    ,
										 Partner_Address_Line1_Txt    ,
										 Partner_Address_Line2_Txt    ,
										 Partner_Address_Line3_Txt    ,
										 Partner_Address_Line4_Txt    ,
										 Partner_Address_Line5_Txt    ,
										 Partner_Contact_City_Nm    ,
										 Partner_Contact_County_Nm    ,
										 Partner_Contact_County_Cd    ,
										 Partner_Contact_Postal_Zone_Cd    ,
										 Partner_Contact_State_Cd    ,
										 Partner_Contact_State_Nm    ,
										 Partner_Contact_Country_Cd    ,
										 Partner_Contact_Country_Nm    ,
										 Partner_Contact_Latitude_Dgr    ,
										 Partner_Contact_Longitude_Dgr    ,
										 Partner_Contact_TimeZone_Cd    ,
										 Partner_Contact_Phone1_Nbr,
										 Partner_Contact_Phone2_Nbr,
										 Partner_Contact_Phone3_Nbr,																	
										 Partner_Contact_Fax_Nbr    ,
										 Partner_Status_Type_Cd    ,
										 Partner_Status_Dsc      ,
										 Partner_Status_Effective_Ts    ,
										 Service_Level_Cd        ,
										 Service_Level_Dsc       ,
										 Service_Level_Short_Dsc    ,
										 Service_Level_Activity_Cd    ,
										 Service_Level_Activity_Dsc    ,
										 Service_Level_Activity_Short_Dsc    ,
										 Business_Contract_Id    ,
										 Business_Contract_Nm    ,
										 Business_Contract_Dsc    ,
										 Business_Contract_Start_Dt  ,
										 Business_Contract_End_Dt  ,
										 Contract_By_User_Id     ,
										 Contract_By_First_Nm    ,
										 Contract_By_Last_Nm     ,
										 Reason_Cd               ,
										 Reason_Dsc              ,
										 Reason_Short_Dsc        ,
										 Contract_By_Create_Ts    ,
										 Contract_Threshold_Order_Limit_Cnt    ,
										 Contract_Threshold_Maximum_Item_Cnt    ,
										 Contract_Threshold_Minimum_Item_Cnt    ,
										 Contract_Threshold_Minimum_Tote_Cnt    ,
										 Contract_Threshold_Maximum_Tote_Cnt    ,
										 Contract_Threshold_Order_Allocation_Pct    ,
										 Contract_Threshold_Mileage_Nbr    ,
										 Partner_Profile_Effective_Time_Period_Type_Cd    ,
										 Partner_Profile_Effective_Time_Period_First_Effective_Ts    ,
										 Partner_Profile_Effective_Time_Period_Last_Effective_Ts ,   
										 Partner_Id,
										 Partner_Site_Id,
										 Partner_Participant_Id,
									     FileName,
										 DW_Logical_delete_ind,
										 DML_TYPE,
										 SAMEDAY_CHG_IND,
										CASE WHEN Business_Partner_Integration_Id is NULL THEN 'Business_Partner_Integration_Id is NULL'
										WHEN Partner_Nm is NULL THEN 'Partner_Nm is NULL'
										ELSE NULL END AS Exception_Reason,
										CURRENT_TIMESTAMP AS DW_CREATE_TS,
										CREATIONDT, 
										ACTIONTYPECD		
										FROM ${tgt_wrk_tbl}
										WHERE (Business_Partner_Integration_Id IS  NULL
										or Partner_Nm IS NULL)
										AND DW_Logical_delete_ind = FALSE`;

         
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

// ************** Load for Business_Partner_Profile table ENDs *****************

$$;
