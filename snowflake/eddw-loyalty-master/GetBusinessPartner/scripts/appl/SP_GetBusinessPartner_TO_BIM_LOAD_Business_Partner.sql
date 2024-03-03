--liquibase formatted sql
--changeset SYSTEM:SP_GetBusinessPartner_TO_BIM_LOAD_Business_Partner runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETBUSINESSPARTNER_TO_BIM_LOAD_BUSINESS_PARTNER(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$

 	var src_wrk_tbl = SRC_WRK_TBL;
	var cnf_schema = C_LOYAL;
	var wrk_schema = C_STAGE;
	
	var tgt_wrk_tbl = `${CNF_DB}.${wrk_schema}.Business_Partner_wrk`;
	var tgt_tbl = `${CNF_DB}.${cnf_schema}.Business_Partner`;
	var src_wrk_tmp_tbl = `${CNF_DB}.${wrk_schema}.Business_Partner_SRC_WRK`;
	var tgt_exp_tbl = `${CNF_DB}.${wrk_schema}.Business_Partner_EXCEPTIONS`;
    
	 // **************  Load for Business_Partner table BEGIN *****************
   
		var cr_src_wrk_tbl = `CREATE OR REPLACE TABLE ${src_wrk_tmp_tbl} 			
					 AS
					(
					select *
					from(
					
						SELECT DISTINCT
							BusinessPartnerData_PartnerId as Partner_Id, 
							InternalParnterInd as  Internal_Parnter_Ind,
							VendorNbr as  Vendor_Id,
							CustomerAccountNbr as  Wholesale_Customer_Nbr,
							CustomerSiteNbr as  Customer_Site_Nbr,
							PartnerSiteId as  Partner_Site_Id,
							PartnerParticipantId as  Partner_Participant_Id,
							PartnerSiteNm as Partner_Site_Nm,
							PartnerTypeCd_Code as  Partner_Site_Type_Cd,
							PartnerTypeCd_Description as  Partner_Site_Type_Dsc,
							PartnerTypeCd_ShortDescription as  Partner_Site_Type_Short_Dsc,
							--PartnerSiteData_OrganizationTypeCd as  Organization_Type_Cd,
							--PartnerSiteData_OrganizationValueTxt as  Organization_Value_Txt,
							PartnerSiteActiveInd as  Partner_Site_Active_Ind,
							PartnerSiteStatus_StatusTypeCd_Type as  Partner_Site_Status_Type_Cd,
							PartnerSiteStatus_Description as  Partner_Site_Status_Dsc,
							PartnerSiteStatus_EffectiveDtTm as  Partner_Site_Status_Effective_Ts    ,
							SiteAddress_AddressUsageTypeCd as  Partner_Site_Address_Usage_Type_Cd    ,
							SiteAddress_AddressLine1txt as  Partner_Site_Address_Line1_txt    ,
							SiteAddress_AddressLine2txt as  Partner_Site_Address_Line2_txt    ,
							SiteAddress_AddressLine3txt as  Partner_Site_Address_Line3_txt    ,
							SiteAddress_AddressLine4txt as  Partner_Site_Address_Line4_txt    ,
							SiteAddress_AddressLine5txt as  Partner_Site_Address_Line5_txt    ,
							SiteAddress_CityNm as  Partner_Site_City_Nm    ,
							SiteAddress_CountyNm as  Partner_Site_County_Nm    ,
							SiteAddress_CountyCd as  Partner_Site_County_Cd    ,
							SiteAddress_PostalZoneCd as  Partner_Site_Postal_Zone_Cd    ,
							SiteAddress_StateCd as  Partner_Site_State_Cd,
							SiteAddress_StateNm as  Partner_Site_State_Nm,
							SiteAddress_CountryCd as  Partner_Site_Country_Cd,
							SiteAddress_CountryNm as  Partner_Site_Country_Nm,
							nullif(trim(SiteAddress_LatitudeDegree),'') as  Partner_Site_Latitude_Dgr,
							nullif(trim(SiteAddress_LongitudeDegree),'') as  Partner_Site_Longitude_Dgr,
							SiteAddress_TimeZoneCd as  Partner_Site_TimeZone_Cd,
							SiteAddress_PhoneNbr, 
							SiteAddress_FaxNbr as  Partner_Site_Fax_Nbr,
							PartnerSiteCommentTxt as  Partner_Site_Comment_Txt    ,
							PartnerSiteEffectiveTimePeriod_typeCode as  Partner_Site_Effective_Time_Period_Type_Cd    ,
							TIMESTAMP_NTZ_FROM_PARTS(try_to_date(PartnerSiteEffectiveTimePeriod_FirstEffectiveDt),try_to_time(PartnerSiteEffectiveTimePeriod_FirstEffectiveTm)) AS Partner_Site_First_Effective_Ts,
							TIMESTAMP_NTZ_FROM_PARTS(try_to_date(PartnerSiteEffectiveTimePeriod_LastEffectiveDt),try_to_time(PartnerSiteEffectiveTimePeriod_LastEffectiveTm)) AS Partner_Site_Last_Effective_Ts,
							PartnerSiteAuditData_CreateTs as  Partner_Site_Create_Ts, 
							PartnerSiteAuditData_CreateUserId as  Partner_Site_Create_User_Id,
							PartnerSiteAuditData_UpdateDtTm as  Partner_Site_Update_Ts,
							PartnerSiteAuditData_UpdateUserId as  Partner_Site_Update_User_Id,
							PartnerEffectiveTimePeriod_typeCode as  Partner_Effective_Time_Period_Type_Cd    ,
							TIMESTAMP_NTZ_FROM_PARTS(try_to_date(PartnerEffectiveTimePeriod_FirstEffectiveDt),try_to_time(PartnerEffectiveTimePeriod_FirstEffectiveTm)) AS Partner_First_Effective_Ts,
							TIMESTAMP_NTZ_FROM_PARTS(try_to_date(PartnerEffectiveTimePeriod_LastEffectiveDt),try_to_time(PartnerEffectiveTimePeriod_LastEffectiveTm)) AS Partner_Last_Effective_Ts,
							PartnerAuditData_CreateDtTm as  Partner_Create_Ts ,
							PartnerAuditData_CreateUserId as  Partner_Create_User_Id,
							PartnerAuditData_UpdateTs as  Partner_Update_Ts,
							PartnerAuditData_UpdateUserId as  Partner_Update_User_Id,
							ActionTypeCd,
							creationdt,
							fileName,
							row_number() over ( PARTITION BY BusinessPartnerData_PartnerId,PartnerSiteId,Partner_Participant_Id
							ORDER BY to_timestamp_ntz(creationdt) desc) as rn
							FROM ${src_wrk_tbl}
							WHERE (BusinessPartnerData_PartnerId is not null and PartnerSiteId is not null and  Partner_Participant_Id is not null)
							OR (PartnerSiteId is not null AND Partner_Participant_Id is not null)
							
							
						)
						
					 where rn = 1	
					)`;	
							

	try {
        snowflake.execute (
            {sqlText: cr_src_wrk_tbl  }
            );
        }
    catch (err)  {
         return `Creation of Source work temp table table ${src_wrk_tmp_tbl} Failed with error: ${err}`;   // Return a error message.
     }
		 
	// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table. 
	var sql_crt_tgt_wrk_tbl = `CREATE OR REPLACE TABLE ` + tgt_wrk_tbl + ` as 
		        SELECT	DISTINCT 
			     CASE WHEN DML_TYPE = 'I' THEN mx_ci_intg_id + rnum ELSE Business_Partner_Integration_Id
				 END AS Business_Partner_Integration_Id,
				 Partner_Id              ,
				 Internal_Parnter_Ind    ,
				 Vendor_Id               ,
				 Wholesale_Customer_Nbr    ,
				 Customer_Site_Nbr       ,
				 Partner_Site_Id         ,
				 Partner_Participant_Id    ,
				 Partner_Site_Nm         ,
				 Partner_Site_Type_Cd    ,
				 Partner_Site_Type_Dsc    ,
				 Partner_Site_Type_Short_Dsc    ,
				 --Organization_Type_Cd    ,
				 --Organization_Value_Txt    ,
				 Partner_Site_Active_Ind    ,
				 Partner_Site_Status_Type_Cd    ,
				 Partner_Site_Status_Dsc    ,
				 Partner_Site_Status_Effective_Ts    ,
				 Partner_Site_Address_Usage_Type_Cd    ,
				 Partner_Site_Address_Line1_txt    ,
				 Partner_Site_Address_Line2_txt    ,
				 Partner_Site_Address_Line3_txt    ,
				 Partner_Site_Address_Line4_txt    ,
				 Partner_Site_Address_Line5_txt    ,
				 Partner_Site_City_Nm    ,
				 Partner_Site_County_Nm    ,
				 Partner_Site_County_Cd    ,
				 Partner_Site_Postal_Zone_Cd    ,
				 Partner_Site_State_Cd    ,
				 Partner_Site_State_Nm    ,
				 Partner_Site_Country_Cd    ,
				 Partner_Site_Country_Nm    ,
				 Partner_Site_Latitude_Dgr   ,
				 Partner_Site_Longitude_Dgr    ,
				 Partner_Site_TimeZone_Cd    ,
				 Partner_Site_Phone1_Nbr,
				 Partner_Site_Phone2_Nbr,
				 Partner_Site_Phone3_Nbr,
				 Partner_Site_Fax_Nbr    ,
				 Partner_Site_Comment_Txt    ,
				 Partner_Site_Effective_Time_Period_Type_Cd    ,
				 Partner_Site_First_Effective_Ts    ,
				 Partner_Site_Last_Effective_Ts    ,
				 Partner_Site_Create_Ts    ,
				 Partner_Site_Create_User_Id    ,
				 Partner_Site_Update_Ts    ,
				 Partner_Site_Update_User_Id    ,
				 Partner_Effective_Time_Period_Type_Cd    ,
				 Partner_First_Effective_Ts    ,
			     Partner_Last_Effective_Ts    ,
				 Partner_Create_Ts       ,
				 Partner_Create_User_Id    ,
				 Partner_Update_Ts       ,
				 Partner_Update_User_Id ,					 
				DW_LOGICAL_DELETE_IND,
				FILENAME,
				DML_TYPE,
				SAMEDAY_CHG_IND
				FROM	(
						SELECT	seq_gen.*
								,row_number() over ( PARTITION BY DML_TYPE ORDER BY Partner_Id,Partner_Site_Id,Partner_Participant_Id) as rnum
						FROM	(
				SELECT
					tgt.Business_Partner_Integration_Id
					,srcC.*
					,CASE WHEN tgt.Business_Partner_Integration_Id is NULL then 'I' ELSE 'U' END as DML_Type
					,CASE WHEN tgt.DW_First_Effective_dt = CURRENT_DATE  THEN 1 Else 0 END as Sameday_chg_ind
					,mx_ci_intg_id
				FROM
				(SELECT 
					 Partner_Id              ,
					 Internal_Parnter_Ind    ,
					 Vendor_Id               ,
					 Wholesale_Customer_Nbr    ,
					 Customer_Site_Nbr       ,
					 Partner_Site_Id         ,
					 Partner_Participant_Id    ,
					 Partner_Site_Nm         ,
					 Partner_Site_Type_Cd    ,
					 Partner_Site_Type_Dsc    ,
					 Partner_Site_Type_Short_Dsc    ,
					 --Organization_Type_Cd    ,
					 --Organization_Value_Txt    ,
					 Partner_Site_Active_Ind    ,
					 Partner_Site_Status_Type_Cd    ,
					 Partner_Site_Status_Dsc    ,
					 Partner_Site_Status_Effective_Ts    ,
					 Partner_Site_Address_Usage_Type_Cd    ,
					 Partner_Site_Address_Line1_txt    ,
					 Partner_Site_Address_Line2_txt    ,
					 Partner_Site_Address_Line3_txt    ,
					 Partner_Site_Address_Line4_txt    ,
					 Partner_Site_Address_Line5_txt    ,
					 Partner_Site_City_Nm    ,
					 Partner_Site_County_Nm    ,
					 Partner_Site_County_Cd    ,
					 Partner_Site_Postal_Zone_Cd    ,
					 Partner_Site_State_Cd    ,
					 Partner_Site_State_Nm    ,
					 Partner_Site_Country_Cd    ,
					 Partner_Site_Country_Nm    ,
					 Partner_Site_Latitude_Dgr   ,
					 Partner_Site_Longitude_Dgr    ,
					 Partner_Site_TimeZone_Cd    ,
					 Partner_Site_Phone1_Nbr,
					 Partner_Site_Phone2_Nbr,
					 Partner_Site_Phone3_Nbr,
					 Partner_Site_Fax_Nbr    ,
					 Partner_Site_Comment_Txt    ,
					 Partner_Site_Effective_Time_Period_Type_Cd    ,
					 Partner_Site_First_Effective_Ts    ,
					 Partner_Site_Last_Effective_Ts    ,
					 Partner_Site_Create_Ts    ,
					 Partner_Site_Create_User_Id    ,
					 Partner_Site_Update_Ts    ,
					 Partner_Site_Update_User_Id    ,
					 Partner_Effective_Time_Period_Type_Cd    ,
					 Partner_First_Effective_Ts    ,
					 Partner_Last_Effective_Ts    ,
					 Partner_Create_Ts       ,
					 Partner_Create_User_Id    ,
					 Partner_Update_Ts       ,
					 Partner_Update_User_Id ,
					 DW_LOGICAL_DELETE_IND,	
					 FILENAME ,
					 RN
				FROM(
					SELECT
					Partner_Id              ,
					 Internal_Parnter_Ind    ,
					 Vendor_Id               ,
					 Wholesale_Customer_Nbr    ,
					 Customer_Site_Nbr       ,
					 Partner_Site_Id         ,
					 Partner_Participant_Id    ,
					 Partner_Site_Nm         ,
					 Partner_Site_Type_Cd    ,
					 Partner_Site_Type_Dsc    ,
					 Partner_Site_Type_Short_Dsc    ,
					 --Organization_Type_Cd    ,
					 --Organization_Value_Txt    ,
					 Partner_Site_Active_Ind    ,
					 Partner_Site_Status_Type_Cd    ,
					 Partner_Site_Status_Dsc    ,
					 Partner_Site_Status_Effective_Ts    ,
					 Partner_Site_Address_Usage_Type_Cd    ,
					 Partner_Site_Address_Line1_txt    ,
					 Partner_Site_Address_Line2_txt    ,
					 Partner_Site_Address_Line3_txt    ,
					 Partner_Site_Address_Line4_txt    ,
					 Partner_Site_Address_Line5_txt    ,
					 Partner_Site_City_Nm    ,
					 Partner_Site_County_Nm    ,
					 Partner_Site_County_Cd    ,
					 Partner_Site_Postal_Zone_Cd    ,
					 Partner_Site_State_Cd    ,
					 Partner_Site_State_Nm    ,
					 Partner_Site_Country_Cd    ,
					 Partner_Site_Country_Nm    ,
					 Partner_Site_Latitude_Dgr   ,
					 Partner_Site_Longitude_Dgr    ,
					 Partner_Site_TimeZone_Cd    ,
					 Partner_Site_Phone1_Nbr,
					 Partner_Site_Phone2_Nbr,
					 Partner_Site_Phone3_Nbr,
					 Partner_Site_Fax_Nbr    ,
					 Partner_Site_Comment_Txt    ,
					 Partner_Site_Effective_Time_Period_Type_Cd    ,
					 Partner_Site_First_Effective_Ts    ,
					 Partner_Site_Last_Effective_Ts    ,
					 Partner_Site_Create_Ts    ,
					 Partner_Site_Create_User_Id    ,
					 Partner_Site_Update_Ts    ,
					 Partner_Site_Update_User_Id    ,
					 Partner_Effective_Time_Period_Type_Cd    ,
					 Partner_First_Effective_Ts    ,
					 Partner_Last_Effective_Ts    ,
					 Partner_Create_Ts       ,
					 Partner_Create_User_Id    ,
					 Partner_Update_Ts       ,
					 Partner_Update_User_Id ,
					 CASE WHEN Upper(ActionTypeCd) = 'DELETE' THEN true ELSE false END AS DW_LOGICAL_DELETE_IND,
					 FILENAME, 
					 row_number() over ( PARTITION BY Partner_Id,Partner_Site_Id,Partner_Participant_Id  ORDER BY to_timestamp_ntz(creationdt) desc) as rn				
			FROM(
			SELECT
					 Partner_Id              ,
					 Internal_Parnter_Ind    ,
					 Vendor_Id               ,
					 Wholesale_Customer_Nbr    ,
					 Customer_Site_Nbr       ,
					 Partner_Site_Id         ,
					 Partner_Participant_Id    ,
					 Partner_Site_Nm         ,
					 Partner_Site_Type_Cd    ,
					 Partner_Site_Type_Dsc    ,
					 Partner_Site_Type_Short_Dsc    ,
					 --Organization_Type_Cd    ,
					 --Organization_Value_Txt    ,
					 Partner_Site_Active_Ind    ,
					 Partner_Site_Status_Type_Cd    ,
					 Partner_Site_Status_Dsc    ,
					 Partner_Site_Status_Effective_Ts    ,
					 Partner_Site_Address_Usage_Type_Cd    ,
					 Partner_Site_Address_Line1_txt    ,
					 Partner_Site_Address_Line2_txt    ,
					 Partner_Site_Address_Line3_txt    ,
					 Partner_Site_Address_Line4_txt    ,
					 Partner_Site_Address_Line5_txt    ,
					 Partner_Site_City_Nm    ,
					 Partner_Site_County_Nm    ,
					 Partner_Site_County_Cd    ,
					 Partner_Site_Postal_Zone_Cd    ,
					 Partner_Site_State_Cd    ,
					 Partner_Site_State_Nm    ,
					 Partner_Site_Country_Cd    ,
					 Partner_Site_Country_Nm    ,
					 Partner_Site_Latitude_Dgr   ,
					 Partner_Site_Longitude_Dgr    ,
					 Partner_Site_TimeZone_Cd    ,
					 MAX(Partner_Site_Phone1_Nbr) AS Partner_Site_Phone1_Nbr,
					 MAX(Partner_Site_Phone2_Nbr) AS Partner_Site_Phone2_Nbr,
					 MAX(Partner_Site_Phone3_Nbr) AS Partner_Site_Phone3_Nbr,
					 Partner_Site_Fax_Nbr    ,
					 Partner_Site_Comment_Txt    ,
					 Partner_Site_Effective_Time_Period_Type_Cd    ,
					 Partner_Site_First_Effective_Ts    ,
					 Partner_Site_Last_Effective_Ts    ,
					 Partner_Site_Create_Ts    ,
					 Partner_Site_Create_User_Id    ,
					 Partner_Site_Update_Ts    ,
					 Partner_Site_Update_User_Id    ,
					 Partner_Effective_Time_Period_Type_Cd    ,
					 Partner_First_Effective_Ts    ,
					 Partner_Last_Effective_Ts    ,
					 Partner_Create_Ts       ,
					 Partner_Create_User_Id    ,
					 Partner_Update_Ts       ,
					 Partner_Update_User_Id ,
					ACTIONTYPECD ,
					CREATIONDT ,
					FILENAME 
						
			FROM(
			SELECT   Partner_Id              ,
					 Internal_Parnter_Ind    ,
					 Vendor_Id               ,
					 Wholesale_Customer_Nbr    ,
					 Customer_Site_Nbr       ,
					 Partner_Site_Id         ,
					 Partner_Participant_Id    ,
					 Partner_Site_Nm         ,
					 Partner_Site_Type_Cd    ,
					 Partner_Site_Type_Dsc    ,
					 Partner_Site_Type_Short_Dsc    ,
					 --Organization_Type_Cd    ,
					 --Organization_Value_Txt    ,
					 Partner_Site_Active_Ind    ,
					 Partner_Site_Status_Type_Cd    ,
					 Partner_Site_Status_Dsc    ,
					 Partner_Site_Status_Effective_Ts    ,
					 Partner_Site_Address_Usage_Type_Cd    ,
					 Partner_Site_Address_Line1_txt    ,
					 Partner_Site_Address_Line2_txt    ,
					 Partner_Site_Address_Line3_txt    ,
					 Partner_Site_Address_Line4_txt    ,
					 Partner_Site_Address_Line5_txt    ,
					 Partner_Site_City_Nm    ,
					 Partner_Site_County_Nm    ,
					 Partner_Site_County_Cd    ,
					 Partner_Site_Postal_Zone_Cd    ,
					 Partner_Site_State_Cd    ,
					 Partner_Site_State_Nm    ,
					 Partner_Site_Country_Cd    ,
					 Partner_Site_Country_Nm    ,
					 Partner_Site_Latitude_Dgr   ,
					 Partner_Site_Longitude_Dgr    ,
					 Partner_Site_TimeZone_Cd    ,
					 --SiteAddress_PhoneNbr,
					 MAX(CASE WHEN rn_1 = 1 THEN SiteAddress_PhoneNbr ELSE NULL END ) AS Partner_Site_Phone1_Nbr,
					 MAX(CASE WHEN rn_1 = 2 THEN SiteAddress_PhoneNbr ELSE NULL END ) AS Partner_Site_Phone2_Nbr,
					 MAX(CASE WHEN rn_1 = 3 THEN SiteAddress_PhoneNbr ELSE NULL END ) AS Partner_Site_Phone3_Nbr,
					 Partner_Site_Fax_Nbr    ,
					 Partner_Site_Comment_Txt    ,
					 Partner_Site_Effective_Time_Period_Type_Cd    ,
					 Partner_Site_First_Effective_Ts    ,
					 Partner_Site_Last_Effective_Ts    ,
					 Partner_Site_Create_Ts    ,
					 Partner_Site_Create_User_Id    ,
					 Partner_Site_Update_Ts    ,
					 Partner_Site_Update_User_Id    ,
					 Partner_Effective_Time_Period_Type_Cd    ,
					 Partner_First_Effective_Ts    ,
					 Partner_Last_Effective_Ts    ,
					 Partner_Create_Ts       ,
					 Partner_Create_User_Id    ,
					 Partner_Update_Ts       ,
					 Partner_Update_User_Id ,
					ACTIONTYPECD ,
					CREATIONDT ,
					FILENAME 
					FROM
					(
									SELECT
										 SRC.Partner_Id              ,
										 SRC.Internal_Parnter_Ind    ,
										 SRC.Vendor_Id               ,
										 SRC.Wholesale_Customer_Nbr    ,
										 SRC.Customer_Site_Nbr       ,
										 SRC.Partner_Site_Id         ,
										 SRC.Partner_Participant_Id    ,
										 SRC.Partner_Site_Nm         ,
										 SRC.Partner_Site_Type_Cd    ,
										 SRC.Partner_Site_Type_Dsc    ,
										 SRC.Partner_Site_Type_Short_Dsc    ,
										 --SRC.Organization_Type_Cd    ,
										 --SRC.Organization_Value_Txt    ,
										 SRC.Partner_Site_Active_Ind    ,
										 SRC.Partner_Site_Status_Type_Cd    ,
										 SRC.Partner_Site_Status_Dsc    ,
										 SRC.Partner_Site_Status_Effective_Ts    ,
										 SRC.Partner_Site_Address_Usage_Type_Cd    ,
										 SRC.Partner_Site_Address_Line1_txt    ,
										 SRC.Partner_Site_Address_Line2_txt    ,
										 SRC.Partner_Site_Address_Line3_txt    ,
										 SRC.Partner_Site_Address_Line4_txt    ,
										 SRC.Partner_Site_Address_Line5_txt    ,
										 SRC.Partner_Site_City_Nm    ,
										 SRC.Partner_Site_County_Nm    ,
										 SRC.Partner_Site_County_Cd    ,
										 SRC.Partner_Site_Postal_Zone_Cd    ,
										 SRC.Partner_Site_State_Cd    ,
										 SRC.Partner_Site_State_Nm    ,
										 SRC.Partner_Site_Country_Cd    ,
										 SRC.Partner_Site_Country_Nm    ,
										 SRC.Partner_Site_Latitude_Dgr   ,
										 SRC.Partner_Site_Longitude_Dgr    ,
										 SRC.Partner_Site_TimeZone_Cd    ,
										 SRC.SiteAddress_PhoneNbr,
										 SRC.Partner_Site_Fax_Nbr    ,
										 SRC.Partner_Site_Comment_Txt    ,
										 SRC.Partner_Site_Effective_Time_Period_Type_Cd    ,
										 SRC.Partner_Site_First_Effective_Ts    ,
										 SRC.Partner_Site_Last_Effective_Ts    ,
										 SRC.Partner_Site_Create_Ts    ,
										 SRC.Partner_Site_Create_User_Id    ,
										 SRC.Partner_Site_Update_Ts    ,
										 SRC.Partner_Site_Update_User_Id    ,
										 SRC.Partner_Effective_Time_Period_Type_Cd    ,
										 SRC.Partner_First_Effective_Ts    ,
										 SRC.Partner_Last_Effective_Ts    ,
										 SRC.Partner_Create_Ts       ,
										 SRC.Partner_Create_User_Id    ,
										 SRC.Partner_Update_Ts       ,
										 SRC.Partner_Update_User_Id,
										 SRC.ActionTypeCd,
										 SRC.creationdt,
										 SRC.fileName
				,row_number() over ( PARTITION BY 
									     SRC.Partner_Id              ,
										 SRC.Internal_Parnter_Ind    ,
										 SRC.Vendor_Id               ,
										 SRC.Wholesale_Customer_Nbr    ,
										 SRC.Customer_Site_Nbr       ,
										 SRC.Partner_Site_Id         ,
										 SRC.Partner_Participant_Id    ,
										 SRC.Partner_Site_Nm         ,
										 SRC.Partner_Site_Type_Cd    ,
										 SRC.Partner_Site_Type_Dsc    ,
										 SRC.Partner_Site_Type_Short_Dsc    ,
										 --SRC.Organization_Type_Cd    ,
										 --SRC.Organization_Value_Txt    ,
										 SRC.Partner_Site_Active_Ind    ,
										 SRC.Partner_Site_Status_Type_Cd    ,
										 SRC.Partner_Site_Status_Dsc    ,
										 SRC.Partner_Site_Status_Effective_Ts    ,
										 SRC.Partner_Site_Address_Usage_Type_Cd    ,
										 SRC.Partner_Site_Address_Line1_txt    ,
										 SRC.Partner_Site_Address_Line2_txt    ,
										 SRC.Partner_Site_Address_Line3_txt    ,
										 SRC.Partner_Site_Address_Line4_txt    ,
										 SRC.Partner_Site_Address_Line5_txt    ,
										 SRC.Partner_Site_City_Nm    ,
										 SRC.Partner_Site_County_Nm    ,
										 SRC.Partner_Site_County_Cd    ,
										 SRC.Partner_Site_Postal_Zone_Cd    ,
										 SRC.Partner_Site_State_Cd    ,
										 SRC.Partner_Site_State_Nm    ,
										 SRC.Partner_Site_Country_Cd    ,
										 SRC.Partner_Site_Country_Nm    ,
										 SRC.Partner_Site_Latitude_Dgr   ,
										 SRC.Partner_Site_Longitude_Dgr    ,
										 SRC.Partner_Site_TimeZone_Cd    ,
										 -- SRC.SiteAddress_PhoneNbr,
										 SRC.Partner_Site_Fax_Nbr    ,
										 SRC.Partner_Site_Comment_Txt    ,
										 SRC.Partner_Site_Effective_Time_Period_Type_Cd    ,
										 SRC.Partner_Site_First_Effective_Ts    ,
										 SRC.Partner_Site_Last_Effective_Ts    ,
										 SRC.Partner_Site_Create_Ts    ,
										 SRC.Partner_Site_Create_User_Id    ,
										 SRC.Partner_Site_Update_Ts    ,
										 SRC.Partner_Site_Update_User_Id    ,
										 SRC.Partner_Effective_Time_Period_Type_Cd    ,
										 SRC.Partner_First_Effective_Ts    ,
										 SRC.Partner_Last_Effective_Ts    ,
										 SRC.Partner_Create_Ts       ,
										 SRC.Partner_Create_User_Id    ,
										 SRC.Partner_Update_Ts       ,
										 SRC.Partner_Update_User_Id
									ORDER BY SRC.SiteAddress_PhoneNbr
									) as rn_1					
											FROM 
														(SELECT DISTINCT 
														Partner_Id              ,
														 Internal_Parnter_Ind    ,
														 Vendor_Id               ,
														 Wholesale_Customer_Nbr    ,
														 Customer_Site_Nbr       ,
														 Partner_Site_Id         ,
														 Partner_Participant_Id    ,
														 Partner_Site_Nm         ,
														 Partner_Site_Type_Cd    ,
														 Partner_Site_Type_Dsc    ,
														 Partner_Site_Type_Short_Dsc    ,
														 --Organization_Type_Cd    ,
														 --Organization_Value_Txt    ,
														 Partner_Site_Active_Ind    ,
														 Partner_Site_Status_Type_Cd    ,
														 Partner_Site_Status_Dsc    ,
														 Partner_Site_Status_Effective_Ts    ,
														 Partner_Site_Address_Usage_Type_Cd    ,
														 Partner_Site_Address_Line1_txt    ,
														 Partner_Site_Address_Line2_txt    ,
														 Partner_Site_Address_Line3_txt    ,
														 Partner_Site_Address_Line4_txt    ,
														 Partner_Site_Address_Line5_txt    ,
														 Partner_Site_City_Nm    ,
														 Partner_Site_County_Nm    ,
														 Partner_Site_County_Cd    ,
														 Partner_Site_Postal_Zone_Cd    ,
														 Partner_Site_State_Cd    ,
														 Partner_Site_State_Nm    ,
														 Partner_Site_Country_Cd    ,
														 Partner_Site_Country_Nm    ,
														 Partner_Site_Latitude_Dgr   ,
														 Partner_Site_Longitude_Dgr    ,
														 Partner_Site_TimeZone_Cd    ,
														 SiteAddress_PhoneNbr,
														 Partner_Site_Fax_Nbr    ,
														 Partner_Site_Comment_Txt    ,
														 Partner_Site_Effective_Time_Period_Type_Cd    ,
														 Partner_Site_First_Effective_Ts    ,
														 Partner_Site_Last_Effective_Ts    ,
														 Partner_Site_Create_Ts    ,
														 Partner_Site_Create_User_Id    ,
														 Partner_Site_Update_Ts    ,
														 Partner_Site_Update_User_Id    ,
														 Partner_Effective_Time_Period_Type_Cd    ,
														 Partner_First_Effective_Ts    ,
														 Partner_Last_Effective_Ts    ,
														 Partner_Create_Ts       ,
														 Partner_Create_User_Id    ,
														 Partner_Update_Ts       ,
														 Partner_Update_User_Id ,
														 ActionTypeCd,
														 creationdt,
														 fileName
														FROM ${src_wrk_tmp_tbl}
														)SRC
														)
														group by 
														Partner_Id              ,
														 Internal_Parnter_Ind    ,
														 Vendor_Id               ,
														 Wholesale_Customer_Nbr    ,
														 Customer_Site_Nbr       ,
														 Partner_Site_Id         ,
														 Partner_Participant_Id    ,
														 Partner_Site_Nm         ,
														 Partner_Site_Type_Cd    ,
														 Partner_Site_Type_Dsc    ,
														 Partner_Site_Type_Short_Dsc    ,
														 --Organization_Type_Cd    ,
														 --Organization_Value_Txt    ,
														 Partner_Site_Active_Ind    ,
														 Partner_Site_Status_Type_Cd    ,
														 Partner_Site_Status_Dsc    ,
														 Partner_Site_Status_Effective_Ts    ,
														 Partner_Site_Address_Usage_Type_Cd    ,
														 Partner_Site_Address_Line1_txt    ,
														 Partner_Site_Address_Line2_txt    ,
														 Partner_Site_Address_Line3_txt    ,
														 Partner_Site_Address_Line4_txt    ,
														 Partner_Site_Address_Line5_txt    ,
														 Partner_Site_City_Nm    ,
														 Partner_Site_County_Nm    ,
														 Partner_Site_County_Cd    ,
														 Partner_Site_Postal_Zone_Cd    ,
														 Partner_Site_State_Cd    ,
														 Partner_Site_State_Nm    ,
														 Partner_Site_Country_Cd    ,
														 Partner_Site_Country_Nm    ,
														 Partner_Site_Latitude_Dgr   ,
														 Partner_Site_Longitude_Dgr    ,
														 Partner_Site_TimeZone_Cd    ,
														 SiteAddress_PhoneNbr,
														 Partner_Site_Fax_Nbr    ,
														 Partner_Site_Comment_Txt    ,
														 Partner_Site_Effective_Time_Period_Type_Cd    ,
														 Partner_Site_First_Effective_Ts    ,
														 Partner_Site_Last_Effective_Ts    ,
														 Partner_Site_Create_Ts    ,
														 Partner_Site_Create_User_Id    ,
														 Partner_Site_Update_Ts    ,
														 Partner_Site_Update_User_Id    ,
														 Partner_Effective_Time_Period_Type_Cd    ,
														 Partner_First_Effective_Ts    ,
														 Partner_Last_Effective_Ts    ,
														 Partner_Create_Ts       ,
														 Partner_Create_User_Id    ,
														 Partner_Update_Ts       ,
														 Partner_Update_User_Id ,
														 ActionTypeCd,
														 creationdt,
														 fileName					

				)
														GROUP BY 
														Partner_Id              ,
														 Internal_Parnter_Ind    ,
														 Vendor_Id               ,
														 Wholesale_Customer_Nbr    ,
														 Customer_Site_Nbr       ,
														 Partner_Site_Id         ,
														 Partner_Participant_Id    ,
														 Partner_Site_Nm         ,
														 Partner_Site_Type_Cd    ,
														 Partner_Site_Type_Dsc    ,
														 Partner_Site_Type_Short_Dsc    ,
														 --Organization_Type_Cd    ,
														 --Organization_Value_Txt    ,
														 Partner_Site_Active_Ind    ,
														 Partner_Site_Status_Type_Cd    ,
														 Partner_Site_Status_Dsc    ,
														 Partner_Site_Status_Effective_Ts    ,
														 Partner_Site_Address_Usage_Type_Cd    ,
														 Partner_Site_Address_Line1_txt    ,
														 Partner_Site_Address_Line2_txt    ,
														 Partner_Site_Address_Line3_txt    ,
														 Partner_Site_Address_Line4_txt    ,
														 Partner_Site_Address_Line5_txt    ,
														 Partner_Site_City_Nm    ,
														 Partner_Site_County_Nm    ,
														 Partner_Site_County_Cd    ,
														 Partner_Site_Postal_Zone_Cd    ,
														 Partner_Site_State_Cd    ,
														 Partner_Site_State_Nm    ,
														 Partner_Site_Country_Cd    ,
														 Partner_Site_Country_Nm    ,
														 Partner_Site_Latitude_Dgr   ,
														 Partner_Site_Longitude_Dgr    ,
														 Partner_Site_TimeZone_Cd    ,
														 --SiteAddress_PhoneNbr,
														 Partner_Site_Fax_Nbr    ,
														 Partner_Site_Comment_Txt    ,
														 Partner_Site_Effective_Time_Period_Type_Cd    ,
														 Partner_Site_First_Effective_Ts    ,
														 Partner_Site_Last_Effective_Ts    ,
														 Partner_Site_Create_Ts    ,
														 Partner_Site_Create_User_Id    ,
														 Partner_Site_Update_Ts    ,
														 Partner_Site_Update_User_Id    ,
														 Partner_Effective_Time_Period_Type_Cd    ,
														 Partner_First_Effective_Ts    ,
														 Partner_Last_Effective_Ts    ,
														 Partner_Create_Ts       ,
														 Partner_Create_User_Id    ,
														 Partner_Update_Ts       ,
														 Partner_Update_User_Id ,
														 ActionTypeCd,
														 creationdt,
														 fileName		
					) 
				--WHERE RN = 1
				) 
				WHERE  RN = 1
					
				)SRCC
				JOIN ( 
				SELECT NVL(MAX(Business_Partner_Integration_Id),0) as mx_ci_intg_id 
				FROM ${tgt_tbl} 
				WHERE DW_CURRENT_VERSION_IND = TRUE) 
				mx on 1=1
				
				LEFT JOIN
				( select
 				 Business_Partner_Integration_Id,
				 Partner_Id              ,
				 Internal_Parnter_Ind    ,
				 Vendor_Id               ,
				 Wholesale_Customer_Nbr    ,
				 Customer_Site_Nbr       ,
				 Partner_Site_Id         ,
				 Partner_Participant_Id    ,
				 Partner_Site_Nm         ,
				 Partner_Site_Type_Cd    ,
				 Partner_Site_Type_Dsc    ,
				 Partner_Site_Type_Short_Dsc    ,
				 --Organization_Type_Cd    ,
				 --Organization_Value_Txt    ,
				 Partner_Site_Active_Ind    ,
				 Partner_Site_Status_Type_Cd    ,
				 Partner_Site_Status_Dsc    ,
				 Partner_Site_Status_Effective_Ts    ,
				 Partner_Site_Address_Usage_Type_Cd    ,
				 Partner_Site_Address_Line1_txt    ,
				 Partner_Site_Address_Line2_txt    ,
				 Partner_Site_Address_Line3_txt    ,
				 Partner_Site_Address_Line4_txt    ,
				 Partner_Site_Address_Line5_txt    ,
				 Partner_Site_City_Nm    ,
				 Partner_Site_County_Nm    ,
				 Partner_Site_County_Cd    ,
				 Partner_Site_Postal_Zone_Cd    ,
				 Partner_Site_State_Cd    ,
				 Partner_Site_State_Nm    ,
				 Partner_Site_Country_Cd    ,
				 Partner_Site_Country_Nm    ,
				 Partner_Site_Latitude_Dgr   ,
				 Partner_Site_Longitude_Dgr    ,
				 Partner_Site_TimeZone_Cd    ,
				 Partner_Site_Phone1_Nbr,
				 Partner_Site_Phone2_Nbr,
				 Partner_Site_Phone3_Nbr,
				 Partner_Site_Fax_Nbr    ,
				 Partner_Site_Comment_Txt    ,
				 Partner_Site_Effective_Time_Period_Type_Cd    ,
				 Partner_Site_First_Effective_Ts    ,
				 Partner_Site_Last_Effective_Ts    ,
				 Partner_Site_Create_Ts    ,
				 Partner_Site_Create_User_Id    ,
				 Partner_Site_Update_Ts    ,
				 Partner_Site_Update_User_Id    ,
				 Partner_Effective_Time_Period_Type_Cd    ,
				 Partner_First_Effective_Ts    ,
			     Partner_Last_Effective_Ts    ,
				 Partner_Create_Ts       ,
				 Partner_Create_User_Id    ,
				 Partner_Update_Ts       ,
				 Partner_Update_User_Id ,	
				 tgt.DW_Logical_delete_ind,
				 tgt.DW_First_Effective_dt								
				From ${tgt_tbl} tgt
				where tgt.DW_CURRENT_VERSION_IND = TRUE
				) as tgt
				 ON ((NVL(tgt.Partner_Id,'-1') = NVL(SRCC.Partner_Id,'-1')
				 AND NVL(tgt.Partner_Site_Id,'-1') = NVL(SRCC.Partner_Site_Id,'-1')
				 AND NVL(tgt.Partner_Participant_Id,'-1') = NVL(SRCC.Partner_Participant_Id,'-1'))
				OR (NVL(tgt.Partner_Site_Id,'-1') = NVL(SRCC.Partner_Site_Id,'-1')
				 AND NVL(tgt.Partner_Participant_Id,'-1') = NVL(SRCC.Partner_Participant_Id,'-1')))
				
				WHERE tgt.Business_Partner_Integration_Id is NULL
				OR (NVL(SRCC.Internal_Parnter_Ind,null) <> NVL(tgt.Internal_Parnter_Ind,null) OR
				NVL(SRCC.Vendor_Id,'-1') <> NVL(tgt.Vendor_Id,'-1') OR
				NVL(SRCC.Wholesale_Customer_Nbr,'-1') <> NVL(tgt.Wholesale_Customer_Nbr,'-1') OR
				NVL(SRCC.Customer_Site_Nbr,'-1') <> NVL(tgt.Customer_Site_Nbr,'-1') OR
				NVL(SRCC.Partner_Site_Nm,'-1') <> NVL(tgt.Partner_Site_Nm,'-1') OR
				NVL(SRCC.Partner_Site_Type_Cd,'-1') <> NVL(tgt.Partner_Site_Type_Cd,'-1') OR
				NVL(SRCC.Partner_Site_Type_Dsc,'-1') <> NVL(tgt.Partner_Site_Type_Dsc,'-1') OR
				NVL(SRCC.Partner_Site_Type_Short_Dsc,'-1') <> NVL(tgt.Partner_Site_Type_Short_Dsc,'-1') OR
				--NVL(SRCC.Organization_Type_Cd,'-1') <> NVL(tgt.Organization_Type_Cd,'-1') OR
				--NVL(SRCC.Organization_Value_Txt,'-1') <> NVL(tgt.Organization_Value_Txt,'-1') OR
				NVL(SRCC.Partner_Site_Active_Ind,null) <> NVL(tgt.Partner_Site_Active_Ind,null) OR
				NVL(SRCC.Partner_Site_Status_Type_Cd,'-1') <> NVL(tgt.Partner_Site_Status_Type_Cd,'-1') OR
				NVL(SRCC.Partner_Site_Status_Dsc,'-1') <> NVL(tgt.Partner_Site_Status_Dsc,'-1') OR
				NVL(SRCC.Partner_Site_Status_Effective_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Partner_Site_Status_Effective_Ts,'9999-12-31 00:00:00.000') OR
				NVL(SRCC.Partner_Site_Address_Usage_Type_Cd,'-1') <> NVL(tgt.Partner_Site_Address_Usage_Type_Cd,'-1') OR
				NVL(SRCC.Partner_Site_Address_Line1_txt,'-1') <> NVL(tgt.Partner_Site_Address_Line1_txt,'-1') OR
				NVL(SRCC.Partner_Site_Address_Line2_txt,'-1') <> NVL(tgt.Partner_Site_Address_Line2_txt,'-1') OR
				NVL(SRCC.Partner_Site_Address_Line3_txt,'-1') <> NVL(tgt.Partner_Site_Address_Line3_txt,'-1') OR
				NVL(SRCC.Partner_Site_Address_Line4_txt,'-1') <> NVL(tgt.Partner_Site_Address_Line4_txt,'-1') OR
				NVL(SRCC.Partner_Site_Address_Line5_txt,'-1') <> NVL(tgt.Partner_Site_Address_Line5_txt,'-1') OR
				NVL(SRCC.Partner_Site_City_Nm,'-1') <> NVL(tgt.Partner_Site_City_Nm,'-1') OR
				NVL(SRCC.Partner_Site_County_Nm,'-1') <> NVL(tgt.Partner_Site_County_Nm,'-1') OR
				NVL(SRCC.Partner_Site_County_Cd,'-1') <> NVL(tgt.Partner_Site_County_Cd,'-1') OR
				NVL(SRCC.Partner_Site_Postal_Zone_Cd,'-1') <> NVL(tgt.Partner_Site_Postal_Zone_Cd,'-1') OR
				NVL(SRCC.Partner_Site_State_Cd,'-1') <> NVL(tgt.Partner_Site_State_Cd,'-1') OR
				NVL(SRCC.Partner_Site_State_Nm,'-1') <> NVL(tgt.Partner_Site_State_Nm,'-1') OR
				NVL(SRCC.Partner_Site_Country_Cd,'-1') <> NVL(tgt.Partner_Site_Country_Cd,'-1') OR
				NVL(SRCC.Partner_Site_Country_Nm,'-1') <> NVL(tgt.Partner_Site_Country_Nm,'-1') OR
				NVL(SRCC.Partner_Site_Latitude_Dgr,'-1') <> NVL(tgt.Partner_Site_Latitude_Dgr,'-1') OR
				NVL(SRCC.Partner_Site_Longitude_Dgr,'-1') <> NVL(tgt.Partner_Site_Longitude_Dgr,'-1') OR
				NVL(SRCC.Partner_Site_TimeZone_Cd,'-1') <> NVL(tgt.Partner_Site_TimeZone_Cd,'-1') OR
				NVL(SRCC.Partner_Site_Phone1_Nbr,'-1') <> NVL(tgt.Partner_Site_Phone1_Nbr,'-1') OR
				NVL(SRCC.Partner_Site_Phone2_Nbr,'-1') <> NVL(tgt.Partner_Site_Phone2_Nbr,'-1') OR
				NVL(SRCC.Partner_Site_Phone3_Nbr,'-1') <> NVL(tgt.Partner_Site_Phone3_Nbr,'-1') OR
				NVL(SRCC.Partner_Site_Fax_Nbr,'-1') <> NVL(tgt.Partner_Site_Fax_Nbr,'-1') OR
				NVL(SRCC.Partner_Site_Comment_Txt,'-1') <> NVL(tgt.Partner_Site_Comment_Txt,'-1') OR
				NVL(SRCC.Partner_Site_Effective_Time_Period_Type_Cd,'-1') <> NVL(tgt.Partner_Site_Effective_Time_Period_Type_Cd,'-1') OR
				NVL(SRCC.Partner_Site_First_Effective_Ts,'9999-12-31') <> NVL(tgt.Partner_Site_First_Effective_Ts,'9999-12-31') OR
				NVL(SRCC.Partner_Site_Last_Effective_Ts,'9999-12-31') <> NVL(tgt.Partner_Site_Last_Effective_Ts,'9999-12-31') OR
				NVL(SRCC.Partner_Site_Create_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Partner_Site_Create_Ts,'9999-12-31 00:00:00.000') OR
				NVL(SRCC.Partner_Site_Create_User_Id,'-1') <> NVL(tgt.Partner_Site_Create_User_Id,'-1') OR
				NVL(SRCC.Partner_Site_Update_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Partner_Site_Update_Ts,'9999-12-31 00:00:00.000') OR
				NVL(SRCC.Partner_Site_Update_User_Id,'-1') <> NVL(tgt.Partner_Site_Update_User_Id,'-1') OR
				NVL(SRCC.Partner_Effective_Time_Period_Type_Cd,'-1') <> NVL(tgt.Partner_Effective_Time_Period_Type_Cd,'-1') OR
				NVL(SRCC.Partner_First_Effective_Ts,'9999-12-31') <> NVL(tgt.Partner_First_Effective_Ts,'9999-12-31') OR
				NVL(SRCC.Partner_Last_Effective_Ts,'9999-12-31') <> NVL(tgt.Partner_Last_Effective_Ts,'9999-12-31') OR
				NVL(SRCC.Partner_Create_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Partner_Create_Ts,'9999-12-31 00:00:00.000') OR
				NVL(SRCC.Partner_Create_User_Id,'-1') <> NVL(tgt.Partner_Create_User_Id,'-1') OR
				NVL(SRCC.Partner_Update_Ts,'9999-12-31 00:00:00.000') <> NVL(tgt.Partner_Update_Ts,'9999-12-31 00:00:00.000') OR
				NVL(SRCC.Partner_Update_User_Id,'-1') <> NVL(tgt.Partner_Update_User_Id,'-1') OR				
				SRCC.DW_Logical_delete_ind <> tgt.DW_Logical_delete_ind
				)										
				) seq_gen
				)`;
   				   									     									     
														 
try {
        snowflake.execute (
    {sqlText: sql_crt_tgt_wrk_tbl  }
    );
        }
catch (err)  {
        return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
 }




    //SCD Type2 transaction begins
    var sql_begin = "BEGIN"
    // Processing Updates of Type 2 SCD
    var sql_updates = `UPDATE `+ tgt_tbl +` as tgt
        SET DW_Last_Effective_dt = CURRENT_DATE - 1
        ,DW_CURRENT_VERSION_IND = FALSE
        ,DW_LAST_UPDATE_TS = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
        ,DW_SOURCE_UPDATE_NM = filename
    FROM (
        SELECT Business_Partner_Integration_Id
        ,max(filename) as filename
        FROM `+ tgt_wrk_tbl +`
        WHERE DML_Type = 'U'
        AND Sameday_chg_ind = 0
		group by Business_Partner_Integration_Id
    ) src
    WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
    AND tgt.DW_CURRENT_VERSION_IND = TRUE`;
	    		 
	    	        
	 var sql_sameday = // Processing Sameday updates
	`UPDATE ` + tgt_tbl + ` as tgt
    SET  Partner_Id= src.Partner_Id,
		 Internal_Parnter_Ind= src.Internal_Parnter_Ind,
		 Vendor_Id= src.Vendor_Id,
		 Wholesale_Customer_Nbr= src.Wholesale_Customer_Nbr,
         Customer_Site_Nbr= src.Customer_Site_Nbr,
		 Partner_Site_Id= src.Partner_Site_Id,
         Partner_Participant_Id= src.Partner_Participant_Id,
		 Partner_Site_Nm= src.Partner_Site_Nm,
         Partner_Site_Type_Cd= src.Partner_Site_Type_Cd,
		 Partner_Site_Type_Dsc= src.Partner_Site_Type_Dsc,
         Partner_Site_Type_Short_Dsc= src.Partner_Site_Type_Short_Dsc,
		 --Organization_Type_Cd= src.Organization_Type_Cd,
         --Organization_Value_Txt= src.Organization_Value_Txt,
		 Partner_Site_Active_Ind= src.Partner_Site_Active_Ind,
         Partner_Site_Status_Type_Cd= src.Partner_Site_Status_Type_Cd,
		 Partner_Site_Status_Dsc= src.Partner_Site_Status_Dsc,
         Partner_Site_Status_Effective_Ts= src.Partner_Site_Status_Effective_Ts,
		 Partner_Site_Address_Usage_Type_Cd= src.Partner_Site_Address_Usage_Type_Cd,
         Partner_Site_Address_Line1_txt= src.Partner_Site_Address_Line1_txt,
		 Partner_Site_Address_Line2_txt= src.Partner_Site_Address_Line2_txt,
         Partner_Site_Address_Line3_txt= src.Partner_Site_Address_Line3_txt,
		 Partner_Site_Address_Line4_txt= src.Partner_Site_Address_Line4_txt,
         Partner_Site_Address_Line5_txt= src.Partner_Site_Address_Line5_txt,
		 Partner_Site_City_Nm= src.Partner_Site_City_Nm,
         Partner_Site_County_Nm= src.Partner_Site_County_Nm,
		 Partner_Site_County_Cd= src.Partner_Site_County_Cd,
         Partner_Site_Postal_Zone_Cd= src.Partner_Site_Postal_Zone_Cd,
		 Partner_Site_State_Cd= src.Partner_Site_State_Cd,
         Partner_Site_State_Nm= src.Partner_Site_State_Nm,
		 Partner_Site_Country_Cd= src.Partner_Site_Country_Cd,
         Partner_Site_Country_Nm= src.Partner_Site_Country_Nm,
		 Partner_Site_Latitude_Dgr= src.Partner_Site_Latitude_Dgr,
         Partner_Site_Longitude_Dgr= src.Partner_Site_Longitude_Dgr,
		 Partner_Site_TimeZone_Cd= src.Partner_Site_TimeZone_Cd,
         Partner_Site_Phone1_Nbr= src.Partner_Site_Phone1_Nbr,
		 Partner_Site_Phone2_Nbr= src.Partner_Site_Phone2_Nbr,
         Partner_Site_Phone3_Nbr= src.Partner_Site_Phone3_Nbr,
		 Partner_Site_Fax_Nbr= src.Partner_Site_Fax_Nbr,
         Partner_Site_Comment_Txt= src.Partner_Site_Comment_Txt,
		 Partner_Site_Effective_Time_Period_Type_Cd= src.Partner_Site_Effective_Time_Period_Type_Cd,
         Partner_Site_First_Effective_Ts= src.Partner_Site_First_Effective_Ts,
		 Partner_Site_Last_Effective_Ts= src.Partner_Site_Last_Effective_Ts,
         Partner_Site_Create_Ts= src.Partner_Site_Create_Ts,
		 Partner_Site_Create_User_Id= src.Partner_Site_Create_User_Id,
         Partner_Site_Update_Ts= src.Partner_Site_Update_Ts,
		 Partner_Site_Update_User_Id= src.Partner_Site_Update_User_Id,
         Partner_Effective_Time_Period_Type_Cd= src.Partner_Effective_Time_Period_Type_Cd,
		 Partner_First_Effective_Ts= src.Partner_First_Effective_Ts,
         Partner_Last_Effective_Ts= src.Partner_Last_Effective_Ts,
		 Partner_Create_Ts= src.Partner_Create_Ts,
		 Partner_Create_User_Id= src.Partner_Create_User_Id,
         Partner_Update_Ts= src.Partner_Update_Ts,
		 Partner_Update_User_Id= src.Partner_Update_User_Id,
         DW_Logical_delete_ind = src.DW_Logical_delete_ind,
         DW_LAST_UPDATE_TS = TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP),
         DW_SOURCE_UPDATE_NM = filename		 	     	
    FROM (              
        SELECT 
		     Business_Partner_Integration_Id,
				 Partner_Id              ,
				 Internal_Parnter_Ind    ,
				 Vendor_Id               ,
				 Wholesale_Customer_Nbr    ,
				 Customer_Site_Nbr       ,
				 Partner_Site_Id         ,
				 Partner_Participant_Id    ,
				 Partner_Site_Nm         ,
				 Partner_Site_Type_Cd    ,
				 Partner_Site_Type_Dsc    ,
				 Partner_Site_Type_Short_Dsc    ,
				 --Organization_Type_Cd    ,
				 --Organization_Value_Txt    ,
				 Partner_Site_Active_Ind    ,
				 Partner_Site_Status_Type_Cd    ,
				 Partner_Site_Status_Dsc    ,
				 Partner_Site_Status_Effective_Ts    ,
				 Partner_Site_Address_Usage_Type_Cd    ,
				 Partner_Site_Address_Line1_txt    ,
				 Partner_Site_Address_Line2_txt    ,
				 Partner_Site_Address_Line3_txt    ,
				 Partner_Site_Address_Line4_txt    ,
				 Partner_Site_Address_Line5_txt    ,
				 Partner_Site_City_Nm    ,
				 Partner_Site_County_Nm    ,
				 Partner_Site_County_Cd    ,
				 Partner_Site_Postal_Zone_Cd    ,
				 Partner_Site_State_Cd    ,
				 Partner_Site_State_Nm    ,
				 Partner_Site_Country_Cd    ,
				 Partner_Site_Country_Nm    ,
				 Partner_Site_Latitude_Dgr   ,
				 Partner_Site_Longitude_Dgr    ,
				 Partner_Site_TimeZone_Cd    ,
				 Partner_Site_Phone1_Nbr,
				 Partner_Site_Phone2_Nbr,
				 Partner_Site_Phone3_Nbr,
				 Partner_Site_Fax_Nbr    ,
				 Partner_Site_Comment_Txt    ,
				 Partner_Site_Effective_Time_Period_Type_Cd    ,
				 Partner_Site_First_Effective_Ts    ,
				 Partner_Site_Last_Effective_Ts    ,
				 Partner_Site_Create_Ts    ,
				 Partner_Site_Create_User_Id    ,
				 Partner_Site_Update_Ts    ,
				 Partner_Site_Update_User_Id    ,
				 Partner_Effective_Time_Period_Type_Cd    ,
				 Partner_First_Effective_Ts    ,
			     Partner_Last_Effective_Ts    ,
				 Partner_Create_Ts       ,
				 Partner_Create_User_Id    ,
				 Partner_Update_Ts       ,
				 Partner_Update_User_Id ,	
				 DW_Logical_delete_ind,
				 filename
				FROM  `+ tgt_wrk_tbl +` 
				WHERE DML_Type = 'U'
				AND Sameday_chg_ind = 1
				) src
				WHERE tgt.Business_Partner_Integration_Id = src.Business_Partner_Integration_Id
				AND tgt.DW_CURRENT_VERSION_IND = TRUE `;	
	
	// Processing Inserts
    var sql_inserts = `
	INSERT INTO ` + tgt_tbl + `
				(Business_Partner_Integration_Id,
				 DW_First_Effective_Dt ,
				 DW_Last_Effective_Dt,  				
				 Partner_Id              ,
				 Internal_Parnter_Ind    ,
				 Vendor_Id               ,
				 Wholesale_Customer_Nbr    ,
				 Customer_Site_Nbr       ,
				 Partner_Site_Id         ,
				 Partner_Participant_Id    ,
				 Partner_Site_Nm         ,
				 Partner_Site_Type_Cd    ,
				 Partner_Site_Type_Dsc    ,
				 Partner_Site_Type_Short_Dsc    ,
				 --Organization_Type_Cd    ,
				 --Organization_Value_Txt    ,
				 Partner_Site_Active_Ind    ,
				 Partner_Site_Status_Type_Cd    ,
				 Partner_Site_Status_Dsc    ,
				 Partner_Site_Status_Effective_Ts    ,
				 Partner_Site_Address_Usage_Type_Cd    ,
				 Partner_Site_Address_Line1_txt    ,
				 Partner_Site_Address_Line2_txt    ,
				 Partner_Site_Address_Line3_txt    ,
				 Partner_Site_Address_Line4_txt    ,
				 Partner_Site_Address_Line5_txt    ,
				 Partner_Site_City_Nm    ,
				 Partner_Site_County_Nm    ,
				 Partner_Site_County_Cd    ,
				 Partner_Site_Postal_Zone_Cd    ,
				 Partner_Site_State_Cd    ,
				 Partner_Site_State_Nm    ,
				 Partner_Site_Country_Cd    ,
				 Partner_Site_Country_Nm    ,
				 Partner_Site_Latitude_Dgr   ,
				 Partner_Site_Longitude_Dgr    ,
				 Partner_Site_TimeZone_Cd    ,
				 Partner_Site_Phone1_Nbr,
				 Partner_Site_Phone2_Nbr,
				 Partner_Site_Phone3_Nbr,
				 Partner_Site_Fax_Nbr    ,
				 Partner_Site_Comment_Txt    ,
				 Partner_Site_Effective_Time_Period_Type_Cd    ,
				 Partner_Site_First_Effective_Ts    ,
				 Partner_Site_Last_Effective_Ts    ,
				 Partner_Site_Create_Ts    ,
				 Partner_Site_Create_User_Id    ,
				 Partner_Site_Update_Ts    ,
				 Partner_Site_Update_User_Id    ,
				 Partner_Effective_Time_Period_Type_Cd    ,
				 Partner_First_Effective_Ts    ,
			     Partner_Last_Effective_Ts    ,
				 Partner_Create_Ts       ,
				 Partner_Create_User_Id    ,
				 Partner_Update_Ts       ,
				 Partner_Update_User_Id ,	
				 DW_CREATE_TS ,         
				 DW_LOGICAL_DELETE_IND,  
				 DW_SOURCE_CREATE_NM,   
				 DW_CURRENT_VERSION_IND  
		
    )
			SELECT Distinct
				Business_Partner_Integration_Id,
				CURRENT_DATE,
				 '31-DEC-9999',				 
				 Partner_Id              ,
				 Internal_Parnter_Ind    ,
				 Vendor_Id               ,
				 Wholesale_Customer_Nbr    ,
				 Customer_Site_Nbr       ,
				 Partner_Site_Id         ,
				 Partner_Participant_Id    ,
				 Partner_Site_Nm         ,
				 Partner_Site_Type_Cd    ,
				 Partner_Site_Type_Dsc    ,
				 Partner_Site_Type_Short_Dsc    ,
				 --Organization_Type_Cd    ,
				 --Organization_Value_Txt    ,
				 Partner_Site_Active_Ind    ,
				 Partner_Site_Status_Type_Cd    ,
				 Partner_Site_Status_Dsc    ,
				 Partner_Site_Status_Effective_Ts    ,
				 Partner_Site_Address_Usage_Type_Cd    ,
				 Partner_Site_Address_Line1_txt    ,
				 Partner_Site_Address_Line2_txt    ,
				 Partner_Site_Address_Line3_txt    ,
				 Partner_Site_Address_Line4_txt    ,
				 Partner_Site_Address_Line5_txt    ,
				 Partner_Site_City_Nm    ,
				 Partner_Site_County_Nm    ,
				 Partner_Site_County_Cd    ,
				 Partner_Site_Postal_Zone_Cd    ,
				 Partner_Site_State_Cd    ,
				 Partner_Site_State_Nm    ,
				 Partner_Site_Country_Cd    ,
				 Partner_Site_Country_Nm    ,
				 Partner_Site_Latitude_Dgr   ,
				 Partner_Site_Longitude_Dgr    ,
				 Partner_Site_TimeZone_Cd    ,
				 Partner_Site_Phone1_Nbr,
				 Partner_Site_Phone2_Nbr,
				 Partner_Site_Phone3_Nbr,
				 Partner_Site_Fax_Nbr    ,
				 Partner_Site_Comment_Txt    ,
				 Partner_Site_Effective_Time_Period_Type_Cd    ,
				 Partner_Site_First_Effective_Ts    ,
				 Partner_Site_Last_Effective_Ts    ,
				 Partner_Site_Create_Ts    ,
				 Partner_Site_Create_User_Id    ,
				 Partner_Site_Update_Ts    ,
				 Partner_Site_Update_User_Id    ,
				 Partner_Effective_Time_Period_Type_Cd    ,
				 Partner_First_Effective_Ts    ,
			     Partner_Last_Effective_Ts    ,
				 Partner_Create_Ts       ,
				 Partner_Create_User_Id    ,
				 Partner_Update_Ts       ,
				 Partner_Update_User_Id ,	
				 CURRENT_TIMESTAMP,
				 DW_LOGICAL_DELETE_IND,
				 filename,
				 TRUE
				 FROM `+ tgt_wrk_tbl +`
				 WHERE Sameday_chg_ind = 0
	--and Business_Partner_Integration_Id is not null
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
        return "Loading of Ap invoice_HEADER table "+ tgt_tbl + " Failed with error: " + err;   // Return a error message.
        }

						
// **************	Load for Business_Partner table ENDs *****************

$$;
