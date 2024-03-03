--liquibase formatted sql
--changeset SYSTEM:PET_PROFILE_REPORT runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database <<EDM_VIEW_NAME>>;
use schema <<EDM_VIEW_NAME>>.DW_VIEWS;

create or replace view PET_PROFILE_REPORT(
	HOUSEHOLD_ID,
	PET_ID,
	PET_NAME,
	PET_TYPE,
	BREED_NAME,
	GENDER,
	CELEBRATION_TYPE,
	CELEBRATION_DATE,
	STERILIZATION_STATUS,
	ALLERGY_NAME,
	MEDICAL_CONDITION,
	MEDICATION_NAME,
	PROFILE_DELETE_INDICATOR,
	CREATE_TS,
	UPDATE_TS
) as 
select  
PP.Household_Id,
PP.Pet_Id,
PP.Pet_Nm,
PP.Pet_Type_Cd,
PB.Breed_Nm ,
PP.Pet_Sex_Cd,
PP.Celebration_Type_Cd,
PP.Celebration_Dt,
PP.Sterilization_Status_Ind,
listagg(distinct PA.Allergy_Nm,',') within group (order by PA.Allergy_Nm),
listagg(distinct PMC.Medical_Condition_Nm,',') within group (order by PMC.Medical_Condition_Nm),
listagg(distinct PM.Medication_Nm,',') within group (order by PM.Medication_Nm),
PP.Profile_Delete_Ind,
PP.CREATE_TS,
PP.UPDATE_TS
from EDM_CONFIRMED_PRD.dw_c_loyalty.pet_profile PP 
left JOIN EDM_CONFIRMED_PRD.dw_c_loyalty.PET_BREED PB ON PP.Household_Id = PB.Household_Id AND
PP.Pet_Id = PB.Pet_Id and PP.DW_CURRENT_VERSION_IND = TRUE and PB.DW_CURRENT_VERSION_IND = TRUE
left JOIN  EDM_CONFIRMED_PRD.dw_c_loyalty.pet_allergy PA ON PP.Household_Id = PA.Household_Id AND
PP.Pet_Id = PA.Pet_Id and PA.DW_CURRENT_VERSION_IND = TRUE
left JOIN  EDM_CONFIRMED_PRD.dw_c_loyalty.PET_MEDICAL_CONDITION PMC ON PP.Household_Id = PMC.Household_Id AND
PP.Pet_Id = PMC.Pet_Id and PMC.DW_CURRENT_VERSION_IND = TRUE
left JOIN  EDM_CONFIRMED_PRD.dw_c_loyalty.PET_MEDICATION PM ON PP.Household_Id = PM.Household_Id AND
PP.Pet_Id = PM.Pet_Id And PM.DW_CURRENT_VERSION_IND = TRUE
group by PP.Household_Id,PP.Pet_Id,PP.Pet_Nm,PP.Pet_Type_Cd,PB.Breed_Nm ,PP.Pet_Sex_Cd,PP.Celebration_Type_Cd,PP.Celebration_Dt,
PP.Sterilization_Status_Ind,PP.Profile_Delete_Ind,PP.CREATE_TS,
PP.UPDATE_TS;
