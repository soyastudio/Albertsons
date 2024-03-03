--liquibase formatted sql
--changeset SYSTEM:SP_GETPET_PROFILE_FLAT_TO_BIM_PET_PROFILE runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database <<EDM_DB_NAME>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETPET_PROFILE_FLAT_TO_BIM_PET_PROFILE("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_LOYAL" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$

var ref_db = "<<EDM_DB_NAME_R>>" ; 
var ref_schema = "DW_R_LOYALTY";
var cnf_db = CNF_DB ;
var wrk_schema = C_STAGE ;
var cnf_schema = C_LOYAL;
var src_wrk_tbl = SRC_WRK_TBL;

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Pet_Profile_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Pet_Profile`;
var src_flat_tbl =`${ref_db}.${ref_schema}.GetPetProfile_Flat`;

// ************************************ Truncate and Reload the work table ****************************************
 var truncate_tgt_wrk_table = `Truncate Table ${tgt_wrk_tbl}`;
try {
snowflake.execute ({ sqlText: truncate_tgt_wrk_table });
	}
    catch (err) {
        return `Truncate of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}

// ************** Load for Pet_Profile table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var sql_command =  `INSERT INTO ${tgt_wrk_tbl}
		                WITH src_wrk_tbl_recs as

                            (
						
select distinct hhid as Household_Id,
                id as Pet_Id,
				name as Pet_Nm,
                CURRENT_DATE Dw_First_Effective_Dt,
                '31-DEC-9999' Dw_Last_Effective_Dt,				
				type as Pet_Type_Cd,
				sex as Pet_Sex_Cd,
				celebrationType as Celebration_Type_Cd,
				celebrationDate as Celebration_Dt,
				neuturedSpayedStatus as Sterilization_Status_Ind,
				deleteIndicator as Profile_Delete_Ind,
				createdBy as Create_User_Id,
				creationTs as Create_Ts,
				modifiedBy as Update_User_Id,
				modifiedTs as Update_Ts,
				MODIFIEDTS,
                CURRENT_TIMESTAMP Dw_Create_Ts ,
                FileName Dw_Source_Create_Nm
            ,Row_number() OVER (
			PARTITION BY Household_Id,Pet_Id ORDER BY (Update_Ts) DESC
			) AS rn                
from `+ src_wrk_tbl +`
where Household_Id is not null and Pet_Id is not null
) 
select 
 src.Household_Id
,src.Pet_Id
,src.Pet_Nm
,src.Pet_Type_Cd
,src.Pet_Sex_Cd
,src.Celebration_Type_Cd
,src.Celebration_Dt
,src.Sterilization_Status_Ind
,src.Profile_Delete_Ind
,src.Create_User_Id
,src.Create_Ts
,src.Update_User_Id
,src.Update_Ts
,src.DW_Logical_delete_ind
,src.Dw_Source_Create_Nm
,CASE WHEN tgt.Household_Id IS NULL  AND tgt.Pet_Id IS NULL THEN 'I' ELSE 'U' END AS DML_Type
,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
from 
(
select 
Household_Id
,Pet_Id 
,Pet_Nm
,Pet_Type_Cd
,Pet_Sex_Cd
,Celebration_Type_Cd
,Celebration_Dt
,Sterilization_Status_Ind
,Profile_Delete_Ind
,Create_User_Id
,Create_Ts
,Update_User_Id
,Update_Ts
,false AS DW_Logical_delete_ind
,Dw_Source_Create_Nm
FROM src_wrk_tbl_recs
where rn=1
and Household_Id is not null
and Pet_Id is not null
)src
LEFT JOIN (
SELECT
tgt.Household_Id
,tgt.Pet_Id
,tgt.Pet_Nm
,tgt.Pet_Type_Cd
,tgt.Pet_Sex_Cd
,tgt.Celebration_Type_Cd
,tgt.Celebration_Dt
,tgt.Sterilization_Status_Ind
,tgt.Profile_Delete_Ind
,tgt.Create_User_Id
,tgt.Create_Ts
,tgt.Update_User_Id
,tgt.Update_Ts
,tgt.DW_Logical_delete_ind
,tgt.Dw_Source_Create_Nm
,tgt.dw_first_effective_dt
FROM ${tgt_tbl} tgt
WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
) as tgt 
ON tgt.Household_Id = src.Household_Id
AND tgt.Pet_Id = src.Pet_Id
 
where (tgt.Household_Id IS NULL  AND tgt.Pet_Id IS NULL )
OR ( 
     NVL(src.Pet_Nm,'-1') <> NVL(tgt.Pet_Nm,'-1') 
	OR NVL(src.Pet_Type_Cd,'-1') <> NVL(tgt.Pet_Type_Cd,'-1')
	OR NVL(src.Pet_Sex_Cd,'-1') <> NVL(tgt.Pet_Sex_Cd,'-1')
	OR NVL(src.Celebration_Type_Cd,'-1') <> NVL(tgt.Celebration_Type_Cd,'-1')
	OR NVL(to_date(src.Celebration_Dt),'9999-12-31') <> NVL(tgt.Celebration_Dt,'9999-12-31')
	OR NVL(to_boolean(src.Sterilization_Status_Ind),-1) <> NVL(tgt.Sterilization_Status_Ind,-1)
	OR NVL(to_boolean(src.Profile_Delete_Ind),-1) <> NVL(tgt.Profile_Delete_Ind,-1)
	OR NVL(src.Create_User_Id,'-1') <> NVL(tgt.Create_User_Id,'-1')
	OR NVL(to_timestamp(src.Create_Ts),'9999-12-31 00:00:00.000') <> NVL(tgt.Create_Ts,'9999-12-31 00:00:00.000')
	OR NVL(src.Update_User_Id,'-1') <> NVL(tgt.Update_User_Id,'-1')
	OR NVL(to_timestamp(src.Update_Ts),'9999-12-31 00:00:00.000') <> NVL(tgt.Update_Ts,'9999-12-31 00:00:00.000')
	
 OR (src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND)
)
`;
try {
        
        snowflake.execute ({sqlText: sql_command});
            
        }
    catch (err)  {
        throw "Creation of Pet_Allergy_wrk work table Failed with error: "+ err;   // Return a error message.
        }
var sql_begin = 'BEGIN'


//SCD Type2 transaction begins
// Processing Different Day Updates of Type 2 SCD

var sql_updates =
`UPDATE ${tgt_tbl} as tgt
 SET DW_Last_Effective_dt = CURRENT_DATE-1
,DW_CURRENT_VERSION_IND = FALSE
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = src.DW_SOURCE_CREATE_NM
FROM ( SELECT
 Household_Id
,Pet_Id 
,Pet_Nm
,Pet_Type_Cd
,Pet_Sex_Cd
,Celebration_Type_Cd
,Celebration_Dt
,Sterilization_Status_Ind
,Profile_Delete_Ind
,Create_User_Id
,Create_Ts
,Update_User_Id
,Update_Ts
,DW_SOURCE_CREATE_NM
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 0
AND Household_Id  IS NOT NULL
AND Pet_Id IS NOT NULL
) src
WHERE tgt.Household_Id = src.Household_Id  
AND tgt.Pet_Id = src.Pet_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE
AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;
   

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
SET  
 Pet_Nm  =src.Pet_Nm                     
,Pet_Type_Cd = src.Pet_Type_Cd
,Pet_Sex_Cd = src.Pet_Sex_Cd
,Celebration_Type_Cd = src.Celebration_Type_Cd
,Celebration_Dt = src.Celebration_Dt
,Sterilization_Status_Ind = src.Sterilization_Status_Ind
,Profile_Delete_Ind = src.Profile_Delete_Ind
,Create_User_Id = src.Create_User_Id
,Create_Ts = src.Create_Ts
,Update_User_Id = src.Update_User_Id
,Update_Ts = src.Update_Ts
,DW_LOGICAL_DELETE_IND = src.DW_LOGICAL_DELETE_IND 
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = src.DW_SOURCE_CREATE_NM
FROM 
( SELECT
Household_Id
,Pet_Id 
,Pet_Nm
,Pet_Type_Cd
,Pet_Sex_Cd
,Celebration_Type_Cd
,Celebration_Dt
,Sterilization_Status_Ind
,Profile_Delete_Ind
,Create_User_Id
,Create_Ts
,Update_User_Id
,Update_Ts
,DW_Logical_delete_ind
,DW_SOURCE_CREATE_NM
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 1
AND Household_Id  IS NOT NULL
AND Pet_Id IS NOT NULL
) src
WHERE tgt.Household_Id = src.Household_Id
AND tgt.Pet_Id = src.Pet_Id
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
(
Household_Id
,Pet_Id 
,DW_First_Effective_Dt
,DW_Last_Effective_Dt 
,Pet_Nm
,Pet_Type_Cd
,Pet_Sex_Cd
,Celebration_Type_Cd
,Celebration_Dt
,Sterilization_Status_Ind
,Profile_Delete_Ind
,Create_User_Id
,Create_Ts
,Update_User_Id
,Update_Ts
,DW_CREATE_TS 
,DW_LOGICAL_DELETE_IND 
,DW_SOURCE_CREATE_NM 
,DW_CURRENT_VERSION_IND
)
 SELECT DISTINCT
Household_Id
,Pet_Id 
,CURRENT_DATE
,'31-DEC-9999'
,Pet_Nm
,Pet_Type_Cd
,Pet_Sex_Cd
,Celebration_Type_Cd
,Celebration_Dt
,Sterilization_Status_Ind
,Profile_Delete_Ind
,Create_User_Id
,Create_Ts
,Update_User_Id
,Update_Ts
,CURRENT_TIMESTAMP
,DW_Logical_delete_ind
,DW_SOURCE_CREATE_NM
,TRUE
FROM ${tgt_wrk_tbl}
WHERE Sameday_chg_ind = 0
AND  Household_Id  IS NOT NULL
AND Pet_Id IS NOT NULL
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
        return `Loading of Pet_Profile table ${tgt_tbl} Failed with error:  ${err}`;   // Return a error message.
       
}


// ************** Load for Pet_Profile table ENDs *****************


$$;
