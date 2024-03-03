--liquibase formatted sql
--changeset SYSTEM:SP_GETPET_PROFILE_FLAT_TO_BIM_PET_BREED runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database <<EDM_DB_NAME>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETPET_PROFILE_FLAT_TO_BIM_PET_BREED("SRC_WRK_TBL" VARCHAR(16777216), "CNF_DB" VARCHAR(16777216), "C_LOYAL" VARCHAR(16777216), "C_STAGE" VARCHAR(16777216))
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

var tgt_wrk_tbl = `${cnf_db}.${wrk_schema}.Pet_Breed_wrk`;
var tgt_tbl = `${cnf_db}.${cnf_schema}.Pet_Breed`;
var src_flat_tbl =`${ref_db}.${ref_schema}.GetPetProfile_Flat`;

// ************************************ Truncate and Reload the work table ****************************************
 var truncate_tgt_wrk_table = `Truncate Table ${tgt_wrk_tbl}`;
try {
snowflake.execute ({ sqlText: truncate_tgt_wrk_table });
	}
    catch (err) {
        return `Truncate of work table ${tgt_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
				}

// ************** Load for Pet_Breed table BEGIN *****************
// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.


	    var cr_src_wrk_tbl = `INSERT INTO ${tgt_wrk_tbl}
		                              with flat_tmp as
                           ( select 
                            hhid Household_Id,
							id Pet_Id,
							breed Breed_Nm ,
                            filename,
                            CURRENT_TIMESTAMP Dw_Create_Ts ,
							MODIFIEDTS,
                            CURRENT_DATE Dw_First_Effective_Dt,
                            Row_number() OVER (
		                	PARTITION BY Household_Id,Pet_Id ORDER BY (MODIFIEDTS) DESC) AS rn 
                            from ` + src_wrk_tbl +` 
                           where Household_Id is not null and Pet_Id is not null and Breed_Nm is not null                             
                           )
						   
select 
src.Household_Id
,src.Pet_Id
,src.Breed_Nm
,src.DW_Logical_delete_ind
,src.filename
,CASE WHEN tgt.Household_Id IS NULL  AND tgt.Pet_Id IS NULL AND tgt.Breed_Nm IS NULL THEN 'I' ELSE 'U' END AS DML_Type
,CASE WHEN tgt.dw_first_effective_dt = CURRENT_DATE THEN 1  ELSE 0 END AS Sameday_chg_ind 
from  
(SELECT  
Household_Id,
Pet_Id ,
breed.value::string AS Breed_Nm,
false AS DW_Logical_delete_ind,
filename,
rn
FROM flat_tmp,LATERAL FLATTEN(input => Breed_Nm, outer => TRUE ) as breed
WHERE rn=1 AND
Household_Id is not null 
and Pet_Id is not null 
and Breed_Nm is not null) as src
LEFT JOIN (
SELECT
tgt.Household_Id
,tgt.Pet_Id
,tgt.Breed_Nm
,tgt.dw_logical_delete_ind
,tgt.dw_first_effective_dt
FROM ${tgt_tbl} tgt
WHERE tgt.DW_CURRENT_VERSION_IND = TRUE
) as tgt 
ON tgt.Household_Id = src.Household_Id
AND tgt.Pet_Id = src.Pet_Id
AND tgt.Breed_Nm = src.Breed_Nm
where tgt.Household_Id IS NULL  AND tgt.Pet_Id IS NULL  AND tgt.Breed_Nm IS NULL
OR ( 
                                                        
 src.DW_LOGICAL_DELETE_IND  <>  tgt.DW_LOGICAL_DELETE_IND
)								
`;						   
try {
        
        snowflake.execute ({sqlText: cr_src_wrk_tbl});
            
        }
    catch (err)  {
        throw "Creation of Pet_Breed_wrk work table Failed with error: "+ err;   // Return a error message.
        }
var sql_begin = 'BEGIN'


//SCD Type2 transaction begins
// Processing Different Day Updates of Type 2 SCD

var sql_updates =
`UPDATE ${tgt_tbl} as tgt
 SET DW_Last_Effective_dt = CURRENT_DATE-1
,DW_CURRENT_VERSION_IND = FALSE
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = src.filename
FROM ( SELECT
 Household_Id
,Pet_Id 
,Breed_Nm
,filename
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 0
AND Household_Id  IS NOT NULL
AND Pet_Id IS NOT NULL
AND Breed_Nm IS NOT NULL
) src
WHERE tgt.Household_Id = src.Household_Id  
AND tgt.Pet_Id = src.Pet_Id
AND tgt.Breed_Nm = src.Breed_Nm
AND tgt.DW_CURRENT_VERSION_IND = TRUE
AND tgt.DW_LOGICAL_DELETE_IND = FALSE`;
   

// Processing Sameday updates
var sql_sameday = ` UPDATE ${tgt_tbl} as tgt
SET  
 DW_LOGICAL_DELETE_IND = src.DW_LOGICAL_DELETE_IND 
,DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP
,DW_SOURCE_UPDATE_NM = src.filename
FROM 
( SELECT
Household_Id
,Pet_Id 
,Breed_Nm
,DW_Logical_delete_ind
,filename
FROM ${tgt_wrk_tbl}
WHERE DML_Type = 'U'
AND Sameday_chg_ind = 1
AND Household_Id  IS NOT NULL
AND Pet_Id IS NOT NULL
AND Breed_Nm IS NOT NULL
) src
WHERE tgt.Household_Id = src.Household_Id
AND tgt.Pet_Id = src.Pet_Id
AND tgt.Breed_Nm = src.Breed_Nm
AND tgt.DW_CURRENT_VERSION_IND = TRUE`;


// Processing Inserts
var sql_inserts = `INSERT INTO ${tgt_tbl}
(
Household_Id,
Pet_Id, 
Breed_Nm,
DW_First_Effective_Dt ,
DW_Last_Effective_Dt,
DW_CREATE_TS ,
DW_LOGICAL_DELETE_IND ,
DW_SOURCE_CREATE_NM ,
DW_CURRENT_VERSION_IND
)
 SELECT DISTINCT
Household_Id
,Pet_Id 
,Breed_Nm
,CURRENT_DATE
,'31-DEC-9999'
,CURRENT_TIMESTAMP
,DW_Logical_delete_ind
,filename
,TRUE
FROM ${tgt_wrk_tbl}
WHERE Sameday_chg_ind = 0
AND Household_Id  IS NOT NULL
AND Pet_Id IS NOT NULL
AND Breed_Nm IS NOT NULL
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
        return `Loading of Pet_Breed table ${tgt_tbl} Failed with error:  ${err}`;   // Return a error message.
       
}


// ************** Load for Pet_Breed table ENDs *****************


$$;
