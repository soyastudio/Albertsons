--liquibase formatted sql
--changeset SYSTEM:SP_GETPETPROFILE_TO_FLAT_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP

use database <<EDM_DB_NAME_R>>;
use schema DW_APPL;

CREATE OR REPLACE PROCEDURE SP_GETPETPROFILE_TO_FLAT_LOAD()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS 
$$
    
    var wrk_schema = "DW_R_STAGE";
    var ref_db = "<<EDM_DB_NAME_R>>";
    var ref_schema = "DW_R_LOYALTY";
	var appl_schema = "DW_APPL";

    var variant_nm = 'GETPETPROFILE_ADF_FLAT';
	var bod_nm = 'GetPetProfile';
    var src_tbl = `${ref_db}.${appl_schema}.${variant_nm}_R_STREAM`;
    var src_wrk_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_wrktmp`;
    var src_rerun_tbl = `${ref_db}.${wrk_schema}.${variant_nm}_Rerun`;
    var tgt_flat_tbl = `${ref_db}.${ref_schema}.${bod_nm}_FLAT`;


	var sql_crt_src_wrk_tbl = `CREATE OR REPLACE TABLE ${src_wrk_tbl} AS
                            select * from ${src_tbl} where METADATA$ACTION ='INSERT'
                            UNION ALL 
                            select * from ${src_rerun_tbl}`;
    try {
        snowflake.execute ({ sqlText: sql_crt_src_wrk_tbl });
    } catch (err)  {
        throw `Inserting into Source Work table ${src_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
// Empty the rerun queue table

	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;
	
    try {
        snowflake.execute ({sqlText: sql_empty_rerun_tbl });
    } catch (err) { 
        throw `Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
// To load rerun queue table when encountered a failure

	var sql_ins_rerun_tbl = `INSERT INTO ${src_rerun_tbl} SELECT * FROM ${src_wrk_tbl}`;
	
// Load for Flat Table Begins	
	var insert_into_flat_dml =`INSERT INTO `+ tgt_flat_tbl +`
WITH LVL_1_FLATTEN  as
			(select 		
				 tbl.PETID           as PETID 
				,tbl.NAME            as NAME
				,tbl.PETTYPE         as PETTYPE
				,tbl.PROFILE         as PROFILE
				,tbl.HOUSEHOLDID     as HOUSEHOLDID
				,tbl.CREATEDDATE     as CREATEDDATE
				,tbl.MODIFIEDDATE    as MODIFIEDDATE
				,tbl.DELETEINDICATOR as DELETEINDICATOR
				,tbl.CREATEDBY       as CREATEDBY
				,tbl.MODIFIEDBY      as MODIFIEDBY
				,tbl.COMMENTS        as COMMENTS
				,tbl.FILENAME        as FILENAME
				,tbl.DW_CREATE_TS    as DW_CREATE_TS
			from `+ src_wrk_tbl +` tbl
			,LATERAL FLATTEN(tbl.PROFILE) PET
			)
                     select distinct filename ,
					 hhid,                 
					 id ,                  
					 name,                 
					 type ,                
					 sex   ,               
					 CelebrationType,      
					 To_date(CelebrationDate,'MM-DD-YYYY') CelebrationDate ,                            
					 neuturedSpayedStatus, 
					 deleteindicator ,
					 createdBy  ,          
					 TO_TIMESTAMP_NTZ(CreationTs)  ,         
					 modifiedBy,           
					 UpdateTs ,            
					 breed  ,              
					 medicalconditions ,   
					 medications ,         
					 allergies ,
                     current_timestamp() as DW_CREATETS
			FROM(			
  select 
    PET.FILENAME AS FILENAME
   ,PET.HOUSEHOLDID as hhid
   ,PET.PETID as id
   ,PET.NAME as name
   ,PET.PETTYPE as type
   ,PET.PROFILE:sex::string as sex
   ,PET.PROFILE:celebrationType::string as CelebrationType
   ,PET.PROFILE:celebrationDate::string as CelebrationDate 
   ,PET.PROFILE:sterilizationStatus::string as neuturedSpayedStatus
   ,PET.DELETEINDICATOR as deleteindicator    
   ,PET.CREATEDBY as createdBy
   ,TO_TIMESTAMP_NTZ(PET.CREATEDDATE) as CreationTs
   ,PET.MODIFIEDBY as modifiedBy
   ,PET.MODIFIEDDATE as UpdateTs
   ,PET.PROFILE:breed::array as breed
   ,PET.PROFILE:medicalConditions::array as medicalconditions
   ,PET.PROFILE:medications::array as medications
   ,PET.PROFILE:allergies::array as allergies   
 from lvl_1_flatten PET) as PET;`
	try {
            snowflake.execute (
            {sqlText: insert_into_flat_dml  }
            );
        }
    catch (err)  { 
		    snowflake.execute ( 
						{sqlText: sql_ins_rerun_tbl }
						); 
            throw "Loading of table "+ tgt_flat_tbl +" Failed with error: " + err;   // Return a error message.
        }
$$;

