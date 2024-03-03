--liquibase formatted sql
--changeset SYSTEM:SP_GETSHOPPINGLIST_TO_FLAT_LOAD runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME_R>>.DW_APPL.SP_GETSHOPPINGLIST_TO_FLAT_LOAD()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
	
    	
    var cur_db = snowflake.execute( {sqlText: `Select current_database()`} ); 
	cur_db.next(); 
	var env = cur_db.getColumnValue(1); 
	env = env.split('_'); 
	env = env[env.length - 1]; 
	var env_tbl_nm = `EDM_Environment_Variable_${env}`; 
	var env_schema_nm = 'DW_R_MASTERDATA'; 
	var env_db_nm = `EDM_REFINED_${env}`; 
	var sheader = `'\\\\,\\\\s*headers'`;

	try { 
    var rs = snowflake.execute( {sqlText: `SELECT * FROM ${env_db_nm}.${env_schema_nm}.${env_tbl_nm}`} ); 
    var metaparams = {};
    while (rs.next()){
      metaparams[rs.getColumnValue(1)] = rs.getColumnValue(2);
    }
    var ref_db = metaparams['REF_DB']; 
    var ref_schema = metaparams['R_LOYAL']; 
    var app_schema = metaparams['APPL']; 
    var wrk_schema = metaparams['R_STAGE']; 
	} catch (err) { 
    throw `Error while fetching data from EDM_Environment_Variable_${env}`; 
	}
    var variant_nm = 'ESED_ShoppingList';
	var variant_nm_temp = 'ESED_ShoppingList_Temp';
	var bod_nm = 'GetShoppingList';
    var src_tbl = `${ref_db}.${app_schema}.${variant_nm_temp}_R_STREAM`;
    var src_wrk_tbl = `${ref_db}.${wrk_schema}.${variant_nm_temp}_wrk`;
    var src_rerun_tbl = `${ref_db}.${wrk_schema}.${variant_nm_temp}_Rerun`;
    var tgt_flat_tbl = `${ref_db}.${ref_schema}.${bod_nm}_FLAT`;
	
	// persist stream data in work table for the current transaction, includes data from previous failed run
	var sql_crt_src_wrk_tbl = `create or replace transient table ${src_wrk_tbl} as
                            select * from ${src_tbl} where METADATA$ACTION ='INSERT'
                            UNION ALL 
                            select * from ${src_rerun_tbl}`;
    try {
        snowflake.execute ({ sqlText: sql_crt_src_wrk_tbl });
    } catch (err)  {
        throw `Creation of Source Work table ${src_wrk_tbl} Failed with error: ${err}`;   // Return a error message.
    }
	
	// Empty the rerun queue table
	var sql_empty_rerun_tbl = `TRUNCATE TABLE ${src_rerun_tbl}`;
	
    try {
        snowflake.execute ({sqlText: sql_empty_rerun_tbl });
    } catch (err) { 
        throw `Truncation of rerun queue table ${src_rerun_tbl} Failed with error: ${err}`;   // Return a error message.
    }
    
	// query to load rerun queue table when encountered a failure
	var sql_ins_rerun_tbl = `CREATE OR REPLACE TRANSIENT TABLE ${src_rerun_tbl} DATA_RETENTION_TIME_IN_DAYS = 0 AS 
							            SELECT * FROM ${src_wrk_tbl}`;
							
							
	// Empty the Variant table
	var sql_empty_variant_tbl = `TRUNCATE TABLE  ${ref_db}.${ref_schema}.${variant_nm}`;
	
	// insert in variant table 	
	var insrt_variant =  `Insert into  ${ref_db}.${ref_schema}.${variant_nm}
    select 
     filename
   , PARSE_JSON(regexp_replace( 
      substring(SRC_txt, 1,
              case when (regexp_instr(SRC_txt,  ${sheader}) > 0 )
                     then regexp_instr(SRC_txt, ${sheader})-1 
                    else length(SRC_txt)
              end), 'GenericMessage\\\\s*\\\\[payload=','')) as parsed_payload
     ,CREATED_TS
   from ${src_wrk_tbl};`			


 try {
        snowflake.execute({ sqlText: insrt_variant });
    } catch (err) {
        throw `Insert of Variant table failed with error: ${err}`;   // Return a error message.
    }   
							
											
	var insert_into_flat_dml =`INSERT INTO ${tgt_flat_tbl}
WITH LVL_1_FLATTEN  as
(
  select 
    src.SRC_JSON:"@"::string AS BODNm
   ,src.FILENAME AS FILENAME
   ,src.src_json:apiUid::string as apiUid
   ,src.src_json:banner::string as banner
   ,src.src_json:card::string as card
   ,src.src_json:clips::string as clips
   ,src.src_json:hhid::string as hhid
   ,src.src_json:postalCd::string as postalCd
   ,src.src_json:storeId::string as storeId
   ,src.src_json:swyApiKey::string as swyApiKey
   ,src.src_json:swyVersion::string as swyVersion
   ,src.src_json:userId::string as userId 
   ,src.src_json:swyLogonId::string as swyLogonId     
   ,(value) as val
  from ${ref_db}.${ref_schema}.${variant_nm} src, lateral flatten(input => src.src_json:clips)
)
select FILENAME,BODNm,apiUid,banner,card,hhid,postalCd,storeId,swyApiKey,swyVersion,userId,swyLogonId
,val:clipId::string clipId
,val:clipSrc::string clipSrc
,val:clipTs::string clipTs
,val:clipType::string clipType
,val:extOfferId::string extOfferId
,val:offerId::string offerId
,val:offerType::string offerType
,val:program::string program
,val:provider::string provider
,val:srcAppId::string srcAppId
,val:vndrBannerCd::string vndrBannerCd
,current_timestamp() as DW_CREATE_TS
,val:eventTs::string eventTs	
from LVL_1_FLATTEN ;`

  	try {
        snowflake.execute({ sqlText: insert_into_flat_dml});
		snowflake.execute({ sqlText: sql_empty_variant_tbl});
    }
    catch (err)  { 
	    snowflake.execute ( {sqlText: sql_ins_rerun_tbl} ); 
        throw `Loading of table ${tgt_flat_tbl} Failed with error: ${err}`;   // Return a error message.
    }
$$;
