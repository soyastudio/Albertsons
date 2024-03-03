--liquibase formatted sql
--changeset SYSTEM:SP_PRODUCTGROUP_TO_FLAT_LOAD_RERUN runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME_R>>;
use schema <<EDM_DB_NAME_R>>.DW_R_PRODUCT;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME_R>>.DW_R_PRODUCT.SP_PRODUCTGROUP_TO_FLAT_LOAD_RERUN()
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
	
  
    // Global Variable Declaration
    var wrk_schema = "DW_R_STAGE";
    var ref_db = "EDM_REFINED_PRD";
    var ref_schema = "DW_R_PRODUCT";
    var src_tbl = ref_db + "." + ref_schema + ".ESED_ProductGroup_R_STREAM";
    var src_wrk_tbl = ref_db + "." + wrk_schema + ".ESED_ProductGroup_wrk";
var src_rerun_tbl = ref_db + "." + wrk_schema + ".ESED_ProductGroup_Rerun";
    var tgt_flat_tbl = ref_db + "." + ref_schema + ".ProductGroup_Flat";
// check if rerun queue table exists otherwise create it
var sql_crt_rerun_tbl = `CREATE TABLE IF NOT EXISTS `+ src_rerun_tbl + ` DATA_RETENTION_TIME_IN_DAYS = 0
    AS SELECT * FROM `+ src_wrk_tbl +`  where 1=2; `;
try {
      snowflake.execute (
          {sqlText: sql_crt_rerun_tbl  }
          );
  }
  catch (err)  {
    throw "Creation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
  }
// persist stream data in work table for the current transaction, includes data from previous failed run
var sql_crt_src_wrk_tbl = `create or replace table `+ src_wrk_tbl +`  as
select * from `+ src_tbl +`
UNION ALL
select * from `+ src_rerun_tbl;
    try {
        snowflake.execute (
            {sqlText: sql_crt_src_wrk_tbl  }
            );
        }
    catch (err)  {
        throw "Creation of Source Work table "+ src_wrk_tbl +" Failed with error: " + err;   // Return a error message.
        }
// Empty the rerun queue table
var sql_empty_rerun_tbl = `TRUNCATE TABLE `+ src_rerun_tbl +` `;
try {
        snowflake.execute ({sqlText: sql_empty_rerun_tbl });
  }
  catch (err) {
    throw "Truncation of rerun queue table "+ src_rerun_tbl +" Failed with error: " + err;   // Return a error message.
  }
// query to load rerun queue table when encountered a failure
    var sql_ins_rerun_tbl = `INSERT OVERWRITE INTO  `+ src_rerun_tbl+` SELECT * FROM `+ src_wrk_tbl +``;

    var insert_into_flat_dml =`INSERT INTO `+ tgt_flat_tbl +`
            with LVL_1_FLATTEN as
            (select
            tbl.filename as filename
            ,tbl.src_json as src_json
            ,productgroup.value as value
            ,productgroup.seq as seq
            from `+ src_wrk_tbl +` tbl
            ,LATERAL FLATTEN(tbl.SRC_JSON) productgroup
            )
           
select distinct
            filename
            ,PayloadPart
            ,PageNum
            ,TotalPages
            ,EntityId
            ,PayLoadType
            ,EntityType
            ,SourceAction
            ,payload_id
            ,payload_name
            ,payload_description
            ,payload_productGroupIds_upcIds
            ,payload_productGroupIds_departmentSectionIds
            ,payload_productGroupIds_manufactureIds
            ,payload_createTs
            ,payload_updateTs
            ,payload_createdUser_userId
            ,payload_createdUser_firstName
            ,payload_createduser_lastName
            ,payload_updatedUser_userId
            ,payload_updatedUser_firstName
            ,payload_updatedUser_lastName
            ,lastUpdateTs
            ,current_timestamp() as DW_CREATE_TS
            ,payload_version
            ,payload_productGroupType
            ,payload_productGroupInfo_mobId
            ,payload_productGroupInfo_upcVersion
            ,payload_productGroupInfo_upcDescription
            ,payload_productGroupInfo_origin
            ,payload_productgroupinfo_status
            ,payload_productgroupinfo_groupId
            ,payload_productgroupinfo_groupName
            ,payload_productgroupinfo_categoryId
            ,payload_productgroupinfo_categoryDescription
            ,payload_productgroupinfo_manufacturerCode
            ,payload_productgroupinfo_qty
            ,payload_productgroupinfo_uom
            ,payload_productgroupinfo_price
            ,payload_productgroupinfo_score
            ,payload_productgroupinfo_brandId
            ,payload_productgroupinfo_brandName
            ,payload_productgroupinfo_createTs
            ,payload_productgroupinfo_updateTs
            ,payload_productgroupinfo_createdUser
            ,payload_productgroupinfo_updatedUser
            ,Payload_Productgroupinfo_Comments_commentTs
            ,Payload_Productgroupinfo_Comments_comment
            ,Payload_Productgroupinfo_Comments_commentBy
,payload_productgroupinfo_listtype
            from
            (
            select filename
            ,productgroup.src_json:payloadPart::string as PayloadPart
            ,productgroup.src_json:pageNum::string as PageNum
            ,productgroup.src_json:totalPages::string as totalPages
            ,productgroup.src_json:entityId::string as entityId
            ,productgroup.src_json:payLoadType::string as payLoadType
            ,productgroup.src_json:entityType::string as entityType
            ,productgroup.src_json:sourceAction::string as sourceAction
            ,productgroup.src_json:payload:id::string as payload_id
            ,productgroup.src_json:payload:name::string as payload_name
            ,productgroup.src_json:payload:description::string as payload_description
            ,productgroup.src_json:payload:createTs::string as payload_createTs
            ,productgroup.src_json:payload:updateTs::string as payload_updateTs
            ,productgroup.src_json:payload:createdUser:userId::string as payload_createduser_userId
            ,productgroup.src_json:payload:createdUser:firstName::string as payload_createduser_firstName
   ,productgroup.src_json:payload:createdUser:lastName::string as payload_createduser_lastName
            ,productgroup.src_json:payload:updatedUser:userId::string as payload_updateduser_userId
            ,productgroup.src_json:payload:updatedUser:firstName::string as payload_updateduser_firstName
            ,productgroup.src_json:payload:updatedUser:lastName::string as payload_updateduser_lastName
            ,productgroup.src_json:lastUpdateTs::string as lastUpdateTs
            ,a.VALUE:upc::array as payload_productGroupIds_upcIds
            ,src_json:payload:productGroupIds:departmentSectionIds::array as payload_productGroupIds_departmentSectionIds
            ,src_json:payload:productGroupIds:manufactureIds::array as payload_productGroupIds_manufactureIds
            ,productgroup.seq as seq
            ,productgroup.src_json:payload:version::string as payload_version
            ,productgroup.src_json:payload:productGroupType::string as payload_productGroupType
            ,productgroup.src_json:payload:productGroupInfo:mobId::string as payload_productGroupInfo_mobId
            ,a.VALUE:upcVersion::string as payload_productGroupInfo_upcVersion
            ,a.VALUE:upcDescription::string as payload_productGroupInfo_upcDescription
   ,a.VALUE:origin::string as payload_productGroupInfo_origin
            ,a.VALUE:status::string as payload_productGroupInfo_status
            ,a.VALUE:groupId::string as payload_productGroupInfo_groupId
            ,a.VALUE:groupName::string as payload_productGroupInfo_groupName
            ,a.VALUE:categoryId::string as payload_productGroupInfo_categoryId
            ,a.VALUE:categoryDescription::string as payload_productGroupInfo_categoryDescription
            ,a.VALUE:manufacturerCode::string as payload_productGroupInfo_manufacturerCode
            ,a.VALUE:qty::string as payload_productGroupInfo_qty
            ,a.VALUE:uom::string as payload_productGroupInfo_uom
            ,a.VALUE:price::string as payload_productGroupInfo_price
            ,a.VALUE:score::string as payload_productGroupInfo_score
            ,a.VALUE:brandId::string as payload_productGroupInfo_brandId
            ,a.VALUE:brandName::string as payload_productGroupInfo_brandName
            ,a.VALUE:createTs::string as payload_productGroupInfo_createTs
            ,a.VALUE:updateTs::string as payload_productGroupInfo_updateTs
            ,a.VALUE:createdUser::string as payload_productGroupInfo_createdUser
            ,a.VALUE:updatedUser::string as payload_productGroupInfo_updatedUser
            ,b.VALUE:commentTs::string as payload_productGroupInfo_Comments_commentTs
            ,b.VALUE:comment::string as payload_productGroupInfo_Comments_comment
            ,b.VALUE:commentBy::string as payload_productGroupInfo_Comments_commentBy
       ,'Final' as payload_productgroupinfo_listtype
             from lvl_1_flatten productgroup
            ,LATERAL FLATTEN(input => productgroup.src_json:payload:productGroupInfo:finalUpcs, outer => TRUE) as a
            ,LATERAL FLATTEN(input => a.VALUE:comments, outer => TRUE) as b
			where upper(payload_productGroupType) = 'BASE'
			
			union all

			select filename
            ,productgroup.src_json:payloadPart::string as PayloadPart
            ,productgroup.src_json:pageNum::string as PageNum
            ,productgroup.src_json:totalPages::string as totalPages
            ,productgroup.src_json:entityId::string as entityId
            ,productgroup.src_json:payLoadType::string as payLoadType
            ,productgroup.src_json:entityType::string as entityType
            ,productgroup.src_json:sourceAction::string as sourceAction
            ,productgroup.src_json:payload:id::string as payload_id
            ,productgroup.src_json:payload:name::string as payload_name
            ,productgroup.src_json:payload:description::string as payload_description
            ,productgroup.src_json:payload:createTs::string as payload_createTs
            ,productgroup.src_json:payload:updateTs::string as payload_updateTs
            ,productgroup.src_json:payload:createdUser:userId::string as payload_createduser_userId
            ,productgroup.src_json:payload:createdUser:firstName::string as payload_createduser_firstName
			,productgroup.src_json:payload:createdUser:lastName::string as payload_createduser_lastName
            ,productgroup.src_json:payload:updatedUser:userId::string as payload_updateduser_userId
            ,productgroup.src_json:payload:updatedUser:firstName::string as payload_updateduser_firstName
            ,productgroup.src_json:payload:updatedUser:lastName::string as payload_updateduser_lastName
            ,productgroup.src_json:lastUpdateTs::string as lastUpdateTs
            ,a.VALUE:upc::array as payload_productGroupIds_upcIds
            ,src_json:payload:productGroupIds:departmentSectionIds::array as payload_productGroupIds_departmentSectionIds
            ,src_json:payload:productGroupIds:manufactureIds::array as payload_productGroupIds_manufactureIds
            ,productgroup.seq as seq
            ,productgroup.src_json:payload:version::string as payload_version
            ,productgroup.src_json:payload:productGroupType::string as payload_productGroupType
            ,productgroup.src_json:payload:productGroupInfo:mobId::string as payload_productGroupInfo_mobId
            ,a.VALUE:upcVersion::string as payload_productGroupInfo_upcVersion
            ,a.VALUE:upcDescription::string as payload_productGroupInfo_upcDescription
			,a.VALUE:origin::string as payload_productGroupInfo_origin
            ,a.VALUE:status::string as payload_productGroupInfo_status
            ,a.VALUE:groupId::string as payload_productGroupInfo_groupId
            ,a.VALUE:groupName::string as payload_productGroupInfo_groupName
            ,a.VALUE:categoryId::string as payload_productGroupInfo_categoryId
            ,a.VALUE:categoryDescription::string as payload_productGroupInfo_categoryDescription
            ,a.VALUE:manufacturerCode::string as payload_productGroupInfo_manufacturerCode
            ,a.VALUE:qty::string as payload_productGroupInfo_qty
            ,a.VALUE:uom::string as payload_productGroupInfo_uom
            ,a.VALUE:price::string as payload_productGroupInfo_price
            ,a.VALUE:score::string as payload_productGroupInfo_score
            ,a.VALUE:brandId::string as payload_productGroupInfo_brandId
            ,a.VALUE:brandName::string as payload_productGroupInfo_brandName
            ,a.VALUE:createTs::string as payload_productGroupInfo_createTs
            ,a.VALUE:updatedDate::string as payload_productGroupInfo_updateTs
            ,a.VALUE:createdUser::string as payload_productGroupInfo_createdUser
            ,a.VALUE:updatedUser::string as payload_productGroupInfo_updatedUser
            ,b.VALUE:commentTs::string as payload_productGroupInfo_Comments_commentTs
            ,b.VALUE:comment::string as payload_productGroupInfo_Comments_comment
            ,b.VALUE:commentBy::string as payload_productGroupInfo_Comments_commentBy
			,'Final' as payload_productgroupinfo_listtype
             from lvl_1_flatten productgroup
            ,LATERAL FLATTEN(input => productgroup.src_json:payload:productGroupInfo:finalUpcsNonBasePg, outer => TRUE) as a
            ,LATERAL FLATTEN(input => a.VALUE:comments, outer => TRUE) as b
			where upper(payload_productGroupType) = 'NON_BASE'
             
              union all
             
              select filename
            ,productgroup.src_json:payloadPart::string as PayloadPart
            ,productgroup.src_json:pageNum::string as PageNum
            ,productgroup.src_json:totalPages::string as totalPages
            ,productgroup.src_json:entityId::string as entityId
            ,productgroup.src_json:payLoadType::string as payLoadType
            ,productgroup.src_json:entityType::string as entityType
            ,productgroup.src_json:sourceAction::string as sourceAction
            ,productgroup.src_json:payload:id::string as payload_id
            ,productgroup.src_json:payload:name::string as payload_name
            ,productgroup.src_json:payload:description::string as payload_description
            ,productgroup.src_json:payload:createTs::string as payload_createTs
            ,productgroup.src_json:payload:updateTs::string as payload_updateTs
            ,productgroup.src_json:payload:createdUser:userId::string as payload_createduser_userId
            ,productgroup.src_json:payload:createdUser:firstName::string as payload_createduser_firstName
   ,productgroup.src_json:payload:createdUser:lastName::string as payload_createduser_lastName
            ,productgroup.src_json:payload:updatedUser:userId::string as payload_updateduser_userId
            ,productgroup.src_json:payload:updatedUser:firstName::string as payload_updateduser_firstName
   ,productgroup.src_json:payload:updatedUser:lastName::string as payload_updateduser_lastName
            ,productgroup.src_json:lastUpdateTs::string as lastUpdateTs
            ,a.VALUE:upc::array as payload_productGroupIds_upcIds
            ,src_json:payload:productGroupIds:departmentSectionIds::array as payload_productGroupIds_departmentSectionIds
            ,src_json:payload:productGroupIds:manufactureIds::array as payload_productGroupIds_manufactureIds
            ,productgroup.seq as seq
            ,productgroup.src_json:payload:version::string as payload_version
            ,productgroup.src_json:payload:productGroupType::string as payload_productGroupType
            ,productgroup.src_json:payload:productGroupInfo:mobId::string as payload_productGroupInfo_mobId
            ,a.VALUE:upcVersion::string as payload_productGroupInfo_upcVersion
            ,a.VALUE:upcDescription::string as payload_productGroupInfo_upcDescription
   ,a.VALUE:origin::string as payload_productGroupInfo_origin
            ,a.VALUE:status::string as payload_productGroupInfo_status
            ,a.VALUE:groupId::string as payload_productGroupInfo_groupId
            ,a.VALUE:groupName::string as payload_productGroupInfo_groupName
            ,a.VALUE:categoryId::string as payload_productGroupInfo_categoryId
            ,a.VALUE:categoryDescription::string as payload_productGroupInfo_categoryDescription
            ,a.VALUE:manufacturerCode::string as payload_productGroupInfo_manufacturerCode
            ,a.VALUE:qty::string as payload_productGroupInfo_qty
            ,a.VALUE:uom::string as payload_productGroupInfo_uom
            ,a.VALUE:price::string as payload_productGroupInfo_price
            ,a.VALUE:score::string as payload_productGroupInfo_score
            ,a.VALUE:brandId::string as payload_productGroupInfo_brandId
            ,a.VALUE:brandName::string as payload_productGroupInfo_brandName
            ,a.VALUE:createTs::string as payload_productGroupInfo_createTs
            ,a.VALUE:updateTs::string as payload_productGroupInfo_updateTs
            ,a.VALUE:createdUser::string as payload_productGroupInfo_createdUser
            ,a.VALUE:updatedUser::string as payload_productGroupInfo_updatedUser
            ,b.VALUE:commentTs::string as payload_productGroupInfo_Comments_commentTs
            ,b.VALUE:comment::string as payload_productGroupInfo_Comments_comment
            ,b.VALUE:commentBy::string as payload_productGroupInfo_Comments_commentBy
   ,'Suggested' as payload_productgroupinfo_listtype
             from lvl_1_flatten productgroup
            ,LATERAL FLATTEN(input => productgroup.src_json:payload:productGroupInfo:suggestedUpcs, outer => TRUE) as a
            ,LATERAL FLATTEN(input => a.VALUE:comments, outer => TRUE) as b
			
			 union all
             
              select filename
            ,productgroup.src_json:payloadPart::string as PayloadPart
            ,productgroup.src_json:pageNum::string as PageNum
            ,productgroup.src_json:totalPages::string as totalPages
            ,productgroup.src_json:entityId::string as entityId
            ,productgroup.src_json:payLoadType::string as payLoadType
            ,productgroup.src_json:entityType::string as entityType
            ,productgroup.src_json:sourceAction::string as sourceAction
            ,productgroup.src_json:payload:id::string as payload_id
            ,productgroup.src_json:payload:name::string as payload_name
            ,productgroup.src_json:payload:description::string as payload_description
            ,productgroup.src_json:payload:createTs::string as payload_createTs
            ,productgroup.src_json:payload:updateTs::string as payload_updateTs
            ,productgroup.src_json:payload:createdUser:userId::string as payload_createduser_userId
            ,productgroup.src_json:payload:createdUser:firstName::string as payload_createduser_firstName
   ,productgroup.src_json:payload:createdUser:lastName::string as payload_createduser_lastName
            ,productgroup.src_json:payload:updatedUser:userId::string as payload_updateduser_userId
            ,productgroup.src_json:payload:updatedUser:firstName::string as payload_updateduser_firstName
   ,productgroup.src_json:payload:updatedUser:lastName::string as payload_updateduser_lastName
            ,productgroup.src_json:lastUpdateTs::string as lastUpdateTs
            ,a.VALUE:upc::array as payload_productGroupIds_upcIds
            ,src_json:payload:productGroupIds:departmentSectionIds::array as payload_productGroupIds_departmentSectionIds
            ,src_json:payload:productGroupIds:manufactureIds::array as payload_productGroupIds_manufactureIds
            ,productgroup.seq as seq
            ,productgroup.src_json:payload:version::string as payload_version
            ,productgroup.src_json:payload:productGroupType::string as payload_productGroupType
            ,productgroup.src_json:payload:productGroupInfo:mobId::string as payload_productGroupInfo_mobId
            ,a.VALUE:upcVersion::string as payload_productGroupInfo_upcVersion
            ,a.VALUE:upcDescription::string as payload_productGroupInfo_upcDescription
   ,a.VALUE:origin::string as payload_productGroupInfo_origin
            ,a.VALUE:status::string as payload_productGroupInfo_status
            ,a.VALUE:groupId::string as payload_productGroupInfo_groupId
            ,a.VALUE:groupName::string as payload_productGroupInfo_groupName
            ,a.VALUE:categoryId::string as payload_productGroupInfo_categoryId
            ,a.VALUE:categoryDescription::string as payload_productGroupInfo_categoryDescription
            ,a.VALUE:manufacturerCode::string as payload_productGroupInfo_manufacturerCode
            ,a.VALUE:qty::string as payload_productGroupInfo_qty
            ,a.VALUE:uom::string as payload_productGroupInfo_uom
            ,a.VALUE:price::string as payload_productGroupInfo_price
            ,a.VALUE:score::string as payload_productGroupInfo_score
            ,a.VALUE:brandId::string as payload_productGroupInfo_brandId
            ,a.VALUE:brandName::string as payload_productGroupInfo_brandName
            ,a.VALUE:createTs::string as payload_productGroupInfo_createTs
            ,a.VALUE:updateTs::string as payload_productGroupInfo_updateTs
            ,a.VALUE:createdUser::string as payload_productGroupInfo_createdUser
            ,a.VALUE:updatedUser::string as payload_productGroupInfo_updatedUser
            ,b.VALUE:commentTs::string as payload_productGroupInfo_Comments_commentTs
            ,b.VALUE:comment::string as payload_productGroupInfo_Comments_comment
            ,b.VALUE:commentBy::string as payload_productGroupInfo_Comments_commentBy
   ,'Rejected' as payload_productgroupinfo_listtype
             from lvl_1_flatten productgroup
            ,LATERAL FLATTEN(input => productgroup.src_json:payload:productGroupInfo:rejectedUpcs, outer => TRUE) as a
            ,LATERAL FLATTEN(input => a.VALUE:comments, outer => TRUE) as b
              
               union all
             
              select filename
            ,productgroup.src_json:payloadPart::string as PayloadPart
            ,productgroup.src_json:pageNum::string as PageNum
            ,productgroup.src_json:totalPages::string as totalPages
            ,productgroup.src_json:entityId::string as entityId
            ,productgroup.src_json:payLoadType::string as payLoadType
            ,productgroup.src_json:entityType::string as entityType
            ,productgroup.src_json:sourceAction::string as sourceAction
            ,productgroup.src_json:payload:id::string as payload_id
            ,productgroup.src_json:payload:name::string as payload_name
            ,productgroup.src_json:payload:description::string as payload_description
            ,productgroup.src_json:payload:createTs::string as payload_createTs
            ,productgroup.src_json:payload:updateTs::string as payload_updateTs
            ,productgroup.src_json:payload:createdUser:userId::string as payload_createduser_userId
            ,productgroup.src_json:payload:createdUser:firstName::string as payload_createduser_firstName
   ,productgroup.src_json:payload:createdUser:lastName::string as payload_createduser_lastName
            ,productgroup.src_json:payload:updatedUser:userId::string as payload_updateduser_userId
            ,productgroup.src_json:payload:updatedUser:firstName::string as payload_updateduser_firstName
   ,productgroup.src_json:payload:updatedUser:lastName::string as payload_updateduser_lastName
            ,productgroup.src_json:lastUpdateTs::string as lastUpdateTs
            ,a.VALUE:upc::array as payload_productGroupIds_upcIds
            ,src_json:payload:productGroupIds:departmentSectionIds::array as payload_productGroupIds_departmentSectionIds
            ,src_json:payload:productGroupIds:manufactureIds::array as payload_productGroupIds_manufactureIds
            ,productgroup.seq as seq
            ,productgroup.src_json:payload:version::string as payload_version
            ,productgroup.src_json:payload:productGroupType::string as payload_productGroupType
            ,productgroup.src_json:payload:productGroupInfo:mobId::string as payload_productGroupInfo_mobId
            ,a.VALUE:upcVersion::string as payload_productGroupInfo_upcVersion
            ,a.VALUE:upcDescription::string as payload_productGroupInfo_upcDescription
   ,a.VALUE:origin::string as payload_productGroupInfo_origin
            ,a.VALUE:status::string as payload_productGroupInfo_status
            ,a.VALUE:groupId::string as payload_productGroupInfo_groupId
            ,a.VALUE:groupName::string as payload_productGroupInfo_groupName
            ,a.VALUE:categoryId::string as payload_productGroupInfo_categoryId
            ,a.VALUE:categoryDescription::string as payload_productGroupInfo_categoryDescription
            ,a.VALUE:manufacturerCode::string as payload_productGroupInfo_manufacturerCode
            ,a.VALUE:qty::string as payload_productGroupInfo_qty
            ,a.VALUE:uom::string as payload_productGroupInfo_uom
            ,a.VALUE:price::string as payload_productGroupInfo_price
            ,a.VALUE:score::string as payload_productGroupInfo_score
            ,a.VALUE:brandId::string as payload_productGroupInfo_brandId
            ,a.VALUE:brandName::string as payload_productGroupInfo_brandName
            ,a.VALUE:createTs::string as payload_productGroupInfo_createTs
            ,a.VALUE:updateTs::string as payload_productGroupInfo_updateTs
            ,a.VALUE:createdUser::string as payload_productGroupInfo_createdUser
            ,a.VALUE:updatedUser::string as payload_productGroupInfo_updatedUser
            ,b.VALUE:commentTs::string as payload_productGroupInfo_Comments_commentTs
            ,b.VALUE:comment::string as payload_productGroupInfo_Comments_comment
            ,b.VALUE:commentBy::string as payload_productGroupInfo_Comments_commentBy
   ,'Dropped Original' as payload_productgroupinfo_listtype
             from lvl_1_flatten productgroup
            ,LATERAL FLATTEN(input => productgroup.src_json:payload:productGroupInfo:droppedOriginalUpcs, outer => TRUE) as a
            ,LATERAL FLATTEN(input => a.VALUE:comments, outer => TRUE) as b
           
            ) productgroup
			;`;

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
