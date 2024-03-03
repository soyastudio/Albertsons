--liquibase formatted sql
--changeset SYSTEM:OCGP_PROGRAM runOnChange:true splitStatements:false OBJECT_TYPE:VIEW

use database <<EDM_DB_NAME_VIEW>>;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW OCGP_PROGRAM(
			PROGRAM_CD
                      ,DW_FIRST_EFFECTIVE_DT
                      ,DW_LAST_EFFECTIVE_DT
                      ,PROGRAM_ID
                      ,PROGRAM_NM
                      ,PROGRAM_DSC
                      ,PROGRAM_TYPE_CD
                      ,PROGRAM_START_DT
                      ,PROGRAM_END_DT
                      ,MAINTENANCE_MODE_IND
		      ,POST_PROGRAM_LIVE_DT 
                      ,DW_CREATE_TS
                      ,DW_LAST_UPDATE_TS
                      ,DW_LOGICAL_DELETE_IND
                      ,DW_SOURCE_CREATE_NM
                      ,DW_SOURCE_UPDATE_NM
                      ,DW_CURRENT_VERSION_IND
			) COMMENT='VIEW FOR OCGP_PROGRAM'
			AS
			SELECT
			PROGRAM_CD
                      ,DW_FIRST_EFFECTIVE_DT
                      ,DW_LAST_EFFECTIVE_DT
                      ,PROGRAM_ID
                      ,PROGRAM_NM
                      ,PROGRAM_DSC
                      ,PROGRAM_TYPE_CD
                      ,PROGRAM_START_DT
                      ,PROGRAM_END_DT
                      ,MAINTENANCE_MODE_IND
		      ,POST_PROGRAM_LIVE_DT
                      ,DW_CREATE_TS
                      ,DW_LAST_UPDATE_TS
                      ,DW_LOGICAL_DELETE_IND
                      ,DW_SOURCE_CREATE_NM
                      ,DW_SOURCE_UPDATE_NM
                      ,DW_CURRENT_VERSION_IND
			FROM <<EDM_DB_NAME>>.DW_C_LOYALTY.OCGP_PROGRAM;
