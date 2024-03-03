--liquibase formatted sql
--changeset SYSTEM:CUSTOMER_SESSION_PAGE runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_<<ENV>>;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW CUSTOMER_SESSION_PAGE
(	
	PAGE_INTEGRATION_ID
	,PAGE_NM
	,PAGE_TYPE_CD
	,PAGE_SUBSECTION1_DSC
	,PAGE_SUBSECTION2_DSC
	,PAGE_SUBSECTION3_DSC
	,PAGE_SUBSECTION4_DSC
	,DW_CREATE_TS
	,DW_LAST_UPDATE_TS
	,DW_LOGICAL_DELETE_IND
	,DW_SOURCE_CREATE_NM
	,DW_SOURCE_UPDATE_NM
	,DW_CURRENT_VERSION_IND
	
) COPY GRANTS comment = 'VIEW FOR CUSTOMER_SESSION_PAGE' 
AS
SELECT 	
	PAGE_INTEGRATION_ID
	,PAGE_NM
	,PAGE_TYPE_CD
	,PAGE_SUBSECTION1_DSC
	,PAGE_SUBSECTION2_DSC
	,PAGE_SUBSECTION3_DSC
	,PAGE_SUBSECTION4_DSC
	,DW_CREATE_TS
	,DW_LAST_UPDATE_TS
	,DW_LOGICAL_DELETE_IND
	,DW_SOURCE_CREATE_NM
	,DW_SOURCE_UPDATE_NM
	,DW_CURRENT_VERSION_IND
FROM EDM_CONFIRMED_<<ENV>>.DW_C_USER_ACTIVITY."CUSTOMER_SESSION_PAGE";