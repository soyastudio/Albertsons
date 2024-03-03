--liquibase formatted sql
--changeset SYSTEM:CLICK_STREAM_CONTROL_TABLE runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW Click_Stream_Control_Table
(
     Click_Stream_Integration_Id           COMMENT 'Click Stream Sequence ID'        			     
    ,Hit_Id_High        			       COMMENT 'Hit Id High Value'        			     
    ,Hit_Id_Low          			       COMMENT 'Hit Id Low Value'  			     
    ,Visit_Page_Nbr  			           COMMENT 'Visiting Page Number'  			     
    ,Visit_Nbr 	                           COMMENT 'Visit Number' 	     
    ,Source_Create_Ts         			   COMMENT 'Source Created Timestamp'	     
    ,DW_CREATE_TS    			           COMMENT 'Datawarehouse Created Timestamp' 
)
COPY GRANTS
comment = 'VIEW for Click_Stream_Control_Table' 
AS
SELECT
	 Click_Stream_Integration_Id        			    
    ,Hit_Id_High        			    
    ,Hit_Id_Low   			    
    ,Visit_Page_Nbr  			    
    ,Visit_Nbr 	    
    ,Source_Create_Ts         			    
    ,DW_CREATE_TS 
FROM  EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.Click_Stream_Control_Table ;