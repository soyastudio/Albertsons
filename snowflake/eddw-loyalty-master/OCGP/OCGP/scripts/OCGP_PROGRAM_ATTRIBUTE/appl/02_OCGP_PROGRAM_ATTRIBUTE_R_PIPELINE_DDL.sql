--liquibase formatted sql
--changeset SYSTEM:EDM_OCGP_PROGRAM_ATTRIBUTE_R_PIPELINE_DDL runOnChange:true splitStatements:false OBJECT_TYPE:pipe

USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA DW_APPL;

Create or replace pipe EDM_OCGP_PROGRAM_ATTRIBUTE_PIPE_<<ENV>>BLOB_INC
auto_ingest = true
integration = <<PIPE_INTEGRATION>>
as
COPY INTO <<EDM_DB_NAME_R>>.DW_R_LOYALTY.OCGP_PROGRAM_ATTRIBUTE_FLAT 
FROM
(select  $1 ,$2 ,$3 ,$4 ,$5 ,metadata$filename ,current_timestamp
from @EDM_OCGP_STAGE_<<ENV>>BLOB_INC/OCGP/
)
file_format = CSV_OCGP
pattern='.*Program_Attribute.*.csv'
on_error = 'SKIP_FILE';


Create or replace stream OCGP_PROGRAM_ATTRIBUTE_FLAT_R_STREAM ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.OCGP_PROGRAM_ATTRIBUTE_FLAT;
