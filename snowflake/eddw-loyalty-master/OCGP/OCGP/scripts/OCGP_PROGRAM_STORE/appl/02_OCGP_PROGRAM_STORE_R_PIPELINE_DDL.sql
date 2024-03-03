--liquibase formatted sql
--changeset SYSTEM:EDM_OCGP_PROGRAM_STORE_R_PIPELINE_DDL runOnChange:true splitStatements:false OBJECT_TYPE:pipe

USE DATABASE <<EDM_DB_NAME_R>>;
USE SCHEMA DW_APPL;

---creation of pipeline  
Create or replace pipe EDM_OCGP_PROGRAM_STORE_PIPE_<<ENV>>BLOB_INC
auto_ingest = true
integration = <<PIPE_INTEGRATION>>
as
COPY INTO EDM_REFINED_<<ENV>>.DW_R_LOYALTY.OCGP_PROGRAM_STORE_FLAT 
FROM
(select  $1 ,$2 ,metadata$filename ,current_timestamp
from @EDM_OCGP_STAGE_<<ENV>>BLOB_INC/OCGP/
)
file_format = CSV_OCGP
pattern='.*store_program.*.csv'
on_error = 'SKIP_FILE';

---creation of stream
Create or replace stream OCGP_PROGRAM_STORE_FLAT_R_STREAM ON TABLE <<EDM_DB_NAME_R>>.DW_R_LOYALTY.OCGP_PROGRAM_STORE_FLAT;
