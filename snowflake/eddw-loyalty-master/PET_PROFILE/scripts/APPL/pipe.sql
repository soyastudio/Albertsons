use database EDM_REFINED_PRD;
use schema DW_APPL;

create or replace pipe EDDW_PETPROFILE_PIPE_ADF_PRDBLOB_INC 
auto_ingest=true 
integration='EDDW_PRD_SNOWPIPEINTEGRATION' 
as
COPY INTO EDM_REFINED_PRD.dw_r_loyalty.GETPETPROFILE_ADF_FLAT
FROM
(select  $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,metadata$filename ,current_timestamp
from @EDDW_PETPROFILE_ADF_STAGE_PRDBLOB_INC/PetProfile/
)
file_format = (type = 'csv' SKIP_HEADER = 1)
pattern='.*pet_profile.*.txt'
on_error = 'SKIP_FILE';
