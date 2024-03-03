#!/usr/bin/ksh
#------------------------------------------------------------------------------#
# File          :  edm_eddw_006_ext_gremlin_email_biz.ksh
# Desc          :  NPS data
# version       :
# Date          :
# Time          :
# WhatString    :
#------------------------------------------------------------------------------#
# ModificatiON  :  Auth  :
#               :  Date  :
#               :  Change:
#               :  Desc  : Initial version
#------------------------------------------------------------------------------#

#------------------------------------------------------------------------------#
function PrepareData
#------------------------------------------------------------------------------#
{
    echo -e "\n***START :  PrepareData - `date` ***"
    set -x

snowsql --private-key-path ${SNOW_PKEY_FILE} \
         -o exit_on_error=true \
         -o log_level=ERROR \
         -o echo=true << prepare_extract_sql

ALTER SESSION SET QUERY_TAG='EDDW:edm_EDDW_NPS - PrepareData';

CREATE OR REPLACE TABLE ${DW_CONFIRMED_DB}.${CONF_STAGE_SC}.${StageTableName}

AS

  SELECT * FROM EDM_VIEWS${EDM_ENV}.DW_VIEWS.Txn_NPS_Survey_Faulty_Data
  where txn_dte = current_date-1;
prepare_extract_sql

    Res=$?
    if [ ${Res} -ne 0 ];then
        echo "ERROR:  Error in process PrepareData"
        p_ErrorDetails="'Prepare SQL Failed.'"
        FeedExtractJobFailed
        exit 1
     fi

    echo "***END : PrepareData - `date` ***\n"

} #END PrepareData

#------------------------------------------------------------------------------#
function ExtractNPS
#------------------------------------------------------------------------------#
{
    echo -e "\n***START :  ExtractNPS  - `date` ***"
    set -x

    OutputFile=$1

result=$(snowsql --private-key-path ${SNOW_PKEY_FILE} \
                                         -o exit_on_error=true \
                     -o log_level=ERROR \
                     -o echo=true \
                     -o header=false \
                     -o friendly=false \
                     -o output_format=plain \
                     -o timing=false << sql_stmt

select count(*) from EDM_VIEWS${EDM_ENV}.DW_VIEWS.Txn_NPS_Survey_Faulty_Data where txn_dte = current_date-1
sql_stmt
)

Header='TRUE'
cnt=`echo "${result}" | tail -1`

snowsql --private-key-path ${SNOW_PKEY_FILE} \
      -o exit_on_error=true \
      -o log_level=ERROR \
      -o echo=true << extract_sql
 


ALTER SESSION SET QUERY_TAG='EDDW:edm_appcd1_NPS - ExtractNPS';

-- Export V12 Data
COPY INTO  @${DW_CONFIRMED_DB}.${CONF_APPL_SC}.file_stage/${OutputFile}
FROM (
    SELECT
       *
    FROM ${DW_CONFIRMED_DB}.${CONF_STAGE_SC}.${StageTableName}
--WHERE
  --  extract_Application_nm = '${ApplicationName}'
    --AND extract_nm = '${JobName}'
)
file_format = (
  TYPE = 'csv'
  FILE_EXTENSION = 'csv'
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF=()
  COMPRESSION = $2
)
MAX_FILE_SIZE = 5368709000
SINGLE = FALSE
HEADER= $3;

get @${DW_CONFIRMED_DB}.${CONF_APPL_SC}.file_stage/${OutputFile} file:///${InDir}/;
rm @${DW_CONFIRMED_DB}.${CONF_APPL_SC}.file_stage/${OutputFile};

extract_sql

if [ $cnt -ne 0 ]; then Res=$?
else Res=0
fi
    
    if [ $Res -ne 0 ];then
        echo "ERROR:  Error in process store hierarchy"
        p_ErrorDetails="'Extract SQL Failed.'"
        FeedExtractJobFailed
        exit 1
    fi

    echo "***END      :  ExtractNPS - `date` ***\n"

} #END ExtractNPS

#------------------------------------------------------------------------------#
function ToSendMail
#------------------------------------------------------------------------------#

{
#!/bin/bash
DATESTAMP=`date +%Y%m%d`
DATEFORMAIL=`date -d "yesterday" +%F`
mail_id="NPSIssues@albertsons.com,mohanapriya.mogileeswaran@albertsons.com,radha.desai@albertsons.com,satya.pasala@safeway.com,Greg.Hartnett@albertsons.com"
Mail_body="One or more new stores have come on our radar today as having experienced a “gremlin” issue beginning on ${DATEFORMAIL}. Please find the store id, register number and transaction log in the attachment for the date in question. You can filter by the store ID or register columns to see the unique stores/registers impacted. Next steps include IT instituting a short-term fix, submitting a service request and the Manila Team (IT.EBI.Manila.Support@albertsons.com) moving the erroneous data to a non-reporting table."
Mail_Subject="New-Gremlin-Detected"
mail -s $Mail_Subject -a $OutDir/NPS__${DATESTAMP}.csv $mail_id <<< $Mail_body
echo "mail sent"
} #END ToSendMail

#------------------------------------------------------------------------------#
# Main()
#------------------------------------------------------------------------------#


. ~/prod/bin/.edm.cfg
. ~/prod/bin/.edm_lib.ksh
. ~/prod/bin/.edm_util_lib.ksh

echo " CONNECTING TO ENVIRONMENT  ${EDM_ENV}"
python_BIN=~/prod/python_bin

#rm -f /home/edd01dw/prod/data/in/EDDW/OSSRS_NPS.BIN.NPS*.*
ScriptName=`basename $0`
ScriptNameWoExtn=$(echo ${ScriptName%.ksh})

echo "***START    :  ${ScriptName} for LOT ${LOT_ID} - `date` ***\n"

DATESTAMP=`date +%Y%m%d`
LotIdDate=$(LibLotToDate ${LOT_ID})

if [ "$#" -lt 4 ]; then
    echo "Illegal number of parameters"
    exit 1
fi


JobName=$3
CompressionType=$4
ApplicationName='eddw'
StageTableName=t_${ScriptNameWoExtn}
OutputFile=${JobName}_${DATESTAMP}

InDir=${EDM_IN}/${ApplicationName}
OutDir=${EDM_OUT}/${ApplicationName}

## Job Log Initiate Start.
## All p_ variables are used inside job log functions. Do NOT rename any
p_ApplicationName="'${ApplicationName}'"
p_ScriptProcName="'${ScriptName}'"
p_ExtractJobName="'${JobName}'"
p_LotId=${LOT_ID}
p_JobExecutionType="'H'"
p_JobParameterTxt="'$1,$2,$3,$4'"
p_ExtractStageTableName="'${DW_CONFIRMED_DB}.${CONF_STAGE_SC}.${StageTableName}'"
p_ExtractFileName="'${OutputFile}'"
p_ExtractTableName=NULL
p_ExtractFilePath="'${OutDir}'"
p_JobRunAutoId=`FeedExtractJobInitiate | tail -n1`
echo "${p_JobRunAutoId}"
## Job Log Initiate End
if [ $CompressionType != "NONE" -a $CompressionType != "GZIP" ]; then
    echo "ERROR: Invalid Compression Type for Feeds. Either pass NONE or GZIP AS a parameter for creating Feed files"
    exit 1
fi

#Clean up old files--need to uncomment
#rm -f $InDir/${JobName}*.csv
#rm -f $InDir/${JobName}*.gz

#'Prepare' function call inserts data into a stage table. Also the sql statements used during insert are stored into a variable, which is then logged into log table
PrepareData
#p_ExtractSQL="'`PrepareData | sed 's/'\''/'\'\''/g'`'"
#FeedExtractLogExtractSQL
#prepareSQLStatus=`echo "$p_ExtractSQL" | tail -n1 | head -c 20`
#if [ "${prepareSQLStatus}" != "***END : PrepareData" ]; then
#    echo "Prepare Failed !"
#    exit 1
#fi

Header='TRUE'
OutputHeader='NONE'

#Call function to extract data FROM snowflake to files
ExtractNPS $OutputFile "NONE" $Header

#Call function to merge all extracted files into ONe
EdmMergeExtracts $OutputFile "${OutputHeader}" "N"

#Call function to split greater than 4GB file
EdmSplitFile $InDir/$OutputFile '5' '6' $InDir

OutFiles=$(ls ${InDir}/${OutputFile}*.csv 2> /dev/null | wc -l)

#if [ "$OutFiles" != "0" ]; then
 #   #Invoke python Script to encrypt OfferToken
#    echo "Calling python script for encrypting OfferToken : START"
 #   python3 ${python_BIN}/edm_EDDW_004_ext_cdp_V12.py ${InDir}/${OutputFile}.csv ${InDir}/${OutputFile}.csv ${python_BIN}/EDDW_py.cfg NON_PROD
 #   echo "Calling python script for encrypting OfferToken : END"
#fi

if [ $CompressionType = "GZIP" ]; then
    gzip -v ${InDir}/${OutputFile}.csv
    mv $InDir/${OutputFile}.csv.gz $OutDir
else
    mv $InDir/${OutputFile}.csv $OutDir
fi

#Call function to send mail to users
ToSendMail $OutDir

#Mark job AS Success in job log table
#Leaving following AS NULL would make the FeedExtractJobSuccess function calculate these ON its own using the file path and file mASk
p_RecordCount=NULL
p_ExtracFileSizeKb=NULL
p_ExtractFileCount=NULL
p_ExtractFileName="'$OutputFile'"
FeedExtractJobSuccess

echo "PROCESS COMPLETED"
echo "***END      :  ${ScriptName} for LOT ${LOT_ID} - `date` ***"
