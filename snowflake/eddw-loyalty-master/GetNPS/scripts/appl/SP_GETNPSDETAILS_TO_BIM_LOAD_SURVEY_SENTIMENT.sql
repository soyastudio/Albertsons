--liquibase formatted sql
--changeset SYSTEM:SP_GETNPSDETAILS_TO_BIM_LOAD_SURVEY_SENTIMENT runOnChange:true splitStatements:false OBJECT_TYPE:SP
use database <<EDM_DB_NAME>>;
use schema <<EDM_DB_NAME>>.DW_APPL;

CREATE OR REPLACE PROCEDURE <<EDM_DB_NAME>>.DW_APPL.SP_GETNPSDETAILS_TO_BIM_LOAD_SURVEY_SENTIMENT(SRC_WRK_TBL VARCHAR, CNF_DB VARCHAR, C_LOYAL VARCHAR, C_STAGE VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$


//var cnf_db = CNF_DB;

//var wrk_schema = C_STAGE;

var cnf_schema = C_LOYAL;

var lkp_schema = C_LOYAL;

var src_wrk_tbl = SRC_WRK_TBL; var tgt_wrk_tbl = `${CNF_DB}.${C_STAGE}.SURVEY_SENTIMENT_WRK`;

var tgt_tbl = `${CNF_DB}.${cnf_schema}.SURVEY_SENTIMENT`;

var lkp_tb1 = `${CNF_DB}.${lkp_schema}.SURVEY_RESPONSE`;

var tgt_exp_tbl = `${CNF_DB}.${C_STAGE}.SURVEY_SENTIMENT_Exceptions`;

// ************** Load for SURVEY SENTIMENT table BEGIN *****************

// identify if the columns have changed between Target table and changed dataset and create a work table specific to the BIM table.

var create_tgt_wrk_table = `CREATE OR REPLACE TRANSIENT TABLE ${tgt_wrk_tbl} as

SELECT DISTINCT

src.SURVEY_ID

,src.SURVEY_QUESTION_SEQUENCE_NBR

,src.SURVEY_COMMENT_TXT

,src.COMMENT_PHRASE_TXT

,src.SURVEY_SENTIMENT_CD

,src.SURVEY_TYPE_CATEGORY_DSC

,src.SURVEY_SENTIMENT_TOPIC_TXT

,src.filename

,src.DW_LOGICAL_DELETE_IND

,CASE

WHEN (

tgt.SURVEY_ID IS NULL

and tgt.SURVEY_QUESTION_SEQUENCE_NBR is NULL

and tgt.SURVEY_COMMENT_TXT is NULL

and tgt.COMMENT_PHRASE_TXT is NULL

)

THEN 'I'

ELSE 'U'

END AS DML_Type

,CASE

WHEN tgt.DW_First_Effective_dt = CURRENT_DATE

THEN 1

Else 0

END as Sameday_chg_ind

FROM ( SELECT

SURVEY_ID

,SURVEY_QUESTION_SEQUENCE_NBR

,SURVEY_COMMENT_TXT

,COMMENT_PHRASE_TXT

,SURVEY_SENTIMENT_CD

,SURVEY_TYPE_CATEGORY_DSC

,SURVEY_SENTIMENT_TOPIC_TXT

,filename

,DW_LOGICAL_DELETE_IND

FROM (

SELECT

SURVEY_ID

,SURVEY_QUESTION_SEQUENCE_NBR

,SURVEY_COMMENT_TXT

,COMMENT_PHRASE_TXT

,SURVEY_SENTIMENT_CD

,SURVEY_TYPE_CATEGORY_DSC

,SURVEY_SENTIMENT_TOPIC_TXT

,filename

,DW_CREATE_TS

,false as DW_LOGICAL_DELETE_IND

,Row_number() OVER (

PARTITION BY SURVEY_ID,SURVEY_QUESTION_SEQUENCE_NBR,SURVEY_COMMENT_TXT,COMMENT_PHRASE_TXT

order by(DW_CREATE_TS) DESC) as rn

FROM(

SELECT

S.SURVEY_ID

,SURVEY_QUESTION_SEQUENCE_NBR

,SURVEY_COMMENT_TXT

,COMMENT_PHRASE_TXT

,SURVEY_SENTIMENT_CD

,SURVEY_TYPE_CATEGORY_DSC

,SURVEY_SENTIMENT_TOPIC_TXT

,filename

,DW_CREATE_TS

FROM

(

SELECT

SurveyID as SURVEY_ID

,Comment as SURVEY_COMMENT_TXT

,Phrase as COMMENT_PHRASE_TXT

,Sentiment as SURVEY_SENTIMENT_CD

,Field as SURVEY_TYPE_CATEGORY_DSC

,Topic as SURVEY_SENTIMENT_TOPIC_TXT

,filename

,DW_CREATE_TS

FROM ${src_wrk_tbl}

UNION ALL

SELECT DISTINCT

SURVEY_ID

// ,SURVEY_QUESTION_SEQUENCE_NBR

,SURVEY_COMMENT_TXT

,COMMENT_PHRASE_TXT

,SURVEY_SENTIMENT_CD

,SURVEY_TYPE_CATEGORY_DSC

,SURVEY_SENTIMENT_TOPIC_TXT

,filename

,DW_Create_ts

FROM ${tgt_exp_tbl}

)S

LEFT JOIN

(

SELECT DISTINCT SURVEY_ID,SURVEY_QUESTION_SEQUENCE_NBR

FROM ${lkp_tb1}

) E

ON NVL(S.SURVEY_ID,'-1')=NVL(E.SURVEY_ID,'-1')

)

) where rn=1 //and SURVEY_QUESTION_SEQUENCE_NBR is not NULL

) src

LEFT JOIN

(

SELECT DISTINCT

SURVEY_ID

,SURVEY_QUESTION_SEQUENCE_NBR

,SURVEY_COMMENT_TXT

,COMMENT_PHRASE_TXT

,SURVEY_SENTIMENT_CD

,SURVEY_TYPE_CATEGORY_DSC

,SURVEY_SENTIMENT_TOPIC_TXT

,DW_First_Effective_dt

,DW_LOGICAL_DELETE_IND

FROM

${tgt_tbl} tgt

WHERE DW_CURRENT_VERSION_IND = TRUE

)as tgt

ON

nvl(src.SURVEY_ID ,'-1') = nvl(tgt.SURVEY_ID ,'-1')

and nvl(src.SURVEY_QUESTION_SEQUENCE_NBR,'-1') = nvl(tgt.SURVEY_QUESTION_SEQUENCE_NBR ,'-1')

and nvl(src.SURVEY_COMMENT_TXT,'-1') = nvl(tgt.SURVEY_COMMENT_TXT,'-1')

and nvl(src.COMMENT_PHRASE_TXT,'-1') = nvl(tgt.COMMENT_PHRASE_TXT,'-1')

WHERE (

tgt.SURVEY_ID IS NULL

AND tgt.SURVEY_QUESTION_SEQUENCE_NBR is NULL

AND tgt.SURVEY_COMMENT_TXT is NULL

AND tgt.COMMENT_PHRASE_TXT is NULL

)

OR

(

NVL(src.SURVEY_ID,'-1') <> NVL(tgt.SURVEY_ID,'-1')

OR NVL(src.SURVEY_QUESTION_SEQUENCE_NBR ,'-1') <> NVL(tgt.SURVEY_QUESTION_SEQUENCE_NBR ,'-1')

OR NVL(src.SURVEY_COMMENT_TXT,'-1') <> NVL(tgt.SURVEY_COMMENT_TXT,'-1')

OR NVL(src.COMMENT_PHRASE_TXT,'-1') <> NVL(tgt.COMMENT_PHRASE_TXT,'-1')

OR NVL(src.SURVEY_SENTIMENT_CD,'-1') <> NVL(tgt.SURVEY_SENTIMENT_CD,'-1')

OR NVL(src.SURVEY_TYPE_CATEGORY_DSC,'-1') <> NVL(tgt.SURVEY_TYPE_CATEGORY_DSC,'-1')

OR NVL(src.SURVEY_SENTIMENT_TOPIC_TXT,'-1') <>NVL(tgt.SURVEY_SENTIMENT_TOPIC_TXT,'-1')

OR src.DW_LOGICAL_DELETE_IND <> tgt.DW_LOGICAL_DELETE_IND

)`;try {

snowflake.execute ({ sqlText: create_tgt_wrk_table });

}

catch (err) {

return `Creation of work table ${tgt_wrk_tbl} Failed with error: ${err}`; // Return a error message.

}

// Transaction for Updates, Insert begins

var sql_begin = "BEGIN"

// SCD Type2 - Processing Different day updates

var sql_updates = `UPDATE ${tgt_tbl} as tgt

SET

DW_Last_Effective_dt = CURRENT_DATE - 1,

DW_CURRENT_VERSION_IND = FALSE,

DW_Logical_delete_ind=TRUE,

DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,

DW_SOURCE_UPDATE_NM = filename

FROM (

SELECT

SURVEY_ID

,SURVEY_QUESTION_SEQUENCE_NBR

,SURVEY_COMMENT_TXT

,COMMENT_PHRASE_TXT

,filename

FROM ${tgt_wrk_tbl}

WHERE

DML_Type = 'U'

AND Sameday_chg_ind = 0

) src

WHERE

nvl(src.SURVEY_ID,'-1') = nvl(tgt.SURVEY_ID,'-1')

AND nvl(src.SURVEY_QUESTION_SEQUENCE_NBR,'-1') = nvl(tgt.SURVEY_QUESTION_SEQUENCE_NBR,'-1')

AND nvl(src.SURVEY_COMMENT_TXT,'-1')= nvl(tgt.SURVEY_COMMENT_TXT,'-1')

AND nvl(src.COMMENT_PHRASE_TXT,'-1') = nvl(tgt.COMMENT_PHRASE_TXT,'-1')

AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// SCD Type1 - Processing Sameday updates

var sql_sameday = `UPDATE ${tgt_tbl} as tgt

SET SURVEY_ID = src.SURVEY_ID,

SURVEY_QUESTION_SEQUENCE_NBR = src.SURVEY_QUESTION_SEQUENCE_NBR,

SURVEY_COMMENT_TXT = src.SURVEY_COMMENT_TXT,

COMMENT_PHRASE_TXT=src.COMMENT_PHRASE_TXT,

SURVEY_SENTIMENT_CD = src.SURVEY_SENTIMENT_CD,

SURVEY_TYPE_CATEGORY_DSC = src.SURVEY_TYPE_CATEGORY_DSC,

SURVEY_SENTIMENT_TOPIC_TXT=src.SURVEY_SENTIMENT_TOPIC_TXT,

DW_Logical_delete_ind = src.DW_Logical_delete_ind,

DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP,

DW_SOURCE_UPDATE_NM = filename

FROM (

SELECT

SURVEY_ID

,SURVEY_QUESTION_SEQUENCE_NBR

,SURVEY_COMMENT_TXT

,COMMENT_PHRASE_TXT

,SURVEY_SENTIMENT_CD

,SURVEY_TYPE_CATEGORY_DSC

,SURVEY_SENTIMENT_TOPIC_TXT

,filename

,DW_Logical_delete_ind

FROM ${tgt_wrk_tbl}

WHERE

DML_Type = 'U'

AND Sameday_chg_ind = 1

) src WHERE

nvl(src.SURVEY_ID,'-1') = nvl(tgt.SURVEY_ID,'-1')

AND nvl(src.SURVEY_QUESTION_SEQUENCE_NBR,'-1') = nvl(tgt.SURVEY_QUESTION_SEQUENCE_NBR,'-1')

AND nvl(src.SURVEY_COMMENT_TXT,'-1')= nvl(tgt.SURVEY_COMMENT_TXT,'-1')

AND nvl(src.COMMENT_PHRASE_TXT,'-1') = nvl(tgt.COMMENT_PHRASE_TXT,'-1')

AND tgt.DW_CURRENT_VERSION_IND = TRUE`;

// Processing Inserts

var sql_inserts = `INSERT INTO ${tgt_tbl}

(

SURVEY_ID

,SURVEY_QUESTION_SEQUENCE_NBR

,SURVEY_COMMENT_TXT

,COMMENT_PHRASE_TXT

,DW_First_Effective_Dt

,DW_Last_Effective_Dt

,SURVEY_SENTIMENT_CD

,SURVEY_TYPE_CATEGORY_DSC

,SURVEY_SENTIMENT_TOPIC_TXT

,DW_CREATE_TS

,DW_LOGICAL_DELETE_IND

,DW_SOURCE_CREATE_NM

,DW_CURRENT_VERSION_IND

)

SELECT

SURVEY_ID

,SURVEY_QUESTION_SEQUENCE_NBR

,SURVEY_COMMENT_TXT

,COMMENT_PHRASE_TXT

,CURRENT_DATE

,'31-DEC-9999'

,SURVEY_SENTIMENT_CD

,SURVEY_TYPE_CATEGORY_DSC

,SURVEY_SENTIMENT_TOPIC_TXT

,CURRENT_TIMESTAMP

,DW_LOGICAL_DELETE_IND

,filename

,TRUE

FROM ${tgt_wrk_tbl}

WHERE

Sameday_chg_ind = 0

AND SURVEY_ID IS NOT NULL

AND SURVEY_QUESTION_SEQUENCE_NBR IS NOT NULL`;

var sql_commit = "COMMIT";

var sql_rollback = "ROLLBACK";

try {

snowflake.execute ({ sqlText: sql_begin });

snowflake.execute ({ sqlText: sql_updates });

snowflake.execute ({ sqlText: sql_sameday });

snowflake.execute ({ sqlText: sql_inserts });

snowflake.execute ({ sqlText: sql_commit });

} catch (err) {

snowflake.execute ({ sqlText: sql_rollback });

return `Loading of table ${tgt_tbl} Failed with error: ${err}` ; // Return a error message.

}

var truncate_exceptions = `DELETE FROM ${tgt_exp_tbl}`;

var sql_exceptions = `INSERT INTO ${tgt_exp_tbl}

select Distinct

SURVEY_ID

,SURVEY_QUESTION_SEQUENCE_NBR

,SURVEY_SENTIMENT_CD

,SURVEY_COMMENT_TXT

,SURVEY_TYPE_CATEGORY_DSC

,COMMENT_PHRASE_TXT

,SURVEY_SENTIMENT_TOPIC_TXT

,filename

,DML_Type

,Sameday_chg_ind

,CASE WHEN SURVEY_ID is NULL THEN 'SURVEY_ID is NULL'

WHEN SURVEY_QUESTION_SEQUENCE_NBR is NULL THEN 'SURVEY_QUESTION_SEQUENCE_NBR is NULL'

ELSE NULL END AS Exception_Reason

,CURRENT_TIMESTAMP AS DW_CREATE_TS

FROM ${tgt_wrk_tbl}

WHERE SURVEY_ID IS NULL

or SURVEY_QUESTION_SEQUENCE_NBR IS NULL

`;

try

{

snowflake.execute (

{sqlText: sql_begin }

);

snowflake.execute(

{sqlText: truncate_exceptions}

);

snowflake.execute (

{sqlText: sql_exceptions }

);

snowflake.execute (

{sqlText: sql_commit }

);

}

catch (err) {

snowflake.execute (

{sqlText: sql_rollback }

);

return `Insert into tgt Exception table ${tgt_exp_tbl} Failed with error: ${err}`; // Return a error message.

}

// ************** Load for SURVEY SENTIMENT Table ENDs *****************


$$;
