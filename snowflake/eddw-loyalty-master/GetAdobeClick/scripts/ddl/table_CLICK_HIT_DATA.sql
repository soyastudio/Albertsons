--liquibase formatted sql
--changeset SYSTEM:CLICK_HIT_DATA runOnChange:true splitStatements:false OBJECT_TYPE:TABLE
use database EDM_CONFIRMED_PRD;
use schema DW_C_USER_ACTIVITY;

create or replace TABLE CLICK_HIT_DATA cluster by (to_date(DW_CREATETS))(
	ACCEPT_LANGUAGE VARCHAR(16777216),
	ADCLASSIFICATIONCREATIVE VARCHAR(16777216),
	ADLOAD VARCHAR(16777216),
	AEMASSETID VARCHAR(16777216),
	AEMASSETSOURCE VARCHAR(16777216),
	AEMCLICKEDASSETID VARCHAR(16777216),
	BROWSER VARCHAR(16777216),
	BROWSER_HEIGHT VARCHAR(16777216),
	BROWSER_WIDTH VARCHAR(16777216),
	C_COLOR VARCHAR(16777216),
	CAMPAIGN VARCHAR(16777216),
	CARRIER VARCHAR(16777216),
	CHANNEL VARCHAR(16777216),
	CLICK_ACTION VARCHAR(16777216),
	CLICK_ACTION_TYPE VARCHAR(16777216),
	CLICK_CONTEXT VARCHAR(16777216),
	CLICK_CONTEXT_TYPE VARCHAR(16777216),
	CLICK_SOURCEID VARCHAR(16777216),
	CLICK_TAG VARCHAR(16777216),
	CLICKMAPLINK VARCHAR(16777216),
	CLICKMAPLINKBYREGION VARCHAR(16777216),
	CLICKMAPPAGE VARCHAR(16777216),
	CLICKMAPREGION VARCHAR(16777216),
	CODE_VER VARCHAR(16777216),
	COLOR VARCHAR(16777216),
	CONNECTION_TYPE VARCHAR(16777216),
	COOKIES VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	CT_CONNECT_TYPE VARCHAR(16777216),
	CURR_FACTOR VARCHAR(16777216),
	CURR_RATE VARCHAR(16777216),
	CURRENCY VARCHAR(16777216),
	CUST_HIT_TIME_GMT VARCHAR(16777216),
	CUST_VISID VARCHAR(16777216),
	DAILY_VISITOR VARCHAR(16777216),
	DATE_TIME VARCHAR(16777216),
	DOMAIN VARCHAR(16777216),
	DUPLICATE_EVENTS VARCHAR(16777216),
	DUPLICATE_PURCHASE VARCHAR(16777216),
	DUPLICATED_FROM VARCHAR(16777216),
	EF_ID VARCHAR(16777216),
	EVAR1 VARCHAR(16777216),
	EVAR2 VARCHAR(16777216),
	EVAR3 VARCHAR(16777216),
	EVAR4 VARCHAR(16777216),
	EVAR5 VARCHAR(16777216),
	EVAR6 VARCHAR(16777216),
	EVAR7 VARCHAR(16777216),
	EVAR8 VARCHAR(16777216),
	EVAR9 VARCHAR(16777216),
	EVAR10 VARCHAR(16777216),
	EVAR11 VARCHAR(16777216),
	EVAR12 VARCHAR(16777216),
	EVAR13 VARCHAR(16777216),
	EVAR14 VARCHAR(16777216),
	EVAR15 VARCHAR(16777216),
	EVAR16 VARCHAR(16777216),
	EVAR17 VARCHAR(16777216),
	EVAR18 VARCHAR(16777216),
	EVAR19 VARCHAR(16777216),
	EVAR20 VARCHAR(16777216),
	EVAR21 VARCHAR(16777216),
	EVAR22 VARCHAR(16777216),
	EVAR23 VARCHAR(16777216),
	EVAR24 VARCHAR(16777216),
	EVAR25 VARCHAR(16777216),
	EVAR26 VARCHAR(16777216),
	EVAR27 VARCHAR(16777216),
	EVAR28 VARCHAR(16777216),
	EVAR29 VARCHAR(16777216),
	EVAR30 VARCHAR(16777216),
	EVAR31 VARCHAR(16777216),
	EVAR32 VARCHAR(16777216),
	EVAR33 VARCHAR(16777216),
	EVAR34 VARCHAR(16777216),
	EVAR35 VARCHAR(16777216),
	EVAR36 VARCHAR(16777216),
	EVAR37 VARCHAR(16777216),
	EVAR38 VARCHAR(16777216),
	EVAR39 VARCHAR(16777216),
	EVAR40 VARCHAR(16777216),
	EVAR41 VARCHAR(16777216),
	EVAR42 VARCHAR(16777216),
	EVAR43 VARCHAR(16777216),
	EVAR44 VARCHAR(16777216),
	EVAR45 VARCHAR(16777216),
	EVAR46 VARCHAR(16777216),
	EVAR47 VARCHAR(16777216),
	EVAR48 VARCHAR(16777216),
	EVAR49 VARCHAR(16777216),
	EVAR50 VARCHAR(16777216),
	EVAR51 VARCHAR(16777216),
	EVAR52 VARCHAR(16777216),
	EVAR53 VARCHAR(16777216),
	EVAR54 VARCHAR(16777216),
	EVAR55 VARCHAR(16777216),
	EVAR56 VARCHAR(16777216),
	EVAR57 VARCHAR(16777216),
	EVAR58 VARCHAR(16777216),
	EVAR59 VARCHAR(16777216),
	EVAR60 VARCHAR(16777216),
	EVAR61 VARCHAR(16777216),
	EVAR62 VARCHAR(16777216),
	EVAR63 VARCHAR(16777216),
	EVAR64 VARCHAR(16777216),
	EVAR65 VARCHAR(16777216),
	EVAR66 VARCHAR(16777216),
	EVAR67 VARCHAR(16777216),
	EVAR68 VARCHAR(16777216),
	EVAR69 VARCHAR(16777216),
	EVAR70 VARCHAR(16777216),
	EVAR71 VARCHAR(16777216),
	EVAR72 VARCHAR(16777216),
	EVAR73 VARCHAR(16777216),
	EVAR74 VARCHAR(16777216),
	EVAR75 VARCHAR(16777216),
	EVAR76 VARCHAR(16777216),
	EVAR77 VARCHAR(16777216),
	EVAR78 VARCHAR(16777216),
	EVAR79 VARCHAR(16777216),
	EVAR80 VARCHAR(16777216),
	EVAR81 VARCHAR(16777216),
	EVAR82 VARCHAR(16777216),
	EVAR83 VARCHAR(16777216),
	EVAR84 VARCHAR(16777216),
	EVAR85 VARCHAR(16777216),
	EVAR86 VARCHAR(16777216),
	EVAR87 VARCHAR(16777216),
	EVAR88 VARCHAR(16777216),
	EVAR89 VARCHAR(16777216),
	EVAR90 VARCHAR(16777216),
	EVAR91 VARCHAR(16777216),
	EVAR92 VARCHAR(16777216),
	EVAR93 VARCHAR(16777216),
	EVAR94 VARCHAR(16777216),
	EVAR95 VARCHAR(16777216),
	EVAR96 VARCHAR(16777216),
	EVAR97 VARCHAR(16777216),
	EVAR98 VARCHAR(16777216),
	EVAR99 VARCHAR(16777216),
	EVAR100 VARCHAR(16777216),
	EVAR101 VARCHAR(16777216),
	EVAR102 VARCHAR(16777216),
	EVAR103 VARCHAR(16777216),
	EVAR104 VARCHAR(16777216),
	EVAR105 VARCHAR(16777216),
	EVAR106 VARCHAR(16777216),
	EVAR107 VARCHAR(16777216),
	EVAR108 VARCHAR(16777216),
	EVAR109 VARCHAR(16777216),
	EVAR110 VARCHAR(16777216),
	EVAR111 VARCHAR(16777216),
	EVAR112 VARCHAR(16777216),
	EVAR113 VARCHAR(16777216),
	EVAR114 VARCHAR(16777216),
	EVAR115 VARCHAR(16777216),
	EVAR116 VARCHAR(16777216),
	EVAR117 VARCHAR(16777216),
	EVAR118 VARCHAR(16777216),
	EVAR119 VARCHAR(16777216),
	EVAR120 VARCHAR(16777216),
	EVAR121 VARCHAR(16777216),
	EVAR122 VARCHAR(16777216),
	EVAR123 VARCHAR(16777216),
	EVAR124 VARCHAR(16777216),
	EVAR125 VARCHAR(16777216),
	EVAR126 VARCHAR(16777216),
	EVAR127 VARCHAR(16777216),
	EVAR128 VARCHAR(16777216),
	EVAR129 VARCHAR(16777216),
	EVAR130 VARCHAR(16777216),
	EVAR131 VARCHAR(16777216),
	EVAR132 VARCHAR(16777216),
	EVAR133 VARCHAR(16777216),
	EVAR134 VARCHAR(16777216),
	EVAR135 VARCHAR(16777216),
	EVAR136 VARCHAR(16777216),
	EVAR137 VARCHAR(16777216),
	EVAR138 VARCHAR(16777216),
	EVAR139 VARCHAR(16777216),
	EVAR140 VARCHAR(16777216),
	EVAR141 VARCHAR(16777216),
	EVAR142 VARCHAR(16777216),
	EVAR143 VARCHAR(16777216),
	EVAR144 VARCHAR(16777216),
	EVAR145 VARCHAR(16777216),
	EVAR146 VARCHAR(16777216),
	EVAR147 VARCHAR(16777216),
	EVAR148 VARCHAR(16777216),
	EVAR149 VARCHAR(16777216),
	EVAR150 VARCHAR(16777216),
	EVAR151 VARCHAR(16777216),
	EVAR152 VARCHAR(16777216),
	EVAR153 VARCHAR(16777216),
	EVAR154 VARCHAR(16777216),
	EVAR155 VARCHAR(16777216),
	EVAR156 VARCHAR(16777216),
	EVAR157 VARCHAR(16777216),
	EVAR158 VARCHAR(16777216),
	EVAR159 VARCHAR(16777216),
	EVAR160 VARCHAR(16777216),
	EVAR161 VARCHAR(16777216),
	EVAR162 VARCHAR(16777216),
	EVAR163 VARCHAR(16777216),
	EVAR164 VARCHAR(16777216),
	EVAR165 VARCHAR(16777216),
	EVAR166 VARCHAR(16777216),
	EVAR167 VARCHAR(16777216),
	EVAR168 VARCHAR(16777216),
	EVAR169 VARCHAR(16777216),
	EVAR170 VARCHAR(16777216),
	EVAR171 VARCHAR(16777216),
	EVAR172 VARCHAR(16777216),
	EVAR173 VARCHAR(16777216),
	EVAR174 VARCHAR(16777216),
	EVAR175 VARCHAR(16777216),
	EVAR176 VARCHAR(16777216),
	EVAR177 VARCHAR(16777216),
	EVAR178 VARCHAR(16777216),
	EVAR179 VARCHAR(16777216),
	EVAR180 VARCHAR(16777216),
	EVAR181 VARCHAR(16777216),
	EVAR182 VARCHAR(16777216),
	EVAR183 VARCHAR(16777216),
	EVAR184 VARCHAR(16777216),
	EVAR185 VARCHAR(16777216),
	EVAR186 VARCHAR(16777216),
	EVAR187 VARCHAR(16777216),
	EVAR188 VARCHAR(16777216),
	EVAR189 VARCHAR(16777216),
	EVAR190 VARCHAR(16777216),
	EVAR191 VARCHAR(16777216),
	EVAR192 VARCHAR(16777216),
	EVAR193 VARCHAR(16777216),
	EVAR194 VARCHAR(16777216),
	EVAR195 VARCHAR(16777216),
	EVAR196 VARCHAR(16777216),
	EVAR197 VARCHAR(16777216),
	EVAR198 VARCHAR(16777216),
	EVAR199 VARCHAR(16777216),
	EVAR200 VARCHAR(16777216),
	EVAR201 VARCHAR(16777216),
	EVAR202 VARCHAR(16777216),
	EVAR203 VARCHAR(16777216),
	EVAR204 VARCHAR(16777216),
	EVAR205 VARCHAR(16777216),
	EVAR206 VARCHAR(16777216),
	EVAR207 VARCHAR(16777216),
	EVAR208 VARCHAR(16777216),
	EVAR209 VARCHAR(16777216),
	EVAR210 VARCHAR(16777216),
	EVAR211 VARCHAR(16777216),
	EVAR212 VARCHAR(16777216),
	EVAR213 VARCHAR(16777216),
	EVAR214 VARCHAR(16777216),
	EVAR215 VARCHAR(16777216),
	EVAR216 VARCHAR(16777216),
	EVAR217 VARCHAR(16777216),
	EVAR218 VARCHAR(16777216),
	EVAR219 VARCHAR(16777216),
	EVAR220 VARCHAR(16777216),
	EVAR221 VARCHAR(16777216),
	EVAR222 VARCHAR(16777216),
	EVAR223 VARCHAR(16777216),
	EVAR224 VARCHAR(16777216),
	EVAR225 VARCHAR(16777216),
	EVAR226 VARCHAR(16777216),
	EVAR227 VARCHAR(16777216),
	EVAR228 VARCHAR(16777216),
	EVAR229 VARCHAR(16777216),
	EVAR230 VARCHAR(16777216),
	EVAR231 VARCHAR(16777216),
	EVAR232 VARCHAR(16777216),
	EVAR233 VARCHAR(16777216),
	EVAR234 VARCHAR(16777216),
	EVAR235 VARCHAR(16777216),
	EVAR236 VARCHAR(16777216),
	EVAR237 VARCHAR(16777216),
	EVAR238 VARCHAR(16777216),
	EVAR239 VARCHAR(16777216),
	EVAR240 VARCHAR(16777216),
	EVAR241 VARCHAR(16777216),
	EVAR242 VARCHAR(16777216),
	EVAR243 VARCHAR(16777216),
	EVAR244 VARCHAR(16777216),
	EVAR245 VARCHAR(16777216),
	EVAR246 VARCHAR(16777216),
	EVAR247 VARCHAR(16777216),
	EVAR248 VARCHAR(16777216),
	EVAR249 VARCHAR(16777216),
	EVAR250 VARCHAR(16777216),
	EVENT_LIST VARCHAR(16777216),
	EXCLUDE_HIT VARCHAR(16777216),
	FIRST_HIT_PAGE_URL VARCHAR(16777216),
	FIRST_HIT_PAGENAME VARCHAR(16777216),
	FIRST_HIT_REF_DOMAIN VARCHAR(16777216),
	FIRST_HIT_REF_TYPE VARCHAR(16777216),
	FIRST_HIT_REFERRER VARCHAR(16777216),
	FIRST_HIT_TIME_GMT VARCHAR(16777216),
	GEO_CITY VARCHAR(16777216),
	GEO_COUNTRY VARCHAR(16777216),
	GEO_DMA VARCHAR(16777216),
	GEO_REGION VARCHAR(16777216),
	GEO_ZIP VARCHAR(16777216),
	HIER1 VARCHAR(16777216),
	HIER2 VARCHAR(16777216),
	HIER3 VARCHAR(16777216),
	HIER4 VARCHAR(16777216),
	HIER5 VARCHAR(16777216),
	HIT_SOURCE VARCHAR(16777216),
	HIT_TIME_GMT VARCHAR(16777216),
	HITID_HIGH VARCHAR(16777216),
	HITID_LOW VARCHAR(16777216),
	HOMEPAGE VARCHAR(16777216),
	HOURLY_VISITOR VARCHAR(16777216),
	IP VARCHAR(16777216),
	IP2 VARCHAR(16777216),
	J_JSCRIPT VARCHAR(16777216),
	JAVA_ENABLED VARCHAR(16777216),
	JAVASCRIPT VARCHAR(16777216),
	LANGUAGE VARCHAR(16777216),
	LAST_HIT_TIME_GMT VARCHAR(16777216),
	LAST_PURCHASE_NUM VARCHAR(16777216),
	LAST_PURCHASE_TIME_GMT VARCHAR(16777216),
	LATLON1 VARCHAR(16777216),
	LATLON23 VARCHAR(16777216),
	LATLON45 VARCHAR(16777216),
	MC_AUDIENCES VARCHAR(16777216),
	MCVISID VARCHAR(16777216),
	MOBILE_ID VARCHAR(16777216),
	MOBILEACQUISITIONCLICKS VARCHAR(16777216),
	MOBILEACTION VARCHAR(16777216),
	MOBILEACTIONINAPPTIME VARCHAR(16777216),
	MOBILEACTIONTOTALTIME VARCHAR(16777216),
	MOBILEAPPID VARCHAR(16777216),
	MOBILEAPPPERFORMANCEAFFECTEDUSERS VARCHAR(16777216),
	MOBILEAPPPERFORMANCEAPPID VARCHAR(16777216),
	MOBILEAPPPERFORMANCEAPPIDAPPPERFAPPNAME VARCHAR(16777216),
	MOBILEAPPPERFORMANCEAPPIDAPPPERFPLATFORM VARCHAR(16777216),
	MOBILEAPPPERFORMANCECRASHES VARCHAR(16777216),
	MOBILEAPPPERFORMANCECRASHID VARCHAR(16777216),
	MOBILEAPPPERFORMANCECRASHIDAPPPERFCRASHNAME VARCHAR(16777216),
	MOBILEAPPPERFORMANCELOADS VARCHAR(16777216),
	MOBILEAPPSTOREAVGRATING VARCHAR(16777216),
	MOBILEAPPSTOREDOWNLOADS VARCHAR(16777216),
	MOBILEAPPSTOREINAPPREVENUE VARCHAR(16777216),
	MOBILEAPPSTOREINAPPROYALTIES VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTID VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDAPPSTOREUSER VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDAPPLICATIONNAME VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDAPPLICATIONVERSION VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDAPPSTORENAME VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDCATEGORYNAME VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDCOUNTRYNAME VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDDEVICEMANUFACTURER VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDDEVICENAME VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDINAPPNAME VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDPLATFORMNAMEVERSION VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDRANKCATEGORYTYPE VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDREGIONNAME VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDREVIEWCOMMENT VARCHAR(16777216),
	MOBILEAPPSTOREOBJECTIDREVIEWTITLE VARCHAR(16777216),
	MOBILEAPPSTOREONEOFFREVENUE VARCHAR(16777216),
	MOBILEAPPSTOREONEOFFROYALTIES VARCHAR(16777216),
	MOBILEAPPSTOREPURCHASES VARCHAR(16777216),
	MOBILEAPPSTORERANK VARCHAR(16777216),
	MOBILEAPPSTORERANKDIVISOR VARCHAR(16777216),
	MOBILEAPPSTORERATING VARCHAR(16777216),
	MOBILEAPPSTORERATINGDIVISOR VARCHAR(16777216),
	MOBILEAVGPREVSESSIONLENGTH VARCHAR(16777216),
	MOBILEBEACONMAJOR VARCHAR(16777216),
	MOBILEBEACONMINOR VARCHAR(16777216),
	MOBILEBEACONPROXIMITY VARCHAR(16777216),
	MOBILEBEACONUUID VARCHAR(16777216),
	MOBILECAMPAIGNCONTENT VARCHAR(16777216),
	MOBILECAMPAIGNMEDIUM VARCHAR(16777216),
	MOBILECAMPAIGNNAME VARCHAR(16777216),
	MOBILECAMPAIGNSOURCE VARCHAR(16777216),
	MOBILECAMPAIGNTERM VARCHAR(16777216),
	MOBILECRASHES VARCHAR(16777216),
	MOBILECRASHRATE VARCHAR(16777216),
	MOBILEDAILYENGAGEDUSERS VARCHAR(16777216),
	MOBILEDAYOFWEEK VARCHAR(16777216),
	MOBILEDAYSSINCEFIRSTUSE VARCHAR(16777216),
	MOBILEDAYSSINCELASTUPGRADE VARCHAR(16777216),
	MOBILEDAYSSINCELASTUSE VARCHAR(16777216),
	MOBILEDEEPLINKID VARCHAR(16777216),
	MOBILEDEEPLINKIDNAME VARCHAR(16777216),
	MOBILEDEVICE VARCHAR(16777216),
	MOBILEHOUROFDAY VARCHAR(16777216),
	MOBILEINSTALLDATE VARCHAR(16777216),
	MOBILEINSTALLS VARCHAR(16777216),
	MOBILELAUNCHES VARCHAR(16777216),
	MOBILELAUNCHESSINCELASTUPGRADE VARCHAR(16777216),
	MOBILELAUNCHNUMBER VARCHAR(16777216),
	MOBILELTV VARCHAR(16777216),
	MOBILELTVTOTAL VARCHAR(16777216),
	MOBILEMESSAGEBUTTONNAME VARCHAR(16777216),
	MOBILEMESSAGECLICKS VARCHAR(16777216),
	MOBILEMESSAGEID VARCHAR(16777216),
	MOBILEMESSAGEIDDEST VARCHAR(16777216),
	MOBILEMESSAGEIDNAME VARCHAR(16777216),
	MOBILEMESSAGEIDTYPE VARCHAR(16777216),
	MOBILEMESSAGEIMPRESSIONS VARCHAR(16777216),
	MOBILEMESSAGEONLINE VARCHAR(16777216),
	MOBILEMESSAGEPUSHOPTIN VARCHAR(16777216),
	MOBILEMESSAGEPUSHPAYLOADID VARCHAR(16777216),
	MOBILEMESSAGEPUSHPAYLOADIDNAME VARCHAR(16777216),
	MOBILEMESSAGEVIEWS VARCHAR(16777216),
	MOBILEMONTHLYENGAGEDUSERS VARCHAR(16777216),
	MOBILEOSENVIRONMENT VARCHAR(16777216),
	MOBILEOSVERSION VARCHAR(16777216),
	MOBILEPLACEACCURACY VARCHAR(16777216),
	MOBILEPLACECATEGORY VARCHAR(16777216),
	MOBILEPLACEDWELLTIME VARCHAR(16777216),
	MOBILEPLACEENTRY VARCHAR(16777216),
	MOBILEPLACEEXIT VARCHAR(16777216),
	MOBILEPLACEID VARCHAR(16777216),
	MOBILEPREVSESSIONLENGTH VARCHAR(16777216),
	MOBILEPUSHOPTIN VARCHAR(16777216),
	MOBILEPUSHPAYLOADID VARCHAR(16777216),
	MOBILERELAUNCHCAMPAIGNCONTENT VARCHAR(16777216),
	MOBILERELAUNCHCAMPAIGNMEDIUM VARCHAR(16777216),
	MOBILERELAUNCHCAMPAIGNSOURCE VARCHAR(16777216),
	MOBILERELAUNCHCAMPAIGNTERM VARCHAR(16777216),
	MOBILERELAUNCHCAMPAIGNTRACKINGCODE VARCHAR(16777216),
	MOBILERELAUNCHCAMPAIGNTRACKINGCODENAME VARCHAR(16777216),
	MOBILERESOLUTION VARCHAR(16777216),
	MOBILEUPGRADES VARCHAR(16777216),
	MONTHLY_VISITOR VARCHAR(16777216),
	MVVAR1 VARCHAR(16777216),
	MVVAR2 VARCHAR(16777216),
	MVVAR3 VARCHAR(16777216),
	NAMESPACE VARCHAR(16777216),
	NEW_VISIT VARCHAR(16777216),
	OS VARCHAR(16777216),
	P_PLUGINS VARCHAR(16777216),
	PAGE_EVENT VARCHAR(16777216),
	PAGE_EVENT_VAR1 VARCHAR(16777216),
	PAGE_EVENT_VAR2 VARCHAR(16777216),
	PAGE_EVENT_VAR3 VARCHAR(16777216),
	PAGE_TYPE VARCHAR(16777216),
	PAGE_URL VARCHAR(16777216),
	PAGENAME VARCHAR(16777216),
	PAID_SEARCH VARCHAR(16777216),
	PARTNER_PLUGINS VARCHAR(16777216),
	PERSISTENT_COOKIE VARCHAR(16777216),
	PLUGINS VARCHAR(16777216),
	POINTOFINTEREST VARCHAR(16777216),
	POINTOFINTERESTDISTANCE VARCHAR(16777216),
	POST_ADCLASSIFICATIONCREATIVE VARCHAR(16777216),
	POST_ADLOAD VARCHAR(16777216),
	POST_BROWSER_HEIGHT VARCHAR(16777216),
	POST_BROWSER_WIDTH VARCHAR(16777216),
	POST_CAMPAIGN VARCHAR(16777216),
	POST_CHANNEL VARCHAR(16777216),
	POST_CLICKMAPLINK VARCHAR(16777216),
	POST_CLICKMAPLINKBYREGION VARCHAR(16777216),
	POST_CLICKMAPPAGE VARCHAR(16777216),
	POST_CLICKMAPREGION VARCHAR(16777216),
	POST_COOKIES VARCHAR(16777216),
	POST_CURRENCY VARCHAR(16777216),
	POST_CUST_HIT_TIME_GMT VARCHAR(16777216),
	POST_CUST_VISID VARCHAR(16777216),
	POST_EF_ID VARCHAR(16777216),
	POST_EVAR1 VARCHAR(16777216),
	POST_EVAR2 VARCHAR(16777216),
	POST_EVAR3 VARCHAR(16777216),
	POST_EVAR4 VARCHAR(16777216),
	POST_EVAR5 VARCHAR(16777216),
	POST_EVAR6 VARCHAR(16777216),
	POST_EVAR7 VARCHAR(16777216),
	POST_EVAR8 VARCHAR(16777216),
	POST_EVAR9 VARCHAR(16777216),
	POST_EVAR10 VARCHAR(16777216),
	POST_EVAR11 VARCHAR(16777216),
	POST_EVAR12 VARCHAR(16777216),
	POST_EVAR13 VARCHAR(16777216),
	POST_EVAR14 VARCHAR(16777216),
	POST_EVAR15 VARCHAR(16777216),
	POST_EVAR16 VARCHAR(16777216),
	POST_EVAR17 VARCHAR(16777216),
	POST_EVAR18 VARCHAR(16777216),
	POST_EVAR19 VARCHAR(16777216),
	POST_EVAR20 VARCHAR(16777216),
	POST_EVAR21 VARCHAR(16777216),
	POST_EVAR22 VARCHAR(16777216),
	POST_EVAR23 VARCHAR(16777216),
	POST_EVAR24 VARCHAR(16777216),
	POST_EVAR25 VARCHAR(16777216),
	POST_EVAR26 VARCHAR(16777216),
	POST_EVAR27 VARCHAR(16777216),
	POST_EVAR28 VARCHAR(16777216),
	POST_EVAR29 VARCHAR(16777216),
	POST_EVAR30 VARCHAR(16777216),
	POST_EVAR31 VARCHAR(16777216),
	POST_EVAR32 VARCHAR(16777216),
	POST_EVAR33 VARCHAR(16777216),
	POST_EVAR34 VARCHAR(16777216),
	POST_EVAR35 VARCHAR(16777216),
	POST_EVAR36 VARCHAR(16777216),
	POST_EVAR37 VARCHAR(16777216),
	POST_EVAR38 VARCHAR(16777216),
	POST_EVAR39 VARCHAR(16777216),
	POST_EVAR40 VARCHAR(16777216),
	POST_EVAR41 VARCHAR(16777216),
	POST_EVAR42 VARCHAR(16777216),
	POST_EVAR43 VARCHAR(16777216),
	POST_EVAR44 VARCHAR(16777216),
	POST_EVAR45 VARCHAR(16777216),
	POST_EVAR46 VARCHAR(16777216),
	POST_EVAR47 VARCHAR(16777216),
	POST_EVAR48 VARCHAR(16777216),
	POST_EVAR49 VARCHAR(16777216),
	POST_EVAR50 VARCHAR(16777216),
	POST_EVAR51 VARCHAR(16777216),
	POST_EVAR52 VARCHAR(16777216),
	POST_EVAR53 VARCHAR(16777216),
	POST_EVAR54 VARCHAR(16777216),
	POST_EVAR55 VARCHAR(16777216),
	POST_EVAR56 VARCHAR(16777216),
	POST_EVAR57 VARCHAR(16777216),
	POST_EVAR58 VARCHAR(16777216),
	POST_EVAR59 VARCHAR(16777216),
	POST_EVAR60 VARCHAR(16777216),
	POST_EVAR61 VARCHAR(16777216),
	POST_EVAR62 VARCHAR(16777216),
	POST_EVAR63 VARCHAR(16777216),
	POST_EVAR64 VARCHAR(16777216),
	POST_EVAR65 VARCHAR(16777216),
	POST_EVAR66 VARCHAR(16777216),
	POST_EVAR67 VARCHAR(16777216),
	POST_EVAR68 VARCHAR(16777216),
	POST_EVAR69 VARCHAR(16777216),
	POST_EVAR70 VARCHAR(16777216),
	POST_EVAR71 VARCHAR(16777216),
	POST_EVAR72 VARCHAR(16777216),
	POST_EVAR73 VARCHAR(16777216),
	POST_EVAR74 VARCHAR(16777216),
	POST_EVAR75 VARCHAR(16777216),
	POST_EVAR76 VARCHAR(16777216),
	POST_EVAR77 VARCHAR(16777216),
	POST_EVAR78 VARCHAR(16777216),
	POST_EVAR79 VARCHAR(16777216),
	POST_EVAR80 VARCHAR(16777216),
	POST_EVAR81 VARCHAR(16777216),
	POST_EVAR82 VARCHAR(16777216),
	POST_EVAR83 VARCHAR(16777216),
	POST_EVAR84 VARCHAR(16777216),
	POST_EVAR85 VARCHAR(16777216),
	POST_EVAR86 VARCHAR(16777216),
	POST_EVAR87 VARCHAR(16777216),
	POST_EVAR88 VARCHAR(16777216),
	POST_EVAR89 VARCHAR(16777216),
	POST_EVAR90 VARCHAR(16777216),
	POST_EVAR91 VARCHAR(16777216),
	POST_EVAR92 VARCHAR(16777216),
	POST_EVAR93 VARCHAR(16777216),
	POST_EVAR94 VARCHAR(16777216),
	POST_EVAR95 VARCHAR(16777216),
	POST_EVAR96 VARCHAR(16777216),
	POST_EVAR97 VARCHAR(16777216),
	POST_EVAR98 VARCHAR(16777216),
	POST_EVAR99 VARCHAR(16777216),
	POST_EVAR100 VARCHAR(16777216),
	POST_EVAR101 VARCHAR(16777216),
	POST_EVAR102 VARCHAR(16777216),
	POST_EVAR103 VARCHAR(16777216),
	POST_EVAR104 VARCHAR(16777216),
	POST_EVAR105 VARCHAR(16777216),
	POST_EVAR106 VARCHAR(16777216),
	POST_EVAR107 VARCHAR(16777216),
	POST_EVAR108 VARCHAR(16777216),
	POST_EVAR109 VARCHAR(16777216),
	POST_EVAR110 VARCHAR(16777216),
	POST_EVAR111 VARCHAR(16777216),
	POST_EVAR112 VARCHAR(16777216),
	POST_EVAR113 VARCHAR(16777216),
	POST_EVAR114 VARCHAR(16777216),
	POST_EVAR115 VARCHAR(16777216),
	POST_EVAR116 VARCHAR(16777216),
	POST_EVAR117 VARCHAR(16777216),
	POST_EVAR118 VARCHAR(16777216),
	POST_EVAR119 VARCHAR(16777216),
	POST_EVAR120 VARCHAR(16777216),
	POST_EVAR121 VARCHAR(16777216),
	POST_EVAR122 VARCHAR(16777216),
	POST_EVAR123 VARCHAR(16777216),
	POST_EVAR124 VARCHAR(16777216),
	POST_EVAR125 VARCHAR(16777216),
	POST_EVAR126 VARCHAR(16777216),
	POST_EVAR127 VARCHAR(16777216),
	POST_EVAR128 VARCHAR(16777216),
	POST_EVAR129 VARCHAR(16777216),
	POST_EVAR130 VARCHAR(16777216),
	POST_EVAR131 VARCHAR(16777216),
	POST_EVAR132 VARCHAR(16777216),
	POST_EVAR133 VARCHAR(16777216),
	POST_EVAR134 VARCHAR(16777216),
	POST_EVAR135 VARCHAR(16777216),
	POST_EVAR136 VARCHAR(16777216),
	POST_EVAR137 VARCHAR(16777216),
	POST_EVAR138 VARCHAR(16777216),
	POST_EVAR139 VARCHAR(16777216),
	POST_EVAR140 VARCHAR(16777216),
	POST_EVAR141 VARCHAR(16777216),
	POST_EVAR142 VARCHAR(16777216),
	POST_EVAR143 VARCHAR(16777216),
	POST_EVAR144 VARCHAR(16777216),
	POST_EVAR145 VARCHAR(16777216),
	POST_EVAR146 VARCHAR(16777216),
	POST_EVAR147 VARCHAR(16777216),
	POST_EVAR148 VARCHAR(16777216),
	POST_EVAR149 VARCHAR(16777216),
	POST_EVAR150 VARCHAR(16777216),
	POST_EVAR151 VARCHAR(16777216),
	POST_EVAR152 VARCHAR(16777216),
	POST_EVAR153 VARCHAR(16777216),
	POST_EVAR154 VARCHAR(16777216),
	POST_EVAR155 VARCHAR(16777216),
	POST_EVAR156 VARCHAR(16777216),
	POST_EVAR157 VARCHAR(16777216),
	POST_EVAR158 VARCHAR(16777216),
	POST_EVAR159 VARCHAR(16777216),
	POST_EVAR160 VARCHAR(16777216),
	POST_EVAR161 VARCHAR(16777216),
	POST_EVAR162 VARCHAR(16777216),
	POST_EVAR163 VARCHAR(16777216),
	POST_EVAR164 VARCHAR(16777216),
	POST_EVAR165 VARCHAR(16777216),
	POST_EVAR166 VARCHAR(16777216),
	POST_EVAR167 VARCHAR(16777216),
	POST_EVAR168 VARCHAR(16777216),
	POST_EVAR169 VARCHAR(16777216),
	POST_EVAR170 VARCHAR(16777216),
	POST_EVAR171 VARCHAR(16777216),
	POST_EVAR172 VARCHAR(16777216),
	POST_EVAR173 VARCHAR(16777216),
	POST_EVAR174 VARCHAR(16777216),
	POST_EVAR175 VARCHAR(16777216),
	POST_EVAR176 VARCHAR(16777216),
	POST_EVAR177 VARCHAR(16777216),
	POST_EVAR178 VARCHAR(16777216),
	POST_EVAR179 VARCHAR(16777216),
	POST_EVAR180 VARCHAR(16777216),
	POST_EVAR181 VARCHAR(16777216),
	POST_EVAR182 VARCHAR(16777216),
	POST_EVAR183 VARCHAR(16777216),
	POST_EVAR184 VARCHAR(16777216),
	POST_EVAR185 VARCHAR(16777216),
	POST_EVAR186 VARCHAR(16777216),
	POST_EVAR187 VARCHAR(16777216),
	POST_EVAR188 VARCHAR(16777216),
	POST_EVAR189 VARCHAR(16777216),
	POST_EVAR190 VARCHAR(16777216),
	POST_EVAR191 VARCHAR(16777216),
	POST_EVAR192 VARCHAR(16777216),
	POST_EVAR193 VARCHAR(16777216),
	POST_EVAR194 VARCHAR(16777216),
	POST_EVAR195 VARCHAR(16777216),
	POST_EVAR196 VARCHAR(16777216),
	POST_EVAR197 VARCHAR(16777216),
	POST_EVAR198 VARCHAR(16777216),
	POST_EVAR199 VARCHAR(16777216),
	POST_EVAR200 VARCHAR(16777216),
	POST_EVAR201 VARCHAR(16777216),
	POST_EVAR202 VARCHAR(16777216),
	POST_EVAR203 VARCHAR(16777216),
	POST_EVAR204 VARCHAR(16777216),
	POST_EVAR205 VARCHAR(16777216),
	POST_EVAR206 VARCHAR(16777216),
	POST_EVAR207 VARCHAR(16777216),
	POST_EVAR208 VARCHAR(16777216),
	POST_EVAR209 VARCHAR(16777216),
	POST_EVAR210 VARCHAR(16777216),
	POST_EVAR211 VARCHAR(16777216),
	POST_EVAR212 VARCHAR(16777216),
	POST_EVAR213 VARCHAR(16777216),
	POST_EVAR214 VARCHAR(16777216),
	POST_EVAR215 VARCHAR(16777216),
	POST_EVAR216 VARCHAR(16777216),
	POST_EVAR217 VARCHAR(16777216),
	POST_EVAR218 VARCHAR(16777216),
	POST_EVAR219 VARCHAR(16777216),
	POST_EVAR220 VARCHAR(16777216),
	POST_EVAR221 VARCHAR(16777216),
	POST_EVAR222 VARCHAR(16777216),
	POST_EVAR223 VARCHAR(16777216),
	POST_EVAR224 VARCHAR(16777216),
	POST_EVAR225 VARCHAR(16777216),
	POST_EVAR226 VARCHAR(16777216),
	POST_EVAR227 VARCHAR(16777216),
	POST_EVAR228 VARCHAR(16777216),
	POST_EVAR229 VARCHAR(16777216),
	POST_EVAR230 VARCHAR(16777216),
	POST_EVAR231 VARCHAR(16777216),
	POST_EVAR232 VARCHAR(16777216),
	POST_EVAR233 VARCHAR(16777216),
	POST_EVAR234 VARCHAR(16777216),
	POST_EVAR235 VARCHAR(16777216),
	POST_EVAR236 VARCHAR(16777216),
	POST_EVAR237 VARCHAR(16777216),
	POST_EVAR238 VARCHAR(16777216),
	POST_EVAR239 VARCHAR(16777216),
	POST_EVAR240 VARCHAR(16777216),
	POST_EVAR241 VARCHAR(16777216),
	POST_EVAR242 VARCHAR(16777216),
	POST_EVAR243 VARCHAR(16777216),
	POST_EVAR244 VARCHAR(16777216),
	POST_EVAR245 VARCHAR(16777216),
	POST_EVAR246 VARCHAR(16777216),
	POST_EVAR247 VARCHAR(16777216),
	POST_EVAR248 VARCHAR(16777216),
	POST_EVAR249 VARCHAR(16777216),
	POST_EVAR250 VARCHAR(16777216),
	POST_EVENT_LIST VARCHAR(16777216),
	POST_HIER1 VARCHAR(16777216),
	POST_HIER2 VARCHAR(16777216),
	POST_HIER3 VARCHAR(16777216),
	POST_HIER4 VARCHAR(16777216),
	POST_HIER5 VARCHAR(16777216),
	POST_JAVA_ENABLED VARCHAR(16777216),
	POST_KEYWORDS VARCHAR(16777216),
	POST_MC_AUDIENCES VARCHAR(16777216),
	POST_MOBILEACTION VARCHAR(16777216),
	POST_MOBILEAPPID VARCHAR(16777216),
	POST_MOBILECAMPAIGNCONTENT VARCHAR(16777216),
	POST_MOBILECAMPAIGNMEDIUM VARCHAR(16777216),
	POST_MOBILECAMPAIGNNAME VARCHAR(16777216),
	POST_MOBILECAMPAIGNSOURCE VARCHAR(16777216),
	POST_MOBILECAMPAIGNTERM VARCHAR(16777216),
	POST_MOBILEDAYOFWEEK VARCHAR(16777216),
	POST_MOBILEDAYSSINCEFIRSTUSE VARCHAR(16777216),
	POST_MOBILEDAYSSINCELASTUSE VARCHAR(16777216),
	POST_MOBILEDEVICE VARCHAR(16777216),
	POST_MOBILEHOUROFDAY VARCHAR(16777216),
	POST_MOBILEINSTALLDATE VARCHAR(16777216),
	POST_MOBILELAUNCHNUMBER VARCHAR(16777216),
	POST_MOBILELTV VARCHAR(16777216),
	POST_MOBILEMESSAGEBUTTONNAME VARCHAR(16777216),
	POST_MOBILEMESSAGECLICKS VARCHAR(16777216),
	POST_MOBILEMESSAGEID VARCHAR(16777216),
	POST_MOBILEMESSAGEIDDEST VARCHAR(16777216),
	POST_MOBILEMESSAGEIDNAME VARCHAR(16777216),
	POST_MOBILEMESSAGEIDTYPE VARCHAR(16777216),
	POST_MOBILEMESSAGEIMPRESSIONS VARCHAR(16777216),
	POST_MOBILEMESSAGEONLINE VARCHAR(16777216),
	POST_MOBILEMESSAGEPUSHOPTIN VARCHAR(16777216),
	POST_MOBILEMESSAGEPUSHPAYLOADID VARCHAR(16777216),
	POST_MOBILEMESSAGEPUSHPAYLOADIDNAME VARCHAR(16777216),
	POST_MOBILEMESSAGEVIEWS VARCHAR(16777216),
	POST_MOBILEOSVERSION VARCHAR(16777216),
	POST_MOBILEPUSHOPTIN VARCHAR(16777216),
	POST_MOBILEPUSHPAYLOADID VARCHAR(16777216),
	POST_MOBILERESOLUTION VARCHAR(16777216),
	POST_MVVAR1 VARCHAR(16777216),
	POST_MVVAR2 VARCHAR(16777216),
	POST_MVVAR3 VARCHAR(16777216),
	POST_PAGE_EVENT VARCHAR(16777216),
	POST_PAGE_EVENT_VAR1 VARCHAR(16777216),
	POST_PAGE_EVENT_VAR2 VARCHAR(16777216),
	POST_PAGE_EVENT_VAR3 VARCHAR(16777216),
	POST_PAGE_TYPE VARCHAR(16777216),
	POST_PAGE_URL VARCHAR(16777216),
	POST_PAGENAME VARCHAR(16777216),
	POST_PAGENAME_NO_URL VARCHAR(16777216),
	POST_PARTNER_PLUGINS VARCHAR(16777216),
	POST_PERSISTENT_COOKIE VARCHAR(16777216),
	POST_POINTOFINTEREST VARCHAR(16777216),
	POST_POINTOFINTERESTDISTANCE VARCHAR(16777216),
	POST_PRODUCT_LIST VARCHAR(16777216),
	POST_PROP1 VARCHAR(16777216),
	POST_PROP2 VARCHAR(16777216),
	POST_PROP3 VARCHAR(16777216),
	POST_PROP4 VARCHAR(16777216),
	POST_PROP5 VARCHAR(16777216),
	POST_PROP6 VARCHAR(16777216),
	POST_PROP7 VARCHAR(16777216),
	POST_PROP8 VARCHAR(16777216),
	POST_PROP9 VARCHAR(16777216),
	POST_PROP10 VARCHAR(16777216),
	POST_PROP11 VARCHAR(16777216),
	POST_PROP12 VARCHAR(16777216),
	POST_PROP13 VARCHAR(16777216),
	POST_PROP14 VARCHAR(16777216),
	POST_PROP15 VARCHAR(16777216),
	POST_PROP16 VARCHAR(16777216),
	POST_PROP17 VARCHAR(16777216),
	POST_PROP18 VARCHAR(16777216),
	POST_PROP19 VARCHAR(16777216),
	POST_PROP20 VARCHAR(16777216),
	POST_PROP21 VARCHAR(16777216),
	POST_PROP22 VARCHAR(16777216),
	POST_PROP23 VARCHAR(16777216),
	POST_PROP24 VARCHAR(16777216),
	POST_PROP25 VARCHAR(16777216),
	POST_PROP26 VARCHAR(16777216),
	POST_PROP27 VARCHAR(16777216),
	POST_PROP28 VARCHAR(16777216),
	POST_PROP29 VARCHAR(16777216),
	POST_PROP30 VARCHAR(16777216),
	POST_PROP31 VARCHAR(16777216),
	POST_PROP32 VARCHAR(16777216),
	POST_PROP33 VARCHAR(16777216),
	POST_PROP34 VARCHAR(16777216),
	POST_PROP35 VARCHAR(16777216),
	POST_PROP36 VARCHAR(16777216),
	POST_PROP37 VARCHAR(16777216),
	POST_PROP38 VARCHAR(16777216),
	POST_PROP39 VARCHAR(16777216),
	POST_PROP40 VARCHAR(16777216),
	POST_PROP41 VARCHAR(16777216),
	POST_PROP42 VARCHAR(16777216),
	POST_PROP43 VARCHAR(16777216),
	POST_PROP44 VARCHAR(16777216),
	POST_PROP45 VARCHAR(16777216),
	POST_PROP46 VARCHAR(16777216),
	POST_PROP47 VARCHAR(16777216),
	POST_PROP48 VARCHAR(16777216),
	POST_PROP49 VARCHAR(16777216),
	POST_PROP50 VARCHAR(16777216),
	POST_PROP51 VARCHAR(16777216),
	POST_PROP52 VARCHAR(16777216),
	POST_PROP53 VARCHAR(16777216),
	POST_PROP54 VARCHAR(16777216),
	POST_PROP55 VARCHAR(16777216),
	POST_PROP56 VARCHAR(16777216),
	POST_PROP57 VARCHAR(16777216),
	POST_PROP58 VARCHAR(16777216),
	POST_PROP59 VARCHAR(16777216),
	POST_PROP60 VARCHAR(16777216),
	POST_PROP61 VARCHAR(16777216),
	POST_PROP62 VARCHAR(16777216),
	POST_PROP63 VARCHAR(16777216),
	POST_PROP64 VARCHAR(16777216),
	POST_PROP65 VARCHAR(16777216),
	POST_PROP66 VARCHAR(16777216),
	POST_PROP67 VARCHAR(16777216),
	POST_PROP68 VARCHAR(16777216),
	POST_PROP69 VARCHAR(16777216),
	POST_PROP70 VARCHAR(16777216),
	POST_PROP71 VARCHAR(16777216),
	POST_PROP72 VARCHAR(16777216),
	POST_PROP73 VARCHAR(16777216),
	POST_PROP74 VARCHAR(16777216),
	POST_PROP75 VARCHAR(16777216),
	POST_PURCHASEID VARCHAR(16777216),
	POST_REFERRER VARCHAR(16777216),
	POST_S_KWCID VARCHAR(16777216),
	POST_SEARCH_ENGINE VARCHAR(16777216),
	POST_SOCIALACCOUNTANDAPPIDS VARCHAR(16777216),
	POST_SOCIALASSETTRACKINGCODE VARCHAR(16777216),
	POST_SOCIALAUTHOR VARCHAR(16777216),
	POST_SOCIALAVERAGESENTIMENT VARCHAR(16777216),
	POST_SOCIALAVERAGESENTIMENT_DEPRECATED VARCHAR(16777216),
	POST_SOCIALCONTENTPROVIDER VARCHAR(16777216),
	POST_SOCIALFBSTORIES VARCHAR(16777216),
	POST_SOCIALFBSTORYTELLERS VARCHAR(16777216),
	POST_SOCIALINTERACTIONCOUNT VARCHAR(16777216),
	POST_SOCIALINTERACTIONTYPE VARCHAR(16777216),
	POST_SOCIALLANGUAGE VARCHAR(16777216),
	POST_SOCIALLATLONG VARCHAR(16777216),
	POST_SOCIALLIKEADDS VARCHAR(16777216),
	POST_SOCIALLINK VARCHAR(16777216),
	POST_SOCIALLINK_DEPRECATED VARCHAR(16777216),
	POST_SOCIALMENTIONS VARCHAR(16777216),
	POST_SOCIALOWNEDDEFINITIONINSIGHTTYPE VARCHAR(16777216),
	POST_SOCIALOWNEDDEFINITIONINSIGHTVALUE VARCHAR(16777216),
	POST_SOCIALOWNEDDEFINITIONMETRIC VARCHAR(16777216),
	POST_SOCIALOWNEDDEFINITIONPROPERTYVSPOST VARCHAR(16777216),
	POST_SOCIALOWNEDPOSTIDS VARCHAR(16777216),
	POST_SOCIALOWNEDPROPERTYID VARCHAR(16777216),
	POST_SOCIALOWNEDPROPERTYNAME VARCHAR(16777216),
	POST_SOCIALOWNEDPROPERTYPROPERTYVSAPP VARCHAR(16777216),
	POST_SOCIALPAGEVIEWS VARCHAR(16777216),
	POST_SOCIALPOSTVIEWS VARCHAR(16777216),
	POST_SOCIALPROPERTY VARCHAR(16777216),
	POST_SOCIALPROPERTY_DEPRECATED VARCHAR(16777216),
	POST_SOCIALPUBCOMMENTS VARCHAR(16777216),
	POST_SOCIALPUBPOSTS VARCHAR(16777216),
	POST_SOCIALPUBRECOMMENDS VARCHAR(16777216),
	POST_SOCIALPUBSUBSCRIBERS VARCHAR(16777216),
	POST_SOCIALTERM VARCHAR(16777216),
	POST_SOCIALTERMSLIST VARCHAR(16777216),
	POST_SOCIALTERMSLIST_DEPRECATED VARCHAR(16777216),
	POST_SOCIALTOTALSENTIMENT VARCHAR(16777216),
	POST_STATE VARCHAR(16777216),
	POST_SURVEY VARCHAR(16777216),
	POST_T_TIME_INFO VARCHAR(16777216),
	POST_TNT VARCHAR(16777216),
	POST_TNT_ACTION VARCHAR(16777216),
	POST_TRANSACTIONID VARCHAR(16777216),
	POST_VIDEO VARCHAR(16777216),
	POST_VIDEOAD VARCHAR(16777216),
	POST_VIDEOADINPOD VARCHAR(16777216),
	POST_VIDEOADLENGTH VARCHAR(16777216),
	POST_VIDEOADNAME VARCHAR(16777216),
	POST_VIDEOADPLAYERNAME VARCHAR(16777216),
	POST_VIDEOADPOD VARCHAR(16777216),
	POST_VIDEOADVERTISER VARCHAR(16777216),
	POST_VIDEOAUTHORIZED VARCHAR(16777216),
	POST_VIDEOCAMPAIGN VARCHAR(16777216),
	POST_VIDEOCHANNEL VARCHAR(16777216),
	POST_VIDEOCHAPTER VARCHAR(16777216),
	POST_VIDEOCONTENTTYPE VARCHAR(16777216),
	POST_VIDEODAYPART VARCHAR(16777216),
	POST_VIDEOEPISODE VARCHAR(16777216),
	POST_VIDEOFEEDTYPE VARCHAR(16777216),
	POST_VIDEOGENRE VARCHAR(16777216),
	POST_VIDEOLENGTH VARCHAR(16777216),
	POST_VIDEOMVPD VARCHAR(16777216),
	POST_VIDEONAME VARCHAR(16777216),
	POST_VIDEONETWORK VARCHAR(16777216),
	POST_VIDEOPATH VARCHAR(16777216),
	POST_VIDEOPLAYERNAME VARCHAR(16777216),
	POST_VIDEOQOEBITRATEAVERAGEEVAR VARCHAR(16777216),
	POST_VIDEOQOEBITRATECHANGECOUNTEVAR VARCHAR(16777216),
	POST_VIDEOQOEBUFFERCOUNTEVAR VARCHAR(16777216),
	POST_VIDEOQOEBUFFERTIMEEVAR VARCHAR(16777216),
	POST_VIDEOQOEDROPPEDFRAMECOUNTEVAR VARCHAR(16777216),
	POST_VIDEOQOEERRORCOUNTEVAR VARCHAR(16777216),
	POST_VIDEOQOETIMETOSTARTEVAR VARCHAR(16777216),
	POST_VIDEOSEASON VARCHAR(16777216),
	POST_VIDEOSEGMENT VARCHAR(16777216),
	POST_VIDEOSHOW VARCHAR(16777216),
	POST_VIDEOSHOWTYPE VARCHAR(16777216),
	POST_VISID_HIGH VARCHAR(16777216),
	POST_VISID_LOW VARCHAR(16777216),
	POST_VISID_TYPE VARCHAR(16777216),
	POST_ZIP VARCHAR(16777216),
	PREV_PAGE VARCHAR(16777216),
	PRODUCT_LIST VARCHAR(16777216),
	PRODUCT_MERCHANDISING VARCHAR(16777216),
	PROP1 VARCHAR(16777216),
	PROP2 VARCHAR(16777216),
	PROP3 VARCHAR(16777216),
	PROP4 VARCHAR(16777216),
	PROP5 VARCHAR(16777216),
	PROP6 VARCHAR(16777216),
	PROP7 VARCHAR(16777216),
	PROP8 VARCHAR(16777216),
	PROP9 VARCHAR(16777216),
	PROP10 VARCHAR(16777216),
	PROP11 VARCHAR(16777216),
	PROP12 VARCHAR(16777216),
	PROP13 VARCHAR(16777216),
	PROP14 VARCHAR(16777216),
	PROP15 VARCHAR(16777216),
	PROP16 VARCHAR(16777216),
	PROP17 VARCHAR(16777216),
	PROP18 VARCHAR(16777216),
	PROP19 VARCHAR(16777216),
	PROP20 VARCHAR(16777216),
	PROP21 VARCHAR(16777216),
	PROP22 VARCHAR(16777216),
	PROP23 VARCHAR(16777216),
	PROP24 VARCHAR(16777216),
	PROP25 VARCHAR(16777216),
	PROP26 VARCHAR(16777216),
	PROP27 VARCHAR(16777216),
	PROP28 VARCHAR(16777216),
	PROP29 VARCHAR(16777216),
	PROP30 VARCHAR(16777216),
	PROP31 VARCHAR(16777216),
	PROP32 VARCHAR(16777216),
	PROP33 VARCHAR(16777216),
	PROP34 VARCHAR(16777216),
	PROP35 VARCHAR(16777216),
	PROP36 VARCHAR(16777216),
	PROP37 VARCHAR(16777216),
	PROP38 VARCHAR(16777216),
	PROP39 VARCHAR(16777216),
	PROP40 VARCHAR(16777216),
	PROP41 VARCHAR(16777216),
	PROP42 VARCHAR(16777216),
	PROP43 VARCHAR(16777216),
	PROP44 VARCHAR(16777216),
	PROP45 VARCHAR(16777216),
	PROP46 VARCHAR(16777216),
	PROP47 VARCHAR(16777216),
	PROP48 VARCHAR(16777216),
	PROP49 VARCHAR(16777216),
	PROP50 VARCHAR(16777216),
	PROP51 VARCHAR(16777216),
	PROP52 VARCHAR(16777216),
	PROP53 VARCHAR(16777216),
	PROP54 VARCHAR(16777216),
	PROP55 VARCHAR(16777216),
	PROP56 VARCHAR(16777216),
	PROP57 VARCHAR(16777216),
	PROP58 VARCHAR(16777216),
	PROP59 VARCHAR(16777216),
	PROP60 VARCHAR(16777216),
	PROP61 VARCHAR(16777216),
	PROP62 VARCHAR(16777216),
	PROP63 VARCHAR(16777216),
	PROP64 VARCHAR(16777216),
	PROP65 VARCHAR(16777216),
	PROP66 VARCHAR(16777216),
	PROP67 VARCHAR(16777216),
	PROP68 VARCHAR(16777216),
	PROP69 VARCHAR(16777216),
	PROP70 VARCHAR(16777216),
	PROP71 VARCHAR(16777216),
	PROP72 VARCHAR(16777216),
	PROP73 VARCHAR(16777216),
	PROP74 VARCHAR(16777216),
	PROP75 VARCHAR(16777216),
	PURCHASEID VARCHAR(16777216),
	QUARTERLY_VISITOR VARCHAR(16777216),
	REF_DOMAIN VARCHAR(16777216),
	REF_TYPE VARCHAR(16777216),
	REFERRER VARCHAR(16777216),
	RESOLUTION VARCHAR(16777216),
	S_KWCID VARCHAR(16777216),
	S_RESOLUTION VARCHAR(16777216),
	SAMPLED_HIT VARCHAR(16777216),
	SEARCH_ENGINE VARCHAR(16777216),
	SEARCH_PAGE_NUM VARCHAR(16777216),
	SECONDARY_HIT VARCHAR(16777216),
	SERVICE VARCHAR(16777216),
	SOCIALACCOUNTANDAPPIDS VARCHAR(16777216),
	SOCIALASSETTRACKINGCODE VARCHAR(16777216),
	SOCIALAUTHOR VARCHAR(16777216),
	SOCIALAVERAGESENTIMENT VARCHAR(16777216),
	SOCIALAVERAGESENTIMENT_DEPRECATED VARCHAR(16777216),
	SOCIALCONTENTPROVIDER VARCHAR(16777216),
	SOCIALFBSTORIES VARCHAR(16777216),
	SOCIALFBSTORYTELLERS VARCHAR(16777216),
	SOCIALINTERACTIONCOUNT VARCHAR(16777216),
	SOCIALINTERACTIONTYPE VARCHAR(16777216),
	SOCIALLANGUAGE VARCHAR(16777216),
	SOCIALLATLONG VARCHAR(16777216),
	SOCIALLIKEADDS VARCHAR(16777216),
	SOCIALLINK VARCHAR(16777216),
	SOCIALLINK_DEPRECATED VARCHAR(16777216),
	SOCIALMENTIONS VARCHAR(16777216),
	SOCIALOWNEDDEFINITIONINSIGHTTYPE VARCHAR(16777216),
	SOCIALOWNEDDEFINITIONINSIGHTVALUE VARCHAR(16777216),
	SOCIALOWNEDDEFINITIONMETRIC VARCHAR(16777216),
	SOCIALOWNEDDEFINITIONPROPERTYVSPOST VARCHAR(16777216),
	SOCIALOWNEDPOSTIDS VARCHAR(16777216),
	SOCIALOWNEDPROPERTYID VARCHAR(16777216),
	SOCIALOWNEDPROPERTYNAME VARCHAR(16777216),
	SOCIALOWNEDPROPERTYPROPERTYVSAPP VARCHAR(16777216),
	SOCIALPAGEVIEWS VARCHAR(16777216),
	SOCIALPOSTVIEWS VARCHAR(16777216),
	SOCIALPROPERTY VARCHAR(16777216),
	SOCIALPROPERTY_DEPRECATED VARCHAR(16777216),
	SOCIALPUBCOMMENTS VARCHAR(16777216),
	SOCIALPUBPOSTS VARCHAR(16777216),
	SOCIALPUBRECOMMENDS VARCHAR(16777216),
	SOCIALPUBSUBSCRIBERS VARCHAR(16777216),
	SOCIALTERM VARCHAR(16777216),
	SOCIALTERMSLIST VARCHAR(16777216),
	SOCIALTERMSLIST_DEPRECATED VARCHAR(16777216),
	SOCIALTOTALSENTIMENT VARCHAR(16777216),
	SOURCEID VARCHAR(16777216),
	STATE VARCHAR(16777216),
	STATS_SERVER VARCHAR(16777216),
	T_TIME_INFO VARCHAR(16777216),
	TNT VARCHAR(16777216),
	TNT_ACTION VARCHAR(16777216),
	TNT_POST_VISTA VARCHAR(16777216),
	TRANSACTIONID VARCHAR(16777216),
	TRUNCATED_HIT VARCHAR(16777216),
	UA_COLOR VARCHAR(16777216),
	UA_OS VARCHAR(16777216),
	UA_PIXELS VARCHAR(16777216),
	USER_AGENT VARCHAR(16777216),
	USER_HASH VARCHAR(16777216),
	USER_SERVER VARCHAR(16777216),
	USERID VARCHAR(16777216),
	USERNAME VARCHAR(16777216),
	VA_CLOSER_DETAIL VARCHAR(16777216),
	VA_CLOSER_ID VARCHAR(16777216),
	VA_FINDER_DETAIL VARCHAR(16777216),
	VA_FINDER_ID VARCHAR(16777216),
	VA_INSTANCE_EVENT VARCHAR(16777216),
	VA_NEW_ENGAGEMENT VARCHAR(16777216),
	VIDEO VARCHAR(16777216),
	VIDEOAD VARCHAR(16777216),
	VIDEOADINPOD VARCHAR(16777216),
	VIDEOADLENGTH VARCHAR(16777216),
	VIDEOADNAME VARCHAR(16777216),
	VIDEOADPLAYERNAME VARCHAR(16777216),
	VIDEOADPOD VARCHAR(16777216),
	VIDEOADVERTISER VARCHAR(16777216),
	VIDEOAUDIOALBUM VARCHAR(16777216),
	VIDEOAUDIOARTIST VARCHAR(16777216),
	VIDEOAUDIOAUTHOR VARCHAR(16777216),
	VIDEOAUDIOLABEL VARCHAR(16777216),
	VIDEOAUDIOPUBLISHER VARCHAR(16777216),
	VIDEOAUDIOSTATION VARCHAR(16777216),
	VIDEOAUTHORIZED VARCHAR(16777216),
	VIDEOAVERAGEMINUTEAUDIENCE VARCHAR(16777216),
	VIDEOCAMPAIGN VARCHAR(16777216),
	VIDEOCHANNEL VARCHAR(16777216),
	VIDEOCHAPTER VARCHAR(16777216),
	VIDEOCHAPTERCOMPLETE VARCHAR(16777216),
	VIDEOCHAPTERSTART VARCHAR(16777216),
	VIDEOCHAPTERTIME VARCHAR(16777216),
	VIDEOCONTENTTYPE VARCHAR(16777216),
	VIDEODAYPART VARCHAR(16777216),
	VIDEOEPISODE VARCHAR(16777216),
	VIDEOFEEDTYPE VARCHAR(16777216),
	VIDEOGENRE VARCHAR(16777216),
	VIDEOLENGTH VARCHAR(16777216),
	VIDEOMVPD VARCHAR(16777216),
	VIDEONAME VARCHAR(16777216),
	VIDEONETWORK VARCHAR(16777216),
	VIDEOPATH VARCHAR(16777216),
	VIDEOPAUSE VARCHAR(16777216),
	VIDEOPAUSECOUNT VARCHAR(16777216),
	VIDEOPAUSETIME VARCHAR(16777216),
	VIDEOPLAY VARCHAR(16777216),
	VIDEOPLAYERNAME VARCHAR(16777216),
	VIDEOPROGRESS10 VARCHAR(16777216),
	VIDEOPROGRESS25 VARCHAR(16777216),
	VIDEOPROGRESS50 VARCHAR(16777216),
	VIDEOPROGRESS75 VARCHAR(16777216),
	VIDEOPROGRESS96 VARCHAR(16777216),
	VIDEOQOEBITRATEAVERAGE VARCHAR(16777216),
	VIDEOQOEBITRATEAVERAGEEVAR VARCHAR(16777216),
	VIDEOQOEBITRATECHANGE VARCHAR(16777216),
	VIDEOQOEBITRATECHANGECOUNTEVAR VARCHAR(16777216),
	VIDEOQOEBUFFER VARCHAR(16777216),
	VIDEOQOEBUFFERCOUNTEVAR VARCHAR(16777216),
	VIDEOQOEBUFFERTIMEEVAR VARCHAR(16777216),
	VIDEOQOEDROPBEFORESTART VARCHAR(16777216),
	VIDEOQOEDROPPEDFRAMECOUNTEVAR VARCHAR(16777216),
	VIDEOQOEDROPPEDFRAMES VARCHAR(16777216),
	VIDEOQOEERROR VARCHAR(16777216),
	VIDEOQOEERRORCOUNTEVAR VARCHAR(16777216),
	VIDEOQOEEXTNERALERRORS VARCHAR(16777216),
	VIDEOQOEPLAYERSDKERRORS VARCHAR(16777216),
	VIDEOQOETIMETOSTARTEVAR VARCHAR(16777216),
	VIDEORESUME VARCHAR(16777216),
	VIDEOSEASON VARCHAR(16777216),
	VIDEOSEGMENT VARCHAR(16777216),
	VIDEOSHOW VARCHAR(16777216),
	VIDEOSHOWTYPE VARCHAR(16777216),
	VIDEOSTREAMTYPE VARCHAR(16777216),
	VIDEOTOTALTIME VARCHAR(16777216),
	VIDEOUNIQUETIMEPLAYED VARCHAR(16777216),
	VISID_HIGH VARCHAR(16777216),
	VISID_LOW VARCHAR(16777216),
	VISID_NEW VARCHAR(16777216),
	VISID_TIMESTAMP VARCHAR(16777216),
	VISID_TYPE VARCHAR(16777216),
	VISIT_KEYWORDS VARCHAR(16777216),
	VISIT_NUM VARCHAR(16777216),
	VISIT_PAGE_NUM VARCHAR(16777216),
	VISIT_REF_DOMAIN VARCHAR(16777216),
	VISIT_REF_TYPE VARCHAR(16777216),
	VISIT_REFERRER VARCHAR(16777216),
	VISIT_SEARCH_ENGINE VARCHAR(16777216),
	VISIT_START_PAGE_URL VARCHAR(16777216),
	VISIT_START_PAGENAME VARCHAR(16777216),
	VISIT_START_TIME_GMT VARCHAR(16777216),
	WEEKLY_VISITOR VARCHAR(16777216),
	YEARLY_VISITOR VARCHAR(16777216),
	ZIP VARCHAR(16777216),
	DW_SOURCE_CREATE_NM VARCHAR(16777216),
	DW_CREATETS TIMESTAMP_LTZ(9)
)COMMENT='This table contains information about CLICK_HIT_DATA'
;