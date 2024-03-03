--liquibase formatted sql
--changeset SYSTEM:CLICK_HIT_DATA runOnChange:true splitStatements:false OBJECT_TYPE:VIEW
use database EDM_VIEWS_PRD;
use schema DW_VIEWS;

CREATE OR REPLACE VIEW click_hit_data COPY GRANTS AS
SELECT
   accept_language

  ,adclassificationcreative

  ,adload 

  ,aemassetid 

  ,aemassetsource 

  ,aemclickedassetid   

  ,browser

  ,browser_height 

  ,browser_width  

  ,c_color

  ,campaign   

  ,carrier

  ,channel

  ,click_action   

  ,click_action_type   

 ,click_context  

  ,click_context_type  

  ,click_sourceid 

  ,click_tag  

  ,clickmaplink   

  ,clickmaplinkbyregion

  ,clickmappage   

  ,clickmapregion 

  ,code_ver   

  ,color  

  ,connection_type

  ,cookies

  ,country

  ,ct_connect_type

  ,curr_factor

  ,curr_rate  

  ,currency   

  ,cust_hit_time_gmt   

  ,cust_visid 

  ,daily_visitor  

  ,date_time  

  ,domain 

  ,duplicate_events

  ,duplicate_purchase  

  ,duplicated_from

  ,ef_id  

  ,evar1  

  ,evar2  

  ,evar3  

 ,evar4  

  ,evar5  

  ,evar6  

  ,evar7  

  ,evar8  

  ,evar9  

  ,evar10 

  ,evar11 

  ,evar12 

  ,evar13 

  ,evar14 

  ,evar15 

  ,evar16 

  ,evar17 

  ,evar18 

  ,evar19 

  ,evar20 

  ,evar21 

  ,evar22 

  ,evar23 

  ,evar24 

  ,evar25 

  ,evar26 

  ,evar27 

  ,evar28 

  ,evar29 

  ,evar30 

  ,evar31 

  ,evar32 

 ,evar33 

  ,evar34 

  ,evar35 

  ,evar36 

  ,evar37 

  ,evar38 

  ,evar39 

  ,evar40 

  ,evar41 

  ,evar42 

  ,evar43 

  ,evar44 

  ,evar45 

  ,evar46 

  ,evar47 

  ,evar48 

  ,evar49 

  ,evar50 

  ,evar51 

  ,evar52 

  ,evar53 

  ,evar54 

  ,evar55 

  ,evar56 

  ,evar57 

  ,evar58 

  ,evar59 

  ,evar60 

  ,evar61 

 ,evar62 

  ,evar63 

  ,evar64 

  ,evar65 

  ,evar66 

  ,evar67 

  ,evar68 

  ,evar69 

  ,evar70 

  ,evar71 

  ,evar72 

  ,evar73 

  ,evar74 

  ,evar75 

  ,evar76 

  ,evar77 

  ,evar78 

  ,evar79 

  ,evar80 

  ,evar81 

  ,evar82 

  ,evar83 

  ,evar84 

  ,evar85 

  ,evar86 

  ,evar87 

  ,evar88 

  ,evar89 

  ,evar90 

 ,evar91 

  ,evar92 

  ,evar93 

  ,evar94 

  ,evar95 

  ,evar96 

  ,evar97 

  ,evar98 

  ,evar99 

  ,evar100

  ,evar101

  ,evar102

  ,evar103

  ,evar104

  ,evar105

  ,evar106

  ,evar107

  ,evar108

  ,evar109

  ,evar110

  ,evar111

  ,evar112

  ,evar113

  ,evar114

  ,evar115

  ,evar116

  ,evar117

  ,evar118

  ,evar119

 ,evar120

  ,evar121

  ,evar122

  ,evar123

  ,evar124

  ,evar125

  ,evar126

  ,evar127

  ,evar128

  ,evar129

  ,evar130

  ,evar131

  ,evar132

  ,evar133

  ,evar134

  ,evar135

  ,evar136

  ,evar137

  ,evar138

  ,evar139

  ,evar140

  ,evar141

  ,evar142

  ,evar143

  ,evar144

  ,evar145

  ,evar146

  ,evar147

  ,evar148

 ,evar149

  ,evar150

  ,evar151

  ,evar152

  ,evar153

  ,evar154

  ,evar155

  ,evar156

  ,evar157

  ,evar158

  ,evar159

  ,evar160

  ,evar161

  ,evar162

  ,evar163

  ,evar164

  ,evar165

  ,evar166

  ,evar167

  ,evar168

  ,evar169

  ,evar170

  ,evar171

  ,evar172

  ,evar173

  ,evar174

  ,evar175

  ,evar176

  ,evar177

 ,evar178

  ,evar179

  ,evar180

  ,evar181

  ,evar182

  ,evar183

  ,evar184

  ,evar185

  ,evar186

  ,evar187

  ,evar188

  ,evar189

  ,evar190

  ,evar191

  ,evar192

  ,evar193

  ,evar194

  ,evar195

  ,evar196

  ,evar197

  ,evar198

  ,evar199

  ,evar200

  ,evar201

  ,evar202

  ,evar203

  ,evar204

  ,evar205

  ,evar206

 ,evar207

  ,evar208

  ,evar209

  ,evar210

  ,evar211

  ,evar212

  ,evar213

  ,evar214

  ,evar215

  ,evar216

  ,evar217

  ,evar218

  ,evar219

  ,evar220

  ,evar221

  ,evar222

  ,evar223

  ,evar224

  ,evar225

  ,evar226

  ,evar227

  ,evar228

  ,evar229

  ,evar230

  ,evar231

  ,evar232

  ,evar233

  ,evar234

  ,evar235

 ,evar236

  ,evar237

  ,evar238

  ,evar239

  ,evar240

  ,evar241

  ,evar242

  ,evar243

  ,evar244

  ,evar245

  ,evar246

  ,evar247

  ,evar248

  ,evar249

  ,evar250

  ,event_list 

  ,exclude_hit

  ,first_hit_page_url  

  ,first_hit_pagename  

  ,first_hit_ref_domain

  ,first_hit_ref_type  

  ,first_hit_referrer  

  ,first_hit_time_gmt  

  ,geo_city   

  ,geo_country

  ,geo_dma

  ,geo_region 

  ,geo_zip

  ,hier1  

 ,hier2  

  ,hier3  

  ,hier4  

  ,hier5  

  ,hit_source 

  ,hit_time_gmt   

  ,hitid_high 

  ,hitid_low  

  ,homepage   

  ,hourly_visitor 

  ,ip 

  ,ip2

  ,j_jscript  

  ,java_enabled   

  ,javascript 

  ,language   

  ,last_hit_time_gmt   

  ,last_purchase_num   

  ,last_purchase_time_gmt  

  ,latlon1

  ,latlon23   

  ,latlon45   

  ,mc_audiences   

  ,mcvisid

  ,mobile_id  

  ,mobileacquisitionclicks 

  ,mobileaction   

  ,mobileactioninapptime   

  ,mobileactiontotaltime   

 ,mobileappid

  ,mobileappperformanceaffectedusers   

  ,mobileappperformanceappid   

  ,mobileappperformanceappidappperfappname 

  ,mobileappperformanceappidappperfplatform

  ,mobileappperformancecrashes 

  ,mobileappperformancecrashid 

  ,mobileappperformancecrashidappperfcrashname 

  ,mobileappperformanceloads   

  ,mobileappstoreavgrating 

  ,mobileappstoredownloads 

  ,mobileappstoreinapprevenue  

  ,mobileappstoreinapproyalties

  ,mobileappstoreobjectid  

  ,mobileappstoreobjectidappstoreuser  

  ,mobileappstoreobjectidapplicationname   

  ,mobileappstoreobjectidapplicationversion

  ,mobileappstoreobjectidappstorename  

  ,mobileappstoreobjectidcategoryname  

  ,mobileappstoreobjectidcountryname   

  ,mobileappstoreobjectiddevicemanufacturer

  ,mobileappstoreobjectiddevicename

  ,mobileappstoreobjectidinappname 

  ,mobileappstoreobjectidplatformnameversion   

  ,mobileappstoreobjectidrankcategorytype  

  ,mobileappstoreobjectidregionname

  ,mobileappstoreobjectidreviewcomment 

  ,mobileappstoreobjectidreviewtitle   

  ,mobileappstoreoneoffrevenue 

 ,mobileappstoreoneoffroyalties   

  ,mobileappstorepurchases 

  ,mobileappstorerank  

  ,mobileappstorerankdivisor   

  ,mobileappstorerating

  ,mobileappstoreratingdivisor 

  ,mobileavgprevsessionlength  

  ,mobilebeaconmajor   

  ,mobilebeaconminor   

  ,mobilebeaconproximity   

  ,mobilebeaconuuid

  ,mobilecampaigncontent   

  ,mobilecampaignmedium

  ,mobilecampaignname  

  ,mobilecampaignsource

  ,mobilecampaignterm  

  ,mobilecrashes  

  ,mobilecrashrate

  ,mobiledailyengagedusers 

  ,mobiledayofweek

  ,mobiledayssincefirstuse 

  ,mobiledayssincelastupgrade  

  ,mobiledayssincelastuse  

  ,mobiledeeplinkid

  ,mobiledeeplinkidname

  ,mobiledevice   

  ,mobilehourofday

  ,mobileinstalldate   

  ,mobileinstalls 

 ,mobilelaunches 

  ,mobilelaunchessincelastupgrade  

  ,mobilelaunchnumber  

  ,mobileltv  

  ,mobileltvtotal 

  ,mobilemessagebuttonname 

  ,mobilemessageclicks 

  ,mobilemessageid

  ,mobilemessageiddest 

  ,mobilemessageidname 

  ,mobilemessageidtype 

  ,mobilemessageimpressions

  ,mobilemessageonline 

  ,mobilemessagepushoptin  

  ,mobilemessagepushpayloadid  

  ,mobilemessagepushpayloadidname  

  ,mobilemessageviews  

  ,mobilemonthlyengagedusers   

  ,mobileosenvironment 

  ,mobileosversion

  ,mobileplaceaccuracy 

  ,mobileplacecategory 

  ,mobileplacedwelltime

  ,mobileplaceentry

  ,mobileplaceexit

  ,mobileplaceid  

  ,mobileprevsessionlength 

  ,mobilepushoptin

  ,mobilepushpayloadid 

 ,mobilerelaunchcampaigncontent   

  ,mobilerelaunchcampaignmedium

  ,mobilerelaunchcampaignsource

  ,mobilerelaunchcampaignterm  

  ,mobilerelaunchcampaigntrackingcode  

  ,mobilerelaunchcampaigntrackingcodename  

  ,mobileresolution

  ,mobileupgrades 

  ,monthly_visitor

  ,mvvar1 

  ,mvvar2 

  ,mvvar3 

  ,namespace  

  ,new_visit  

  ,os 

  ,p_plugins  

  ,page_event 

  ,page_event_var1

  ,page_event_var2

  ,page_event_var3

  ,page_type  

  ,page_url   

  ,pagename   

  ,paid_search

  ,partner_plugins

  ,persistent_cookie   

  ,plugins

  ,pointofinterest

  ,pointofinterestdistance 

 ,post_adclassificationcreative   

  ,post_adload

  ,post_browser_height 

  ,post_browser_width  

  ,post_campaign  

  ,post_channel   

  ,post_clickmaplink   

  ,post_clickmaplinkbyregion   

  ,post_clickmappage   

  ,post_clickmapregion 

  ,post_cookies   

  ,post_currency  

  ,post_cust_hit_time_gmt  

  ,post_cust_visid

  ,post_ef_id 

  ,post_evar1 

  ,post_evar2 

  ,post_evar3 

  ,post_evar4 

  ,post_evar5 

  ,post_evar6 

  ,post_evar7 

  ,post_evar8 

  ,post_evar9 

  ,post_evar10

  ,post_evar11

  ,post_evar12

  ,post_evar13

  ,post_evar14

 ,post_evar15

  ,post_evar16

  ,post_evar17

  ,post_evar18

  ,post_evar19

  ,post_evar20

  ,post_evar21

  ,post_evar22

  ,post_evar23

  ,post_evar24

  ,post_evar25

  ,post_evar26

  ,post_evar27

  ,post_evar28

  ,post_evar29

  ,post_evar30

  ,post_evar31

  ,post_evar32

  ,post_evar33

  ,post_evar34

  ,post_evar35

  ,post_evar36

  ,post_evar37

  ,post_evar38

  ,post_evar39

  ,post_evar40

  ,post_evar41

  ,post_evar42

  ,post_evar43

 ,post_evar44

  ,post_evar45

  ,post_evar46

  ,post_evar47

  ,post_evar48

  ,post_evar49

  ,post_evar50

  ,post_evar51

  ,post_evar52

  ,post_evar53

  ,post_evar54

  ,post_evar55

  ,post_evar56

  ,post_evar57

  ,post_evar58

  ,post_evar59

  ,post_evar60

  ,post_evar61

  ,post_evar62

  ,post_evar63

  ,post_evar64

  ,post_evar65

  ,post_evar66

  ,post_evar67

  ,post_evar68

  ,post_evar69

  ,post_evar70

  ,post_evar71

  ,post_evar72

 ,post_evar73

  ,post_evar74

  ,post_evar75

  ,post_evar76

  ,post_evar77

  ,post_evar78

  ,post_evar79

  ,post_evar80

  ,post_evar81

  ,post_evar82

  ,post_evar83

  ,post_evar84

  ,post_evar85

  ,post_evar86

  ,post_evar87

  ,post_evar88

  ,post_evar89

  ,post_evar90

  ,post_evar91

  ,post_evar92

  ,post_evar93

  ,post_evar94

  ,post_evar95

  ,post_evar96

  ,post_evar97

  ,post_evar98

  ,post_evar99

  ,post_evar100   

  ,post_evar101   

 ,post_evar102   

  ,post_evar103   

  ,post_evar104   

  ,post_evar105   

  ,post_evar106   

  ,post_evar107   

  ,post_evar108   

  ,post_evar109   

  ,post_evar110   

  ,post_evar111   

  ,post_evar112   

  ,post_evar113   

  ,post_evar114   

  ,post_evar115   

  ,post_evar116   

  ,post_evar117   

  ,post_evar118   

  ,post_evar119   

  ,post_evar120   

  ,post_evar121   

  ,post_evar122   

  ,post_evar123   

  ,post_evar124   

  ,post_evar125   

  ,post_evar126   

  ,post_evar127   

  ,post_evar128   

  ,post_evar129   

  ,post_evar130   

 ,post_evar131   

  ,post_evar132   

  ,post_evar133   

  ,post_evar134   

  ,post_evar135   

  ,post_evar136   

  ,post_evar137   

  ,post_evar138   

  ,post_evar139   

  ,post_evar140   

  ,post_evar141   

  ,post_evar142   

  ,post_evar143   

  ,post_evar144   

  ,post_evar145   

  ,post_evar146   

  ,post_evar147   

  ,post_evar148   

  ,post_evar149   

  ,post_evar150   

  ,post_evar151   

  ,post_evar152   

  ,post_evar153   

  ,post_evar154   

  ,post_evar155   

  ,post_evar156   

  ,post_evar157   

  ,post_evar158   

  ,post_evar159   

 ,post_evar160   

  ,post_evar161   

  ,post_evar162   

  ,post_evar163   

  ,post_evar164   

  ,post_evar165   

  ,post_evar166   

  ,post_evar167   

  ,post_evar168   

  ,post_evar169   

  ,post_evar170   

  ,post_evar171   

  ,post_evar172   

  ,post_evar173   

  ,post_evar174   

  ,post_evar175   

  ,post_evar176   

  ,post_evar177   

  ,post_evar178   

  ,post_evar179   

  ,post_evar180   

  ,post_evar181   

  ,post_evar182   

  ,post_evar183   

  ,post_evar184   

  ,post_evar185   

  ,post_evar186   

  ,post_evar187   

  ,post_evar188   

 ,post_evar189   

  ,post_evar190   

  ,post_evar191   

  ,post_evar192   

  ,post_evar193   

  ,post_evar194   

  ,post_evar195   

  ,post_evar196   

  ,post_evar197   

  ,post_evar198   

  ,post_evar199   

  ,post_evar200   

  ,post_evar201   

  ,post_evar202   

  ,post_evar203   

  ,post_evar204   

  ,post_evar205   

  ,post_evar206   

  ,post_evar207   

  ,post_evar208   

  ,post_evar209   

  ,post_evar210   

  ,post_evar211   

  ,post_evar212   

  ,post_evar213   

  ,post_evar214   

  ,post_evar215   

  ,post_evar216   

  ,post_evar217   

 ,post_evar218   

  ,post_evar219   

  ,post_evar220   

  ,post_evar221   

  ,post_evar222   

  ,post_evar223   

  ,post_evar224   

  ,post_evar225   

  ,post_evar226   

  ,post_evar227   

  ,post_evar228   

  ,post_evar229   

  ,post_evar230   

  ,post_evar231   

  ,post_evar232   

  ,post_evar233   

  ,post_evar234   

  ,post_evar235   

  ,post_evar236   

  ,post_evar237   

  ,post_evar238   

  ,post_evar239   

  ,post_evar240   

  ,post_evar241   

  ,post_evar242   

  ,post_evar243   

  ,post_evar244   

  ,post_evar245   

  ,post_evar246   

 ,post_evar247   

  ,post_evar248   

  ,post_evar249   

  ,post_evar250   

  ,post_event_list

  ,post_hier1 

  ,post_hier2 

  ,post_hier3 

  ,post_hier4 

  ,post_hier5 

  ,post_java_enabled   

  ,post_keywords  

  ,post_mc_audiences   

  ,post_mobileaction   

  ,post_mobileappid

  ,post_mobilecampaigncontent  

  ,post_mobilecampaignmedium   

  ,post_mobilecampaignname 

  ,post_mobilecampaignsource   

  ,post_mobilecampaignterm 

  ,post_mobiledayofweek

  ,post_mobiledayssincefirstuse

  ,post_mobiledayssincelastuse 

  ,post_mobiledevice   

  ,post_mobilehourofday

  ,post_mobileinstalldate  

  ,post_mobilelaunchnumber 

  ,post_mobileltv 

  ,post_mobilemessagebuttonname

 ,post_mobilemessageclicks

  ,post_mobilemessageid

  ,post_mobilemessageiddest

  ,post_mobilemessageidname

  ,post_mobilemessageidtype

  ,post_mobilemessageimpressions   

  ,post_mobilemessageonline

  ,post_mobilemessagepushoptin 

  ,post_mobilemessagepushpayloadid 

  ,post_mobilemessagepushpayloadidname 

  ,post_mobilemessageviews 

  ,post_mobileosversion

  ,post_mobilepushoptin

  ,post_mobilepushpayloadid

  ,post_mobileresolution   

  ,post_mvvar1

  ,post_mvvar2

  ,post_mvvar3

  ,post_page_event

  ,post_page_event_var1

  ,post_page_event_var2

  ,post_page_event_var3

  ,post_page_type 

  ,post_page_url  

  ,post_pagename  

  ,post_pagename_no_url

  ,post_partner_plugins

  ,post_persistent_cookie  

  ,post_pointofinterest

 ,post_pointofinterestdistance

  ,post_product_list   

  ,post_prop1 

  ,post_prop2 

  ,post_prop3 

  ,post_prop4 

  ,post_prop5 

  ,post_prop6 

  ,post_prop7 

  ,post_prop8 

  ,post_prop9 

  ,post_prop10

  ,post_prop11

  ,post_prop12

  ,post_prop13

  ,post_prop14

  ,post_prop15

  ,post_prop16

  ,post_prop17

  ,post_prop18

  ,post_prop19

  ,post_prop20

  ,post_prop21

  ,post_prop22

  ,post_prop23

  ,post_prop24

  ,post_prop25

  ,post_prop26

  ,post_prop27

 ,post_prop28

  ,post_prop29

  ,post_prop30

  ,post_prop31

  ,post_prop32

  ,post_prop33

  ,post_prop34

  ,post_prop35

  ,post_prop36

  ,post_prop37

  ,post_prop38

  ,post_prop39

  ,post_prop40

  ,post_prop41

  ,post_prop42

  ,post_prop43

  ,post_prop44

  ,post_prop45

  ,post_prop46

  ,post_prop47

  ,post_prop48

  ,post_prop49

  ,post_prop50

  ,post_prop51

  ,post_prop52

  ,post_prop53

  ,post_prop54

  ,post_prop55

  ,post_prop56

 ,post_prop57

  ,post_prop58

  ,post_prop59

  ,post_prop60

  ,post_prop61

  ,post_prop62

  ,post_prop63

  ,post_prop64

  ,post_prop65

  ,post_prop66

  ,post_prop67

  ,post_prop68

  ,post_prop69

  ,post_prop70

  ,post_prop71

  ,post_prop72

  ,post_prop73

  ,post_prop74

  ,post_prop75

  ,post_purchaseid

  ,post_referrer  

  ,post_s_kwcid   

  ,post_search_engine  

  ,post_socialaccountandappids 

  ,post_socialassettrackingcode

  ,post_socialauthor   

  ,post_socialaveragesentiment 

  ,post_socialaveragesentiment_deprecated  

  ,post_socialcontentprovider  

 ,post_socialfbstories

  ,post_socialfbstorytellers   

  ,post_socialinteractioncount 

  ,post_socialinteractiontype  

  ,post_sociallanguage 

  ,post_sociallatlong  

  ,post_sociallikeadds 

  ,post_sociallink

  ,post_sociallink_deprecated  

  ,post_socialmentions 

  ,post_socialowneddefinitioninsighttype   

  ,post_socialowneddefinitioninsightvalue  

  ,post_socialowneddefinitionmetric

  ,post_socialowneddefinitionpropertyvspost

  ,post_socialownedpostids 

  ,post_socialownedpropertyid  

  ,post_socialownedpropertyname

  ,post_socialownedpropertypropertyvsapp   

  ,post_socialpageviews

  ,post_socialpostviews

  ,post_socialproperty 

  ,post_socialproperty_deprecated  

  ,post_socialpubcomments  

  ,post_socialpubposts 

  ,post_socialpubrecommends

  ,post_socialpubsubscribers   

  ,post_socialterm

  ,post_socialtermslist

  ,post_socialtermslist_deprecated 

 ,post_socialtotalsentiment   

  ,post_state 

  ,post_survey

  ,post_t_time_info

  ,post_tnt   

  ,post_tnt_action

  ,post_transactionid  

  ,post_video 

  ,post_videoad   

  ,post_videoadinpod   

  ,post_videoadlength  

  ,post_videoadname

  ,post_videoadplayername  

  ,post_videoadpod

  ,post_videoadvertiser

  ,post_videoauthorized

  ,post_videocampaign  

  ,post_videochannel   

  ,post_videochapter   

  ,post_videocontenttype   

  ,post_videodaypart   

  ,post_videoepisode   

  ,post_videofeedtype  

  ,post_videogenre

  ,post_videolength

  ,post_videomvpd 

  ,post_videoname 

  ,post_videonetwork   

  ,post_videopath 

 ,post_videoplayername

  ,post_videoqoebitrateaverageevar 

  ,post_videoqoebitratechangecountevar 

  ,post_videoqoebuffercountevar

  ,post_videoqoebuffertimeevar 

  ,post_videoqoedroppedframecountevar  

  ,post_videoqoeerrorcountevar 

  ,post_videoqoetimetostartevar

  ,post_videoseason

  ,post_videosegment   

  ,post_videoshow 

  ,post_videoshowtype  

  ,post_visid_high

  ,post_visid_low 

  ,post_visid_type

  ,post_zip   

  ,prev_page  

  ,product_list   

  ,product_merchandising   

  ,prop1  

  ,prop2  

  ,prop3  

  ,prop4  

  ,prop5  

  ,prop6  

  ,prop7  

  ,prop8  

  ,prop9  

  ,prop10 

 ,prop11 

  ,prop12 

  ,prop13 

  ,prop14 

  ,prop15 

  ,prop16 

  ,prop17 

  ,prop18 

  ,prop19 

  ,prop20 

  ,prop21 

  ,prop22 

  ,prop23 

  ,prop24 

  ,prop25 

  ,prop26 

  ,prop27 

  ,prop28 

  ,prop29 

  ,prop30 

  ,prop31 

  ,prop32 

  ,prop33 

  ,prop34 

  ,prop35 

  ,prop36 

  ,prop37 

  ,prop38 

  ,prop39 

 ,prop40 

  ,prop41 

  ,prop42 

  ,prop43 

  ,prop44 

  ,prop45 

  ,prop46 

  ,prop47 

  ,prop48 

  ,prop49 

  ,prop50 

  ,prop51 

  ,prop52 

  ,prop53 

  ,prop54 

  ,prop55 

  ,prop56 

  ,prop57 

  ,prop58 

  ,prop59 

  ,prop60 

  ,prop61 

  ,prop62 

  ,prop63 

  ,prop64 

  ,prop65 

  ,prop66 

  ,prop67 

  ,prop68 

 ,prop69 

  ,prop70 

  ,prop71 

  ,prop72 

  ,prop73 

  ,prop74 

  ,prop75 

  ,purchaseid 

  ,quarterly_visitor   

  ,ref_domain 

  ,ref_type   

  ,referrer   

  ,resolution 

  ,s_kwcid

  ,s_resolution   

  ,sampled_hit

  ,search_engine  

  ,search_page_num

  ,secondary_hit  

  ,service

  ,socialaccountandappids  

  ,socialassettrackingcode 

  ,socialauthor   

  ,socialaveragesentiment  

  ,socialaveragesentiment_deprecated   

  ,socialcontentprovider   

  ,socialfbstories

  ,socialfbstorytellers

  ,socialinteractioncount  

 ,socialinteractiontype   

  ,sociallanguage 

  ,sociallatlong  

  ,sociallikeadds 

  ,sociallink 

  ,sociallink_deprecated   

  ,socialmentions 

  ,socialowneddefinitioninsighttype

  ,socialowneddefinitioninsightvalue   

  ,socialowneddefinitionmetric 

  ,socialowneddefinitionpropertyvspost 

  ,socialownedpostids  

  ,socialownedpropertyid   

  ,socialownedpropertyname 

  ,socialownedpropertypropertyvsapp

  ,socialpageviews

  ,socialpostviews

  ,socialproperty 

  ,socialproperty_deprecated   

  ,socialpubcomments   

  ,socialpubposts 

  ,socialpubrecommends 

  ,socialpubsubscribers

  ,socialterm 

  ,socialtermslist

  ,socialtermslist_deprecated  

  ,socialtotalsentiment

  ,sourceid   

  ,state  

 ,stats_server   

  ,t_time_info

  ,tnt

  ,tnt_action 

  ,tnt_post_vista 

  ,transactionid  

  ,truncated_hit  

  ,ua_color   

  ,ua_os  

  ,ua_pixels  

  ,user_agent 

  ,user_hash  

  ,user_server

  ,userid 

  ,username   

  ,va_closer_detail

  ,va_closer_id   

  ,va_finder_detail

  ,va_finder_id   

  ,va_instance_event   

  ,va_new_engagement   

  ,video  

  ,videoad

  ,videoadinpod   

  ,videoadlength  

  ,videoadname

  ,videoadplayername   

  ,videoadpod 

  ,videoadvertiser

 ,videoaudioalbum

  ,videoaudioartist

  ,videoaudioauthor

  ,videoaudiolabel

  ,videoaudiopublisher 

  ,videoaudiostation   

  ,videoauthorized

  ,videoaverageminuteaudience  

  ,videocampaign  

  ,videochannel   

  ,videochapter   

  ,videochaptercomplete

  ,videochapterstart   

  ,videochaptertime

  ,videocontenttype

  ,videodaypart   

  ,videoepisode   

  ,videofeedtype  

  ,videogenre 

  ,videolength

  ,videomvpd  

  ,videoname  

  ,videonetwork   

  ,videopath  

  ,videopause 

  ,videopausecount

  ,videopausetime 

  ,videoplay  

  ,videoplayername

 ,videoprogress10

  ,videoprogress25

  ,videoprogress50

  ,videoprogress75

  ,videoprogress96

  ,videoqoebitrateaverage  

  ,videoqoebitrateaverageevar  

  ,videoqoebitratechange   

  ,videoqoebitratechangecountevar  

  ,videoqoebuffer 

  ,videoqoebuffercountevar 

  ,videoqoebuffertimeevar  

  ,videoqoedropbeforestart 

  ,videoqoedroppedframecountevar   

  ,videoqoedroppedframes   

  ,videoqoeerror  

  ,videoqoeerrorcountevar  

  ,videoqoeextneralerrors  

  ,videoqoeplayersdkerrors 

  ,videoqoetimetostartevar 

  ,videoresume

  ,videoseason

  ,videosegment   

  ,videoshow  

  ,videoshowtype  

  ,videostreamtype

  ,videototaltime 

  ,videouniquetimeplayed   

  ,visid_high 

 ,visid_low  

  ,visid_new  

  ,visid_timestamp

  ,visid_type 

  ,visit_keywords 

  ,visit_num  

  ,visit_page_num 

  ,visit_ref_domain

  ,visit_ref_type 

  ,visit_referrer 

  ,visit_search_engine 

  ,visit_start_page_url

  ,visit_start_pagename

  ,visit_start_time_gmt

  ,weekly_visitor 

  ,yearly_visitor 

  ,zip
  
  ,dw_source_create_nm 
  
  ,DW_CREATETS
from EDM_CONFIRMED_PRD.DW_C_USER_ACTIVITY.click_hit_data;