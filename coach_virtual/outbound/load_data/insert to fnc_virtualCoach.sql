INSERT INTO fnc_virtual_coach.fdethybrysdata (select
addressregion,
birthdate::date,
calendaryear,
campaignautomationactionuuid,
campaigncategory,
campaigncontentlink,
campaigncontentlinkgroup,
campaigncontentlinkname,
campaigncontentlinkurl,
campaignexecutionstatus,
campaignid,
campaignname,
campaignowner,
campaignpriority,
campaignprocesstype,
campaignversion,
cityname,
communicationcategory,
communicationcategoryname,
communicationmedium,
companyname,
contactpostalcode,
country,
currency,
department,
emailaddress,
fullname,
function,
gendercode,
id,
interaction,
interactionamount,
interactioncontact,
interactioncontactid,
interactioncontent,
interactioncontentsubject,
interactionreason,
interactionsourceobjectstatus,
interactiontimestamputc::date,
interactiontype,
interactionutcdate::date,
interactionutctime,
maritalstatus,
marketingarea,
marketingorchestrationid,
marketingprogram,
marketingprogramid,
marketingprogramname,
mediatype,
mobilenumber,
nmbrofmisgmarketingpermissions,
numberofanyclicks,
numberofbounces,
numberofdeliveredemailmessages,
numberofdeliveredmessages,
numberofemailcomplaints,
numberofhardbounces,
numberofinteractions,
numberoflimitreached,
numberofmarketingofferclicks,
numberofmissingcommdata,
numberofopenedmessages,
numberofsentmessages,
numberofsoftbounces,
numberoftotalclicks,
numberofuniqueclicks,
numberofuniqueopens,
numberofunopenedmessages,
rootinteraction,
targetgroup,
targetgroupuuid,
yy1_codigoebelista_mps,
yy1_documentoidentidad_mps,
yearmonth,
yearquarter,
yearweek,
null,
campanaexposicion as aniocampana
	FROM lan_virtual_coach.fdethybrysdata
);
