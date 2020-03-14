-- Se actualizan los campos que no tienen el formato de fecha correcto
UPDATE lan_virtual_coach.fdethybrysdata_v2
SET interactiontimestamputc = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(interactiontimestamputc,'/Date(',''),'+0000)/','')::BIGINT)/1000 * interval '1 second'),
	birthdate = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(birthdate,'/Date(',''),')/','')::BIGINT)/1000 * interval '1 second'),
	interactionutcdate = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(interactionutcdate,'/Date(',''),')/','')::BIGINT)/1000 * interval '1 second'),
	executionrundatetime = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(executionrundatetime,'/Date(',''),'+0000)/','')::BIGINT)/1000 * interval '1 second'),
	campaignexecutionrundate = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(campaignexecutionrundate,'/Date(',''),')/','')::BIGINT)/1000 * interval '1 second'),
	fechapasepedido = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(fechapasepedido,'/Date(',''),')/','')::BIGINT)/1000 * interval '1 second'),
	fechafinfacturacion = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(fechafinfacturacion,'/Date(',''),')/','')::BIGINT)/1000 * interval '1 second'),
	fechainiciofacturacion = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(fechainiciofacturacion,'/Date(',''),')/','')::BIGINT)/1000 * interval '1 second'),
	fechafinventa = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(fechafinventa,'/Date(',''),')/','')::BIGINT)/1000 * interval '1 second'),
	fechainiciodeventa = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(fechainiciodeventa,'/Date(',''),')/','')::BIGINT)/1000 * interval '1 second');

--Se inserta la data al historico con todos los campos que envía hybris
INSERT INTO wrk_virtual_coach.fdethybrysdataatrb_history
(select
addressregion,
birthdate::date,
calendaryear,
campaignautomationactionuui as campaignautomationactionuuid,
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
interactionsourceobjectstat as interactionsourceobjectstatus,
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
nmbrofmisgmarketingpermissi as nmbrofmisgmarketingpermissions,
numberofanyclicks,
numberofbounces,
numberofdeliveredemailmessa as numberofdeliveredemailmessages,
numberofdeliveredmessages,
numberofemailcomplaints,
numberofhardbounces,
numberofinteractions,
numberoflimitreached,
numberofmarketingofferclick as numberofmarketingofferclicks,
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
campanaexposicion as aniocampana,
executionrundatetime::date,
campaignexecutionrundate::date,
recipientemaildomain,
yy1_idconsultoracbo_mps,
nombresocia,
desseccion,
codseccion,
gerentezona,
deszona,
codzona,
desregion,
codregion,
flag100digital,
flaginscritaganamas,
flaginscritavirtualcoach,
flagappsocia,
flagappconsultora,
flagessocia,
fechaingresobelcorp,
desaliasnombre,
desdireccion,
campanaingreso,
flagcelular,
flagcorreo,
nrodocidentidad,
tipodocumento,
nrocampananuevas,
nropedidosnuevas,
fechapasepedido::date,
fechafinfacturacion::date,
fechainiciofacturacion::date,
fechafinventa::date,
fechainiciodeventa::date,
flagdeuda,
flagpasopedido,
flagconstancianuevas,
flagtippingpoint4,
flagtippingpoint5,
flagtippingpoint6,
flagofertadigitaluc,
nroofertadigitalpu5c,
flagipunicopu5c,
flagipunico,
desconstancianuevas,
dessegmentodigital,
descomportamiento,
tipoconsultora,
idcontactoconsultoracbo,
idcontacto,
idorigencontacto,
(getdate() - interval '5 HOURS')
FROM lan_virtual_coach.fdethybrysdata_v2);