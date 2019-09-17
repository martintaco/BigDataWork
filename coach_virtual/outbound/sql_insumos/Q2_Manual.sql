
/*¨
 * Paso 1
 */

DROP TABLE IF EXISTS #PaisCampanas_tmp;
SELECT DISTINCT campanaexposicion AS aniocampana_proceso
INTO #PaisCampanas_tmp
FROM lan_virtual_coach.fdethybrysdata
WHERE length(aniocampana_proceso) = 6;



 /*Se agrega un inner join para que se haga un mactch con la tabla de Campañas de facturación*/

DROP TABLE IF EXISTS #PaisCampanas;
SELECT DISTINCT country as codpais, a.aniocampana, '000000' AS ANIOCAMPANA_U6C, '000000' AS ANIOCAMPANA_U1C
INTO #PaisCampanas
FROM fnc_virtual_coach.fdethybrysdata a
inner join fnc_analitico.ctr_cierre_generico c on a.country = c.cod_pais and a.aniocampana >= c.aniocampana
WHERE EXISTS
             (      SELECT       *
                    FROM #PaisCampanas_tmp b
                    WHERE a.aniocampana = b.aniocampana_proceso
             )
and length(codpais) = 2
and c.estado_sicc = '0'
order by 1,2;

/*Ejecucion para campanas fijas*/
delete from #PaisCampanas where aniocampana != '201913';

update #PaisCampanas
set aniocampana = '201911'
where aniocampana = '201913';
-----------------------------------------

DROP TABLE IF EXISTS #fdettemplates_fdethybrysdata;
SELECT 	DISTINCT
		aniocampana as aniocampanaexpo, country as codpais, '' as despublico,
CASE
         WHEN communicationmedium = 'EMAIL' THEN 'Mail'
         WHEN communicationmedium = 'MOBILE_APP' THEN 'Push'
         WHEN communicationmedium = 'SMS' THEN 'SMS'
         ELSE 'Otros'
         END destipo
, '' as descategoria, '' as dessubcategoria, campaignname as descampania, interactioncontentsubject as destitulo, '' as codcuc, '' as codventa
INTO 	#fdettemplates_fdethybrysdata
FROM 	fnc_virtual_coach.fdethybrysdata A
WHERE EXISTS
		(	SELECT 	*
			FROM 	#PaisCampanas b
			WHERE 	a.country = b.codpais AND
					a.aniocampana = b.aniocampana
		);

DROP TABLE IF EXISTS #fdettemplates;
SELECT *
INTO #fdettemplates
FROM fnc_virtual_coach.fdettemplates a
WHERE EXISTS
		(	SELECT 	*
			FROM 	#PaisCampanas b
			WHERE 	a.codpais = b.codpais AND
					a.aniocampanaexpo = b.aniocampana
		);


INSERT INTO #fdettemplates
(aniocampanaexpo, codpais, despublico, destipo, descategoria, dessubcategoria, descampania, destitulo, codcuc, codventa)
SELECT
aniocampanaexpo, codpais, despublico, destipo, descategoria, dessubcategoria, descampania, destitulo, codcuc, codventa
FROM #fdettemplates_fdethybrysdata a
WHERE NOT EXISTS
	(	SELECT	*
		FROM 	#fdettemplates  b
		WHERE	a.aniocampanaexpo = b.aniocampanaexpo AND
				a.codpais = b.codpais AND
				LTRIM(RTRIM(UPPER(a.destitulo))) = LTRIM(RTRIM(UPPER(b.destitulo))) AND
				LTRIM(RTRIM(UPPER(a.descampania))) = LTRIM(RTRIM(UPPER(b.descampania)))
	);

DELETE fnc_virtual_coach.fdettemplates
WHERE  EXISTS
	(	SELECT	*
		FROM 	#fdettemplates  b
		WHERE	fdettemplates.aniocampanaexpo = b.aniocampanaexpo AND
				fdettemplates.codpais = b.codpais AND
				LTRIM(RTRIM(UPPER(fdettemplates.destitulo))) = LTRIM(RTRIM(UPPER(b.destitulo))) AND
				LTRIM(RTRIM(UPPER(fdettemplates.descampania))) = LTRIM(RTRIM(UPPER(b.descampania)))
	);

INSERT INTO fnc_virtual_coach.fdettemplates
(aniocampanaexpo, codpais, despublico, destipo, descategoria, dessubcategoria, descampania, destitulo, codcuc, codventa)
SELECT
aniocampanaexpo, codpais, despublico, destipo, descategoria, dessubcategoria, descampania, destitulo, codcuc, codventa
FROM #fdettemplates;

DELETE dom_virtual_coach.fdettemplates
WHERE EXISTS
		(	SELECT 	*
			FROM 	#PaisCampanas b
			WHERE 	fdettemplates.codpais = b.codpais AND
					fdettemplates.aniocampanaexpo = b.aniocampana
		);

INSERT INTO dom_virtual_coach.fdettemplates
(aniocampanaexpo, codpais, despublico, destipo, descategoria, dessubcategoria, descampania, destitulo, codcuc, codventa)
SELECT
aniocampanaexpo, codpais, despublico, destipo, descategoria, dessubcategoria, descampania, destitulo, codcuc, codventa
FROM #fdettemplates;

-----------------------------------------History----------------------------------------------
INSERT INTO wrk_virtual_coach.fdettemplates_history
(aniocampanaexpo, codpais, despublico, destipo, descategoria, dessubcategoria, descampania, destitulo, codcuc, codventa, origindayfile)
SELECT
aniocampanaexpo, codpais, despublico, destipo, descategoria, dessubcategoria, descampania, destitulo, codcuc, codventa, (getdate() - interval '5 HOURS')
FROM #fdettemplates;
----------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS #fdethybrysdata;
SELECT *
INTO #fdethybrysdata
FROM fnc_virtual_coach.fdethybrysdata a
WHERE EXISTS
		(	SELECT 	*
			FROM 	#PaisCampanas b
			WHERE 	a.country = b.codpais AND
					a.aniocampana= b.aniocampana
		);





DROP TABLE IF EXISTS #Interacciones;
SELECT 	campaignname,
       	communicationmedium,
       	country,
       	emailaddress,
       	fullname,
       	interactioncontentsubject,
       	MAX(CAST(numberofsentmessages AS INTEGER) ) Enviados,
       	MAX(CAST(numberofdeliveredmessages AS INTEGER)) Recibidos,
       	MAX(CAST(numberofopenedmessages AS INTEGER)) Abiertos,
       	MAX(CAST(numberofuniqueclicks AS INTEGER)) ClicsUnicos,
       	MAX(CAST(numberoftotalclicks AS INTEGER)) Clics,
       	yy1_codigoebelista_mps CodEbelista,
       	yy1_documentoidentidad_mps DocIdentidad,
       	CAST('20' + right (id, 6) AS DATE) Fecha,
       	'' AS interactiontype,
		CASE WHEN interactioncontentsubject LIKE '%NUEVA%' THEN 'NUEVA'
		WHEN interactioncontentsubject LIKE '%ESTA%' THEN 'ESTABLECIDA'
		ELSE 'OTRO' END TIPOMAIL,
		aniocampana
INTO 	#Interacciones
FROM 	#fdethybrysdata
GROUP BY campaignname,
       	communicationmedium,
       	country,
       	emailaddress,
       	fullname,
       	interactioncontentsubject,
       	yy1_codigoebelista_mps,
       	yy1_documentoidentidad_mps,
       	CAST('20' + right (id, 6) AS DATE),
       	--interactiontype,
		aniocampana;

UPDATE 	#Interacciones
SET 	Enviados = 1
WHERE 	Enviados > 1;

UPDATE 	#Interacciones
SET 	Recibidos = 1
WHERE 	Recibidos > 1;

UPDATE 	#Interacciones
SET 	Abiertos = 1
WHERE 	Abiertos > 1;

UPDATE 	#Interacciones
SET 	ClicsUnicos = 1
WHERE 	ClicsUnicos > 1;

DELETE 	dom_virtual_coach.fdetiteraccion
WHERE	EXISTS
		(	SELECT 	*
			FROM 	#PaisCampanas b
			WHERE 	fdetiteraccion.codpais = b.codpais AND
					fdetiteraccion.aniocampana = b.aniocampana
		);

/*¨
 * Paso 8
 */
INSERT	INTO dom_virtual_coach.fdetiteraccion(
       	Fecha, CodEbelista, CampaniaMarketing, TituloContenido, CodPais, Pais, DocIdentidad, CorreoElectronico,
       	MensajesEnviados, MensajesEntregados, MensajesAbiertos, ClicsUnicos, CualquierClic, TipoInteraccion, tipomail, aniocampana)
SELECT 	DISTINCT
		max(Fecha), CodEbelista, campaignname, interactioncontentsubject, a.country,
		(select B.despais from fnc_analitico.dwh_dpais b where ltrim(trim(a.country)) = ltrim(rtrim(b.codpais))),
		DocIdentidad,
       	emailaddress,
       	Enviados,
       	Recibidos,
       	Abiertos,
       	ClicsUnicos,
       	Clics,
       	Interactiontype,
       	tipomail,
       	aniocampana
FROM 	#Interacciones a
WHERE 	country IS NOT NULL
group by CodEbelista, campaignname, interactioncontentsubject, a.country,
		DocIdentidad,
       	emailaddress,
       	Enviados,
       	Recibidos,
       	Abiertos,
       	ClicsUnicos,
       	Clics,
       	Interactiontype,
       	tipomail,
       	aniocampana;
------------------------------------------------History---------------------------------------
INSERT	INTO dom_virtual_coach.fdetiteraccion(
       	Fecha, CodEbelista, CampaniaMarketing, TituloContenido, CodPais, Pais, DocIdentidad, CorreoElectronico,
       	MensajesEnviados, MensajesEntregados, MensajesAbiertos, ClicsUnicos, CualquierClic, TipoInteraccion, tipomail, aniocampana)
SELECT 	DISTINCT
		max(Fecha), CodEbelista, campaignname, interactioncontentsubject, a.country,
		(select B.despais from fnc_analitico.dwh_dpais b where ltrim(trim(a.country)) = ltrim(rtrim(b.codpais))),
		DocIdentidad,
       	emailaddress,
       	Enviados,
       	Recibidos,
       	Abiertos,
       	ClicsUnicos,
       	Clics,
       	Interactiontype,
       	tipomail,
       	aniocampana
FROM 	#Interacciones a
WHERE 	country IS NOT NULL
group by CodEbelista, campaignname, interactioncontentsubject, a.country,
		DocIdentidad,
       	emailaddress,
       	Enviados,
       	Recibidos,
       	Abiertos,
       	ClicsUnicos,
       	Clics,
       	Interactiontype,
       	tipomail,
       	aniocampana;

update wrk_virtual_coach.fdetiteraccion_history
set origindayfile = (getdate() - interval '5 HOURS')
where origindayfile is null;
----------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS #Interacciones;
