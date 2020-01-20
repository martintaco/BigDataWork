DROP TABLE IF EXISTS #USABILIDAD_RANK_USABILIDAD_CAMPANIA_PAIS;
DROP TABLE IF EXISTS #USABILIDAD_CAMPANIA_PAIS;
DROP TABLE IF EXISTS #USABILIDAD;
DROP TABLE IF EXISTS #USABILIDAD_DAYS_CONNECTED;
DROP TABLE IF EXISTS #USABILIDAD_RESULTADO;

DROP TABLE IF EXISTS #RDV_RANK_CAMPANIA_PAIS;
DROP TABLE IF EXISTS #RDV_CAMPANIA_PAIS;
DROP TABLE IF EXISTS #STATUS_VISITA;
DROP TABLE IF EXISTS #RDV_DAYS_CONNECTED;
DROP TABLE IF EXISTS #RDV_RESULTADO;

CREATE TEMPORARY TABLE #USABILIDAD_RANK_USABILIDAD_CAMPANIA_PAIS( orden int, campania int4, pais varchar(2));
CREATE TEMPORARY TABLE #USABILIDAD_CAMPANIA_PAIS( campania int4, pais varchar(2), factor decimal(10,2));
CREATE TEMPORARY TABLE #USABILIDAD( campania int4, pais varchar(2), region varchar(5), zona varchar(4), seccion varchar(5), usuario varchar(30), rol varchar(5), factor decimal(10,2), opcion_pantalla varchar(100), days_connectedhome int, days_connectedrdd int);
CREATE TEMPORARY TABLE #USABILIDAD_DAYS_CONNECTED ( campania int4, pais varchar(2), region varchar(5), zona varchar(4), seccion varchar(5), usuario varchar(30), rol varchar(5), factor decimal(10,2), days_connectedhome int, days_connectedrdd int);
CREATE TEMPORARY TABLE #USABILIDAD_RESULTADO( pais varchar(2), campania int4, region varchar(5), zona varchar(4), seccion varchar(9), CodSocia varchar(30), diashabiles int, diasrequeridos int, days_connectedhome int, flagusaapp int, days_connectedrdd int, flagusardd int);

CREATE TEMPORARY TABLE #RDV_RANK_CAMPANIA_PAIS( orden int, campania varchar(6), pais varchar(2));
CREATE TEMPORARY TABLE #RDV_CAMPANIA_PAIS( campania varchar(6), pais varchar(2), factor decimal(10,2));
CREATE TEMPORARY TABLE #STATUS_VISITA( campania varchar(6), pais varchar(2), region varchar(3), zona varchar(4), seccion varchar(1), usuario varchar(25), factor decimal(10,2), fechavisita varchar(10), StatusVisita varchar(50), Days_ConnectedRdV int);
CREATE TEMPORARY TABLE #RDV_DAYS_CONNECTED( campania varchar(6), pais varchar(2), region varchar(3), zona varchar(4), seccion varchar(1), usuario varchar(25), factor decimal(10,2), days_connectedrdv int);
CREATE TEMPORARY TABLE #RDV_RESULTADO( pais varchar(2), campania varchar(6), region varchar(3), zona varchar(4), seccion varchar(5), codsocia varchar(25), diashabiles int, diasrequeridos int, days_connectedrdv int, flagusardv int);

/*Usabilidad : Inicio*/

INSERT INTO #USABILIDAD_RANK_USABILIDAD_CAMPANIA_PAIS(orden, campania, pais)
SELECT
	RANK() OVER(PARTITION BY resumen_log_usabilidad.pais ORDER BY resumen_log_usabilidad.campania DESC)
	,resumen_log_usabilidad.campania
	,resumen_log_usabilidad.pais
FROM
	Dom_Digital.resumen_log_usabilidad resumen_log_usabilidad
WHERE
	resumen_log_usabilidad.Campania IS NOT NULL
AND LEN(resumen_log_usabilidad.Campania) = 6
AND RTRIM(resumen_log_usabilidad.Pais) <> ''
--AND resumen_log_usabilidad.Pais IN ('PE','BO','PA')
GROUP BY
	resumen_log_usabilidad.Campania
	,resumen_log_usabilidad.Pais;

INSERT INTO #USABILIDAD_CAMPANIA_PAIS(campania, pais, factor)
SELECT
	USABILIDAD_RANK_USABILIDAD_CAMPANIA_PAIS.campania
	,USABILIDAD_RANK_USABILIDAD_CAMPANIA_PAIS.pais
	,0.80
FROM
	#USABILIDAD_RANK_USABILIDAD_CAMPANIA_PAIS USABILIDAD_RANK_USABILIDAD_CAMPANIA_PAIS
WHERE
	USABILIDAD_RANK_USABILIDAD_CAMPANIA_PAIS.orden <= 6;


	INSERT INTO #USABILIDAD( campania, pais, region, zona, seccion, usuario, rol, factor, opcion_pantalla, days_connectedhome, days_connectedrdd)
	SELECT
	   resumen_log_usabilidad.campania
	   ,resumen_log_usabilidad.pais
	   ,resumen_log_usabilidad.region
	   ,resumen_log_usabilidad.zona
	   ,resumen_log_usabilidad.seccion
	   ,resumen_log_usabilidad.usuario
	   ,resumen_log_usabilidad.rol
	   ,USABILIDAD_CAMPANIA_PAIS.factor
	   ,resumen_log_usabilidad.opcion_pantalla
	   ,CASE WHEN resumen_log_usabilidad.opcion_pantalla = 'HOME' THEN COUNT(DISTINCT (CASE WHEN date_part(dow, resumen_log_usabilidad.fecha) NOT IN (0,6) THEN to_char(resumen_log_usabilidad.fecha, 'DD/MM/YYYY') ELSE NULL END)) ELSE NULL END
	   ,CASE WHEN resumen_log_usabilidad.opcion_pantalla = 'RUTA_DESARROLLO' THEN COUNT(DISTINCT (CASE WHEN date_part(dow, resumen_log_usabilidad.fecha) NOT IN (0,6) THEN to_char(resumen_log_usabilidad.fecha, 'DD/MM/YYYY') ELSE NULL END)) ELSE NULL END
	FROM
	   dom_digital.resumen_log_usabilidad resumen_log_usabilidad
	INNER JOIN
		#USABILIDAD_CAMPANIA_PAIS USABILIDAD_CAMPANIA_PAIS
	ON USABILIDAD_CAMPANIA_PAIS.campania = resumen_log_usabilidad.campania
	AND USABILIDAD_CAMPANIA_PAIS.pais = resumen_log_usabilidad.pais
	WHERE
		resumen_log_usabilidad.pais <> ''
		AND resumen_log_usabilidad.region <> ''
		AND resumen_log_usabilidad.zona <> ''
		AND resumen_log_usabilidad.seccion <> ''
		AND resumen_log_usabilidad.usuario <> ''
		AND resumen_log_usabilidad.fecha >= '2017-01-01'
		AND RTRIM(resumen_log_usabilidad.opcion_pantalla) IN ('HOME','RUTA_DESARROLLO')
	GROUP BY 1,2,3,4,5,6,7,8,9;


INSERT INTO #USABILIDAD_DAYS_CONNECTED( campania, pais, region, zona, seccion, usuario, rol, factor, days_connectedhome, days_connectedrdd)
SELECT
	USABILIDAD.campania
	,USABILIDAD.pais
	,USABILIDAD.region
	,USABILIDAD.zona
	,USABILIDAD.seccion
	,USABILIDAD.usuario
	,USABILIDAD.rol
	,USABILIDAD.factor
	,SUM(USABILIDAD.days_connectedhome) days_connectedhome
	,SUM(USABILIDAD.days_connectedrdd) days_connectedrdd
FROM #USABILIDAD USABILIDAD
GROUP BY 1,2,3,4,5,6,7,8;

	INSERT INTO #USABILIDAD_RESULTADO( pais, campania, region, zona, seccion, codSocia, diashabiles, diasrequeridos, days_connectedhome, flagusaapp, days_connectedrdd, flagusardd)
	SELECT
		USABILIDAD_DAYS_CONNECTED.pais
		,USABILIDAD_DAYS_CONNECTED.campania		
		,USABILIDAD_DAYS_CONNECTED.region
		,det_dias_paises.zona
		,RTRIM(USABILIDAD_DAYS_CONNECTED.zona) || RTRIM(USABILIDAD_DAYS_CONNECTED.seccion)
		,USABILIDAD_DAYS_CONNECTED.usuario
		,det_dias_paises.diashabiles
		,TRUNC(det_dias_paises.diashabiles::DECIMAL(10,2) * USABILIDAD_DAYS_CONNECTED.factor) diasrequeridos
		,USABILIDAD_DAYS_CONNECTED.days_connectedhome
		,CASE WHEN TRUNC(det_dias_paises.diashabiles::DECIMAL(10,2) * USABILIDAD_DAYS_CONNECTED.factor) <= USABILIDAD_DAYS_CONNECTED.days_connectedhome THEN 1 ELSE 0 END
		,USABILIDAD_DAYS_CONNECTED.days_connectedrdd
		,CASE WHEN TRUNC(det_dias_paises.diashabiles::DECIMAL(10,2) * USABILIDAD_DAYS_CONNECTED.factor) <= USABILIDAD_DAYS_CONNECTED.days_connectedrdd THEN 1 ELSE 0 END
	FROM #USABILIDAD_DAYS_CONNECTED USABILIDAD_DAYS_CONNECTED
	INNER JOIN
		dom_digital.det_dias_paises det_dias_paises
	ON
		det_dias_paises.aniocampana = USABILIDAD_DAYS_CONNECTED.campania
	AND det_dias_paises.codpais = USABILIDAD_DAYS_CONNECTED.pais
	AND det_dias_paises.seccion = (RTRIM(USABILIDAD_DAYS_CONNECTED.zona) || RTRIM(USABILIDAD_DAYS_CONNECTED.seccion))
	ORDER BY 1,2,3,4,5,6;

DROP TABLE #USABILIDAD_RANK_USABILIDAD_CAMPANIA_PAIS;
DROP TABLE #USABILIDAD_CAMPANIA_PAIS;
DROP TABLE #USABILIDAD;
DROP TABLE #USABILIDAD_DAYS_CONNECTED;

/*Usabilidad : Fin*/

/*Registro de Visita : Inicio*/

INSERT INTO #RDV_RANK_CAMPANIA_PAIS( orden, campania, pais)
SELECT
	RANK() OVER(PARTITION BY Det_CabPlanRutaRdD.pais ORDER BY Det_CabPlanRutaRdD.campania DESC)
	,Det_CabPlanRutaRdD.campania
	,Det_CabPlanRutaRdD.pais
FROM
	Dom_Digital.Det_CabPlanRutaRdD Det_CabPlanRutaRdD
WHERE
	Det_CabPlanRutaRdD.campania IS NOT NULL
AND LEN(Det_CabPlanRutaRdD.campania) = 6
AND RTRIM(Det_CabPlanRutaRdD.pais) <> ''
--AND Det_CabPlanRutaRdD.pais IN ('PE','BO','PA')
GROUP BY
	Det_CabPlanRutaRdD.campania
	,Det_CabPlanRutaRdD.pais;

INSERT INTO #RDV_CAMPANIA_PAIS(campania, pais, factor)
SELECT
	RDV_RANK_CAMPANIA_PAIS.campania
	,RDV_RANK_CAMPANIA_PAIS.pais
	,0.80
FROM
	#RDV_RANK_CAMPANIA_PAIS RDV_RANK_CAMPANIA_PAIS
WHERE
	RDV_RANK_CAMPANIA_PAIS.orden <= 6;

              
INSERT INTO #STATUS_VISITA (campania, pais, region, zona, seccion, usuario, factor, fechavisita, StatusVisita, Days_ConnectedRdV)
SELECT
	 Det_CabPlanRutaRdD.campania
	,Det_CabPlanRutaRdD.pais
	,Det_CabPlanRutaRdD.region
	,Det_CabPlanRutaRdD.zona
	,Det_CabPlanRutaRdD.seccion
	,Det_CabPlanRutaRdD.usuario
	,RDV_CAMPANIA_PAIS.factor
	,to_char(Det_PlanRutaRdD.fechavisita, 'DD/MM/YYYY') as  fechavisita
	,CASE WHEN Det_PlanRutaRdD.fechavisita IS NOT NULL
		THEN 'Visitada'
		ELSE '' END AS "StatusVisita"
	,CASE WHEN Det_PlanRutaRdD.fechavisita IS NOT NULL THEN COUNT(DISTINCT (CASE WHEN date_part(dow,Det_PlanRutaRdD.fechavisita) NOT IN (0,6) 
	THEN to_char(Det_PlanRutaRdD.fechavisita, 'DD/MM/YYYY') ELSE NULL END)) ELSE NULL END Days_ConnectedRdV
FROM
	dom_digital.Det_CabPlanRutaRdD Det_CabPlanRutaRdD
INNER JOIN Dom_Digital.Det_PlanRutaRdD Det_PlanRutaRdD
	ON Det_CabPlanRutaRdD.Id = Det_PlanRutaRdD.PlanVisitaId
	AND Det_CabPlanRutaRdD.Pais = Det_PlanRutaRdD.Pais
INNER JOIN #RDV_CAMPANIA_PAIS RDV_CAMPANIA_PAIS
ON RDV_CAMPANIA_PAIS.campania = Det_CabPlanRutaRdD.campania
AND RDV_CAMPANIA_PAIS.pais = Det_CabPlanRutaRdD.pais
WHERE
	Det_CabPlanRutaRdD.pais <> ''
AND Det_CabPlanRutaRdD.region <> ''
AND Det_CabPlanRutaRdD.zona <> ''
AND Det_CabPlanRutaRdD.seccion <> ''
AND Det_CabPlanRutaRdD.usuario <> ''
AND Det_PlanRutaRdD.fechavisita >= '2017-01-01'
GROUP BY 1,2,3,4,5,6,7,Det_PlanRutaRdD.fechavisita;

INSERT INTO #RDV_DAYS_CONNECTED( campania, pais, region, zona, seccion, usuario, factor, days_connectedrdv)
SELECT STATUS_VISITA.campania
, STATUS_VISITA.pais
, STATUS_VISITA.region
, STATUS_VISITA.zona
, STATUS_VISITA.seccion
, STATUS_VISITA.usuario
, STATUS_VISITA.factor
, case when STATUS_VISITA.days_connectedrdv = 1 then count(distinct STATUS_VISITA.fechavisita) else 0 end days_connectedrdv
FROM #STATUS_VISITA STATUS_VISITA
WHERE STATUS_VISITA.StatusVisita = 'Visitada'
GROUP BY 1,2,3,4,5,6,7,STATUS_VISITA.days_connectedrdv;


INSERT INTO #RDV_RESULTADO( pais, campania, region, zona, seccion, codsocia, diashabiles, diasrequeridos, days_connectedrdv, flagusardv)
SELECT
		RDV_DAYS_CONNECTED.pais
		,RDV_DAYS_CONNECTED.campania
		,RDV_DAYS_CONNECTED.region
		,det_dias_paises.zona
		,RTRIM(RDV_DAYS_CONNECTED.zona) || RTRIM(RDV_DAYS_CONNECTED.seccion)
		,RDV_DAYS_CONNECTED.usuario
		,det_dias_paises.diashabiles
		,TRUNC(det_dias_paises.diashabiles::DECIMAL(10,2) * RDV_DAYS_CONNECTED.factor) AS diasrequeridos
		,RDV_DAYS_CONNECTED.days_connectedrdv
		,CASE WHEN TRUNC(det_dias_paises.diashabiles::DECIMAL(10,2) * RDV_DAYS_CONNECTED.factor) <= RDV_DAYS_CONNECTED.days_connectedrdv THEN 1 ELSE 0 END
FROM #RDV_DAYS_CONNECTED RDV_DAYS_CONNECTED
INNER JOIN dom_digital.det_dias_paises det_dias_paises
ON det_dias_paises.aniocampana = RDV_DAYS_CONNECTED.campania
AND det_dias_paises.codpais = RDV_DAYS_CONNECTED.pais
AND det_dias_paises.seccion = (RTRIM(RDV_DAYS_CONNECTED.zona) || RTRIM(RDV_DAYS_CONNECTED.seccion))
WHERE RDV_DAYS_CONNECTED.days_connectedrdv > 0
ORDER BY 1,2,3,4,5,6,7;

DROP TABLE #RDV_RANK_CAMPANIA_PAIS;
DROP TABLE #RDV_CAMPANIA_PAIS;
DROP TABLE #STATUS_VISITA;
DROP TABLE #RDV_DAYS_CONNECTED;

/*Usabilidad : Fin*/

SELECT USABILIDAD_RESULTADO.Campania
, USABILIDAD_RESULTADO.Pais
, USABILIDAD_RESULTADO.Region
, USABILIDAD_RESULTADO.Zona
, USABILIDAD_RESULTADO.Seccion
, USABILIDAD_RESULTADO.CodSocia
, coalesce(USABILIDAD_RESULTADO.diashabiles,0) AS CantDiashabiles
, coalesce(USABILIDAD_RESULTADO.diasrequeridos,0) AS CantDiasRequeridos
, coalesce(USABILIDAD_RESULTADO.days_connectedhome,0) AS CantDiasapp
, coalesce(USABILIDAD_RESULTADO.days_connectedrdd,0) AS CantDiasrdd
, coalesce(RDV_RESULTADO.days_connectedrdv,0) AS CantDiasrdv
, coalesce(USABILIDAD_RESULTADO.FlagUsaApp,0) AS FlagUsaApp
, coalesce(USABILIDAD_RESULTADO.FlagUsaRdD,0) AS FlagUsaRdD
, coalesce(RDV_RESULTADO.FlagUsaRdV,0) AS FlagUsaRdV
INTO #Table1
FROM #USABILIDAD_RESULTADO USABILIDAD_RESULTADO
LEFT JOIN #RDV_RESULTADO RDV_RESULTADO
ON USABILIDAD_RESULTADO.Campania = RDV_RESULTADO.Campania
AND USABILIDAD_RESULTADO.Pais = RDV_RESULTADO.Pais
AND USABILIDAD_RESULTADO.Seccion = RDV_RESULTADO.Seccion
AND USABILIDAD_RESULTADO.CodSocia = RDV_RESULTADO.CodSocia;

DROP TABLE #USABILIDAD_RESULTADO;
DROP TABLE #RDV_RESULTADO;

unload($$ select * from #Table1 $$)
to 's3://belc-bigdata-domain-dlk-prd/dom-hana/Res_Uso_consultora/UsabilidadDLK_'
access_key_id '{ACCESS_KEY}'
secret_access_key '{SECRET_KEY}'
delimiter '\t'
NULL AS 'NULL'
ALLOWOVERWRITE
PARALLEL OFF
ESCAPE
ADDQUOTES;

DROP TABLE IF EXISTS #Table1;