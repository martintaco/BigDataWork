CREATE TEMP TABLE #RANK_CAMPANIA_PAIS AS
SELECT
	RANK() OVER(PARTITION BY resumen_log_usabilidad.Pais ORDER BY resumen_log_usabilidad.Campania DESC) AS Rank#
	,resumen_log_usabilidad.Campania
	,resumen_log_usabilidad.Pais
FROM
	Dom_Digital.resumen_log_usabilidad resumen_log_usabilidad
WHERE
	resumen_log_usabilidad.Campania IS NOT NULL
AND LEN(resumen_log_usabilidad.Campania) = 6
AND RTRIM(resumen_log_usabilidad.Pais) <> ''
GROUP BY
	resumen_log_usabilidad.Campania
	,resumen_log_usabilidad.Pais;

 
CREATE TEMP TABLE #CAMPANIA_PAIS AS
SELECT
	RANK_CAMPANIA_PAIS.Campania
	,RANK_CAMPANIA_PAIS.Pais,0.80 AS "Factor"
FROM
	#RANK_CAMPANIA_PAIS RANK_CAMPANIA_PAIS
WHERE
	RANK_CAMPANIA_PAIS.Rank# <= 6;
 
 
CREATE TEMP TABLE #Days_Connected AS
SELECT
	A.campania
	,A.pais
	,A.region
	,A.zona
	,A.seccion
	,A.usuario
	,A.rol
	,A.factor
	,SUM(A.Days_ConnectedHome) Days_ConnectedHome
	,SUM(A.Days_ConnectedRdD) Days_ConnectedRdD
	,SUM(A.Days_ConnectedRdV) Days_ConnectedRdV
FROM(
	SELECT
	   resumen_log_usabilidad.campania
	   ,resumen_log_usabilidad.pais
	   ,resumen_log_usabilidad.region
	   ,resumen_log_usabilidad.zona
	   ,resumen_log_usabilidad.seccion
	   ,resumen_log_usabilidad.usuario
	   ,resumen_log_usabilidad.rol
	   ,CAMPANIA_PAIS.factor
	   ,resumen_log_usabilidad.opcion_pantalla
	   ,CASE WHEN resumen_log_usabilidad.opcion_pantalla = 'HOME' THEN COUNT(DISTINCT (CASE WHEN date_part(dow, resumen_log_usabilidad.fecha) NOT IN (0,6) THEN to_char(resumen_log_usabilidad.fecha, 'DD/MM/YYYY') ELSE NULL END)) ELSE NULL END Days_ConnectedHome
	   ,CASE WHEN resumen_log_usabilidad.opcion_pantalla = 'RUTA_DESARROLLO' THEN COUNT(DISTINCT (CASE WHEN date_part(dow, resumen_log_usabilidad.fecha) NOT IN (0,6) THEN to_char(resumen_log_usabilidad.fecha, 'DD/MM/YYYY') ELSE NULL END)) ELSE NULL END Days_ConnectedRdD
	   ,CASE WHEN resumen_log_usabilidad.opcion_pantalla = 'REGISTRAR_VISITA' THEN COUNT(DISTINCT (CASE WHEN date_part(dow, resumen_log_usabilidad.fecha) NOT IN (0,6) THEN to_char(resumen_log_usabilidad.fecha, 'DD/MM/YYYY') ELSE NULL END)) ELSE NULL END Days_ConnectedRdV
	FROM
	   dom_digital.resumen_log_usabilidad resumen_log_usabilidad
	INNER JOIN
		#CAMPANIA_PAIS CAMPANIA_PAIS
	ON CAMPANIA_PAIS.campania = resumen_log_usabilidad.campania
	AND CAMPANIA_PAIS.pais = resumen_log_usabilidad.pais
	WHERE
		resumen_log_usabilidad.pais <> ''
		AND resumen_log_usabilidad.region <> ''
		AND resumen_log_usabilidad.zona <> ''
		AND resumen_log_usabilidad.seccion <> ''
		AND resumen_log_usabilidad.fecha >= '2017-01-01'
		AND RTRIM(resumen_log_usabilidad.opcion_pantalla) IN ('HOME','RUTA_DESARROLLO','REGISTRAR_VISITA')
	GROUP BY 1,2,3,4,5,6,7,8,9
	) A
GROUP BY 1,2,3,4,5,6,7,8;
 

SELECT
	Days_Connected.campania Campania
	,Days_Connected.pais Pais
	,Days_Connected.region Region
	,det_dias_paises.zona Zona
	,RTRIM(Days_Connected.zona) || RTRIM(Days_Connected.seccion) Seccion
	,Days_Connected.usuario CodSocia
	,det_dias_paises.diashabiles CantDiashabiles
	,TRUNC(det_dias_paises.diashabiles::DECIMAL(10,2) * Days_Connected.factor) CantDiasRequeridos
	,Days_Connected.Days_ConnectedHome CantDiasapp
	,Days_Connected.Days_ConnectedRdD CantDiasrdd
	,Days_Connected.Days_ConnectedRdV CantDiasrdv
	,CASE WHEN TRUNC(det_dias_paises.diashabiles::DECIMAL(10,2) * Days_Connected.factor) <= Days_Connected.Days_ConnectedHome THEN 1 ELSE 0 END FlagUsaApp
	,CASE WHEN TRUNC(det_dias_paises.diashabiles::DECIMAL(10,2) * Days_Connected.factor) <= Days_Connected.Days_ConnectedRdD THEN 1 ELSE 0 END FlagUsaRdD
	,CASE WHEN TRUNC(det_dias_paises.diashabiles::DECIMAL(10,2) * Days_Connected.factor) <= Days_Connected.Days_ConnectedRdV THEN 1 ELSE 0 END FlagUsaRdV
FROM
	#Days_Connected Days_Connected
INNER JOIN
	dom_digital.det_dias_paises det_dias_paises
ON
	det_dias_paises.aniocampana = Days_Connected.campania
	AND det_dias_paises.codpais = Days_Connected.pais
	AND det_dias_paises.seccion = (RTRIM(Days_Connected.zona) || RTRIM(Days_Connected.seccion))
ORDER BY 1,2,3,4,5,6;