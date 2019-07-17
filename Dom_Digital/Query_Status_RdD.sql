/*Status RdD*/

DROP TABLE IF EXISTS #RANK_CAMPANIA_PAIS;
DROP TABLE IF EXISTS #CAMPANIA_PAIS;
DROP TABLE IF EXISTS #RESULTADO;
DROP TABLE IF EXISTS #Table1;

CREATE TEMP TABLE #RANK_CAMPANIA_PAIS AS
SELECT RANK() OVER(PARTITION BY Det_CabPlanRutaRdD.Pais ORDER BY Det_CabPlanRutaRdD.Campania DESC) AS Rank#
,Det_CabPlanRutaRdD.Campania
,Det_CabPlanRutaRdD.Pais
FROM Dom_Digital.Det_CabPlanRutaRdD Det_CabPlanRutaRdD
GROUP BY Det_CabPlanRutaRdD.Campania, Det_CabPlanRutaRdD.Pais;

CREATE TEMP TABLE #CAMPANIA_PAIS AS
SELECT RANK_CAMPANIA_PAIS.Campania,RANK_CAMPANIA_PAIS.Pais
FROM #RANK_CAMPANIA_PAIS RANK_CAMPANIA_PAIS
WHERE RANK_CAMPANIA_PAIS.Rank# <= 2;

CREATE TEMP TABLE #RESULTADO AS
SELECT
Det_CabPlanRutaRdD.Pais
,Det_CabPlanRutaRdD.Campania
,Det_PlanRutaRdD.codigoconsultora
,CASE WHEN Det_PlanRutaRdD.fechaplanificada IS NOT NULL
		AND Det_PlanRutaRdD.fechareprogramacion IS NULL
		AND Det_PlanRutaRdD.fechavisita IS NULL
	THEN 'Planificada'
WHEN Det_PlanRutaRdD.fechaplanificada IS NOT NULL
		AND Det_PlanRutaRdD.fechareprogramacion IS NOT NULL
		AND Det_PlanRutaRdD.fechavisita IS NULL
	THEN 'Replanificada'
WHEN Det_PlanRutaRdD.fechavisita IS NOT NULL
	THEN 'Visitada'
	ELSE '' END AS "StatusVisita"
FROM Dom_Digital.Det_CabPlanRutaRdD Det_CabPlanRutaRdD
INNER JOIN Dom_Digital.Det_PlanRutaRdD Det_PlanRutaRdD
ON Det_CabPlanRutaRdD.Id = Det_PlanRutaRdD.PlanVisitaId
AND Det_CabPlanRutaRdD.Pais = Det_PlanRutaRdD.Pais
INNER JOIN #CAMPANIA_PAIS CAMPANIA_PAIS
ON CAMPANIA_PAIS.Campania = Det_CabPlanRutaRdD.Campania
AND CAMPANIA_PAIS.Pais = Det_CabPlanRutaRdD.Pais
GROUP BY 1,2,3,4;

SELECT
	Pais,Campania,codigoconsultora
	,LISTAGG(StatusVisita,',') WITHIN GROUP (ORDER BY StatusVisita) StatusVisita
into #Table1
from #RESULTADO
GROUP BY Pais,Campania,codigoconsultora
ORDER BY Pais,Campania,codigoconsultora;

unload($$ select * from #Table1 $$)
to 's3://belc-bigdata-domain-dlk-prd/dom-hana/Res_Status_Consultora/Status_'
access_key_id '{ACCESS_KEY}'
secret_access_key '{SECRET_KEY}'
delimiter '\t'
NULL AS 'NULL'
ALLOWOVERWRITE
PARALLEL OFF
ESCAPE
ADDQUOTES
;
