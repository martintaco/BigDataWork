/*Query para Semana consultora*/

DROP TABLE IF EXISTS #Table1;

Select pais_ingreso,campania, Left((created_at::DATE),10) as fecha,usuario, 1 as FlagLog,aplicacion as origenlog ,opcion_pantalla as TipoLog
Into #Table1
from dom_digital.log_usabilidad 
where pais_ingreso <> 'PR' and campania in (select top 2 distinct aniocampanaweb from fnc_analitico.dwh_flogingresoportal order by 1 desc) and aplicacion = 'APPCONSULTORAS' and opcion_pantalla ='HOME' and usuario is not null
Union
Select pais_ingreso,campania,Left((created_at::DATE),10) as fecha,usuario, 1 as FlagLog,aplicacion as origenlog ,opcion_pantalla as TipoLog
from dom_digital.log_usabilidad 
where pais_ingreso = 'PR' and campania in (select top 2 distinct aniocampanaweb from fnc_analitico.dwh_flogingresoportal where codpais = 'PR' order by 1 desc) and aplicacion = 'APPCONSULTORAS' and opcion_pantalla ='HOME' and usuario is not null 
group by pais_ingreso,campania,created_at,usuario,aplicacion,opcion_pantalla order by 1,2;

Insert INTO #Table1
Select Codpais,aniocampanaweb,Left((fechahora::DATE),10) as fechaLogeo,codebelista, 1 as FLAGLOG, 'PORTALWEB' as OrigenLog, 'HOME' as TipoLog
from fnc_analitico.dwh_flogingresoportal where codpais <> 'PR' and aniocampanaweb in (select top 2 distinct aniocampanaweb from fnc_analitico.dwh_flogingresoportal order by 1 desc) and codebelista is not null
UNION
Select Codpais,aniocampanaweb,Left((fechahora::DATE),10) as fechaLogeo,codebelista, 1 as FLAGLOG, 'PORTALWEB' as OrigenLog, 'HOME' as TipoLog
from fnc_analitico.dwh_flogingresoportal where codpais = 'PR' and aniocampanaweb in (select top 2 distinct aniocampanaweb from fnc_analitico.dwh_flogingresoportal where codpais = 'PR' order by 1 desc) and codebelista is not null
group by codpais,aniocampanaweb,fechalogeo,codebelista order by 1,2;

