drop table IF EXISTS Temp_data;

select a.CodPais,
CodRegion,CodZona,CodSeccion, a.AnioCampana,A.CodEbelista,A.CodStatus,A.CodComportamientoRolling as SegmentoRolling,
Constancia,
CASE when C.SaldoBanco > 0 then 1 ELSE 0 END as FlagDeuda,
COALESCE(C.SaldoBanco,0) as SaldoBanco,FlagPasoPedido as Pedido,COALESCE(A.FlagPasoPedidoWeb,0) as CompraGana,A.flagofertadigital as Flagpedidodigital, Case When A.flagexperienciaganamas = '1' then 1 else 0 end as SuscritaGana
INTO TEMPORARY Temp_data
from fnc_analitico.dwh_fstaebecam A
inner join fnc_analitico.dwh_debelista B On A.codpais = B.codpais and A.codebelista = B.codebelista 
left join fnc_analitico.dwh_dnrofactura C on A.codpais = C.codpais and A.codebelista = C.codebelista and A.AnioCampana = C.AnioCampana
left join fnc_analitico.dwh_dgeografiacampana H on A.codpais = H.codpais and A.codterritorio = H.codterritorio and A.aniocampana = H.aniocampana
WHERE A.codpais != 'PR' and A.aniocampana in (SELECT TOP 3 distinct aniocampana from fnc_analitico.dwh_fstaebecam order by 1 desc)
GROUP BY A.codpais,CodRegion,CodZona,CodSeccion,A.AnioCampana,A.CodEbelista,A.CodStatus,A.CodComportamientoRolling,
Constancia,C.SaldoBanco,FlagPasoPedido,A.FlagPasoPedidoWeb,A.flagofertadigital,
A.flagexperienciaganamas
ORDER BY 2,5,6;

UPDATE Temp_data 
SET constancia = NULL 
WHERE constancia = '';

UPDATE Temp_data 
SET Pedido = '0' 
WHERE Pedido is null;

UPDATE Temp_data 
SET SegmentoRolling = '0' 
WHERE SegmentoRolling is null;

DELETE FROM dom_digital.det_consultora_sb;

INSERT INTO dom_digital.det_consultora_sb
SELECT A.CodPais,A.CodRegion,A.CodZona,A.CodSeccion,A.AnioCampana,A.CodEbelista,C.CodStatus_sicc,A.SegmentoRolling,COALESCE(A.Constancia,'Sin S') as Constancia,
MAX(FlagDeuda) AS FlagDeuda,SUM(SaldoBanco) AS SaldoBanco,MAX(Pedido) AS Pedido,MAX(COALESCE(FlagPedidoDigital,0)) AS FlagPedidoDigital,MAX(SuscritaGana) AS SuscritaGana
FROM Temp_data A
LEFT JOIN fnc_analitico.dwh_dstatus C on A.codpais = C.codpais and A.codstatus = C.codstatus
WHERE c.codstatus_sicc in (2,3,4,6,8)
GROUP BY A.CodPais,A.CodRegion,A.CodZona,A.CodSeccion,A.AnioCampana,A.CodEbelista,C.CodStatus_sicc,A.SegmentoRolling, Constancia
ORDER BY 5,6;

/*Parte 2 del Query para Incluir PR*/

drop table IF EXISTS Temp_data;

select a.CodPais,
CodRegion,CodZona,CodSeccion, a.AnioCampana,A.CodEbelista,A.CodStatus,A.CodComportamientoRolling as SegmentoRolling,
Constancia,
CASE when C.SaldoBanco > 0 then 1 ELSE 0 END as FlagDeuda,
COALESCE(C.SaldoBanco,0) as SaldoBanco,FlagPasoPedido as Pedido,COALESCE(A.FlagPasoPedidoWeb,0) as CompraGana,A.flagofertadigital as Flagpedidodigital, Case When A.flagexperienciaganamas = '1' then 1 else 0 end as SuscritaGana
INTO TEMPORARY Temp_data
from fnc_analitico.dwh_fstaebecam A
inner join fnc_analitico.dwh_debelista B On A.codpais = B.codpais and A.codebelista = B.codebelista
left join fnc_analitico.dwh_dnrofactura C on A.codpais = C.codpais and A.codebelista = C.codebelista and A.AnioCampana = C.AnioCampana
left join fnc_analitico.dwh_dgeografiacampana H on A.codpais = H.codpais and A.codterritorio = H.codterritorio and A.aniocampana = H.aniocampana
WHERE A.Codpais = 'PR' and A.aniocampana in (SELECT TOP 3 distinct aniocampana from fnc_analitico.dwh_fstaebecam where codpais = 'PR' order by 1 desc)
GROUP BY A.codpais,CodRegion,CodZona,CodSeccion,A.AnioCampana,A.CodEbelista,A.CodStatus,A.CodComportamientoRolling,
Constancia,C.SaldoBanco,FlagPasoPedido,A.FlagPasoPedidoWeb,A.flagofertadigital,
A.flagexperienciaganamas
ORDER BY 2,5,6;

UPDATE Temp_data
SET constancia = NULL
WHERE constancia = '';

UPDATE Temp_data 
SET Pedido = '0' 
WHERE Pedido is null;

UPDATE Temp_data 
SET SegmentoRolling = '0' 
WHERE SegmentoRolling is null;

INSERT INTO dom_digital.det_consultora_sb
SELECT A.CodPais,A.CodRegion,A.CodZona,A.CodSeccion,A.AnioCampana,A.CodEbelista,C.CodStatus_sicc,A.SegmentoRolling,COALESCE(A.Constancia,'Sin S') as Constancia,
MAX(FlagDeuda) AS FlagDeuda,SUM(SaldoBanco) AS SaldoBanco,MAX(Pedido) AS Pedido,MAX(COALESCE(FlagPedidoDigital,0)) AS FlagPedidoDigital,MAX(SuscritaGana) AS SuscritaGana
FROM Temp_data A
LEFT JOIN fnc_analitico.dwh_dstatus C on A.codpais = C.codpais and A.codstatus = C.codstatus
WHERE c.codstatus_sicc in (2,3,4,6,8)
GROUP BY A.CodPais,A.CodRegion,A.CodZona,A.CodSeccion,A.AnioCampana,A.CodEbelista,C.CodStatus_sicc,A.SegmentoRolling, Constancia
ORDER BY 5,6;

update dom_digital.det_consultora_sb
set codseccion = codzona||codseccion
where length(codseccion) = 1;
