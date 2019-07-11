drop table IF EXISTS Temp_data;

select a.CodPais,
CodRegion,CodZona,CodSeccion, a.AnioCampana,A.CodEbelista,A.CodStatus,A.CodComportamientoRolling as SegmentoRolling,
Constancia,
CASE when C.SaldoBanco > 0 then 1 ELSE 0 END as FlagDeuda,
COALESCE(C.SaldoBanco,0) as SaldoBanco,FlagPasoPedido as Pedido,COALESCE(D.FlagPasoPedidoWeb,0) as CompraGana, Case When d.EstadoSuscripcionRevistaDigital = '1' then 1 else 0 end as SuscritaGana
INTO TEMPORARY Temp_data
from fnc_analitico.dwh_fstaebecam A
inner join fnc_analitico.dwh_debelista B On A.codpais = B.codpais and A.codebelista = B.codebelista 
left join fnc_analitico.dwh_dnrofactura C on A.codpais = C.codpais and A.codebelista = C.codebelista and A.AnioCampana = C.AnioCampana
left join fnc_analitico.dwh_fcompdigcon D on A.codpais = D.codpais and A.codebelista = D.codebelista and A.aniocampana = D.AnioCampanaWeb
left join fnc_analitico.dwh_dgeografiacampana H on A.codpais = H.codpais and A.codterritorio = H.codterritorio and A.aniocampana = H.aniocampana
WHERE A.codpais != 'PR' and A.aniocampana in (SELECT TOP 2 distinct aniocampanaweb from fnc_analitico.dwh_fcompdigcon order by 1 desc)
GROUP BY A.codpais,CodRegion,CodZona,CodSeccion,A.AnioCampana,A.CodEbelista,A.CodStatus,A.CodComportamientoRolling,
Constancia,C.SaldoBanco,FlagPasoPedido,D.FlagPasoPedidoWeb,
d.EstadoSuscripcionRevistaDigital
ORDER BY 2,5,6;

UPDATE Temp_data 
SET constancia = NULL 
WHERE constancia = '';

DROP TABLE IF EXISTS temp_flagdigital;

SELECT A.codpais,A.aniocampana,A.CodEbelista,
CASE WHEN A.codtipooferta >= '200' then 1 else 0 end FlagPedidoDigital 
INTO TEMPORARY temp_flagdigital
FROM fnc_analitico.dwh_fvtaproebecam A
LEFT JOIN fnc_analitico.dwh_dtipooferta B on A.codtipooferta = B.codtipooferta
INNER JOIN fnc_analitico.dwh_debelista D on A.codebelista = D.codebelista 
WHERE A.codpais != 'PR' and A.aniocampana in (SELECT TOP 2 distinct aniocampanaweb from fnc_analitico.dwh_fcompdigcon order by 1 desc)
AND A.RealVtaMNNeto > 0
AND A.CodTipoDocumento = 'N'
GROUP BY A.codpais,A.aniocampana,A.CodEbelista, A.codtipooferta
ORDER BY 3;

DROP TABLE IF EXISTS Temp_FlagPedidoDigital;

SELECT Codpais, Aniocampana, CodEbelista,Max(FlagpedidoDigital) as FlagPedidoDigital
INTO TEMPORARY Temp_FlagPedidoDigital
FROM temp_flagdigital
GROUP BY codpais,aniocampana,CodEbelista;

DELETE FROM dom_digital.det_consultora_sb;

INSERT INTO dom_digital.det_consultora_sb
SELECT A.CodPais,A.CodRegion,A.CodZona,A.CodSeccion,A.AnioCampana,A.CodEbelista,C.CodStatus_sicc,A.SegmentoRolling,COALESCE(A.Constancia,'Sin S') as Constancia,
MAX(FlagDeuda) AS FlagDeuda,SUM(SaldoBanco) AS SaldoBanco,MAX(CASE Pedido WHEN 'true' THEN 1 ELSE 0 END) AS Pedido,MAX(COALESCE(FlagPedidoDigital,0)) AS FlagPedidoDigital,MAX(SuscritaGana) AS SuscritaGana
FROM Temp_data A
LEFT JOIN Temp_FlagPedidoDigital B on A.CodPais = B.codpais 
AND A.AnioCampana = B.AnioCampana and A.CodEbelista = B.CodEbelista
LEFT JOIN fnc_analitico.dwh_dstatus C on A.codpais = C.codpais and A.codstatus = C.codstatus
WHERE A.CODSTATUS in (1,2,3,4,7,8)
GROUP BY A.CodPais,A.CodRegion,A.CodZona,A.CodSeccion,A.AnioCampana,A.CodEbelista,C.CodStatus_sicc,A.SegmentoRolling, Constancia
ORDER BY 5,6;

/*Parte 2 del Query para Incluir PR*/

drop table IF EXISTS Temp_data;

select a.CodPais,
CodRegion,CodZona,CodSeccion, a.AnioCampana,A.CodEbelista,A.CodStatus,A.CodComportamientoRolling as SegmentoRolling,
Constancia,
CASE when C.SaldoBanco > 0 then 1 ELSE 0 END as FlagDeuda,
COALESCE(C.SaldoBanco,0) as SaldoBanco,FlagPasoPedido as Pedido,COALESCE(D.FlagPasoPedidoWeb,0) as CompraGana, Case When d.EstadoSuscripcionRevistaDigital = '1' then 1 else 0 end as SuscritaGana
INTO TEMPORARY Temp_data
from fnc_analitico.dwh_fstaebecam A
inner join fnc_analitico.dwh_debelista B On A.codpais = B.codpais and A.codebelista = B.codebelista
left join fnc_analitico.dwh_dnrofactura C on A.codpais = C.codpais and A.codebelista = C.codebelista and A.AnioCampana = C.AnioCampana
left join fnc_analitico.dwh_fcompdigcon D on A.codpais = D.codpais and A.codebelista = D.codebelista and A.aniocampana = D.AnioCampanaWeb
left join fnc_analitico.dwh_dgeografiacampana H on A.codpais = H.codpais and A.codterritorio = H.codterritorio and A.aniocampana = H.aniocampana
WHERE A.Codpais = 'PR' and A.aniocampana in (SELECT TOP 2 distinct aniocampanaweb from fnc_analitico.dwh_fcompdigcon where codpais = 'PR'order by 1 desc)
GROUP BY A.codpais,CodRegion,CodZona,CodSeccion,A.AnioCampana,A.CodEbelista,A.CodStatus,A.CodComportamientoRolling,
Constancia,C.SaldoBanco,FlagPasoPedido,D.FlagPasoPedidoWeb,
d.EstadoSuscripcionRevistaDigital
ORDER BY 2,5,6;

UPDATE Temp_data
SET constancia = NULL
WHERE constancia = '';

DROP TABLE IF EXISTS temp_flagdigital;

SELECT A.codpais,A.aniocampana,A.CodEbelista,
CASE WHEN A.codtipooferta >= '200' then 1 else 0 end FlagPedidoDigital
INTO TEMPORARY temp_flagdigital
FROM fnc_analitico.dwh_fvtaproebecam A
LEFT JOIN fnc_analitico.dwh_dtipooferta B on A.codtipooferta = B.codtipooferta
INNER JOIN fnc_analitico.dwh_debelista D on A.codebelista = D.codebelista
WHERE A.codpais = 'PR' and A.aniocampana in (SELECT TOP 2 distinct aniocampanaweb from fnc_analitico.dwh_fcompdigcon where codpais = 'PR'order by 1 desc)
AND A.RealVtaMNNeto > 0
AND A.CodTipoDocumento = 'N'
GROUP BY A.codpais,A.aniocampana,A.CodEbelista, A.codtipooferta
ORDER BY 3;

DROP TABLE IF EXISTS Temp_FlagPedidoDigital;

SELECT Codpais, Aniocampana, CodEbelista,Max(FlagpedidoDigital) as FlagPedidoDigital
INTO TEMPORARY Temp_FlagPedidoDigital
FROM temp_flagdigital
GROUP BY Codpais,aniocampana,CodEbelista;

INSERT INTO dom_digital.det_consultora_sb
SELECT A.CodPais,A.CodRegion,A.CodZona,A.CodSeccion,A.AnioCampana,A.CodEbelista,C.CodStatus_sicc,A.SegmentoRolling,COALESCE(A.Constancia,'Sin S') as Constancia,
MAX(FlagDeuda) AS FlagDeuda,SUM(SaldoBanco) AS SaldoBanco,MAX(CASE Pedido WHEN 'true' THEN 1 ELSE 0 END) AS Pedido,MAX(COALESCE(FlagPedidoDigital,0)) AS FlagPedidoDigital,MAX(SuscritaGana) AS SuscritaGana
FROM Temp_data A
LEFT JOIN Temp_FlagPedidoDigital B on A.CodPais = B.codpais
AND A.AnioCampana = B.AnioCampana and A.CodEbelista = B.CodEbelista
LEFT JOIN fnc_analitico.dwh_dstatus C on A.codpais = C.codpais and A.codstatus = C.codstatus
WHERE A.CODSTATUS in (1,2,3,4,7,8)
GROUP BY A.CodPais,A.CodRegion,A.CodZona,A.CodSeccion,A.AnioCampana,A.CodEbelista,C.CodStatus_sicc,A.SegmentoRolling, Constancia
ORDER BY 5,6;


