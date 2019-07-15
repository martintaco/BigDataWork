-- De donde obtendriamos las campañas cerradas con sus paises? de perfiles? uhm

drop table if exists #Tabla;
drop table if exists #Tabla2;
drop table if exists #Tabla3;
drop table if exists #Tabla4;
drop table if exists #Tabla5;
drop table if exists #Venta;
drop table if exists var_parametros;

Create TEMPORARY table var_parametros (CodPais char(2), aniocampanaini char(6), aniocampanafin char(6));

insert into var_parametros  (CodPais, aniocampanafin)
select codpais, max(aniocampanaproceso) as aniocampana from dom_perfiles.mdl_perfilinput
GROUP by codpais;

update var_parametros
set AnioCampanaIni = f_calculaaniocampana(CodPais, AnioCampanafin, -2);

select a.CodPais, a.AnioCampana,CodEbelista,FlagActiva::int,DescripcionRolling, CodTerritorio
into #Tabla
from fnc_analitico.dwh_fstaebecam a
inner join var_parametros par on a.codpais = par.codpais and a.aniocampana between par.aniocampanaini and par.aniocampanafin
where FlagActiva::int = 1;

select A.CodPais,
	b.Aniocampana,
	B.CodTerritorio,
	C.CodEbelista,
	B.DescripcionRolling,
    sum (A.FlagActiva::int) as Activa,
    sum (A.FlagPasoPedido::int) as Pedidos,
    sum (A.FlagIPUnicoZona::int) as IPUnico,
    sum (case when A.FlagCompraOPT::int+A.FlagCompraODD::int+A.FlagCompraOF::int+A.FlagCompraFDC::int+A.FlagCompraSR::int >0 then 1 else 0 end) as ComprasDigitales
into #Tabla2
from fnc_analitico.dwh_fstaebecam as A
inner join var_parametros par on a.codpais = par.codpais and a.aniocampana BETWEEN par.aniocampanaini and par.aniocampanafin
inner Join #Tabla as B
	on A.CodPais = B.CodPais
	and A.CodEbelista = B.CodEbelista
inner Join fnc_analitico.dwh_debelista as C on A.CodPais = C.CodPais and A.CodEbelista = C.CodEbelista
group by A.CodPais, b.Aniocampana, B.CodTerritorio,C.CodEbelista,B.DescripcionRolling;

select E.CodPais,
	E.AnioCampana,
	E.CodEbelista,
	D.CodSeccion,
	D.CodZona,
	D.DesRegion,
	E.DescripcionRolling,
	E.Activa,
	E.Pedidos,
	E.IPUnico,
	E.ComprasDigitales
into #Tabla3
from #Tabla2 as E
left join fnc_analitico.dwh_dgeografiacampana as D
	on E.AnioCampana = D.AnioCampana
	and E.Codpais = D.Codpais
	and E.CodTerritorio = D.CodTerritorio;

select G.CodPais,
	G.CodEbelista,
    sum(G.RealVtaMNNeto) as RealVtaMNNeto,
    sum(G.RealVtaMNNeto/G.RealTCPromedio) as RealVtaUSDNeto
into #Venta

from fnc_analitico.dwh_fvtaproebecam as G
inner join var_parametros par on g.codpais = par.codpais and g.aniocampana BETWEEN par.aniocampanaini and par.aniocampanafin
Inner Join fnc_analitico.dwh_dtipooferta as H
	on G.CodTipoOferta=H.CodTipoOferta
	and G.CodPais=H.CodPais
where H.CodTipoProfit ='01'
and G.RealVtaMNNeto>0
and G.aniocampana=G.aniocampanaref
group by G.CodPais, G.CodEbelista;

select I.*,
	J.RealVtaMNNeto,
	J.RealVtaUSDNeto
into #Tabla4
from #Tabla3 as I
Left Join #Venta as J
	on I.CodPais = J.CodPais
	and I.CodEbelista = J.Codebelista;

select K.CodPais,k.aniocampana,K.Codebelista,
case when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) = 0 then '1'
       when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) > 0 and cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) < 0.5 then '2'
        when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) >= 0.5 and cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) < 1 then '3'
       when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) = 1 and K.ComprasDigitales = 0 then '4'
       when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) = 1 and cast(K.ComprasDigitales as decimal)/ cast(K.Pedidos as decimal) <= 0.5 then '5'
       when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) = 1 and cast(K.ComprasDigitales as decimal)/ cast(K.Pedidos as decimal) > 0.5 then '6' else NULL end as CodSegmentacionDigital,
case when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) = 0 then 'No Digital'
       when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) > 0 and cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) < 0.5 then 'Digital Esporadica'
        when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) >= 0.5 and cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) < 1 then 'Potencial Digital'
       when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) = 1 and K.ComprasDigitales = 0 then 'No Compradora'
       when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) = 1 and cast(K.ComprasDigitales as decimal)/ cast(K.Pedidos as decimal) <= 0.5 then 'Compradora Itinerante'
       when cast(K.IPUnico as decimal)/cast(K.Pedidos as decimal) = 1 and cast(K.ComprasDigitales as decimal)/ cast(K.Pedidos as decimal) > 0.5 then 'Compradora Recurrente' else 'NULL' end as SegmentacionDigital
into #Tabla5
from #Tabla4 as K
where k.Pedidos <> 0;


DELETE FROM fnc_analitico.dwh_segmentacion where aniocampana >= case codpais when 'PR' then (select min(AnioCampanaIni) from var_parametros where codpais = 'PR')
																					else (select min(aniocampanaini) from var_parametros where codpais != 'PR') end;

INSERT INTO fnc_analitico.dwh_segmentacion
SELECT * FROM #Tabla5;

unload($$ Select * from #Tabla5  $$)
to 's3://belc-bigdata-domain-dlk-prd/dom-hana/segmentacion_digital/cierre'
access_key_id 'AKIAJK6A3CSH7NDH2TWA'
secret_access_key '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'
delimiter '\t'
NULL AS 'NULL'
ALLOWOVERWRITE
ADDQUOTES
PARALLEL OFF
;
