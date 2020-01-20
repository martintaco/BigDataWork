drop table if exists #Activas;
create table #Activas as
select a.aniocampana, a.codpais, codebelista, flagactiva, b.descomportamiento 
from fnc_analitico.dwh_fstaebecam a
inner join fnc_analitico.dwh_dcomportamientorolling b on a.codcomportamientorolling = b.codcomportamiento
inner join fnc_analitico.dwh_dstatus c on a.codpais = c.codpais and a.codstatus = c.codstatus
	where a.codpais in ('CO','PE','MX','EC','CL','BO','CR','GT','SV','PA','DO')
	and flagactiva = 1
	and a.aniocampana = '202001';
	--and c.codstatuscorp = 1
	--and b.descomportamiento = 'Nuevas';
--,b.descomportamient
--select * from #Activas

drop table if exists #campanasvalidas;
select distinct codpais, aniocampana into #campanasvalidas from fnc_analitico.dwh_fstaebecam
where aniocampana between f_calculaaniocampana('CO','202001',-2) and '202001';

	
drop table if exists #UUVta;
create table #UUVta as
select A.codpais, B.Aniocampana, A.codebelista,
	(case when D.desmarca = 'ESIKA' then 'EK'
	 when D.desmarca = 'LBEL' then 'LB'
	 when D.desmarca = 'CYZONE' then 'CZ' else 'OM' end) as MarcaFinal ,
	(case when D.Descategoria = 'TRATAMIENTO CORPORAL' then 'TC' 
	 when D.Descategoria = 'FRAGANCIAS' then 'FR' 			
	 when D.Descategoria = 'TRATAMIENTO FACIAL' then 'TF'   
	 when D.Descategoria = 'MAQUILLAJE' then 'MQ'           
	 when D.Descategoria = 'CUIDADO PERSONAL' then 'CP' 	
	 when D.Descategoria = 'ACCESORIOS COSMETICOS' then 'MQ' else 'NC' end) as CategoriaFinal,
sum (B.realuuvendidas) as totUnidades, 
sum(B.realvtamnneto) as totVentaML, 
sum (B.realvtamnneto/B.realtcpromedio) as totVentaUSD
from fnc_analitico.dwh_fvtaproebecam as B
	inner join #Activas as A on A.Codpais = B.codpais and A.codebelista = B.codebelista
	inner join fnc_analitico.dwh_dtipooferta as C on B.Codpais = C.codpais and B.codtipooferta = C.codtipooferta
	inner join fnc_analitico.dwh_dproducto as D on B.codsap = D.Codsap
	inner join #campanasvalidas e on b.codpais = e.codpais and b.aniocampana = e.aniocampana
where B.codpais in ('CO','PE','MX','EC','CL','BO','CR','GT','SV','PA','DO')
--and b.aniocampana between '201914' and '201916'
and C.codtipoprofit = '01' and B.realvtamnneto > 0
group by A.codpais, A.codebelista, B.Aniocampana, MarcaFinal , CategoriaFinal;


drop table if exists #marcaCatPrioriza;
create temporary table #marcaCatPrioriza
(marca varchar(4),
categoria varchar(4),
marcacategoria varchar(4),
prioridad int
);

insert into #marcaCatPrioriza values ('EK','FR','EKFR',1);
insert into #marcaCatPrioriza values ('EK','MQ','EKMQ',2);
insert into #marcaCatPrioriza values ('CZ','MQ','CZMQ',3);
insert into #marcaCatPrioriza values ('CZ','FR','CZFR',4);
insert into #marcaCatPrioriza values ('LB','MQ','LBMQ',5);
insert into #marcaCatPrioriza values ('LB','FR','LBFR',6);
insert into #marcaCatPrioriza values ('EK','TF','EKTF',7);
insert into #marcaCatPrioriza values ('LB','TF','LBTF',8);
insert into #marcaCatPrioriza values ('CZ','TF','CZTF',9);


drop table if exists #Segmentado;
select 
aniocampana, 
codpais, codebelista, 
	sum (totventaUSD) as ventaneta,
	case when ventaneta >0 then 1 else 0 end as Flagpasopedido,
	sum (totventaML) as ventanetaML,
--unidades	
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'FR' then totunidades else 0 end) as UUEKFR,
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'MQ' then totunidades else 0 end) as UUEKMQ,
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'TF' then totunidades else 0 end) as UUEKTF,
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'TC' then totunidades else 0 end) as UUEKTC,
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'CP' then totunidades else 0 end) as UUEKCP,
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'NC' then totunidades else 0 end) as UUEKNC,	
		
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'FR' then totunidades else 0 end) as UULBFR,
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'MQ' then totunidades else 0 end) as UULBMQ,
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'TF' then totunidades else 0 end) as UULBTF,
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'TC' then totunidades else 0 end) as UULBTC,
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'CP' then totunidades else 0 end) as UULBCP,
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'NC' then totunidades else 0 end) as UULBNC,

	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'FR' then totunidades else 0 end) as UUCZFR,
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'MQ' then totunidades else 0 end) as UUCZMQ,
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'TF' then totunidades else 0 end) as UUCZTF,
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'TC' then totunidades else 0 end) as UUCZTC,
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'CP' then totunidades else 0 end) as UUCZCP,
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'NC' then totunidades else 0 end) as UUCZNC,

--venta	
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'FR' then totVentaUSD else 0 end) as VtaEKFR,
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'MQ' then totVentaUSD else 0 end) as VtaEKMQ,
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'TF' then totVentaUSD else 0 end) as VtaEKTF,
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'TC' then totVentaUSD else 0 end) as VtaEKTC,
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'CP' then totVentaUSD else 0 end) as VtaEKCP,
	sum (case when MarcaFinal = 'EK' and CategoriaFinal = 'NC' then totVentaUSD else 0 end) as VtaEKNC,	
		
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'FR' then totVentaUSD else 0 end) as VtaLBFR,
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'MQ' then totVentaUSD else 0 end) as VtaLBMQ,
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'TF' then totVentaUSD else 0 end) as VtaLBTF,
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'TC' then totVentaUSD else 0 end) as VtaLBTC,
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'CP' then totVentaUSD else 0 end) as VtaLBCP,
	sum (case when MarcaFinal = 'LB' and CategoriaFinal = 'NC' then totVentaUSD else 0 end) as VtaLBNC,
	
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'FR' then totVentaUSD else 0 end) as VtaCZFR,
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'MQ' then totVentaUSD else 0 end) as VtaCZMQ,
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'TF' then totVentaUSD else 0 end) as VtaCZTF,
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'TC' then totVentaUSD else 0 end) as VtaCZTC,
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'CP' then totVentaUSD else 0 end) as VtaCZCP,
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'NC' then totVentaUSD else 0 end) as VtaCZNC

	into #Segmentado from #UUVta
	group by 
	aniocampana, 
	codpais, codebelista;


drop table if exists #total;
select 
codpais, codebelista, 
	sum (ventaneta) as VtaUSD,
	sum (Flagpasopedido) as pedidos,
	sum (ventanetaML) as VtaML,
	
	sum (UUEKFR) as UnEKFR, sum(UUEKMQ) as UnEKMQ, sum(UUEKTF) as UnEKTF, sum(UUEKTC) as UnEKTC, sum(UUEKCP) as UnEKCP, sum(UUEKNC) as UnEKNC,	
	sum (UULBFR) as UnLBFR, sum(UULBMQ) as UnLBMQ, sum(UULBTF) as UnLBTF, sum(UULBTC) as UnLBTC, sum(UULBCP) as UnLBCP, sum(UULBNC) as UnLBNC,	
	sum (UUCZFR) as UnCZFR, sum(UUCZMQ) as UnCZMQ, sum(UUCZTF) as UnCZTF, sum(UUCZTC) as UnCZTC, sum(UUCZCP) as UnCZCP, sum(UUCZNC) as UnCZNC,

	sum (VtaEKFR) as VtEKFR, sum(VtaEKMQ) as VtEKMQ, sum(VtaEKTF) as VtEKTF, sum(VtaEKTC) as VtEKTC, sum(VtaEKCP) as VtEKCP, sum(VtaEKNC) as VtEKNC,	
	sum (VtaLBFR) as VtLBFR, sum(VtaLBMQ) as VtLBMQ, sum(VtaLBTF) as VtLBTF, sum(VtaLBTC) as VtLBTC, sum(VtaLBCP) as VtLBCP, sum(VtaLBNC) as VtLBNC,	
	sum (VtaCZFR) as VtCZFR, sum(VtaCZMQ) as VtCZMQ, sum(VtaCZTF) as VtCZTF, sum(VtaCZTC) as VtCZTC, sum(VtaCZCP) as VtCZCP, sum(VtaCZNC) as VtCZNC
into #total from #Segmentado
	group by  
	codpais, codebelista;

drop table if exists #SegmentoFinal;
select codpais, codebelista, VtaUSD, Pedidos, VtaML,
	(case when sum(UnEKFR) + sum(UnEKMQ) + sum(UnEKTF) > 0 then 'EK' else 'NN' end) as FlagEKfinal,
	(case when sum(UnLBFR) + sum(UnLBMQ) + sum(UnLBTF) > 0 then 'LB' else 'NN' end) as FlagLBfinal,
	(case when sum(UnCZFR) + sum(UnCZMQ) + sum(UnCZTF) > 0 then 'CZ' else 'NN' end) as FlagCZfinal,
	
	(case when sum(UnEKFR) + sum(UnLBFR) + sum(UnCZFR) > 0 then 'FR' else 'NN' end) as FlagFRFinal,
	(case when sum(UnEKMQ) + sum(UnLBMQ) + sum(UnCZMQ) > 0 then 'MQ' else 'NN' end) as FlagMQfinal,
	(case when sum(UnEKTF) + sum(UnLBTF) + sum(UnCZTF) > 0 then 'TF' else 'NN' end) as FlagTFfinal,
	
	FlagEKFinal+FlagLBfinal+FlagCZfinal+FlagFRFinal+FlagMQfinal+FlagTFfinal as codigo,
	
	(case when codigo like 'EKLBCZFRMQTF' then 'Multimarca' else 'Resto' end) as FlagMulti,
(case when sum(unEKFR) > 0 then 1 else 0 end) as EKFR,
(case when sum(unEKMQ) > 0 then 1 else 0 end) as EKMQ,
(case when sum(unEKTF) > 0 then 1 else 0 end) as EKTF,
(case when sum(unCZFR) > 0 then 1 else 0 end) as CZFR,
(case when sum(unCZMQ) > 0 then 1 else 0 end) as CZMQ,
(case when sum(unCZTF) > 0 then 1 else 0 end) as CZTF,
(case when sum(unLBFR) > 0 then 1 else 0 end) as LBFR,
(case when sum(unLBMQ) > 0 then 1 else 0 end) as LBMQ,
(case when sum(unLBTF) > 0 then 1 else 0 end) as LBTF,

	(case when vtausd > 0 then 1 else 0 end) as cuenta
into #SegmentoFinal from #Total
group by codpais, codebelista, VtaUSD, Pedidos, VtaML;

drop table if exists #resumen;
create table #resumen as 
SELECT codpais, codebelista, codigo,
(case when codigo = 'EKLBCZFRMQTF'
THEN 1 ELSE 0 END) AS FlagMultimarcaPrincipal,
(case when codigo = 'EKLBNNFRMQTF'
OR codigo = 'EKNNCZFRMQTF'
OR codigo = 'EKNNNNFRMQTF'
OR codigo = 'NNLBCZFRMQTF'
OR codigo = 'NNLBNNFRMQTF'
OR codigo = 'NNNNCZFRMQTF'
OR codigo = 'NNNNNNFRMQTF'
THEN 1 ELSE 0 END) AS FlagMulticategoria,
(case when codigo = 'EKLBCZNNNNNN' 
OR codigo = 'EKLBCZFRMQNN'
OR codigo = 'EKLBCZFRNNNN'
OR codigo = 'EKLBCZNNMQTF'
OR codigo = 'EKLBCZNNMQNN'
OR codigo = 'EKLBCZFRNNTF'
OR codigo = 'EKLBCZNNNNTF'
THEN 1 ELSE 0 END) AS FlagMultimarca
from #SegmentoFinal;

drop table if exists #baseideal;
create table #baseideal as
select t.codpais, t.codebelista, m.marcacategoria, m.marca, m.categoria
from #total t
cross join #marcacatprioriza m;

drop table if exists #basereal0;
create table #basereal0 as
select codpais, codebelista, (marcafinal + categoriafinal) as marcacategoria, marcafinal as marca, categoriafinal as categoria from #uuvta a
group by codpais, codebelista, marcafinal + categoriafinal,marcafinal, categoriafinal;

drop table if exists #basereal;
create table #basereal as
select codpais, codebelista, a.marcacategoria, a.marca, a.categoria from #basereal0 a
inner join #marcacatprioriza b on a.marcacategoria = b.marcacategoria
group by codpais, codebelista, a.marcacategoria ,a.marca, a.categoria;

--caso 1 flagmultimarcaprincipal = 1
drop table if exists #temp1final;
create table #temp1final as 
select t1.codpais, t1.codebelista, m.marcacategoria, m.prioridad From #resumen t1
cross join #marcaCatPrioriza m
where t1.flagmultimarcaprincipal = 1
and m.prioridad = 1;


--caso 4, no es multimarca ni multicategoria
drop table if exists #temp4;
create table #temp4 as
select * from #resumen
where flagmultimarcaprincipal = 0 and flagmulticategoria = 0 and flagmultimarca = 0;


drop table if exists #temp4a;
create table #temp4a as 
select a.codpais, a.codebelista, a.marcacategoria from #baseideal a
inner join #temp4 t on a.codpais = t.codpais and a.codebelista = t.codebelista
left join (select t1.* from #basereal t1
				inner join #marcacatprioriza t2 on t1.marcacategoria = t2.marcacategoria
			) b on a.codpais = b.codpais and a.codebelista = b.codebelista and a.marcacategoria = b.marcacategoria
where b.codpais is null;


drop table if exists #temp4final;
create table #temp4final as 
select b.codpais, b.codebelista, a.marcacategoria, a.prioridad from #marcacatprioriza a
inner join (	select codpais,codebelista, min(prioridad) prioridad from #temp4a a 
				inner join #marcacatprioriza b on a.marcacategoria = b.marcacategoria
				group by codpais,codebelista
			) b on a.prioridad = b.prioridad;

--caso 2, es multimarca, NO es multicategoria
drop table if exists #temp2;
create table #temp2 as
select * from #resumen
where flagmultimarca = 1;

--calcula las categorias validas
drop table if exists #catvalidas;
create table #catvalidas as
select a.codpais, a.codebelista, a.categoria from (
			select a.codpais, a.codebelista, a.categoria from #baseideal a
			inner join #temp2 t on a.codpais = t.codpais and a.codebelista = t.codebelista
			group by a.codpais, a.codebelista, a.categoria
			) a 
			left join #basereal b on a.codpais = b.codpais and a.codebelista = b.codebelista and a.categoria = b.categoria
where b.codpais is null;
			
drop table if exists #temp2a;
create table #temp2a as 
select a.codpais, a.codebelista, a.marcacategoria, a.categoria from (
	select a.codpais, a.codebelista, a.marcacategoria, a.categoria
	from #baseideal a
	inner join #temp2 t on a.codpais = t.codpais and a.codebelista = t.codebelista
	left join ( select t1.* from #basereal t1
				inner join #marcacatprioriza t2 on t1.marcacategoria = t2.marcacategoria
	) b on a.codpais = b.codpais and a.codebelista = b.codebelista and a.marcacategoria = b.marcacategoria
	and a.categoria = b.categoria
	where b.codpais is null
) a
inner join #catvalidas b on a.codpais = b.codpais and a.codebelista = b.codebelista and a.categoria = b.categoria;

drop table if exists #temp2final;
create table #temp2final as
select b.codpais, b.codebelista, a.marcacategoria, a.prioridad from #marcacatprioriza a
inner join (	select codpais,codebelista, min(prioridad) prioridad from #temp2a a
				inner join #marcacatprioriza b on a.marcacategoria = b.marcacategoria
				group by codpais,codebelista
			) b on a.prioridad = b.prioridad;


--caso 3, NO es multimarca, es multicategoria
drop table if exists #temp3;
create table #temp3 as
select * from #resumen
where flagmulticategoria = 1;

--calcula las marcas validas
drop table if exists #marcasvalidas;
create table #marcasvalidas as
select a.codpais, a.codebelista, a.marca from (
			select a.codpais, a.codebelista, a.marca from #baseideal a
			inner join #temp3 t on a.codpais = t.codpais and a.codebelista = t.codebelista
			group by a.codpais, a.codebelista, a.marca
			) a 
			left join #basereal b on a.codpais = b.codpais and a.codebelista = b.codebelista and a.marca = b.marca
where b.codpais is null;

drop table if exists #temp3a;
create table #temp3a as
select a.codpais, a.codebelista, a.marcacategoria, a.marca from (
	select a.codpais, a.codebelista, a.marcacategoria, a.marca
	from #baseideal a
	left join ( select t1.* from #basereal t1
				inner join #marcacatprioriza t2 on t1.marcacategoria = t2.marcacategoria
	) b on a.codpais = b.codpais and a.codebelista = b.codebelista and a.marcacategoria = b.marcacategoria
	and a.marca = b.marca
	where b.marca is null
) a
inner join #marcasvalidas b on a.codpais = b.codpais and a.codebelista = b.codebelista and a.marca = b.marca;

drop table if exists #temp3final;
create table #temp3final as 
select b.codpais, b.codebelista, a.marcacategoria, a.prioridad from #marcacatprioriza a
inner join (	select codpais,codebelista, min(prioridad) prioridad from #temp3a a 
				inner join #marcacatprioriza b on a.marcacategoria = b.marcacategoria
				group by codpais,codebelista
			) b on a.prioridad = b.prioridad;

--select count(0) from #temp3final --104,985
--select count(0) from #temp3 --104,985

drop table if exists #tempfinal;
create table #tempfinal as
select * From #temp1final
union all
select * From #temp2final
union all
select * From #temp3final
union all
select * From #temp4final;

delete from sbx_temp.multimarca
where aniocampana='202001';

select '202001' as aniocampana,a.codpais, a.codebelista, flagmultimarcaprincipal as FlagMultimarca, b.marcacategoria as RecomendacionMultimarca1 
into sbx_temp.multimarca
from #resumen a
left join #tempfinal b on a.codpais = b.codpais and a.codebelista = b.codebelista



