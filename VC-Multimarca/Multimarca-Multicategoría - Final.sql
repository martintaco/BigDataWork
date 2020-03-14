drop table if exists #Activas;
select a.aniocampana, a.codpais, codebelista, flagactiva, b.descomportamiento 
into #Activas 
from fnc_analitico.dwh_fstaebecam a
inner join fnc_analitico.dwh_dcomportamientorolling b on a.codcomportamientorolling = b.codcomportamiento
inner join fnc_analitico.dwh_dstatus c on a.codpais = c.codpais and a.codstatus = c.codstatus
	where a.codpais in ('CO','PE','MX','EC','CL','BO','CR','GT','SV','PA','DO') 
	and flagactiva = 1 
	and a.aniocampana between '201916' and '201916'
	--and c.codstatuscorp = 1  
	--and b.descomportamiento = 'Nuevas';
--,b.descomportamiento	
select * from #Activas
	
drop table if exists #UUVta;
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
into #UUVta from fnc_analitico.dwh_fvtaproebecam as B
	inner join #Activas as A on A.Codpais = B.codpais and A.codebelista = B.codebelista
	inner join fnc_analitico.dwh_dtipooferta as C on B.Codpais = C.codpais and B.codtipooferta = C.codtipooferta
	inner join fnc_analitico.dwh_dproducto as D on B.codsap = D.Codsap
where B.codpais in ('CO','PE','MX','EC','CL','BO','CR','GT','SV','PA','DO') and b.aniocampana between '201914' and '201916' and C.codtipoprofit = '01'
and b.realvtamnneto > 0
group by A.codpais, A.codebelista, B.Aniocampana, MarcaFinal , CategoriaFinal;

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
	codpais, codebelista
	
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
	codpais, codebelista
	
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

	(case when vtausd > 0 then 1 else 0 end) as cuenta
into #SegmentoFinal from #Total
group by codpais, codebelista, VtaUSD, Pedidos, VtaML