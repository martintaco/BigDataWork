drop table if exists #Activas;
select a.codpais, codebelista, flagactiva 
into #Activas 
from fnc_analitico.dwh_fstaebecam a
inner join fnc_analitico.dwh_dcomportamientorolling b on a.codcomportamientorolling = b.codcomportamiento
inner join fnc_analitico.dwh_dstatus c on a.codpais = c.codpais and a.codstatus = c.codstatus
	where a.codpais in ('CO','PE','MX','EC','CL','BO','CR','GT','SV','PA','DO') 
	and flagactiva = 1 
	and a.aniocampana between '201915' and '201915'
	--and c.codstatuscorp = 1  
	--and b.descomportamiento = 'Nuevas';
--,b.descomportamiento	

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
where B.codpais in ('CO','PE','MX','EC','CL','BO','CR','GT','SV','PA','DO') and b.aniocampana between '201913' and '201915' and C.codtipoprofit = '01'
group by A.codpais, A.codebelista, B.Aniocampana, MarcaFinal , CategoriaFinal;

drop table if exists #Segmentado;
select 
aniocampana, 
codpais, codebelista, 
	sum (totventaUSD) as ventaneta,
	case when ventaneta >0 then 1 else 0 end as Flagpasopedido,
	sum (totventaML) as ventanetaML,
	
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
	sum (case when MarcaFinal = 'CZ' and CategoriaFinal = 'NC' then totunidades else 0 end) as UUCZNC	

	into #Segmentado from #UUVta
	group by 
	aniocampana, 
	codpais, codebelista
	
drop table if exists #total15;
select 
codpais, codebelista, 
	sum (ventaneta) as VtaUSD,
	sum (Flagpasopedido) as pedidos,
	sum (ventanetaML) as VtaML,
	
	sum (UUEKFR) as EKFR, sum(UUEKMQ) as EKMQ, sum(UUEKTF) as EKTF, sum(UUEKTC) as EKTC, sum(UUEKCP) as EKCP, sum(UUEKNC) as EKNC,	
	sum (UULBFR) as LBFR, sum(UULBMQ) as LBMQ, sum(UULBTF) as LBTF, sum(UULBTC) as LBTC, sum(UULBCP) as LBCP, sum(UULBNC) as LBNC,	
	sum (UUCZFR) as CZFR, sum(UUCZMQ) as CZMQ, sum(UUCZTF) as CZTF, sum(UUCZTC) as CZTC, sum(UUCZCP) as CZCP, sum(UUCZNC) as CZNC

into #total15 from #Segmentado
	group by  
	codpais, codebelista
	
drop table if exists #SegmentoFinal15;
select codpais, codebelista, VtaUSD, Pedidos, VtaML,
	case when EKFR > 0 then 'A' else 'N' end as CEKFR,
	case when EKMQ > 0 then 'B' else 'N' end as CEKMQ,
	case when EKTF > 0 then 'C' else 'N' end as CEKTF,
	case when LBFR > 0 then 'D' else 'N' end as CLBFR,
	case when LBMQ > 0 then 'E' else 'N' end as CLBMQ,
	case when LBTF > 0 then 'F' else 'N' end as CLBTF,
	case when CZFR > 0 then 'G' else 'N' end as CCZFR,
	case when CZMQ > 0 then 'H' else 'N' end as CCZMQ,
	case when CZTF > 0 then 'I' else 'N' end as CCZTF,
	CEKFR+CEKMQ+CEKTF+CLBFR+CLBMQ+CLBTF+CCZFR+CCZMQ+CCZTF as Codigo,
	(case when Codigo like 'A___E___I' then 'Multimarca' 
		  when Codigo like 'A____F_H_' then 'Multimarca'
		  when Codigo like '_B_D____I' then 'Multimarca'
		  when Codigo like '_B___FG__' then 'Multimarca'
		  when Codigo like '__CD___H_' then 'Multimarca'
		  when Codigo like '__C_E_G__' then 'Multimarca'
		  else 'Resto' end ) as FlagMulti,
	(case when vtausd > 0 then 1 else 0 end) as cuenta
into #SegmentoFinal15 from #Total15