Delete from fnc_analitico.dwh_demandaproyectada 
where codpais||fechaproceso||codsap in 
(select distinct b.codpais||fechaproceso||codsap from lan_dm_analitico.tmp_demandaproyectada a inner join fnc_analitico.dwh_dpais b on a.codcentro = b.codcentro);
									  
insert Into fnc_analitico.dwh_demandaproyectada
select b.codpais, fechaproceso, aniocampana, tipodecambio, codsap, codigoproy, materialstatus, comunidestimada, labst, proyundd, cob01, proytota, vtaproy, estimado
from lan_dm_analitico.tmp_demandaproyectada a
inner join fnc_analitico.dwh_dpais b on a.codcentro = b.codcentro;