Delete from dom_forecast.dwh_demandaproyectada 
where codpais||fechaproceso||codsap in 
(select distinct b.codpais||fechaproceso||codsap from sbx_temp.tmp_demandaproyectada a inner join fnc_analitico.dwh_dpais b on a.codcentro = b.codcentro);
									  
Insert Into dom_forecast.dwh_demandaproyectada
select b.codpais, fechaproceso, aniocampana, tipodecambio, codsap, codigoproy, materialstatus, comunidestimada, labst, proyundd, cob01, proytota, vtaproy, estimado
from sbx_temp.tmp_demandaproyectada a
inner join fnc_analitico.dwh_dpais b on a.codcentro = b.codcentro;