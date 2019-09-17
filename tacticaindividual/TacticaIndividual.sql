---------------------------Inicio de Query----------------------------------------
/*SE ha considerado ejecutar 2 querys por partes, 1 la parte de campañas cerradas y la otra con campañas abiertas*/
Drop TABLE if EXISTS sbx_temp.temp_fvtaproebecam1;
Drop TABLE if EXISTS sbx_temp.temp_fvtaprocammes1;
drop table if EXISTS sbx_temp.temp_Data_generate;
drop table if EXISTS sbx_temp.temp_Data_generate2;
--select count(*) from temp_fvtaproebecam1
/*Parte de fvtaproebecam para generar reporte de unidades y venta*/
select codpais,aniocampana, codsap,codtipooferta,realtcpromedio, sum(realuuvendidas) as UURealVendidas,sum(realuufaltantes) as UURealFaltantes,
sum(realvtamnneto) as VtaRealMNNeto,sum(realvtamnfaltneto) as VtarealmnFaltneto,
sum(costoreposicionmn) as costoreposicionmn
into sbx_temp.temp_fvtaproebecam1
from fnc_analitico.dwh_fvtaproebecam
where aniocampana between {Aniocampana_ini} and {Aniocampana_fin}
and case '{CodPais}' when 'BO' then codpais != 'PR'
			  			when 'PR' then codpais = 'PR' END
group by codpais,aniocampana,codsap,codtipooferta,realtcpromedio;

/*Parte 2 sumatoria de campos de la tabla fvtaprocammes valores estimados*/
select codpais,aniocampana,codsap,codtipooferta,sum(estuuvendidas) as estuuvendidas,sum(estvtamnneto) as estvtamnneto,sum(estvtadolneto) as estvtadolneto
into  table sbx_temp.temp_fvtaprocammes1
from fnc_analitico.dwh_fvtaprocammes
where aniocampana between {Aniocampana_ini} and {Aniocampana_fin}
and case '{CodPais}' when 'BO' then codpais != 'PR'
			  			when 'PR' then codpais = 'PR' END
group by codpais,aniocampana,codsap,codtipooferta;

/*parte 3 para generar el data set completo*/
select a.codpais,b.desunidadnegocio,left(a.aniocampana,4) as aniomarketing,a.aniocampana,
right(a.aniocampana,2) as campania,b.codmarca,b.desmarca,b.codcategoria,case b.descategoria when 'FRAGANCIAS' THEN 'FR'
															WHEN 'MAQUILLAJE' THEN 'MQ'
															WHEN 'TRATAMIENTO FACIAL' THEN 'TF'
															WHEN 'TRATAMIENTO CORPORAL' THEN 'TC'
															WHEN 'CUIDADO PERSONAL' THEN 'CP' ELSE b.desclase END as clase_ajustada,
b.codclase,b.codsubcategoria,b.desclase, destiposolo as tipoajustado,b.codtipo,destipo,cuc, descripcuc as descuc,
b.codsap,b.desproducto,RIGHT(TRIM(codproductogenerico),9) as bpcs,b.codproducto as bpcstono,c.vehiculoventa as TipoMedioDeVentaAjustado,
c.vehiculoventa as TipoMedioDeVenta,f.codsubgrupoto1 as CodGrupoOferta,f.dessubgrupoto1 as GrupoOfertaAjustado,
f.dessubgrupoto1 as GrupoOfertaCosmeticos,a.codtipooferta,c.precionormalmn,c.preciooferta,
round(c.precionormalmn/(case when e.realtcpromedio = '1' then e.esttcpromedio else e.realtcpromedio end ),4) as PrecioNormalDol,
round((c.preciooferta/(case when e.realtcpromedio = '1' then e.esttcpromedio else e.realtcpromedio end )),4) as precioofertadol,
(a.uurealvendidas+a.uurealfaltantes) as UUDemandadas, uurealvendidas, estuuvendidas,
cast((vtarealmnneto+vtarealmnfaltneto) as decimal) as VentaNetaMNDemandada,vtarealmnneto,estvtamnneto,
cast(round((vtarealmnneto+vtarealmnfaltneto)/(case when e.realtcpromedio = '1' then e.esttcpromedio else e.realtcpromedio end ),4) as decimal) as Ventanetadoldemd,
round(vtarealmnneto/(case when e.realtcpromedio = '1' then e.esttcpromedio else e.realtcpromedio end ),4) as Ventanetadolreal,estvtadolneto,
cast((costoreposicionmn/(case when e.realtcpromedio = '1' then e.esttcpromedio else e.realtcpromedio end )) as decimal) as Costoreposiciondolreal,
(costoreposicionmn/NULLIF(((case when e.realtcpromedio = '1' then e.esttcpromedio else e.realtcpromedio end )*uurealvendidas),0))*estuuvendidas as costoreposiciondolest,
(costoreposicionmn/NULLIF(((case when e.realtcpromedio = '1' then e.esttcpromedio else e.realtcpromedio end )*uurealvendidas),0)) as CostodereposicionUntdolReal,
(costoreposicionmn/NULLIF(((case when e.realtcpromedio = '1' then e.esttcpromedio else e.realtcpromedio end )*uurealvendidas),0)) as CostodereposicionUntdolEst,
e.realnropedidos,e.estnropedidos,cast((case when e.realtcpromedio = '1' then e.esttcpromedio else e.realtcpromedio end ) as decimal) as realtcpromedio,
b.um_contenido as submarcas,b.desproductosupergenerico,round(vtarealmnneto/(case when e.realtcpromedio = '1' then e.esttcpromedio else e.realtcpromedio end ),2) as VentaNetadolrealCte,
'' as etiquetadetopsellers
Into sbx_temp.temp_Data_generate
from sbx_temp.temp_fvtaproebecam1 a
left join fnc_analitico.dwh_dproducto b on a.codsap = b.codsap
left join fnc_analitico.dwh_dmatrizcampana c on a.codpais = c.codpais and a.aniocampana = c.aniocampana and a.codsap = c.codsap and a.codtipooferta = c.codtipooferta
left join sbx_temp.temp_fvtaprocammes1 d on a.codpais = d.codpais and a.aniocampana = d.aniocampana and a.codsap = d.codsap and a.codtipooferta = d.codtipooferta
left join fnc_analitico.dwh_fnumpedcam e on a.codpais = e.codpais and a.aniocampana = e.aniocampana
left join fnc_analitico.dwh_dtipooferta f on a.codpais = f.codpais and a.codtipooferta = f.codtipooferta
where a.aniocampana between {Aniocampana_ini} and {Aniocampana_fin}
and case '{CodPais}' when 'BO' then a.codpais != 'PR'
			  			when 'PR' then a.codpais = 'PR' END
and b.descategoria in ('TRATAMIENTO FACIAL','TRATAMIENTO CORPORAL','MAQUILLAJE','FRAGANCIAS','CUIDADO PERSONAL');
--and a.codtipooferta < '200';

Select distinct a.*
into sbx_temp.temp_Data_generate2
from sbx_temp.temp_Data_generate a
inner join fnc_analitico.dwh_dtipooferta b on a.codtipooferta = b.codtipooferta
where b.codtipoprofit = '01';


insert into  sbx_temp.temp_Data_generate3
select codpais,desunidadnegocio,aniomarketing,aniocampana,campania,codmarca,desmarca,codcategoria,clase_ajustada,codclase,codsubcategoria,desclase,tipoajustado,codtipo,destipo,cuc,descuc,codsap,desproducto,bpcs,bpcstono,codgrupooferta,grupoofertaajustado,grupoofertacosmeticos,codtipooferta,
avg(precionormalmn) as precionormalmn,avg(preciooferta) as preciooferta,avg(precionormaldol) as precionormaldol,avg(precioofertadol) as precioofertadol,avg(uudemandadas) as uudemandadas,avg(uurealvendidas) as uurealvendidas,avg(estuuvendidas) as estuuvendidas,avg(ventanetamndemandada) as ventanetamndemandada,avg(vtarealmnneto) as vtarealmnneto,avg(estvtamnneto) as estvtamnneto,avg(ventanetadoldemd) as ventanetadoldemd,avg(ventanetadolreal) as ventanetadolreal,avg(estvtadolneto) as estvtadolneto,avg(costoreposiciondolreal) as costoreposiciondolreal,avg(costoreposiciondolest) as costoreposiciondolest,avg(costodereposicionuntdolreal) as costodereposicionuntdolreal,avg(costodereposicionuntdolest) as costodereposicionuntdolest,avg(realnropedidos) as realnropedidos,avg(estnropedidos) as estnropedidos,avg(realtcpromedio) as realtcpromedio,submarcas,desproductosupergenerico, avg(ventanetadolrealcte) as ventanetadolrealcte,etiquetadetopsellers
--into sbx_temp.temp_Data_generate3
from sbx_temp.temp_Data_generate2
group by codpais,desunidadnegocio,aniomarketing,aniocampana,campania,codmarca,desmarca,codcategoria,clase_ajustada,codclase,codsubcategoria,desclase,tipoajustado,codtipo,destipo,cuc,descuc,codsap,desproducto,bpcs,bpcstono,codgrupooferta,grupoofertaajustado,grupoofertacosmeticos,codtipooferta,submarcas, desproductosupergenerico,etiquetadetopsellers;

Update sbx_temp.temp_Data_generate3
set bpcs = bpcstono
where bpcs is null;

/*Generar parte de estimados - CAMPAÑAS ABIERTAS */

Drop TABLE if EXISTS sbx_temp.temp_fvtaprocammes1;
drop table if EXISTS sbx_temp.temp_Data_generate;
drop table if EXISTS sbx_temp.temp_Data_generate2;

/*Generar campos de valores estimados agrupados*/
select codpais,aniocampana,codsap,codtipooferta,sum(estuuvendidas) as estuuvendidas,sum(estvtamnneto) as estvtamnneto,sum(estvtadolneto) as estvtadolneto,
avg(precioofertamn) as precioofertamn,avg(precioofertadol) as precioofertadol,avg(precionormaldol) as precionormaldol,
avg(precionormalmn) as precionormalmn
into sbx_temp.temp_fvtaprocammes1
from fnc_analitico.dwh_fvtaprocammes
where aniocampana between {Aniocampana_ini2} and {Aniocampana_fin2}
and case '{CodPais}' when 'BO' then codpais != 'PR'
			  			when 'PR' then codpais = 'PR' END
group by codpais,aniocampana,codsap,codtipooferta;

select a.codpais,b.desunidadnegocio,left(a.aniocampana,4) as aniomarketing,a.aniocampana,
right(a.aniocampana,2) as campania,b.codmarca,b.desmarca,b.codcategoria,case b.descategoria when 'FRAGANCIAS' THEN 'FR'
															WHEN 'MAQUILLAJE' THEN 'MQ'
															WHEN 'TRATAMIENTO FACIAL' THEN 'TF'
															WHEN 'TRATAMIENTO CORPORAL' THEN 'TC'
															WHEN 'CUIDADO PERSONAL' THEN 'CP' ELSE b.desclase END as clase_ajustada,
b.codclase,b.codsubcategoria,b.desclase, destiposolo as tipoajustado,b.codtipo,destipo,cuc, descripcuc as descuc,
b.codsap,b.desproducto,RIGHT(TRIM(codproductogenerico),9) as bpcs,b.codproducto as bpcstono,c.vehiculoventa as TipoMedioDeVentaAjustado,
c.vehiculoventa as TipoMedioDeVenta,f.codsubgrupoto1 as CodGrupoOferta,f.dessubgrupoto1 as GrupoOfertaAjustado,
f.dessubgrupoto1 as GrupoOfertaCosmeticos,a.codtipooferta,a.precionormalmn,a.precioofertamn,
 a.PrecioNormalDol, a.precioofertadol,
cast(null as int) as UUDemandadas, cast(null as int) as uurealvendidas, a.estuuvendidas,
cast(null as numeric(15,5)) as VentaNetaMNDemandada,
cast(null as numeric(15,5)) as vtarealmnneto,a.estvtamnneto,
cast(null as numeric(15,5)) as Ventanetadoldemd,
cast(null as numeric(15,5)) as Ventanetadolreal,a.estvtadolneto,
cast(null as numeric(15,5)) as Costoreposiciondolreal,
cast(null as numeric(15,5)) as costoreposiciondolest,
cast(null as numeric(15,5)) as CostodereposicionUntdolReal,
cast(null as numeric(15,5)) as CostodereposicionUntdolEst,
e.realnropedidos,e.estnropedidos,cast(e.esttcpromedio as decimal) as realtcpromedio,
b.um_contenido as submarcas,b.desproductosupergenerico,
cast(null as numeric(15,5)) as VentaNetadolrealCte,
'' as etiquetadetopsellers
Into sbx_temp.temp_Data_generate
from sbx_temp.temp_fvtaprocammes1 a
left join fnc_analitico.dwh_dproducto b on a.codsap = b.codsap
left join fnc_analitico.dwh_dmatrizcampana c on a.codpais = c.codpais and a.aniocampana = c.aniocampana and a.codsap = c.codsap and a.codtipooferta = c.codtipooferta
left join fnc_analitico.dwh_fnumpedcam e on a.codpais = e.codpais and a.aniocampana = e.aniocampana
left join fnc_analitico.dwh_dtipooferta f on a.codpais = f.codpais and a.codtipooferta = f.codtipooferta
where a.aniocampana between {Aniocampana_ini2} and {Aniocampana_fin2}
and case '{CodPais}' when 'BO' then a.codpais != 'PR'
			  			when 'PR' then a.codpais = 'PR' END
and b.descategoria in ('TRATAMIENTO FACIAL','TRATAMIENTO CORPORAL','MAQUILLAJE','FRAGANCIAS','CUIDADO PERSONAL');
--and a.codtipooferta < '200';

Select distinct a.*
into sbx_temp.temp_Data_generate2
from sbx_temp.temp_Data_generate a
inner join fnc_analitico.dwh_dtipooferta b on a.codtipooferta = b.codtipooferta
where b.codtipoprofit = '01';


Update sbx_temp.temp_Data_generate2
set bpcs = bpcstono
where bpcs is null;

insert into  sbx_temp.temp_Data_generate3
select codpais,desunidadnegocio,aniomarketing,aniocampana,campania,codmarca,desmarca,codcategoria,clase_ajustada,codclase,codsubcategoria,desclase,tipoajustado,codtipo,destipo,cuc,descuc,codsap,desproducto,bpcs,bpcstono,codgrupooferta,grupoofertaajustado,grupoofertacosmeticos,codtipooferta,
avg(precionormalmn) as precionormalmn,avg(precioofertamn) as precioofertamn,avg(precionormaldol) as precionormaldol,avg(precioofertadol) as precioofertadol,uudemandadas,uurealvendidas,avg(estuuvendidas) as estuuvendidas,ventanetamndemandada,vtarealmnneto,avg(estvtamnneto) as estvtamnneto,ventanetadoldemd,ventanetadolreal,avg(estvtadolneto) as estvtadolneto,costoreposiciondolreal,costoreposiciondolest,costodereposicionuntdolreal,costodereposicionuntdolest,avg(realnropedidos) as realnropedidos,avg(estnropedidos) as estnropedidos,avg(realtcpromedio) as realtcpromedio,submarcas,desproductosupergenerico,ventanetadolrealcte,etiquetadetopsellers
--into sbx_temp.temp_Data_generate3
from sbx_temp.temp_Data_generate2
group by codpais,desunidadnegocio,aniomarketing,aniocampana,campania,codmarca,desmarca,codcategoria,clase_ajustada,codclase,codsubcategoria,desclase,tipoajustado,codtipo,destipo,cuc,descuc,codsap,desproducto,bpcs,bpcstono,codgrupooferta,grupoofertaajustado,grupoofertacosmeticos,codtipooferta,uudemandadas,uurealvendidas,ventanetamndemandada,vtarealmnneto,ventanetadoldemd,ventanetadolreal,costoreposiciondolreal,costoreposiciondolest,costodereposicionuntdolreal,costodereposicionuntdolest,submarcas,desproductosupergenerico,ventanetadolrealcte,etiquetadetopsellers;


unload($$ select * from sbx_temp.temp_Data_generate3 where precionormalmn is not null $$)
--to 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/NovoApp/TacticaInd/data_TacticaIndividual.txt'
to 's3://belc-bigdata-landing-dlk-qas/forecast-data/Bigdata/NovoApp/TacticaInd/data_TacticaIndividual.txt'
access_key_id '{ACCESS_KEY}'
secret_access_key '{SECRET_KEY}'
delimiter '\t'
NULL AS 'NULL'
ADDQUOTES
ALLOWOVERWRITE
header
PARALLEL OFF;
Drop TABLE if EXISTS sbx_temp.temp_fvtaproebecam1;
Drop TABLE if EXISTS sbx_temp.temp_fvtaprocammes1;
drop table if EXISTS sbx_temp.temp_Data_generate;
drop table if EXISTS sbx_temp.temp_Data_generate2;
drop table if EXISTS sbx_temp.temp_Data_generate3;