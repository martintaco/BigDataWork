import psycopg2
import datetime
import sys

## Autor: David Ascencios

## Uso: python (thisFile).py hostDBRS puertoBDRS nombreBD usuarioRS contrasenhaRS rutaAbsolutaDelArchivo archivoSQL
## Observaciones: El archivo SQL debe estar libre de comentarios (-- o /*) para una ejecucion correcta.
##                De existir algun error en el procedimiento a ejecutar no se commitearan los cambios realizados en queries
##                previas del archivo SQL.

## Tildes omitidas.


start_time = datetime.datetime.now()
print("Inicio : ", start_time.isoformat())

## Verificar argumentos pasados ##
#if(len(sys.argv) != 9):
  #sys.exit("Error: Wrong number of arguments.\nUsage: python (thisFile).py dbhost dbport dbname user password filespath filename")


## Conexion a RS ##
con=psycopg2.connect(host = '',
                     port = '5439',
                     dbname = 'analitico',
                     user = '',
                     password = '')

cursor = con.cursor()

## Almacenamiento del contenido del archivo en variable y obtencion del nombre de archivo ##
PAIS = 'BO'
ANIOCAMPANA_INI = '201905'
ANIOCAMPANA_FIN = '201906'

## Ejecucion del procedimiento ##
try:
    cursor.execute("""
    ---------------------------Inicio de Query----------------------------------------
    /*SE ha considerado ejecutar 2 querys por partes, 1 la parte de campañas cerradas y la otra con campañas abiertas*/
    Drop TABLE if EXISTS temp_fvtaproebecam1;
    Drop TABLE if EXISTS temp_fvtaprocammes1;
    drop table if EXISTS temp_Data_generate;
    drop table if EXISTS temp_Data_generate2;

    /*Parte de fvtaproebecam para generar reporte de unidades y venta*/
    select codpais,aniocampana, codsap,codtipooferta,realtcpromedio, sum(realuuvendidas) as UURealVendidas,sum(realuufaltantes) as UURealFaltantes,
    sum(realvtamnneto) as VtaRealMNNeto,sum(realvtamnfaltneto) as VtarealmnFaltneto,
    sum(costoreposicionmn) as costoreposicionmn
    into temp_fvtaproebecam1
    from fnc_analitico.dwh_fvtaproebecam
    where aniocampana between '""" + ANIOCAMPANA_INI + """' and '""" + ANIOCAMPANA_FIN + """'
    and codpais = '"""+ PAIS +"""'
    group by codpais,aniocampana,codsap,codtipooferta,realtcpromedio;


    /*Parte 2 sumatoria de campos de la tabla fvtaprocammes valores estimados*/
    select codpais,aniocampana,codsap,codtipooferta,sum(estuuvendidas) as estuuvendidas,sum(estvtamnneto) as estvtamnneto,sum(estvtadolneto) as estvtadolneto
    into temp_fvtaprocammes1
    from fnc_analitico.dwh_fvtaprocammes
    where aniocampana between '""" + ANIOCAMPANA_INI + """' and '""" +ANIOCAMPANA_FIN +"""'
    and codpais = '"""+ PAIS +"""'
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
    round(c.precionormalmn/e.realtcpromedio,4) as PrecioNormalDol, round((c.preciooferta/e.realtcpromedio),4) as precioofertadol,
    (a.uurealvendidas+a.uurealfaltantes) as UUDemandadas, uurealvendidas, estuuvendidas,
    cast((vtarealmnneto+vtarealmnfaltneto) as decimal) as VentaNetaMNDemandada,vtarealmnneto,estvtamnneto,
    cast(round((vtarealmnneto+vtarealmnfaltneto)/e.realtcpromedio,4) as decimal) as Ventanetadoldemd,
    round(vtarealmnneto/e.realtcpromedio,4) as Ventanetadolreal,estvtadolneto,
    cast((costoreposicionmn/e.realtcpromedio) as decimal) as Costoreposiciondolreal,
    (costoreposicionmn/NULLIF((e.realtcpromedio*uurealvendidas),0))*estuuvendidas as costoreposiciondolest,
    (costoreposicionmn/NULLIF((e.realtcpromedio*uurealvendidas),0)) as CostodereposicionUntdolReal,
    (costoreposicionmn/NULLIF((e.realtcpromedio*uurealvendidas),0)) as CostodereposicionUntdolEst,
    e.realnropedidos,e.estnropedidos,cast(e.realtcpromedio as decimal) as realtcpromedio,
    b.um_contenido as submarcas,b.desproductosupergenerico,round(vtarealmnneto/e.realtcpromedio,2) as VentaNetadolrealCte,
    '' as etiquetadetopsellers
    Into temp_Data_generate
    from temp_fvtaproebecam1 a
    left join fnc_analitico.dwh_dproducto b on a.codsap = b.codsap
    left join fnc_analitico.dwh_dmatrizcampana c on a.codpais = c.codpais and a.aniocampana = c.aniocampana and a.codsap = c.codsap and a.codtipooferta = c.codtipooferta
    left join temp_fvtaprocammes1 d on a.codpais = d.codpais and a.aniocampana = d.aniocampana and a.codsap = d.codsap and a.codtipooferta = d.codtipooferta
    left join fnc_analitico.dwh_fnumpedcam e on a.codpais = e.codpais and a.aniocampana = e.aniocampana
    left join fnc_analitico.dwh_dtipooferta f on a.codpais = f.codpais and a.codtipooferta = f.codtipooferta
    where a.aniocampana between '""" + ANIOCAMPANA_INI + """' and '""" + ANIOCAMPANA_FIN + """'
    and a.codpais = '"""+ PAIS +"""'
    and b.descategoria in ('TRATAMIENTO FACIAL','TRATAMIENTO CORPORAL','MAQUILLAJE','FRAGANCIAS','CUIDADO PERSONAL')
    and a.codtipooferta < '200';

    Select distinct a.*
    into temp_Data_generate2
    from temp_Data_generate a
    inner join fnc_analitico.dwh_dtipooferta b on a.codtipooferta = b.codtipooferta
    where b.codtipoprofit = '01';

    drop table if EXISTS temp_Data_generate3;

    select codpais,desunidadnegocio,aniomarketing,aniocampana,campania,codmarca,desmarca,codcategoria,clase_ajustada,codclase,codsubcategoria,desclase,tipoajustado,codtipo,destipo,cuc,descuc,codsap,desproducto,bpcs,bpcstono,codgrupooferta,grupoofertaajustado,grupoofertacosmeticos,codtipooferta,
    avg(precionormalmn) as precionormalmn,avg(preciooferta) as preciooferta,avg(precionormaldol) as precionormaldol,avg(precioofertadol) as precioofertadol,avg(uudemandadas) as uudemandadas,avg(uurealvendidas) as uurealvendidas,avg(estuuvendidas) as estuuvendidas,avg(ventanetamndemandada) as ventanetamndemandada,avg(vtarealmnneto) as vtarealmnneto,avg(estvtamnneto) as estvtamnneto,avg(ventanetadoldemd) as ventanetadoldemd,avg(ventanetadolreal) as ventanetadolreal,avg(estvtadolneto) as estvtadolneto,avg(costoreposiciondolreal) as costoreposiciondolreal,avg(costoreposiciondolest) as costoreposiciondolest,avg(costodereposicionuntdolreal) as costodereposicionuntdolreal,avg(costodereposicionuntdolest) as costodereposicionuntdolest,avg(realnropedidos) as realnropedidos,avg(estnropedidos) as estnropedidos,avg(realtcpromedio) as realtcpromedio,submarcas,desproductosupergenerico, avg(ventanetadolrealcte) as ventanetadolrealcte,etiquetadetopsellers
    into temp_Data_generate3
    from temp_Data_generate2
    group by codpais,desunidadnegocio,aniomarketing,aniocampana,campania,codmarca,desmarca,codcategoria,clase_ajustada,codclase,codsubcategoria,desclase,tipoajustado,codtipo,destipo,cuc,descuc,codsap,desproducto,bpcs,bpcstono,codgrupooferta,grupoofertaajustado,grupoofertacosmeticos,codtipooferta,submarcas, desproductosupergenerico,etiquetadetopsellers;

    Update temp_Data_generate3
    set bpcs = bpcstono
    where bpcs is null;

    Insert Into temp_tacticaindividual
    select *
    from temp_Data_generate3
    where precionormalmn is not null;"""
    )
except:
    print("An exception occurred")

end_time = datetime.datetime.now()
print("Fin: ", end_time.isoformat())
