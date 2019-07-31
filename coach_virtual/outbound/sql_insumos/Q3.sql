DROP TABLE IF EXISTS #PaisCampanas_tmp;
SELECT trim(to_char(%s, '999999')) AS aniocampana_proceso
INTO #PaisCampanas_tmp;



DROP TABLE IF EXISTS #PaisCampanas;
SELECT DISTINCT country as codpais, aniocampana, '000000' AS ANIOCAMPANA_U6C, '000000' AS ANIOCAMPANA_U1C
INTO #PaisCampanas
FROM fnc_virtual_coach.fdethybrysdata a
WHERE EXISTS
             (      SELECT       *
                    FROM #PaisCampanas_tmp b
                    WHERE a.aniocampana = b.aniocampana_proceso
             )
AND length(codpais) = 2;




UPDATE #PaisCampanas  SET ANIOCAMPANA_U6C = f_calculaaniocampana(codpais, aniocampana, -6);
UPDATE #PaisCampanas  SET ANIOCAMPANA_U1C = f_calculaaniocampana(codpais, aniocampana, -1);



DROP TABLE IF EXISTS #Consultoras_tmp; 
SELECT        CodPais, CodEbelista, ( SELECT     g.aniocampana
                                                                                           FROM   #PaisCampanas g
                                                                                           WHERE  a.CodPais = g.CodPais AND
                                                                                                         a.AnioCampana = g.ANIOCAMPANA_U1C ) AS aniocampana
INTO   #Consultoras_tmp
FROM   fnc_analitico.dwh_fstaebecam a 
WHERE         EXISTS 
              (      SELECT        * 
                     FROM   #PaisCampanas b
                     WHERE  a.CodPais = b.CodPais AND
                                   a.AnioCampana = b.ANIOCAMPANA_U1C
              ) AND 
              a.FlagActiva = 1;

       
--       dom_virtual_coach.mh_vc_consultora_template 
       
INSERT INTO   #Consultoras_tmp 
(CodPais, CodEbelista, aniocampana)
SELECT        A.CodPais, CodEbelista, ( SELECT   g.aniocampana
                                                                                           FROM   #PaisCampanas g
                                                                                           WHERE  a.CodPais = g.CodPais AND
                                                                                                         a.AnioCampana = g.ANIOCAMPANA) AS aniocampana
FROM   fnc_analitico.dwh_fstaebecam a 
              INNER JOIN fnc_analitico.dwh_dstatus e ON a.codpais = e.codpais and a.codstatus= e.codstatus
WHERE         EXISTS 
              (      SELECT        * 
                     FROM   #PaisCampanas b
                     WHERE  a.CodPais = b.CodPais AND
                                   a.AnioCampana = b.ANIOCAMPANA
              ) AND 
              e.codstatuscorp = 1 ;

DROP TABLE IF EXISTS #Consultoras; 
select distinct CodPais, CodEbelista, aniocampana
into #Consultoras
from #Consultoras_tmp; 


DROP TABLE IF EXISTS #tmp_total_dwh_fvtaproebecam;
select        a.codpais, a.aniocampana, a.codsap, a.codventa, a.codebelista, a.realuuvendidas, a.realvtamncatalogo, a.realtcpromedio, d.cuc as CodCUC
into   #tmp_total_dwh_fvtaproebecam
from   fnc_analitico.dwh_fvtaproebecam a
              LEFT JOIN fnc_analitico.dwh_dproducto d on a.codsap = d.codsap
WHERE         EXISTS 
              (      SELECT        * 
                     FROM   #PaisCampanas b
                     WHERE  a.CodPais = b.CodPais AND
                                   a.AnioCampana BETWEEN B.ANIOCAMPANA_U6C AND b.AnioCampana
              )  and 
              EXISTS 
              (      SELECT        * 
                     FROM   #Consultoras F
                     WHERE  a.CodPais = F.CodPais AND
                                   a.codebelista = f.codebelista
              );     

DROP TABLE IF EXISTS #PaisCampanasCUV;
SELECT        DISTINCT codpais, aniocampanaexpo AS ANIOCAMPANA, CodVenta, destitulo as TituloContenido, descampania as campaniamarketing, '               ' as codsap 
INTO   #PaisCampanasCUV
FROM   fnc_virtual_coach.fdettemplates
WHERE  EXISTS
              (      SELECT        *
                     FROM   #PaisCampanas b 
                     WHERE  fdettemplates.codpais = b.codpais AND
                                   fdettemplates.aniocampanaexpo = b.aniocampana
              )
GROUP BY codpais, CodVenta, aniocampanaexpo, destitulo, descampania
ORDER BY codpais, CodVenta, aniocampanaexpo, destitulo, descampania;

DELETE  #PaisCampanasCUV WHERE NOT LEN(LTRIM(RTRIM(CodVenta))) = 5;


DROP TABLE IF EXISTS #PaisCampanasCUC; 
SELECT        DISTINCT A.codpais, aniocampanaexpo AS ANIOCAMPANA, A.codventa, a.CODCUC, destitulo as TituloContenido, descampania as campaniamarketing, '               ' as codsap , D.desmarca, d.descripcuc as desproductocuc 
INTO   #PaisCampanasCUC 
FROM   fnc_virtual_coach.fdettemplates a
              INNER JOIN fnc_analitico.dwh_dproducto d on a.CODCUC = d.cuc
WHERE  EXISTS
              (      SELECT        *
                     FROM   #PaisCampanas b 
                     WHERE  a.codpais = b.codpais AND
                                   a.aniocampanaexpo = b.aniocampana
              ) AND
              LEN(LTRIM(RTRIM(A.codcuc))) > 0  
              and 
              EXISTS
              (   SELECT  *
                     FROM   #tmp_total_dwh_fvtaproebecam e
                     WHERE  e.codpais = a.codpais and
                                   e.aniocampana = a.aniocampanaexpo and
                                   e.codcuc  = a.codcuc 
              )                    
GROUP BY A.codpais, A.CODCUC, aniocampanaexpo, destitulo, a.CODCUC, A.codventa, descampania, D.desmarca, d.descripcuc
ORDER BY A.codpais, destitulo;

DELETE        #PaisCampanasCUC WHERE LEN(LTRIM(RTRIM(codcuc)))< 9; 


DROP TABLE IF EXISTS #descripcion;
SELECT        CODPAIS, 
              aniocampanaexpo as aniocampana, 
              codcuc,
              codventa,
              codcategoria,
              descripcion
INTO   #descripcion
FROM   sbx_analytics.armsg_lanz_top a
WHERE  EXISTS
              (      SELECT        *
                     FROM   #PaisCampanas b 
                     WHERE  a.codpais = b.codpais AND
                                   a.aniocampanaexpo = b.aniocampana
              ) and
              ltrim(rtrim(a.DESCRIPCION)) IN ('Tops_M_C','Lanzamientos') AND
              a.FLAGUSO = 1;       
       
       
DROP TABLE IF EXISTS #fdetnegocioconsultora;     
select        distinct codpais, aniocampana, codebelista, fechaini, fechafin, flagpasopedido, comportamientorolling, codzona, flagcontrol, flagofertadigital
INTO   #fdetnegocioconsultora
from   fnc_virtual_coach.fdetnegocioconsultora a
WHERE  EXISTS
              (      SELECT        *
                     FROM   #PaisCampanas b 
                     WHERE  a.codpais = b.codpais AND
                                   a.aniocampana = b.aniocampana
              ); 

       
DROP TABLE IF EXISTS #VentaConsultoraCUV;
SELECT        a.CodPais, a.AnioCampana, CodVenta, CodEbelista, SUM(RealUUVendidas) RealUUVendidas, SUM(RealVtaMNCatalogo / RealTCPromedio) RealVtaDolCatalogo
INTO   #VentaConsultoraCUV
FROM   #tmp_total_dwh_fvtaproebecam a
WHERE  EXISTS
              (      SELECT        * 
                     FROM   #PaisCampanasCUV b
                     WHERE  a.codpais = b.codpais AND
                                   a.aniocampana = b.aniocampana AND
                                   ltrim(rtrim(a.codventa)) = ltrim(rtrim(b.codventa))
              ) 
GROUP BY a.CodPais, a.AnioCampana, CodVenta, CodEbelista;

DROP TABLE IF EXISTS #VentaConsultoraProducto;
SELECT        a.codpais, AnioCampana, b.cuc as CodCuc, CodEbelista, SUM(RealUUVendidas) RealUUVendidas, SUM(RealVtaMNCatalogo / RealTCPromedio) RealVtaDolCatalogo,  b.descripcuc as desproductocuc, desmarca
INTO   #VentaConsultoraProducto
FROM   #tmp_total_dwh_fvtaproebecam a
              inner join fnc_analitico.dwh_dproducto b on a.codsap = b.codsap
WHERE  EXISTS
              (      SELECT        * 
                     FROM   #PaisCampanasCUC b
                     WHERE  a.codpais = b.codpais AND
                                   a.aniocampana = b.aniocampana AND
                                   ltrim(rtrim(a.codcuc)) = ltrim(rtrim(b.codcuc))
              )
GROUP By a.codpais, AnioCampana, b.cuc, CodEbelista, b.descripcuc, desmarca;



UPDATE        dom_virtual_coach.fdetiteraccion
SET    CodVenta = b.CodVenta
FROM   #PaisCampanasCUV b
WHERE  LTRIM(RTRIM(fdetiteraccion.CodPais)) = LTRIM(RTRIM(b.CodPais)) AND 
              LTRIM(RTRIM(fdetiteraccion.aniocampana)) = LTRIM(RTRIM(b.aniocampana)) AND 
              LTRIM(RTRIM(fdetiteraccion.TituloContenido)) = LTRIM(RTRIM(b.TituloContenido)) AND 
              LTRIM(RTRIM(fdetiteraccion.CampaniaMarketing)) = LTRIM(RTRIM(b.campaniamarketing));        


UPDATE        dom_virtual_coach.fdetiteraccion
SET    UnidadesCodVenta = b.RealUUVendidas,
              VentaCatalogoDolCodVenta = b.RealVtaDolCatalogo
FROM   #VentaConsultoraCUV b 
WHERE   fdetiteraccion.CodPais = b.codpais AND
              fdetiteraccion.AnioCampana = b.AnioCampana AND 
              fdetiteraccion.CodEbelista = b.CodEbelista AND 
              fdetiteraccion.CodVenta = b.CodVenta;

UPDATE        dom_virtual_coach.fdetiteraccion
SET    CodVenta = b.CodVenta,
              codCuc = b.codCuc,
              desproductocuc = B.desproductocuc,
              desmarca = B.DESMARCA
FROM   #PaisCampanasCUC b
WHERE  LTRIM(RTRIM(fdetiteraccion.CodPais)) = LTRIM(RTRIM(b.CodPais)) AND 
              LTRIM(RTRIM(fdetiteraccion.aniocampana)) = LTRIM(RTRIM(b.aniocampana)) AND 
              LTRIM(RTRIM(fdetiteraccion.TituloContenido)) = LTRIM(RTRIM(b.TituloContenido)) AND
              LTRIM(RTRIM(fdetiteraccion.CampaniaMarketing)) = LTRIM(RTRIM(b.campaniamarketing)) ;                     



UPDATE        dom_virtual_coach.fdetiteraccion
SET    UnidadesProducto = b.RealUUVendidas,
              VentaCatalogoDolProducto = b.RealVtaDolCatalogo,
              desproductocuc = b.desproductocuc,
              desmarca = b.desmarca
FROM   #VentaConsultoraProducto b 
WHERE  fdetiteraccion.CodPais = b.codpais AND
              fdetiteraccion.AnioCampana = b.AnioCampana AND 
              fdetiteraccion.CodEbelista = b.CodEbelista AND 
              fdetiteraccion.CodCUC = b.CodCUC;

       
UPDATE dom_virtual_coach.fdetiteraccion
SET           descripcion = b.descripcion,
              codcategoria = b.codcategoria
FROM   #descripcion b 
WHERE  fdetiteraccion.codpais = b.codpais AND
              fdetiteraccion.aniocampana = b.aniocampana AND
              fdetiteraccion.codcuc= b.codcuc; 
       
UPDATE dom_virtual_coach.fdetiteraccion
SET           flagpasopedido = b.flagpasopedido,
              comportamientorolling = b.comportamientorolling,
              flagofertadigital = b.flagofertadigital,
              fechaini = b.fechaini,
              fechafin = b.fechafin,
              codzona = b.codzona,
              flagcontrol = b.flagcontrol
FROM   #fdetnegocioconsultora b 
WHERE  ltrim(rtrim(fdetiteraccion.codpais)) = ltrim(rtrim(b.codpais)) AND
              ltrim(rtrim(fdetiteraccion.aniocampana)) = ltrim(rtrim(b.aniocampana)) AND
              ltrim(rtrim(fdetiteraccion.codebelista)) = ltrim(rtrim(b.codebelista));
-------------------------------------------History---------------------------------
UPDATE        wrk_virtual_coach.fdetiteraccion_history
SET    CodVenta = b.CodVenta
FROM   #PaisCampanasCUV b
WHERE  LTRIM(RTRIM(fdetiteraccion_history.CodPais)) = LTRIM(RTRIM(b.CodPais)) AND 
              LTRIM(RTRIM(fdetiteraccion_history.aniocampana)) = LTRIM(RTRIM(b.aniocampana)) AND 
              LTRIM(RTRIM(fdetiteraccion_history.TituloContenido)) = LTRIM(RTRIM(b.TituloContenido)) AND 
              LTRIM(RTRIM(fdetiteraccion_history.CampaniaMarketing)) = LTRIM(RTRIM(b.campaniamarketing));        


UPDATE        wrk_virtual_coach.fdetiteraccion_history
SET    UnidadesCodVenta = b.RealUUVendidas,
              VentaCatalogoDolCodVenta = b.RealVtaDolCatalogo
FROM   #VentaConsultoraCUV b 
WHERE   fdetiteraccion_history.CodPais = b.codpais AND
              fdetiteraccion_history.AnioCampana = b.AnioCampana AND 
              fdetiteraccion_history.CodEbelista = b.CodEbelista AND 
              fdetiteraccion_history.CodVenta = b.CodVenta;

UPDATE        wrk_virtual_coach.fdetiteraccion_history
SET    CodVenta = b.CodVenta,
              codCuc = b.codCuc,
              desproductocuc = B.desproductocuc,
              desmarca = B.DESMARCA
FROM   #PaisCampanasCUC b
WHERE  LTRIM(RTRIM(fdetiteraccion_history.CodPais)) = LTRIM(RTRIM(b.CodPais)) AND 
              LTRIM(RTRIM(fdetiteraccion_history.aniocampana)) = LTRIM(RTRIM(b.aniocampana)) AND 
              LTRIM(RTRIM(fdetiteraccion_history.TituloContenido)) = LTRIM(RTRIM(b.TituloContenido)) AND
              LTRIM(RTRIM(fdetiteraccion_history.CampaniaMarketing)) = LTRIM(RTRIM(b.campaniamarketing)) ;                     



UPDATE        wrk_virtual_coach.fdetiteraccion_history
SET    UnidadesProducto = b.RealUUVendidas,
              VentaCatalogoDolProducto = b.RealVtaDolCatalogo,
              desproductocuc = b.desproductocuc,
              desmarca = b.desmarca
FROM   #VentaConsultoraProducto b 
WHERE  fdetiteraccion_history.CodPais = b.codpais AND
              fdetiteraccion_history.AnioCampana = b.AnioCampana AND 
              fdetiteraccion_history.CodEbelista = b.CodEbelista AND 
              fdetiteraccion_history.CodCUC = b.CodCUC;

       
UPDATE wrk_virtual_coach.fdetiteraccion_history
SET           descripcion = b.descripcion,
              codcategoria = b.codcategoria
FROM   #descripcion b 
WHERE  fdetiteraccion_history.codpais = b.codpais AND
              fdetiteraccion_history.aniocampana = b.aniocampana AND
              fdetiteraccion_history.codcuc= b.codcuc; 
       
UPDATE wrk_virtual_coach.fdetiteraccion_history
SET           flagpasopedido = b.flagpasopedido,
              comportamientorolling = b.comportamientorolling,
              flagofertadigital = b.flagofertadigital,
              fechaini = b.fechaini,
              fechafin = b.fechafin,
              codzona = b.codzona,
              flagcontrol = b.flagcontrol
FROM   #fdetnegocioconsultora b 
WHERE  ltrim(rtrim(fdetiteraccion_history.codpais)) = ltrim(rtrim(b.codpais)) AND
              ltrim(rtrim(fdetiteraccion_history.aniocampana)) = ltrim(rtrim(b.aniocampana)) AND
              ltrim(rtrim(fdetiteraccion_history.codebelista)) = ltrim(rtrim(b.codebelista));
-----------------------------------------------------------------------------------


DROP TABLE #PaisCampanas; 
drop table #Consultoras;
DROP TABLE #Consultoras_tmp; 
DROP TABLE #PaisCampanasCUV;
DROP TABLE #VentaConsultoraCUV;
DROP TABLE #PaisCampanasCUC;
DROP TABLE #VentaConsultoraProducto;
DROP TABLE #fdetnegocioconsultora;
drop table #descripcion;


