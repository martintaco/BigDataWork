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

DROP TABLE IF EXISTS #fdetnegocioconsultora;
SELECT        *
INTO   #fdetnegocioconsultora
FROM   fnc_virtual_coach.fdetnegocioconsultora a
WHERE  EXISTS 
              (      SELECT        * 
                     FROM   #PaisCampanas b
                     WHERE  a.CodPais = b.CodPais AND
                                   a.AnioCampana = b.ANIOCAMPANA
              );

DROP TABLE IF EXISTS #fdetiteraccion;
SELECT        *
INTO   #fdetiteraccion
FROM   dom_virtual_coach.fdetiteraccion a
WHERE  EXISTS 
              (      SELECT        * 
                     FROM   #PaisCampanas b
                     WHERE  a.CodPais = b.CodPais AND
                                   a.AnioCampana = b.ANIOCAMPANA
              );


DROP TABLE IF EXISTS #mh_vc_consultoramctl_tmp;
SELECT        CODPAIS, ANIOCAMPANA, CODEBELISTA, unidadeslbel_u6c, unidadesesika_u6c, unidadescyzone_u6c, unidadestf_u6c,
              unidadescp_u6c, unidadesfg_u6c, unidadesmq_u6c, unidadestc_u6c
INTO   #mh_vc_consultoramctl_tmp          
FROM   #fdetnegocioconsultora;

DROP TABLE IF EXISTS #PRE_PCMC;
SELECT        CODPAIS, ANIOCAMPANA, CODEBELISTA, 'UU_LBEL' AS PC_NAMES, 'PC_MARCA' AS OR_MC, unidadeslbel_u6c AS PORCENTAJES
INTO   #PRE_PCMC
FROM   #mh_vc_consultoramctl_tmp
UNION ALL
SELECT        CODPAIS, ANIOCAMPANA, CODEBELISTA, 'UU_ESIKA' AS PC_NAMES, 'PC_MARCA' AS OR_MC, unidadesesika_u6c AS PORCENTAJES
FROM   #mh_vc_consultoramctl_tmp
UNION ALL
SELECT        CODPAIS, ANIOCAMPANA, CODEBELISTA, 'UU_CYZONE' AS PC_NAMES, 'PC_MARCA' AS OR_MC, unidadescyzone_u6c AS PORCENTAJES
FROM   #mh_vc_consultoramctl_tmp
UNION ALL
SELECT        CODPAIS, ANIOCAMPANA, CODEBELISTA, 'UU_TF' AS PC_NAMES, 'PC_CATEGORIA' AS OR_MC, unidadestf_u6c AS PORCENTAJES
FROM   #mh_vc_consultoramctl_tmp
UNION ALL
SELECT        CODPAIS, ANIOCAMPANA, CODEBELISTA, 'UU_CP' AS PC_NAMES, 'PC_CATEGORIA' AS OR_MC, unidadescp_u6c AS PORCENTAJES
FROM   #mh_vc_consultoramctl_tmp
UNION ALL
SELECT        CODPAIS, ANIOCAMPANA, CODEBELISTA, 'UU_FG' AS PC_NAMES, 'PC_CATEGORIA' AS OR_MC, unidadesfg_u6c AS PORCENTAJES
FROM   #mh_vc_consultoramctl_tmp
UNION ALL
SELECT        CODPAIS, ANIOCAMPANA, CODEBELISTA, 'UU_MQ' AS PC_NAMES, 'PC_CATEGORIA' AS OR_MC, unidadesmq_u6c AS PORCENTAJES
FROM   #mh_vc_consultoramctl_tmp
UNION ALL
SELECT        CODPAIS, ANIOCAMPANA, CODEBELISTA, 'UU_TC' AS PC_NAMES, 'PC_CATEGORIA' AS OR_MC, unidadestc_u6c AS PORCENTAJES
FROM   #mh_vc_consultoramctl_tmp;
              
DROP TABLE IF EXISTS #PRE_PCMC_2;
SELECT        *,RANK() OVER (PARTITION BY CODPAIS, CODEBELISTA, OR_MC ORDER BY PORCENTAJES DESC) AS ORDEN
INTO   #PRE_PCMC_2
FROM   #PRE_PCMC;

DROP TABLE IF EXISTS #PRE_PCMC_3;
SELECT DISTINCT 
              CODPAIS, 
              CODEBELISTA, 
              SUM((CASE WHEN PC_NAMES = 'UU_ESIKA' AND OR_MC = 'PC_MARCA' THEN ORDEN ELSE 0 END)) AS OR_ESIKA, 
              SUM((CASE WHEN PC_NAMES = 'UU_CYZONE' AND OR_MC = 'PC_MARCA' THEN ORDEN ELSE 0 END)) AS OR_CYZONE, 
              SUM((CASE WHEN PC_NAMES = 'UU_LBEL' AND OR_MC = 'PC_MARCA' THEN ORDEN ELSE 0 END)) AS OR_LBEL, 
              SUM((CASE WHEN PC_NAMES = 'UU_MQ' AND OR_MC = 'PC_CATEGORIA' THEN ORDEN ELSE 0 END)) AS OR_MQ, 
              SUM((CASE WHEN PC_NAMES = 'UU_CP' AND OR_MC = 'PC_CATEGORIA' THEN ORDEN ELSE 0 END)) AS OR_CP, 
              SUM((CASE WHEN PC_NAMES = 'UU_FG' AND OR_MC = 'PC_CATEGORIA' THEN ORDEN ELSE 0 END)) AS OR_FG, 
              SUM((CASE WHEN PC_NAMES = 'UU_TC' AND OR_MC = 'PC_CATEGORIA' THEN ORDEN ELSE 0 END)) AS OR_TC, 
              SUM((CASE WHEN PC_NAMES = 'UU_TF' AND OR_MC = 'PC_CATEGORIA' THEN ORDEN ELSE 0 END)) AS OR_TF
INTO   #PRE_PCMC_3
FROM   #PRE_PCMC_2
GROUP BY CODPAIS, 
              CODEBELISTA;

DROP TABLE IF EXISTS #mh_vc_consultoramctl;
SELECT        DISTINCT
              a.CODPAIS, 
              a.ANIOCAMPANA, 
              a.CODEBELISTA,  
              ISNULL(A.nrotops                              ,0) AS NRO_TOP,              
              ISNULL(A.nrolanzamientos                                   ,0) AS NRO_LANZ,  
              ISNULL(A.flagpasopedido_u6c                         ,0) AS NROPEDIDOS,
              CASE WHEN ISNULL(A.flagpasopedido_u6c,0) = 0 THEN 0 ELSE (A.nrotops*1.0)/(A.flagpasopedido_u6c*1.0) END AS PROB_TOP,
              CASE WHEN ISNULL(A.flagpasopedido_u6c,0) = 0 THEN 0 ELSE (A.nrolanzamientos*1.0)/(A.flagpasopedido_u6c*1.0) END AS PROB_LANZ,
              ISNULL(B.OR_ESIKA                                                 ,0) AS OR_ESIKA,  
              ISNULL(B.OR_CYZONE                                                ,0) AS OR_CYZONE, 
              ISNULL(B.OR_LBEL                                                  ,0) AS OR_LBEL,   
              ISNULL(B.OR_MQ                                                           ,0) AS OR_MQ,            
              ISNULL(B.OR_CP                                                           ,0) AS OR_CP,            
              ISNULL(B.OR_FG                                                           ,0) AS OR_FG,            
              ISNULL(B.OR_TC                                                           ,0) AS OR_TC,            
              ISNULL(B.OR_TF                                                           ,0) AS OR_TF      
INTO   #mh_vc_consultoramctl
FROM   #fdetnegocioconsultora a
LEFT   JOIN #PRE_PCMC_3 B ON       a.CODPAIS = b.CODPAIS AND a.CODEBELISTA = b.CODEBELISTA;


DROP TABLE IF EXISTS #mh_vc_consultora_template;
SELECT        DISTINCT
              a.CodPais,    
              a.Pais,       
              a.ANIOCAMPANA,
              a.FechaIni, 
              a.FechaFin,
              a.CodEbelista,
              a.CODZONA,
              a.FlagControl,
              a.tipomail,
              a.ComportamientoRolling,
              a.TituloContenido,
              MAX(a.MensajesEnviados) MensajesEnviados,
              MAX(a.MensajesEntregados) MensajesEntregados,
              MAX(a.MensajesAbiertos) MensajesAbiertos,
              MAX(a.ClicsUnicos) ClicsUnicos,
              MAX(CASE WHEN ClicsUnicos<=MensajesAbiertos AND MensajesAbiertos<=MensajesEntregados AND MensajesEntregados<=MensajesEnviados THEN 1 ELSE 0 END) AS FLAGBUENO
INTO   #mh_vc_consultora_template 
FROM   #fdetiteraccion a
GROUP BY a.CodPais,  
              a.Pais,       
              a.ANIOCAMPANA,
              a.FechaIni, 
              a.FechaFin,
              a.CodEbelista,
              a.CODZONA,
              a.FlagControl,
              a.tipomail,
              a.ComportamientoRolling,
              a.TituloContenido;
       

DROP TABLE IF EXISTS #mh_vc_consultorageneral_tmp;
SELECT        DISTINCT
              CodPais,      
              A.ANIOCAMPANA,
              A.CodEbelista,
              max(CASE WHEN ClicsUnicos<=MensajesAbiertos AND MensajesAbiertos<=MensajesEntregados AND MensajesEntregados<=MensajesEnviados THEN 1 ELSE 0 END) AS FLAGBUENO,
              SUM(MensajesEnviados   ) AS MensajesEnviados,   
              SUM(MensajesEntregados      ) AS MensajesEntregados,    
              SUM(MensajesAbiertos ) AS MensajesAbiertos,      
              SUM(ClicsUnicos             ) AS ClicsUnicos,           
              CASE WHEN SUM(MensajesEnviados  )>0 THEN 1 ELSE 0 END AS FLAG_Enviado,
              CASE WHEN SUM(MensajesEntregados)>0 THEN 1 ELSE 0 END AS FLAG_Entregado,
              CASE WHEN SUM(MensajesAbiertos  )>0 THEN 1 ELSE 0 END AS FLAG_Abierto,
              CASE WHEN SUM(ClicsUnicos       )>0 THEN 1 ELSE 0 END AS FLAG_Clics
INTO   #mh_vc_consultorageneral_tmp              
FROM   #mh_vc_consultora_template  a             
GROUP BY CodPais,ANIOCAMPANA,CodEbelista; 


DROP TABLE IF EXISTS #mh_vc_consultorageneral;
SELECT        DISTINCT
              a.CODPAIS,
              a.ANIOCAMPANA,
              a.CODEBELISTA,
              a.FLAGCONTROL,
              a.comportamientorolling AS DESCRIPCIONROLLING,
              a.FLAGPASOPEDIDO,
              a.flagipunico AS FLAGIPUNICOZONA,
              a.FLAGOFERTADIGITAL,  
              a.FLAGPASOPEDIDOWEB,  
              a.flagpasopedido_u6c as NROPEDIDOS_U6C,
              a.flagipunico_u6c AS IPUNICOZONA_U6C,
              a.flagofertadigital_u6c AS NROOFERTADIGITAL_U6C,
              a.flagpasopedidoweb_u6c as NROPEDIDOSWEB_U6C,
              SUM(a.ventacatalogodol) AS REALVTADOL_CX,     
              SUM(a.unidades) AS UU_CX,                        
              SUM(a.ventacatalogodol_u6c) AS REALVTADOL_U6C,   
              SUM(a.unidades_u6c) AS UU_U6C,                   
              SUM(b.MensajesEnviados) AS MensajesEnviados,  
              SUM(b.MensajesEntregados) AS MensajesEntregados,
              SUM(b.MensajesAbiertos) AS MensajesAbiertos,     
              MAX(b.ClicsUnicos) AS ClicsUnicos,        
              MAX(b.FLAG_Enviado) AS FLAG_Enviado,             
              MAX(b.FLAG_Entregado) AS FLAG_Entregado,  
              MAX(b.FLAG_Abierto) AS FLAG_Abierto,             
              MAX(b.FLAG_Clics) AS FLAG_Clics
INTO   #mh_vc_consultorageneral 
FROM   fnc_virtual_coach.fdetnegocioconsultora A 
              LEFT JOIN #mh_vc_consultorageneral_tmp b ON a.codpais = b.codpais and a.aniocampana = b.aniocampana and a.codebelista = b.codebelista
WHERE  EXISTS
              (
              SELECT        *
              FROM   #PaisCampanas b 
              WHERE  a.codpais = b.codpais AND
                            a.aniocampana = b.aniocampana
              )
group by a.CODPAIS,
              a.ANIOCAMPANA,
              a.CODEBELISTA,
              a.FLAGCONTROL,
              a.comportamientorolling,
              a.FLAGPASOPEDIDO,
              a.flagipunico,
              a.FLAGOFERTADIGITAL,  
              a.FLAGPASOPEDIDOWEB,  
              a.flagpasopedido_u6c,
              a.flagipunico_u6c,
              a.flagofertadigital_u6c,
              a.flagpasopedidoweb_u6c;


DROP TABLE IF EXISTS #mh_vc_cons_cuc_control;
SELECT DISTINCT
              A.CODPAIS,
              A.ANIOCAMPANA,
              A.CODEBELISTA,
              a.comportamientorolling as DESCRIPCIONROLLING,
              A.FLAGPASOPEDIDO,
              A.categoriatemplate as DESCRIPCION,
              A.CODCUC,
              A.CodVenta,   
              A.DesProductoCUC,           
              A.DesMarca,   
              A.CodCategoria,
              SUM(ISNULL(unidadesproducto    ,0)) AS UU_CUC, 
              SUM(ISNULL(ventacatalogodolproducto         ,0)) AS VTA_CUC,
              0  AS FLAG_CUC 
INTO   #mh_vc_cons_cuc_control
FROM   #fdetiteraccion a
GROUP BY A.CODPAIS,
              A.ANIOCAMPANA,
              A.CODEBELISTA,
              a.comportamientorolling,
              A.FLAGPASOPEDIDO,
              A.categoriatemplate,
              A.CODCUC,
              A.CodVenta,   
              A.DesProductoCUC,           
              A.DesMarca,   
              A.CodCategoria;

       
DROP TABLE IF EXISTS #mh_vc_cons_cuc_piloto;
SELECT  DISTINCT
              A.CodPais,
              A.ANIOCAMPANA,
              A.CodEbelista,
              A.CODZONA,
              A.FlagControl,
              A.TIPOMAIL,
              A.ComportamientoRolling,
              A.TituloContenido,
              A.DESCRIPCION,
              A.CODCUC,
              A.CodVenta,   
              A.DesProductoCUC,           
              A.DesMarca,   
              A.CodCategoria,
              A.FLAGPASOPEDIDO,
              max(A.MensajesEnviados) AS MensajesEnviados,
              max(A.MensajesEntregados) AS MensajesEntregados,
              max(A.MensajesAbiertos) AS MensajesAbiertos,
              max(A.ClicsUnicos) AS ClicsUnicos,
              max(CASE WHEN ClicsUnicos<=MensajesAbiertos AND MensajesAbiertos<=MensajesEntregados AND MensajesEntregados<=MensajesEnviados THEN 1 ELSE 0 END) AS FLAGBUENO,
              SUM(unidadesproducto) AS UU_CUC, 
              SUM(ventacatalogodolproducto) AS VTA_CUC,
              case when SUM(unidadesproducto)>0 then 1 else 0 end AS FLAG_CUC
INTO   #mh_vc_cons_cuc_piloto
FROM   #fdetiteraccion a
WHERE  not isnull(len(ltrim(rtrim(codcuc))),0) = 0 
GROUP BY A.CodPais,
              A.ANIOCAMPANA,
              A.CodEbelista,
              A.CODZONA,
              A.FlagControl,
              A.TIPOMAIL,
              A.ComportamientoRolling,
              A.TituloContenido,
              A.DESCRIPCION,
              A.CODCUC,
              A.CodVenta,   
              A.DesProductoCUC,           
              A.DesMarca,   
              A.CodCategoria,
              A.FLAGPASOPEDIDO
ORDER BY CODEBELISTA;



DELETE FROM dom_virtual_coach.mh_vc_consultoramctl
WHERE  EXISTS
              (
              SELECT        *
              FROM   #PaisCampanas b 
              WHERE  mh_vc_consultoramctl.codpais = b.codpais AND
                            mh_vc_consultoramctl.aniocampana = b.aniocampana
              );

DELETE FROM dom_virtual_coach.mh_vc_consultora_template
WHERE  EXISTS
              (
              SELECT        *
              FROM   #PaisCampanas b 
              WHERE  mh_vc_consultora_template.codpais = b.codpais AND
                            mh_vc_consultora_template.aniocampana = b.aniocampana
              );


DELETE FROM dom_virtual_coach.mh_vc_consultorageneral
WHERE  EXISTS
              (
              SELECT        *
              FROM   #PaisCampanas b 
              WHERE  mh_vc_consultorageneral.codpais = b.codpais AND
                            mh_vc_consultorageneral.aniocampana = b.aniocampana
              );

DELETE FROM dom_virtual_coach.mh_vc_cons_cuc_piloto
WHERE  EXISTS
              (
              SELECT        *
              FROM   #PaisCampanas b 
              WHERE  mh_vc_cons_cuc_piloto.codpais = b.codpais AND
                            mh_vc_cons_cuc_piloto.aniocampana = b.aniocampana
              );
DELETE FROM dom_virtual_coach.mh_vc_cons_cuc_control
WHERE  EXISTS
              (
              SELECT        *
              FROM   #PaisCampanas b 
              WHERE  mh_vc_cons_cuc_control.codpais = b.codpais AND
                            mh_vc_cons_cuc_control.aniocampana = b.aniocampana
              );

INSERT INTO dom_virtual_coach.mh_vc_consultoramctl
(
codpais, aniocampana, codebelista, nro_top, nro_lanz, nropedidos, prob_top, prob_lanz, or_esika, or_cyzone,
or_lbel, or_mq, or_cp, or_fg, or_tc, or_tf
)
SELECT
codpais, aniocampana, codebelista, nro_top, nro_lanz, nropedidos, prob_top, prob_lanz, or_esika, or_cyzone,
or_lbel, or_mq, or_cp, or_fg, or_tc, or_tf
FROM   #mh_vc_consultoramctl;
--------------------------------History--------------------------------------
INSERT INTO wrk_virtual_coach.mh_vc_consultoramctl_history
(
codpais, aniocampana, codebelista, nro_top, nro_lanz, nropedidos, prob_top, prob_lanz, or_esika, or_cyzone,
or_lbel, or_mq, or_cp, or_fg, or_tc, or_tf,origindayfile
)
SELECT
codpais, aniocampana, codebelista, nro_top, nro_lanz, nropedidos, prob_top, prob_lanz, or_esika, or_cyzone,
or_lbel, or_mq, or_cp, or_fg, or_tc, or_tf,(getdate() - interval '5 HOURS')
FROM   #mh_vc_consultoramctl;
-----------------------------------------------------------------------------


INSERT INTO dom_virtual_coach.mh_vc_consultora_template
(
codpais, aniocampana, fechaini, fechafin, codebelista, codzona, flagcontrol, tipomail, comportamientorolling,
titulocontenido, mensajesenviados, mensajesentregados, mensajesabiertos, clicsunicos, flagbueno
)
SELECT
codpais, aniocampana, fechaini, fechafin, codebelista, codzona, flagcontrol, tipomail, comportamientorolling,
titulocontenido, mensajesenviados, mensajesentregados, mensajesabiertos, clicsunicos, flagbueno
FROM   #mh_vc_consultora_template;

--------------------------------History--------------------------------------
INSERT INTO wrk_virtual_coach.mh_vc_consultora_template_history
(
codpais, aniocampana, fechaini, fechafin, codebelista, codzona, flagcontrol, tipomail, comportamientorolling,
titulocontenido, mensajesenviados, mensajesentregados, mensajesabiertos, clicsunicos, flagbueno, origindayfile
)
SELECT
codpais, aniocampana, fechaini, fechafin, codebelista, codzona, flagcontrol, tipomail, comportamientorolling,
titulocontenido, mensajesenviados, mensajesentregados, mensajesabiertos, clicsunicos, flagbueno, (getdate() - interval '5 HOURS')
FROM   #mh_vc_consultora_template;
-----------------------------------------------------------------------------

INSERT INTO dom_virtual_coach.mh_vc_consultorageneral
(
codpais, aniocampana, codebelista, flagcontrol, descripcionrolling, flagpasopedido, flagipunicozona, flagofertadigital,
flagpasopedidoweb, nropedidos_u6c, ipunicozona_u6c, nroofertadigital_u6c, nropedidosweb_u6c, realvtadol_cx, uu_cx,
realvtadol_u6c, uu_u6c, mensajesenviados, mensajesentregados, mensajesabiertos, clicsunicos, flag_enviado,
flag_entregado, flag_abierto, flag_clics
)
SELECT
codpais, aniocampana, codebelista, flagcontrol, descripcionrolling, flagpasopedido, flagipunicozona, flagofertadigital,
flagpasopedidoweb, nropedidos_u6c, ipunicozona_u6c, nroofertadigital_u6c, nropedidosweb_u6c, realvtadol_cx, uu_cx,
realvtadol_u6c, uu_u6c, mensajesenviados, mensajesentregados, mensajesabiertos, clicsunicos, flag_enviado,
flag_entregado, flag_abierto, flag_clics
FROM   #mh_vc_consultorageneral;

--------------------------------History--------------------------------------

INSERT INTO wrk_virtual_coach.mh_vc_consultorageneral_history
(
codpais, aniocampana, codebelista, flagcontrol, descripcionrolling, flagpasopedido, flagipunicozona, flagofertadigital,
flagpasopedidoweb, nropedidos_u6c, ipunicozona_u6c, nroofertadigital_u6c, nropedidosweb_u6c, realvtadol_cx, uu_cx,
realvtadol_u6c, uu_u6c, mensajesenviados, mensajesentregados, mensajesabiertos, clicsunicos, flag_enviado,
flag_entregado, flag_abierto, flag_clics, origindayfile
)
SELECT
codpais, aniocampana, codebelista, flagcontrol, descripcionrolling, flagpasopedido, flagipunicozona, flagofertadigital,
flagpasopedidoweb, nropedidos_u6c, ipunicozona_u6c, nroofertadigital_u6c, nropedidosweb_u6c, realvtadol_cx, uu_cx,
realvtadol_u6c, uu_u6c, mensajesenviados, mensajesentregados, mensajesabiertos, clicsunicos, flag_enviado,
flag_entregado, flag_abierto, flag_clics, (getdate() - interval '5 HOURS')
FROM   #mh_vc_consultorageneral;
-----------------------------------------------------------------------------

INSERT INTO dom_virtual_coach.mh_vc_cons_cuc_piloto
(
codpais, aniocampana, codebelista, codzona, flagcontrol, tipomail, comportamientorolling, titulocontenido,
descripcion, codcuc, codventa, desproductocuc, desmarca, codcategoria, flagpasopedido, mensajesenviados,
mensajesentregados, mensajesabiertos, clicsunicos, flagbueno, UU_CUC, vta_cuc, FLAG_CUC
)
SELECT
codpais, aniocampana, codebelista, codzona, flagcontrol, tipomail, comportamientorolling, titulocontenido,
descripcion, codcuc, codventa, desproductocuc, desmarca, codcategoria, flagpasopedido, mensajesenviados,
mensajesentregados, mensajesabiertos, clicsunicos, flagbueno, UU_CUC, vta_cuc, FLAG_CUC
FROM   #mh_vc_cons_cuc_piloto;

--------------------------------History--------------------------------------

INSERT INTO wrk_virtual_coach.mh_vc_cons_cuc_piloto_history
(
codpais, aniocampana, codebelista, codzona, flagcontrol, tipomail, comportamientorolling, titulocontenido,
descripcion, codcuc, codventa, desproductocuc, desmarca, codcategoria, flagpasopedido, mensajesenviados,
mensajesentregados, mensajesabiertos, clicsunicos, flagbueno, UU_CUC, vta_cuc, FLAG_CUC, origindayfile
)
SELECT
codpais, aniocampana, codebelista, codzona, flagcontrol, tipomail, comportamientorolling, titulocontenido,
descripcion, codcuc, codventa, desproductocuc, desmarca, codcategoria, flagpasopedido, mensajesenviados,
mensajesentregados, mensajesabiertos, clicsunicos, flagbueno, UU_CUC, vta_cuc, FLAG_CUC, (getdate() - interval '5 HOURS')
FROM   #mh_vc_cons_cuc_piloto;

-----------------------------------------------------------------------------


INSERT INTO dom_virtual_coach.mh_vc_cons_cuc_control
(codpais, aniocampana, codebelista, descripcionrolling, flagpasopedido, descripcion, codcuc,
codventa, desproductocuc, desmarca, codcategoria, uu_cuc, vta_cuc, flag_cuc)
SELECT
codpais, aniocampana, codebelista, descripcionrolling, flagpasopedido, descripcion, codcuc,
codventa, desproductocuc, desmarca, codcategoria, uu_cuc, vta_cuc, flag_cuc
FROM   #mh_vc_cons_cuc_control;

----------------------------------------History-------------------------------
INSERT INTO wrk_virtual_coach.mh_vc_cons_cuc_control_history
(codpais, aniocampana, codebelista, descripcionrolling, flagpasopedido, descripcion, codcuc,
codventa, desproductocuc, desmarca, codcategoria, uu_cuc, vta_cuc, flag_cuc, origindayfile)
SELECT
codpais, aniocampana, codebelista, descripcionrolling, flagpasopedido, descripcion, codcuc,
codventa, desproductocuc, desmarca, codcategoria, uu_cuc, vta_cuc, flag_cuc, (getdate() - interval '5 HOURS')
FROM   #mh_vc_cons_cuc_control;
------------------------------------------------------------------------------

DROP TABLE #mh_vc_consultoramctl_tmp;
DROP TABLE #fdetiteraccion;
DROP TABLE #PaisCampanas;
DROP TABLE #fdetnegocioconsultora;
DROP TABLE #PRE_PCMC;
DROP TABLE #PRE_PCMC_2;
DROP TABLE #PRE_PCMC_3;
DROP TABLE #mh_vc_consultoramctl;
DROP TABLE #mh_vc_consultora_template;
DROP TABLE #mh_vc_consultorageneral_tmp;
DROP TABLE #mh_vc_consultorageneral;
DROP TABLE #mh_vc_cons_cuc_control;
DROP TABLE #mh_vc_cons_cuc_piloto;

