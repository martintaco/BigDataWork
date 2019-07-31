DROP TABLE IF EXISTS #PaisCampanas_tmp;
SELECT DISTINCT campanaexposicion AS aniocampana_proceso
INTO #PaisCampanas_tmp
FROM lan_virtual_coach.fdethybrysdata
WHERE length(aniocampana_proceso) = 6;

 

 

DROP TABLE IF EXISTS #PaisCampanas;
SELECT DISTINCT country as codpais, aniocampana, '000000' AS ANIOCAMPANA_U6C, '000000' AS ANIOCAMPANA_U1C
INTO #PaisCampanas
FROM fnc_virtual_coach.fdethybrysdata a
WHERE EXISTS
             (      SELECT       *
                    FROM #PaisCampanas_tmp b
                    WHERE a.aniocampana = b.aniocampana_proceso
             )
and length(codpais) = 2;

unload ($$ 
select 
		p.codpais
		,p.aniocampana
		,p.codebelista		
		,p.titulocontenido	
		,p.mensajesenviados
		,p.mensajesentregados
		,p.mensajesabiertos
		,p.clicsunicos			
from dom_virtual_coach.mh_vc_consultora_template p
inner join #PaisCampanas pc on pc.codpais = p.codpais and pc.aniocampana = p.aniocampana
/*where flagcontrol = 0*/
$$) 
to 's3://belc-bigdata-landing-dlk-prd/datalake/output/hana-bi/virtual-coach/dlk_VC_consultora_template_' 
access_key_id '{ACCESS_KEY}' 
secret_access_key '{SECRET_KEY}' 
delimiter '\t' 
--maxfilesize 250 mb 
parallel OFF
ALLOWOVERWRITE
;



unload ($$ 
SELECT 
	 p.aniocampanaexpo
	,p.codpais
	,p.destitulo
	,p.despublico
	,p.destipo		
	,p.descategoria
	,p.dessubcategoria
	,p.descampania
	,p.codcuc
	,p.codventa	
FROM dom_virtual_coach.fdettemplates p
inner join #PaisCampanas pc on pc.codpais = p.codpais and pc.aniocampana = p.aniocampanaexpo
$$) 
to 's3://belc-bigdata-landing-dlk-prd/datalake/output/hana-bi/virtual-coach/dlk_VC_template_' 
access_key_id '{ACCESS_KEY}' 
secret_access_key '{SECRET_KEY}' 
delimiter '\t' 
--maxfilesize 250 mb 
parallel OFF
ALLOWOVERWRITE
;


Delete from dom_virtual_coach.det_VC_consolidado
where codpais||aniocampanaenvio in (select codpais||aniocampana from #PaisCampanas);

insert into dom_virtual_coach.det_VC_consolidado
SELECT a.aniocampanaenvio
       , a.codpais
       , a.codebelista
       , a.descampaniamarketing
       , a.destitulocontenido
       , a.despublico
       , a.destipo
       , b.descategoria descategoria
       , b.dessubcategoria dessubcategoria
       , b.codcuc cuc
       , b.codventa codventa
       , a.fechaenvio
       , a.fecharecepcion
       , a.fechaapertura
       , a.fechaclic
       , a.aniocampanarecepcion
       , a.aniocampanaapertura
       , a.aniocampanaclic
       , a.cantidadmensajesenviados
       , a.cantidadmensajesentregados
       , a.cantidadmensajesabiertos
       , a.cantidadmensajescliqueados
  FROM 
  (  SELECT a.aniocampanaenvio
       , a.codpais
       , a.codebelista
       , a.descampaniamarketing
       , a.destitulocontenido
       , CASE 
         WHEN a.destitulocontenido LIKE '%NUEVA%' THEN 'Nueva'
         WHEN a.destitulocontenido LIKE '%EST%' THEN 'Establecida'
         ELSE 'Otros'
         END despublico
       , CASE
         WHEN a.desmediocomunicacion = 'EMAIL' THEN 'Mail'
         WHEN a.desmediocomunicacion = 'MOBILE_APP' THEN 'Push'
         WHEN a.desmediocomunicacion = 'SMS' THEN 'SMS'
         ELSE 'Otros'
         END destipo
       , a.fechaenvio
       , b.fecharecepcion
       , c.fechaapertura
       , d.fechaclic
       , b.aniocampanarecepcion
       , c.aniocampanaapertura
       , d.aniocampanaclic
       , SUM(a.cantidadmensajesenviados) cantidadmensajesenviados
       , SUM(b.cantidadmensajesentregados) cantidadmensajesentregados
       , SUM(c.cantidadmensajesabiertos) cantidadmensajesabiertos
       , SUM(d.cantidadmensajescliqueados) cantidadmensajescliqueados
  FROM 
  (SELECT a.aniocampana as aniocampanaenvio
       , country codpais
       , yy1_codigoebelista_mps codebelista
       , campaignname descampaniamarketing
       , interactioncontentsubject destitulocontenido
       , communicationmedium desmediocomunicacion
       , MIN(interactiontimestamputc) fechaenvio
       , SUM(numberofsentmessages) cantidadmensajesenviados
  FROM fnc_virtual_coach.fdethybrysdata a      
  inner join #PaisCampanas pc on pc.codpais = a.country and pc.aniocampana = a.aniocampana
  WHERE numberofsentmessages > 0
  GROUP BY
       a.aniocampana
       , country
       , yy1_codigoebelista_mps
       , campaignname
       , interactioncontentsubject
       , communicationmedium
       ) a
  LEFT JOIN                        --recepción
  (		SELECT a.aniocampana as aniocampanarecepcion
		       , country codpais
		       , yy1_codigoebelista_mps codebelista
		       , campaignname descampaniamarketing
		       , interactioncontentsubject destitulocontenido
		       , communicationmedium desmediocomunicacion
		       , MIN(interactiontimestamputc) fecharecepcion
		       , SUM(numberofdeliveredmessages) cantidadmensajesentregados
		  FROM fnc_virtual_coach.fdethybrysdata a  
      inner join #PaisCampanas pc on pc.codpais = a.country and pc.aniocampana = a.aniocampana
	    WHERE numberofdeliveredmessages > 0
      GROUP BY
		       a.aniocampana
		       , country
		       , yy1_codigoebelista_mps
		       , campaignname
		       , interactioncontentsubject
		       , communicationmedium
		       ) b
		       ON a.aniocampanaenvio = b.aniocampanarecepcion
           AND a.codpais = b.codpais
           AND a.codebelista = b.codebelista
           AND a.descampaniamarketing = b.descampaniamarketing
		       AND a.destitulocontenido = b.destitulocontenido
		       AND a.desmediocomunicacion = b.desmediocomunicacion
  LEFT JOIN                        --apertura
  (		SELECT a.aniocampana as aniocampanaapertura
		       , country codpais
		       , yy1_codigoebelista_mps codebelista
		       , campaignname descampaniamarketing
		       , interactioncontentsubject destitulocontenido
		       , communicationmedium desmediocomunicacion
		       , MIN(interactiontimestamputc) fechaapertura
		       , SUM(numberofopenedmessages) cantidadmensajesabiertos
		  FROM fnc_virtual_coach.fdethybrysdata a      
      inner join #PaisCampanas pc on pc.codpais = a.country and pc.aniocampana = a.aniocampana
		  WHERE numberofopenedmessages > 0
      GROUP BY
		       a.aniocampana
		       , country
		       , yy1_codigoebelista_mps
		       , campaignname
		       , interactioncontentsubject
		       , communicationmedium
		       ) c
		       
		       ON a.aniocampanaenvio = c.aniocampanaapertura
           AND a.codpais = c.codpais
		       AND a.codebelista = c.codebelista
		       AND a.descampaniamarketing = c.descampaniamarketing
		       AND a.destitulocontenido = c.destitulocontenido
		       AND a.desmediocomunicacion = c.desmediocomunicacion
  LEFT JOIN                        --clic
  (		SELECT a.aniocampana as aniocampanaclic
		       , country codpais
		       , yy1_codigoebelista_mps codebelista
		       , campaignname descampaniamarketing
		       , interactioncontentsubject destitulocontenido
		       , communicationmedium desmediocomunicacion
		       , MIN(interactiontimestamputc) fechaclic
		       , SUM(numberofuniqueclicks) cantidadmensajescliqueados
		  FROM fnc_virtual_coach.fdethybrysdata a  
      inner join #PaisCampanas pc on pc.codpais = a.country and pc.aniocampana = a.aniocampana    
	    WHERE numberofuniqueclicks > 0
      GROUP BY
		       a.aniocampana
		       , country
		       , yy1_codigoebelista_mps
		       , campaignname
		       , interactioncontentsubject
		       , communicationmedium
		       ) d
		       
		       ON a.aniocampanaenvio = d.aniocampanaclic
		       AND a.codpais = d.codpais
		       AND a.codebelista = d.codebelista
		       AND a.descampaniamarketing = d.descampaniamarketing
		       AND a.destitulocontenido = d.destitulocontenido
		       AND a.desmediocomunicacion = d.desmediocomunicacion
GROUP BY 
       a.aniocampanaenvio
       , a.codpais
       , a.codebelista
       , a.descampaniamarketing
       , a.destitulocontenido
       , CASE 
         WHEN a.destitulocontenido LIKE '%NUEVA%' THEN 'Nueva'
         WHEN a.destitulocontenido LIKE '%EST%' THEN 'Establecida'
         ELSE 'Otros'
         END
       , CASE
         WHEN a.desmediocomunicacion = 'EMAIL' THEN 'Mail'
         WHEN a.desmediocomunicacion = 'MOBILE_APP' THEN 'Push'
         WHEN a.desmediocomunicacion = 'SMS' THEN 'SMS'
         ELSE 'Otros'
         END
       , a.fechaenvio
       , b.fecharecepcion
       , c.fechaapertura
       , d.fechaclic
       , b.aniocampanarecepcion
       , c.aniocampanaapertura
       , d.aniocampanaclic
) a 
    LEFT JOIN fnc_virtual_coach.fdettemplates b ON a.codpais = b.codpais AND a.descampaniamarketing = b.descampania AND a.destitulocontenido = b.destitulo AND a.aniocampanaenvio = b.aniocampanaexpo;
unload ($$ select * from dom_virtual_coach.det_VC_consolidado
where codpais||aniocampanaenvio in (select codpais||aniocampana from #PaisCampanas)
$$)
to 's3://belc-bigdata-landing-dlk-prd/datalake/output/hana-bi/virtual-coach/dlk_VC_consolidado_' 
access_key_id '{ACCESS_KEY}' 
secret_access_key '{SECRET_KEY}' 
delimiter '\t'
--maxfilesize 250 mb
parallel OFF 
ALLOWOVERWRITE;