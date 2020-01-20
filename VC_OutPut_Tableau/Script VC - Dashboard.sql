with virtual_coach as (
select aniocampanaenvio as aniocampana,
	   codpais,
	   codebelista, 
	   destipo,
	   sum(cantidadmensajesenviados) as enviados,
	   sum (cantidadmensajesentregados) as recibidos,
	   sum(cantidadmensajesabiertos) as abiertos,
	   sum (cantidadmensajescliqueados) as clicks
  from dom_virtual_coach.det_vc_consolidado
 group by 
	   aniocampanaenvio,
	   codpais,
	   codebelista,
	   destipo
)
select  ebe.codpais, 
        ebe.aniocampana, 
        ebe.codebelista, 
        st.desstatus,
        st.desstatuscorp,
        NVL(destipo,'Sin Mensaje') destipo,
        case when ebe.flagactiva = 1 then 'Sí' else 'No' end flagactiva,
        NVL2(act_ini.codebelista,1,0) flagactivainicial,
        nvl(enviados,0) enviados,
        nvl(recibidos,0) recibidos,
        nvl(abiertos,0) abiertos,
        nvl(clicks,0) clicks,
        nvl(realvtamnneto,0) realvtamnneto,
        nvl(realvtadol,0) realvtadol,
        nvl(pedidos,0) pedidos
   from virtual_coach vc,
        fnc_analitico.dwh_fstaebecam ebe,
        fnc_analitico.dwh_dstatus st,
		(select vta.codpais, vta.aniocampana, vta.codebelista, sum(vta.realvtamnneto) realvtamnneto,
				sum(vta.realvtamnneto/(case when tc.estado_cierre=3 then vta.esttcpromedio else vta.realtcpromedio end)) as realvtadol,
				1 as pedidos
		   from fnc_analitico.dwh_fvtaproebecam vta,
				fnc_analitico.dwh_dtipooferta dtipo,
				(select distinct cod_pais, aniocampana, estado_cierre
				   from fnc_analitico.ctr_cierre_generico 
				) tc,
				fnc_analitico.dwh_fstaebecam ebe
		  where vta.codpais = dtipo.codpais 
			and vta.codtipooferta = dtipo.codtipooferta
			and vta.codpais = tc.cod_pais(+)
			and vta.aniocampana = tc.aniocampana(+)
			and	vta.aniocampana > 201818
			and dtipo.codtipoprofit = '01'
			and vta.aniocampana = ebe.aniocampana
   			and vta.codpais = ebe.codpais
   			and vta.codebelista = ebe.codebelista
			and ebe.flagpasopedido = 1
		  group by vta.codpais, vta.aniocampana, vta.codebelista) vta,
		(select codpais, codebelista, aniocampana,
                case when SUBSTRING(aniocampana,5) = '18' then to_number(SUBSTRING(aniocampana,1,4),'9999') +1||'01'
                else to_number(aniocampana, '999999') + 1 ||'' end aniocampana_sig
           from fnc_analitico.dwh_fstaebecam
          where flagactiva = 1
            and aniocampana >= 201818) act_ini
 where ebe.aniocampana = vc.aniocampana(+)
   and ebe.codpais = vc.codpais(+)
   and ebe.codebelista = vc.codebelista(+)
   and ebe.codpais = st.codpais (+)
   and ebe.codstatus = st.codstatus (+)
   and ebe.aniocampana = vta.aniocampana(+)
   and ebe.codpais = vta.codpais(+)
   and ebe.codebelista = vta.codebelista(+)
   and ebe.aniocampana = act_ini.aniocampana_sig(+)
   and ebe.codpais = act_ini.codpais(+)
   and ebe.codebelista = act_ini.codebelista(+)
   and ebe.aniocampana > 201818