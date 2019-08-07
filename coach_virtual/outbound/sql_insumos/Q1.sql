DROP TABLE IF EXISTS #fdetnegocioconsultora;
SELECT 	TOP 1 *
INTO	#fdetnegocioconsultora
FROM 	fnc_virtual_coach.fdetnegocioconsultora;

DELETE  #fdetnegocioconsultora;

DROP TABLE IF EXISTS #PaisCampanas_tmp;
SELECT trim(to_char(%s, '999999')) AS aniocampana_proceso
INTO #PaisCampanas_tmp;

 /*Se agrega un inner join para que se haga un mactch con la tabla de Campañas de facturación*/

DROP TABLE IF EXISTS #PaisCampanas;
SELECT DISTINCT country as codpais, aniocampana, '000000' AS ANIOCAMPANA_U6C, '000000' AS ANIOCAMPANA_U1C
INTO #PaisCampanas
FROM fnc_virtual_coach.fdethybrysdata a
inner join fnc_analitico.ctr_cierre_generico c on a.country = c.cod_pais and a.aniocampana >= c.aniocampana
WHERE EXISTS
             (      SELECT       *
                    FROM #PaisCampanas_tmp b
                    WHERE a.aniocampana = b.aniocampana_proceso
             )
AND length(codpais) = 2
and c.estado_sicc = '0'
order by 1,2;

UPDATE #PaisCampanas  SET ANIOCAMPANA_U6C = f_calculaaniocampana(codpais, aniocampana, -6);
UPDATE #PaisCampanas  SET ANIOCAMPANA_U1C = f_calculaaniocampana(codpais, aniocampana, -1);



DROP TABLE IF EXISTS #Consultoras_base;
SELECT 	A.CodPais, A.CodEbelista, 				( SELECT 	g.aniocampana
 													FROM 	#PaisCampanas g
										 			WHERE	a.CodPais = g.CodPais AND
										 					a.AnioCampana = g.ANIOCAMPANA_U1C ) AS aniocampana
INTO 	#Consultoras_base
FROM 	fnc_analitico.dwh_fstaebecam a
WHERE  	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	a.CodPais = b.CodPais AND
 					a.AnioCampana = b.ANIOCAMPANA_U1C
 		) AND
		a.FlagActiva = 1;

INSERT INTO	#Consultoras_base
(CodPais, CodEbelista, aniocampana)
SELECT 	A.CodPais, CodEbelista,  				( SELECT 	g.aniocampana
 													FROM 	#PaisCampanas g
										 			WHERE	a.CodPais = g.CodPais AND
										 					a.AnioCampana = g.ANIOCAMPANA ) AS aniocampana
FROM 	fnc_analitico.dwh_fstaebecam a
		INNER JOIN fnc_analitico.dwh_dstatus e ON a.codpais = e.codpais and a.codstatus= e.codstatus
WHERE  	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	a.CodPais = b.CodPais AND
 					a.AnioCampana = b.ANIOCAMPANA
 		) AND
		e.codstatuscorp = 1 ;

INSERT INTO #fdetnegocioconsultora
		(CodPais, CodEbelista, AnioCampana)
SELECT 	 distinct CodPais, CodEbelista, AnioCampana
FROM 	#Consultoras_base;


DROP TABLE IF EXISTS #Consultoras_tmp;
SELECT 	DISTINCT a.CodPais,
		a.CodStatus,
       	a.CodEbelista,
       	a.codterritorio,
       	a.AnioCampana,
       	a.COnstancia,
       	a.FlagPasoPedido,
       	a.FlagIPUnicoZona,
       	a.descripcionrolling as DesNivelComportamiento,
       	a.CodigoFacturaInternet,
       	a.flagofertadigital,
       	a.flagpasopedidoweb,
       	a.flagactiva
INTO	#Consultoras_tmp
FROM   	fnc_analitico.dwh_fstaebecam a
WHERE  	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas d
 			WHERE	a.CodPais = d.CodPais AND
 					a.aniocampana BETWEEN d.AnioCampana_U6C and d.AnioCampana
 		);


DROP TABLE IF EXISTS #Consultoras;
SELECT 	a.CodPais,
       	--c.DesPais,
       	a.CodEbelista,
       	a.codterritorio,
       	e.codstatuscorp,
       	--b.Desapenom,
       	a.AnioCampana,
       	a.COnstancia,
       	a.FlagPasoPedido,
       	a.FlagIPUnicoZona,
       	a.DesNivelComportamiento,
       	a.CodigoFacturaInternet,
       	a.flagofertadigital,
       	a.flagpasopedidoweb,
       	a.flagactiva
INTO	#Consultoras
FROM   	#Consultoras_tmp a
	   	--INNER JOIN fnc_analitico.dwh_debelista b ON a.codpais = b.codpais and a.codEbelista = b.codEbelista
	   	INNER JOIN fnc_analitico.dwh_dstatus e ON a.codpais = e.codpais and a.codstatus= e.codstatus
		--INNER JOIN fnc_analitico.dwh_dpais c ON a.codpais = c.codpais
WHERE  EXISTS
 		(	SELECT 	*
 			FROM 	#Consultoras_base F
 			WHERE	a.CodPais = F.CodPais AND
 					a.codebelista = f.codebelista
 		);

DROP TABLE IF EXISTS #Consultoras_tmp;


DROP TABLE IF EXISTS #tmp_total_dwh_fvtaproebecam_tmp;
select 	a.codpais, a.aniocampana, a.aniocampanaref, a.codsap, a.codtipooferta, a.codventa, a.codebelista, a.realuuvendidas, a.realvtamncatalogo, a.realtcpromedio, a.RealVtaMNNeto, a.RealVtaMNFactura, 0 as nrotop, 0 as nrolanzamiento
into 	#tmp_total_dwh_fvtaproebecam_tmp
from 	fnc_analitico.dwh_fvtaproebecam a
WHERE  	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	a.CodPais = b.CodPais AND
 					a.AnioCampana BETWEEN B.ANIOCAMPANA_U6C AND b.AnioCampana
 		);

DROP TABLE IF EXISTS #tmp_total_dwh_fvtaproebecam;
select 	a.codpais, a.aniocampana, a.aniocampanaref, a.codsap, a.codtipooferta, a.codventa, a.codebelista, a.realuuvendidas, a.realvtamncatalogo, a.realtcpromedio, a.RealVtaMNNeto, a.RealVtaMNFactura, 0 as nrotop, 0 as nrolanzamiento, d.cuc as codcuc
into 	#tmp_total_dwh_fvtaproebecam
from 	#tmp_total_dwh_fvtaproebecam_tmp a
		INNER JOIN fnc_analitico.dwh_dproducto d on a.codsap = d.codsap
		INNER JOIN fnc_analitico.dwh_dtipooferta e on a.codpais = e.codpais and a.codtipooferta = e.codtipooferta
WHERE  EXISTS
 		(	SELECT 	*
 			FROM 	#Consultoras_base F
 			WHERE	a.CodPais = F.CodPais AND
 					a.codebelista = f.codebelista
 		) AND E.codtipoprofit = '01';
DROP TABLE IF EXISTS #tmp_total_dwh_fvtaproebecam_tmp;

DROP TABLE IF EXISTS #TMP_dtiempoactividadzona;
SELECT 	distinct codpais, aniocampana, codregion, codzona, codactividad, fecha, numdia
INTO 	#TMP_dtiempoactividadzona
FROM 	fnc_analitico.dwh_dtiempoactividadzona a
WHERE 	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	a.CodPais = b.CodPais AND
 					a.AnioCampana BETWEEN b.AnioCampana_U1C and b.AnioCampana
 		) AND a.CodActividad = 'RF';


DROP TABLE IF EXISTS #productotopylanzamientos;
SELECT 	A.CODPAIS,
		A.codcuc,
		a.DESCRIPCION,
		CASE WHEN a.DESCRIPCION = 'Tops_M_C' THEN 1 ELSE 0 END AS EsTop
INTO 	#productotopylanzamientos
FROM 	sbx_analytics.armsg_lanz_top a
WHERE  	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas c
 			WHERE	a.CodPais = c.CodPais AND
 					a.aniocampanaexpo = c.AnioCampana
 		) AND
 		ltrim(rtrim(a.DESCRIPCION)) IN ('Tops_M_C','Lanzamientos') AND
		a.FLAGUSO = 1;


DROP TABLE IF EXISTS #dgeografiacampana;
SELECT DISTINCT codpais, aniocampana, codterritorio, codregion, codzona
INTO  #dgeografiacampana
FROM	fnc_analitico.dwh_dgeografiacampana a
WHERE	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas c
 			WHERE	a.CodPais = c.CodPais AND
 					a.aniocampana BETWEEN c.AnioCampana_U6C and c.AnioCampana
 		);

DROP TABLE IF EXISTS #fdetzonascontrol;
SELECT DISTINCT codpais, codzona, flagcontrol
INTO 	#fdetzonascontrol
FROM	fnc_virtual_coach.fdetzonascontrol a
WHERE	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas c
 			WHERE	a.CodPais = c.CodPais
 		);


drop table if EXISTS #tmp_total_dwh_fvtaproebecam_top;
select  a.codpais, a.codebelista, count(distinct a.aniocampana) as  nrotop
INTO	#tmp_total_dwh_fvtaproebecam_top
from 	#tmp_total_dwh_fvtaproebecam a
		inner join #productotopylanzamientos b on a.codpais = b.codpais and a.codcuc = b.codcuc
WHERE	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas c
 			WHERE	a.CodPais = c.CodPais AND
 					a.aniocampana BETWEEN c.AnioCampana_U6C and c.AnioCampana_U1C
 		) AND
		b.estop = 1
group by a.codpais, a.codebelista;


drop table if EXISTS #tmp_total_dwh_fvtaproebecam_lanzamiento;
select  a.codpais, a.codebelista, count(distinct a.aniocampana) as  nrolanzamiento
INTO	#tmp_total_dwh_fvtaproebecam_lanzamiento
from 	#tmp_total_dwh_fvtaproebecam a
		inner join #productotopylanzamientos b on a.codpais = b.codpais and a.codcuc = b.codcuc
WHERE	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas c
 			WHERE	a.CodPais = c.CodPais AND
 					a.aniocampana BETWEEN c.AnioCampana_U6C and c.AnioCampana_U1C
 		) AND
		b.estop = 0
group by a.codpais, a.codebelista;


DROP TABLE IF EXISTS #tmp_dwh_fvtaproebecam;
SELECT	a.codpais, a.aniocampana, a.codebelista, b.desmarca, b.descategoria, a.realuuvendidas, a.realvtamncatalogo, a.realtcpromedio, RealVtaMNNeto, RealVtaMNFactura
INTO 	#tmp_dwh_fvtaproebecam
FROM	#tmp_total_dwh_fvtaproebecam a
		INNER JOIN fnc_analitico.dwh_dproducto b ON A.codsap = B.codsap
WHERE 	EXISTS
		( SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	a.CodPais = b.CodPais AND
 					a.AnioCampana = b.AnioCampana
 		);

DROP TABLE IF EXISTS #VentaCampania;
SELECT 	Codpais,
		AnioCampana,
		CodEbelista,
		SUM(RealVtaMNCatalogo) AS VentaCatalogomn,
		SUM(RealVtaMNNeto) AS VentaNetomn,
		SUM(RealVtaMNFactura) AS VentaFacturamn,
		SUM(RealVtaMNNeto/ RealTCpromedio) AS VentaNetodol,
		SUM(RealVtaMNFactura/ RealTCpromedio) AS VentaFacturadol,
       	SUM(CASE WHEN DesMarca = 'ESIKA' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolES,
       	SUM(CASE WHEN DesMarca = 'CYZONE' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolCY,
       	SUM(CASE WHEN DesMarca = 'L''BEL' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolLB,
       	SUM(CASE WHEN DesCategoria = 'TRATAMIENTO FACIAL' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolTF,
       	SUM(CASE WHEN DesCategoria = 'TRATAMIENTO CORPORAL' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolTC,
      	SUM(CASE WHEN DesCategoria = 'CUIDADO PERSONAL' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolCP,
       	SUM(CASE WHEN DesCategoria = 'MAQUILLAJE' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolMQ,
       	SUM(CASE WHEN DesCategoria = 'FRAGANCIAS' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolFR,
       	SUM(RealVtaMNCatalogo / RealTCpromedio) VentaCatalogoDol,
       	SUM(CASE WHEN DesMarca = 'ESIKA' THEN RealUUVendidas END) UnidadesES,
       	SUM(CASE WHEN DesMarca = 'CYZONE' THEN RealUUVendidas END) UnidadesCY,
       	SUM(CASE WHEN DesMarca = 'L''BEL' THEN RealUUVendidas END) UnidadesLB,
       	SUM(CASE WHEN DesCategoria = 'TRATAMIENTO FACIAL' THEN RealUUVendidas END) UnidadesTF,
       	SUM(CASE WHEN DesCategoria = 'TRATAMIENTO CORPORAL' THEN RealUUVendidas END) UnidadesTC,
       	SUM(CASE WHEN DesCategoria = 'CUIDADO PERSONAL' THEN RealUUVendidas END) UnidadesCP,
       	SUM(CASE WHEN DesCategoria = 'MAQUILLAJE' THEN RealUUVendidas END) UnidadesMQ,
       	SUM(CASE WHEN DesCategoria = 'FRAGANCIAS' THEN RealUUVendidas END) UnidadesFR,
       	SUM(RealUUVendidas) Unidades
INTO 	#VentaCampania
FROM 	#tmp_dwh_fvtaproebecam a
group by aniocampana, codpais, CodEbelista;


DROP TABLE IF EXISTS #tmp_dwh_fvtaproebecam_U6C_U1C;
SELECT	a.codpais, a.codebelista, b.desmarca, b.descategoria, a.realuuvendidas, a.realvtamncatalogo, a.realtcpromedio, a.RealVtaMNNeto, a.RealVtaMNFactura
INTO 	#tmp_dwh_fvtaproebecam_U6C_U1C
FROM	#tmp_total_dwh_fvtaproebecam a
		INNER JOIN fnc_analitico.dwh_dproducto b ON A.codsap = B.codsap
WHERE  	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	a.CodPais = b.CodPais AND
 					a.AnioCampana BETWEEN b.ANIOCAMPANA_U6C AND b.AnioCampana_U1C
 		);

DROP TABLE IF EXISTS #VentaCampania_U6C;
SELECT 	codpais,
		(select aniocampana from #PaisCampanas g WHERE g.codpais = a.codpais) as aniocampana,
		CodEbelista,
		SUM(RealVtaMNCatalogo) AS VentaCatalogomn_U6C,
		SUM(RealVtaMNNeto) AS VentaNetomn_U6C,
		SUM(RealVtaMNFactura) AS VentaFacturamn_U6C,
		SUM(RealVtaMNNeto/ RealTCpromedio) AS VentaNetodol_U6C,
		SUM(RealVtaMNFactura/ RealTCpromedio) AS VentaFacturadol_U6C,
		SUM(CASE WHEN DesMarca = 'ESIKA' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolES_U6C,
       	SUM(CASE WHEN DesMarca = 'CYZONE' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolCY_U6C,
       	SUM(CASE WHEN DesMarca = 'L''BEL' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolLB_U6C,
       	SUM(CASE WHEN DesCategoria = 'TRATAMIENTO FACIAL' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolTF_U6C,
       	SUM(CASE WHEN DesCategoria = 'TRATAMIENTO CORPORAL' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolTC_U6C,
       	SUM(CASE WHEN DesCategoria = 'CUIDADO PERSONAL' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolCP_U6C,
       	SUM(CASE WHEN DesCategoria = 'MAQUILLAJE' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolMQ_U6C,
       	SUM(CASE WHEN DesCategoria = 'FRAGANCIAS' THEN RealVtaMNCatalogo / RealTCpromedio END) VentaCatalogoDolFR_U6C,
       	SUM(RealVtaMNCatalogo / RealTCpromedio) VentaCatalogoDol_U6C,
       	SUM(CASE WHEN DesMarca = 'ESIKA' THEN RealUUVendidas END) UnidadesES_U6C,
       	SUM(CASE WHEN DesMarca = 'CYZONE' THEN RealUUVendidas END) UnidadesCY_U6C,
       	SUM(CASE WHEN DesMarca = 'L''BEL' THEN RealUUVendidas END) UnidadesLB_U6C,
       	SUM(CASE WHEN DesCategoria = 'TRATAMIENTO FACIAL' THEN RealUUVendidas END) UnidadesTF_U6C,
       	SUM(CASE WHEN DesCategoria = 'TRATAMIENTO CORPORAL' THEN RealUUVendidas END) UnidadesTC_U6C,
       	SUM(CASE WHEN DesCategoria = 'CUIDADO PERSONAL' THEN RealUUVendidas END) UnidadesCP_U6C,
       	SUM(CASE WHEN DesCategoria = 'MAQUILLAJE' THEN RealUUVendidas END) UnidadesMQ_U6C,
       	SUM(CASE WHEN DesCategoria = 'FRAGANCIAS' THEN RealUUVendidas END) UnidadesFR_U6C,
       	SUM(RealUUVendidas) Unidades_U6C
INTO 	#VentaCampania_U6C
FROM 	#tmp_dwh_fvtaproebecam_U6C_U1C a
GROUP By codpais,  CodEbelista;


DROP TABLE IF EXISTS #StatusCampania;
SELECT  DISTINCT a.codpais,
		a.aniocampana,
		CodEbelista,
       	COnstancia,
       	FlagPasoPedido,
       	FlagIPUnicoZona,
       	DesNivelComportamiento,
       	d.CodZona,
       	CodigoFacturaInternet,
       	flagofertadigital,
       	Fecha FechaFin,
       	flagpasopedidoweb
INTO 	#StatusCampania
FROM 	#Consultoras a
       INNER JOIN #dgeografiacampana d ON a.codpais = d.codpais and a.AnioCampana = d.AnioCampana AND a.codTerritorio = d.codTerritorio
       INNER JOIN #TMP_dtiempoactividadzona e ON e.codpais = d.codpais and e.AnioCampana = d.AnioCampana AND e.CodZona = d.CodZona
WHERE  	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	a.CodPais = b.CodPais AND
 					a.AnioCampana = b.AnioCampana
 		);

DROP TABLE IF EXISTS #StatusCampania_U6C;
SELECT 	codpais,
		(select aniocampana from #PaisCampanas g WHERE g.codpais = a.codpais) AS Aniocampana,
		CodEbelista,
       	SUM(CASE WHEN FlagIPUnicoZona = TRUE THEN 1 ELSE 0 END) FlagIPUnicoZona_U6C,
       	SUM(CASE WHEN FlagPasoPedido = TRUE THEN 1 ELSE 0 END) FlagPasoPedido_U6C,
       	sum(flagofertadigital) AS flagofertadigital_U6C,
       	SUM(CASE WHEN FlagPasoPedidoWeb = TRUE THEN 1 ELSE 0 END) FlagPasoPedidoWeb_U6C
INTO 	#StatusCampania_U6C
FROM 	#Consultoras a
WHERE  	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	a.CodPais = b.CodPais AND
 					a.AnioCampana BETWEEN b.ANIOCAMPANA_U6C AND b.AnioCampana_U1C
 		) AND
 		FlagActiva = 1
GROUP By codpais, CodEbelista;


DROP TABLE IF EXISTS #calendario;
select  dwh_dtiempoactividadzona.codpais,  dwh_dtiempoactividadzona.aniocampana, Ini.fechaIni, MAX(DATEADD(DAY, 1, Fecha)) as fechaFin
into 	#calendario
from 	fnc_analitico.dwh_dtiempoactividadzona,
		(
			Select  codpais,  aniocampana, MIN(Fecha) as fechaIni --MIN(DATEADD(DAY, 1, Fecha)) as fechaIni
			from fnc_analitico.dwh_dtiempoactividadzona
			where
				EXISTS
		 		(	SELECT 	*
		 			FROM 	#PaisCampanas b
		 			WHERE	dwh_dtiempoactividadzona.CodPais = b.CodPais AND
		 					dwh_dtiempoactividadzona.AnioCampana = b.AnioCampana_U1C
		 		) and
		 		codactividad = 'RF'
			GROUP BY CODPAIS,  aniocampana
		) Ini
where  	dwh_dtiempoactividadzona.codpais = Ini.codpais AND
		EXISTS
		 		(	SELECT 	*
		 			FROM 	#PaisCampanas b
		 			WHERE	dwh_dtiempoactividadzona.CodPais = b.CodPais AND
		 					dwh_dtiempoactividadzona.AnioCampana = b.AnioCampana
		 		) and
		dwh_dtiempoactividadzona.codactividad = 'RF'
GROUP BY dwh_dtiempoactividadzona.CODPAIS,  dwh_dtiempoactividadzona.aniocampana, Ini.fechaIni;

DROP TABLE IF EXISTS #StatusCampania_Menos1;
SELECT 	DISTINCT a.codpais,
		(select aniocampana from #PaisCampanas g WHERE g.codpais = a.codpais) as aniocampana,
		a.codebelista,
       	d.codzona,
       	DATEADD(DAY, 1, e.Fecha) FechaIni
INTO 	#StatusCampania_Menos1
FROM 	#Consultoras a
       	INNER JOIN #dgeografiacampana d ON LTRIM(RTRIM(a.codpais)) = LTRIM(RTRIM(d.codpais)) and LTRIM(RTRIM(a.AnioCampana)) = LTRIM(RTRIM(d.AnioCampana)) AND LTRIM(RTRIM(a.codTerritorio)) = LTRIM(RTRIM(d.codTerritorio))
       	INNER JOIN #TMP_dtiempoactividadzona e ON LTRIM(RTRIM(a.codpais)) = LTRIM(RTRIM(e.codpais)) and LTRIM(RTRIM(a.AnioCampana)) = LTRIM(RTRIM(e.AnioCampana)) AND LTRIM(RTRIM(d.CodZona)) = LTRIM(RTRIM(e.CodZona))
WHERE  	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	a.CodPais = b.CodPais AND
 					a.AnioCampana = b.AnioCampana_U1C
 		);

DROP TABLE IF EXISTS #FechaZona_Menos1;
SELECT 	DISTINCT a.codpais,
		(select aniocampana from #PaisCampanas g WHERE g.codpais = a.codpais) as aniocampana,
		a.CodZona,
       	DATEADD(DAY, 1, a.Fecha) FechaIni
INTO 	#FechaZona_Menos1
FROM 	#TMP_dtiempoactividadzona a
WHERE  	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	a.CodPais = b.CodPais AND
 					a.AnioCampana = b.AnioCampana_U1C
 		);


UPDATE 	#fdetnegocioconsultora
SET 	VentaCatalogomn = b.VentaCatalogomn,
		VentaNetomn = b.VentaNetomn,
		VentaFacturamn = b.VentaFacturamn,
		VentaCatalogodol = b.VentaCatalogodol,
		VentaNetodol = b.VentaNetodol,
		VentaFacturadol = b.VentaFacturadol,
		VentaEsika = b.VentaCatalogoDolES,
       	VentaCyzone = b.VentaCatalogoDolCY,
       	VentaLbel = b.VentaCatalogoDolLB,
       	VentaTF = b.VentaCatalogoDolTF,
       	VentaTC = b.VentaCatalogoDolTC,
       	VentaCP = b.VentaCatalogoDolCP,
       	VentaMQ = b.VentaCatalogoDolMQ,
       	VentaFG = b.VentaCatalogoDolFR,
       	VentaTotal = b.VentaCatalogoDol,
       	UnidadesEsika = b.UnidadesES,
       	UnidadesCyzone = b.UnidadesCY,
       	UnidadesLBel = b.UnidadesLB,
       	UnidadesTF = b.UnidadesTF,
       	UnidadesTC = b.UnidadesTC,
       	UnidadesCP = b.UnidadesCP,
       	UnidadesMQ = b.UnidadesMQ,
       	UnidadesFG = b.UnidadesFR,
       	Unidades = b.Unidades
FROM 	#VentaCampania b
WHERE  	#fdetnegocioconsultora.codpais = b.codpais AND
 		#fdetnegocioconsultora.AnioCampana = b.AnioCampana AND
 		#fdetnegocioconsultora.CodEbelista = b.CodEbelista;

UPDATE 	#fdetnegocioconsultora
SET 	VentaCatalogomn_U6C = b.VentaCatalogomn_U6C,
		VentaNetomn_U6C = b.VentaNetomn_U6C,
		VentaFacturamn_U6C = b.VentaFacturamn_U6C,
		VentaCatalogodol_U6C = b.VentaCatalogodol_U6C,
		VentaNetodol_U6C = b.VentaNetodol_U6C,
		VentaFacturadol_U6C = b.VentaFacturadol_U6C,
		VentaEsika_U6C = b.VentaCatalogoDolES_U6C,
       	VentaCyzone_U6C = b.VentaCatalogoDolCY_U6C,
       	VentaLbel_U6C = b.VentaCatalogoDolLB_U6C,
       	VentaTF_U6C = b.VentaCatalogoDolTF_U6C,
       	VentaTC_U6C = b.VentaCatalogoDolTC_U6C,
       	VentaCP_U6C = b.VentaCatalogoDolCP_U6C,
       	VentaMQ_U6C = b.VentaCatalogoDolMQ_U6C,
       	VentaFG_U6C = b.VentaCatalogoDolFR_U6C,
       	VentaTotal_U6C = b.VentaCatalogoDol_U6C,
       	UnidadesEsika_U6C = b.UnidadesES_U6C,
       	UnidadesCyzone_U6C = b.UnidadesCY_U6C,
       	UnidadesLBel_U6C = b.UnidadesLB_U6C,
       	UnidadesTF_U6C = b.UnidadesTF_U6C,
       	UnidadesTC_U6C = b.UnidadesTC_U6C,
       	UnidadesCP_U6C = b.UnidadesCP_U6C,
       	UnidadesMQ_U6C = b.UnidadesMQ_U6C,
       	UnidadesFG_U6C = b.UnidadesFR_U6C,
       	Unidades_U6C = b.Unidades_U6C
FROM 	#VentaCampania_U6C b
WHERE 	#fdetnegocioconsultora.codpais = b.codpais AND
 		#fdetnegocioconsultora.AnioCampana = b.AnioCampana AND
 		#fdetnegocioconsultora.CodEbelista = b.CodEbelista;

UPDATE 	#fdetnegocioconsultora
SET 	Constancia = b.Constancia,
       	FlagPasoPedido = b.FlagPasoPedido,
       	FlagIPUnico = b.FlagIPUnicoZona,
       	ComportamientoRolling = b.DesNivelComportamiento,
       	CodZona = b.CodZona,
       	CanalIngresoPedido = b.CodigoFacturaInternet,
       	flagofertadigital = b.flagofertadigital,
       	FechaFin = b.FechaFin,
       	FlagPasoPedidoWeb = b.FlagPasoPedidoWeb
FROM 	#StatusCampania b
WHERE 	#fdetnegocioconsultora.codpais = b.codpais AND
 		#fdetnegocioconsultora.aniocampana = b.aniocampana AND
		#fdetnegocioconsultora.CodEbelista = b.CodEbelista;

UPDATE 	#fdetnegocioconsultora
SET 	FlagIPUnico_U6C = b.FlagIPUnicoZona_U6C,
       	FlagPasoPedido_U6C = b.FlagPasoPedido_U6C,
       	flagofertadigital_U6C = b.flagofertadigital_U6C,
       	FlagPasoPedidoWeb_u6c = b.FlagPasoPedidoWeb_u6c
FROM 	#StatusCampania_U6C b
WHERE 	#fdetnegocioconsultora.codpais = b.codpais AND
		#fdetnegocioconsultora.aniocampana = b.aniocampana AND
 		#fdetnegocioconsultora.CodEbelista = b.CodEbelista;

UPDATE 	#fdetnegocioconsultora
SET 	NroLanzamientos = b.nrolanzamiento
FROM 	#tmp_total_dwh_fvtaproebecam_lanzamiento b
WHERE 	#fdetnegocioconsultora.codpais = b.codpais AND
 		#fdetnegocioconsultora.CodEbelista = b.CodEbelista;

UPDATE 	#fdetnegocioconsultora
SET 	NroTops = b.nrotop
FROM 	#tmp_total_dwh_fvtaproebecam_top b
WHERE 	#fdetnegocioconsultora.codpais = b.codpais AND
 		#fdetnegocioconsultora.CodEbelista = b.CodEbelista;




UPDATE 	#fdetnegocioconsultora
SET 	FechaIni = b.FechaIni,
		FechaFin = b.FechaFin
FROM 	#calendario b
WHERE 	#fdetnegocioconsultora.codpais = b.codpais AND
 		#fdetnegocioconsultora.aniocampana = b.aniocampana ;


UPDATE  #fdetnegocioconsultora
SET		flagcontrol = b.flagcontrol
FROM	#fdetzonascontrol b
WHERE	#fdetnegocioconsultora.codpais = b.codpais AND
		#fdetnegocioconsultora.codzona = b.codzona;



UPDATE	#fdetnegocioconsultora
SET		flagpasopedido = CASE WHEN flagpasopedido IS NULL THEN 0 ELSE flagpasopedido END,
		flagpasopedido_u6c = CASE WHEN flagpasopedido_u6c IS NULL THEN 0 ELSE flagpasopedido_u6c END,
 		flagipunico = CASE WHEN flagipunico IS NULL THEN 0 ELSE flagipunico END,
		ventacatalogomn = CASE WHEN ventacatalogomn IS NULL THEN 0 ELSE ventacatalogomn END,
		ventanetomn = CASE WHEN ventanetomn IS NULL THEN 0 ELSE ventanetomn END,
		ventafacturamn = CASE WHEN ventafacturamn IS NULL THEN 0 ELSE ventafacturamn END,
		ventacatalogomn_u6c = CASE WHEN ventacatalogomn_u6c IS NULL THEN 0 ELSE ventacatalogomn_u6c END,
		ventanetomn_u6c = CASE WHEN ventanetomn_u6c IS NULL THEN 0 ELSE ventanetomn_u6c END,
		ventafacturamn_u6c = CASE WHEN ventafacturamn_u6c IS NULL THEN 0 ELSE ventafacturamn_u6c END,
		ventaesika = CASE WHEN ventaesika IS NULL THEN 0 ELSE ventaesika END,
		ventacyzone = CASE WHEN ventacyzone IS NULL THEN 0 ELSE ventacyzone END,
		ventalbel = CASE WHEN ventalbel IS NULL THEN 0 ELSE ventalbel END,
		ventatf = CASE WHEN ventatf IS NULL THEN 0 ELSE ventatf END,
		ventatc = CASE WHEN ventatc IS NULL THEN 0 ELSE ventatc END,
		ventacp = CASE WHEN ventacp IS NULL THEN 0 ELSE ventacp END,
		ventamq = CASE WHEN ventamq IS NULL THEN 0 ELSE ventamq END,
		ventafg = CASE WHEN ventafg IS NULL THEN 0 ELSE ventafg END,
		ventaesika_u6c = CASE WHEN ventaesika_u6c IS NULL THEN 0 ELSE ventaesika_u6c END,
		ventacyzone_u6c = CASE WHEN ventacyzone_u6c IS NULL THEN 0 ELSE ventacyzone_u6c END,
		ventalbel_u6c = CASE WHEN ventalbel_u6c IS NULL THEN 0 ELSE ventalbel_u6c END,
		ventatf_u6c = CASE WHEN ventatf_u6c IS NULL THEN 0 ELSE ventatf_u6c END,
		ventatc_u6c = CASE WHEN ventatc_u6c IS NULL THEN 0 ELSE ventatc_u6c END,
		ventacp_u6c = CASE WHEN ventacp_u6c IS NULL THEN 0 ELSE ventacp_u6c END,
		ventamq_u6c = CASE WHEN ventamq_u6c IS NULL THEN 0 ELSE ventamq_u6c END,
		ventafg_u6c = CASE WHEN ventafg_u6c IS NULL THEN 0 ELSE ventafg_u6c END,
		unidadesesika = CASE WHEN unidadesesika IS NULL THEN 0 ELSE unidadesesika END,
		unidadescyzone = CASE WHEN unidadescyzone IS NULL THEN 0 ELSE unidadescyzone END,
		unidadeslbel = CASE WHEN unidadeslbel IS NULL THEN 0 ELSE unidadeslbel END,
		unidadestf = CASE WHEN unidadestf IS NULL THEN 0 ELSE unidadestf END,
		unidadestc = CASE WHEN unidadestc IS NULL THEN 0 ELSE unidadestc END,
		unidadescp = CASE WHEN unidadescp IS NULL THEN 0 ELSE unidadescp END,
		unidadesmq = CASE WHEN unidadesmq IS NULL THEN 0 ELSE unidadesmq END,
		unidadesfg = CASE WHEN unidadesfg IS NULL THEN 0 ELSE unidadesfg END,
		unidadesesika_u6c = CASE WHEN unidadesesika_u6c IS NULL THEN 0 ELSE unidadesesika_u6c END,
		unidadescyzone_u6c = CASE WHEN unidadescyzone_u6c IS NULL THEN 0 ELSE unidadescyzone_u6c END,
		unidadeslbel_u6c = CASE WHEN unidadeslbel_u6c IS NULL THEN 0 ELSE unidadeslbel_u6c END,
		unidadestf_u6c = CASE WHEN unidadestf_u6c IS NULL THEN 0 ELSE unidadestf_u6c END,
		unidadestc_u6c = CASE WHEN unidadestc_u6c IS NULL THEN 0 ELSE unidadestc_u6c END,
		unidadescp_u6c = CASE WHEN unidadescp_u6c IS NULL THEN 0 ELSE unidadescp_u6c END,
		unidadesmq_u6c = CASE WHEN unidadesmq_u6c IS NULL THEN 0 ELSE unidadesmq_u6c END,
		unidadesfg_u6c = CASE WHEN unidadesfg_u6c IS NULL THEN 0 ELSE unidadesfg_u6c END,
		ventatotal = CASE WHEN ventatotal IS NULL THEN 0 ELSE ventatotal END,
		ventatotal_u6c = CASE WHEN ventatotal_u6c IS NULL THEN 0 ELSE ventatotal_u6c END,
		flagipunico_u6c = CASE WHEN flagipunico_u6c IS NULL THEN 0 ELSE flagipunico_u6c END,
		unidades = CASE WHEN unidades IS NULL THEN 0 ELSE unidades END,
		unidades_u6c = CASE WHEN unidades_u6c IS NULL THEN 0 ELSE unidades_u6c END,
		flagcontrol = CASE WHEN flagcontrol IS NULL THEN 0 ELSE flagcontrol END,
		flagofertadigital = CASE WHEN flagofertadigital IS NULL THEN 0 ELSE flagofertadigital END,
		flagofertadigital_u6c = CASE WHEN flagofertadigital_u6c IS NULL THEN 0 ELSE flagofertadigital_u6c END,
		flagpasopedidoweb = CASE WHEN flagpasopedidoweb IS NULL THEN 0 ELSE flagpasopedidoweb END,
		flagpasopedidoweb_u6c = CASE WHEN flagpasopedidoweb_u6c IS NULL THEN 0 ELSE flagpasopedidoweb_u6c END,
		ventacatalogodol = CASE WHEN ventacatalogodol IS NULL THEN 0 ELSE ventacatalogodol END,
		ventacatalogodol_u6c = CASE WHEN ventacatalogodol_u6c IS NULL THEN 0 ELSE ventacatalogodol_u6c END,
		nrotops = CASE WHEN nrotops IS NULL THEN 0 ELSE nrotops END,
		nrolanzamientos = CASE WHEN nrolanzamientos IS NULL THEN 0 ELSE nrolanzamientos END,
		nrotops_u6c = CASE WHEN nrotops_u6c IS NULL THEN 0 ELSE nrotops_u6c END,
		nrolanzamientos_u6c = CASE WHEN nrolanzamientos_u6c IS NULL THEN 0 ELSE nrolanzamientos_u6c END,
		constancia = CASE WHEN constancia IS NULL THEN '' ELSE constancia END,
		canalingresopedido = CASE WHEN canalingresopedido IS NULL THEN '' ELSE canalingresopedido END,
		comportamientorolling = CASE WHEN comportamientorolling IS NULL THEN '' ELSE comportamientorolling END
WHERE
		EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	#fdetnegocioconsultora.CodPais = b.CodPais AND
 					#fdetnegocioconsultora.AnioCampana = b.AnioCampana
 		);



DELETE 	fnc_virtual_coach.fdetnegocioconsultora
WHERE  	EXISTS
 		(	SELECT 	*
 			FROM 	#PaisCampanas b
 			WHERE	fdetnegocioconsultora.CodPais = b.CodPais AND
 					fdetnegocioconsultora.AnioCampana = b.AnioCampana
 		);





INSERT	INTO fnc_virtual_coach.fdetnegocioconsultora
(codpais, pais, codebelista, desapenom, aniocampana, comportamientorolling, flagpasopedido, flagpasopedido_u6c,
flagipunico, canalingresopedido, codzona, fechaini, fechafin, ventacatalogomn, ventanetomn, ventafacturamn,
ventacatalogomn_u6c, ventanetomn_u6c, ventafacturamn_u6c, ventaesika, ventacyzone, ventalbel, ventatf, ventatc,
ventacp, ventamq, ventafg, ventaesika_u6c, ventacyzone_u6c, ventalbel_u6c, ventatf_u6c, ventatc_u6c, ventacp_u6c,
ventamq_u6c, ventafg_u6c, unidadesesika, unidadescyzone, unidadeslbel, unidadestf, unidadestc, unidadescp,
unidadesmq, unidadesfg, unidadesesika_u6c, unidadescyzone_u6c, unidadeslbel_u6c, unidadestf_u6c, unidadestc_u6c,
unidadescp_u6c, unidadesmq_u6c, unidadesfg_u6c, ventatotal, ventatotal_u6c, constancia, flagipunico_u6c,
unidades, unidades_u6c, flagcontrol, flagofertadigital, flagofertadigital_u6c, flagpasopedidoweb, flagpasopedidoweb_u6c,
ventacatalogodol, ventacatalogodol_u6c, nrotops, nrolanzamientos, ventanetodol, ventafacturadol, ventanetodol_u6c,
ventafacturadol_u6c, nrotops_u6c, nrolanzamientos_u6c )
SELECT
a.codpais, c.despais, a.codebelista, e.desapenom, aniocampana, comportamientorolling, flagpasopedido, flagpasopedido_u6c,
flagipunico, canalingresopedido, codzona, fechaini, fechafin, ventacatalogomn, ventanetomn, ventafacturamn,
ventacatalogomn_u6c, ventanetomn_u6c, ventafacturamn_u6c, ventaesika, ventacyzone, ventalbel, ventatf, ventatc,
ventacp, ventamq, ventafg, ventaesika_u6c, ventacyzone_u6c, ventalbel_u6c, ventatf_u6c, ventatc_u6c, ventacp_u6c,
ventamq_u6c, ventafg_u6c, unidadesesika, unidadescyzone, unidadeslbel, unidadestf, unidadestc, unidadescp,
unidadesmq, unidadesfg, unidadesesika_u6c, unidadescyzone_u6c, unidadeslbel_u6c, unidadestf_u6c, unidadestc_u6c,
unidadescp_u6c, unidadesmq_u6c, unidadesfg_u6c, ventatotal, ventatotal_u6c, constancia, flagipunico_u6c,
unidades, unidades_u6c, flagcontrol, flagofertadigital, flagofertadigital_u6c, flagpasopedidoweb, flagpasopedidoweb_u6c,
ventacatalogodol, ventacatalogodol_u6c, nrotops, nrolanzamientos, ventanetodol, ventafacturadol, ventanetodol_u6c,
ventafacturadol_u6c, nrotops_u6c, nrolanzamientos_u6c
FROM #fdetnegocioconsultora a
INNER JOIN fnc_analitico.dwh_dpais c ON a.codpais = c.codpais
INNER JOIN fnc_analitico.dwh_debelista e ON a.codpais = e.codpais and a.codebelista = e.codebelista;

--------------------------------------------------------History------------------------------------------
INSERT	INTO wrk_virtual_coach.fdetnegocioconsultora_history
(codpais, pais, codebelista, desapenom, aniocampana, comportamientorolling, flagpasopedido, flagpasopedido_u6c,
flagipunico, canalingresopedido, codzona, fechaini, fechafin, ventacatalogomn, ventanetomn, ventafacturamn,
ventacatalogomn_u6c, ventanetomn_u6c, ventafacturamn_u6c, ventaesika, ventacyzone, ventalbel, ventatf, ventatc,
ventacp, ventamq, ventafg, ventaesika_u6c, ventacyzone_u6c, ventalbel_u6c, ventatf_u6c, ventatc_u6c, ventacp_u6c,
ventamq_u6c, ventafg_u6c, unidadesesika, unidadescyzone, unidadeslbel, unidadestf, unidadestc, unidadescp,
unidadesmq, unidadesfg, unidadesesika_u6c, unidadescyzone_u6c, unidadeslbel_u6c, unidadestf_u6c, unidadestc_u6c,
unidadescp_u6c, unidadesmq_u6c, unidadesfg_u6c, ventatotal, ventatotal_u6c, constancia, flagipunico_u6c,
unidades, unidades_u6c, flagcontrol, flagofertadigital, flagofertadigital_u6c, flagpasopedidoweb, flagpasopedidoweb_u6c,
ventacatalogodol, ventacatalogodol_u6c, nrotops, nrolanzamientos, ventanetodol, ventafacturadol, ventanetodol_u6c,
ventafacturadol_u6c, nrotops_u6c, nrolanzamientos_u6c )
SELECT
a.codpais, c.despais, a.codebelista, e.desapenom, aniocampana, comportamientorolling, flagpasopedido, flagpasopedido_u6c,
flagipunico, canalingresopedido, codzona, fechaini, fechafin, ventacatalogomn, ventanetomn, ventafacturamn,
ventacatalogomn_u6c, ventanetomn_u6c, ventafacturamn_u6c, ventaesika, ventacyzone, ventalbel, ventatf, ventatc,
ventacp, ventamq, ventafg, ventaesika_u6c, ventacyzone_u6c, ventalbel_u6c, ventatf_u6c, ventatc_u6c, ventacp_u6c,
ventamq_u6c, ventafg_u6c, unidadesesika, unidadescyzone, unidadeslbel, unidadestf, unidadestc, unidadescp,
unidadesmq, unidadesfg, unidadesesika_u6c, unidadescyzone_u6c, unidadeslbel_u6c, unidadestf_u6c, unidadestc_u6c,
unidadescp_u6c, unidadesmq_u6c, unidadesfg_u6c, ventatotal, ventatotal_u6c, constancia, flagipunico_u6c,
unidades, unidades_u6c, flagcontrol, flagofertadigital, flagofertadigital_u6c, flagpasopedidoweb, flagpasopedidoweb_u6c,
ventacatalogodol, ventacatalogodol_u6c, nrotops, nrolanzamientos, ventanetodol, ventafacturadol, ventanetodol_u6c,
ventafacturadol_u6c, nrotops_u6c, nrolanzamientos_u6c
FROM #fdetnegocioconsultora a
INNER JOIN fnc_analitico.dwh_dpais c ON a.codpais = c.codpais
INNER JOIN fnc_analitico.dwh_debelista e ON a.codpais = e.codpais and a.codebelista = e.codebelista;
-------------------------------------------------------------------------------------------------------------------------


DROP TABLE IF EXISTS #TMP_dtiempoactividadzona;
DROP TABLE IF EXISTS #PaisCampanas;
DROP TABLE IF EXISTS #Consultoras_base ;
DROP TABLE IF EXISTS #Consultoras ;
DROP TABLE IF EXISTS #productotopylanzamientos;
DROP TABLE IF EXISTS #tmp_total_dwh_fvtaproebecam;
DROP TABLE IF EXISTS #tmp_dwh_fvtaproebecam;
DROP TABLE IF EXISTS #VentaCampania;
DROP TABLE IF EXISTS #tmp_dwh_fvtaproebecam_U6C_U1C;
DROP TABLE IF EXISTS #VentaCampania_U6C;
DROP TABLE IF EXISTS #StatusCampania_Menos1;
DROP TABLE IF EXISTS #FechaZona_Menos1;
DROP TABLE IF EXISTS #StatusCampania;
DROP TABLE IF EXISTS #StatusCampania_U6C;
DROP TABLE IF EXISTS #fdetnegocioconsultora;
DROP TABLE IF EXISTS #tmp_total_dwh_fvtaproebecam_lanzamiento;
DROP TABLE IF EXISTS #tmp_total_dwh_fvtaproebecam_top;
