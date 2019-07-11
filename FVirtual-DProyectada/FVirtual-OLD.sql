Update lan_dm_analitico.tmp_faltante_virtual 
set tipooferta = case LEN(tipooferta) when 2 then CONCAT('0',tipooferta) when 1 then CONCAT('00',tipooferta) else tipooferta end;

Delete from fnc_analitico.dwh_faltante_virtual 
where codpais||fechafact||codsap in 
(select distinct b.codpais||fechafact||codsap from lan_dm_analitico.tmp_faltante_virtual a inner join fnc_analitico.dwh_dpais b on a.codcentro = b.codcentro);
									  
Insert Into fnc_analitico.dwh_faltante_virtual
Select b.codpais,Aniocampana,fechafact,codsap,tipooferta,unidfaltante,vtanetafaltante,anunciado
FROM lan_dm_analitico.tmp_faltante_virtual a
INNER join fnc_analitico.dwh_dpais b on a.codcentro = b.codcentro;