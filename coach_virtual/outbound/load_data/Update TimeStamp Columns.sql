-- cada vez que se carga un archivo a la tabla, se elimina la interaccion del dia para evitar duplicados por si se reprocesan 2 cargas en un mismo dia
delete from fnc_virtual_coach.fdethybrysdata
where interactionutcdate = ((getdate() - Interval '5 HOURS')- interval '1 day')::DATE;

-- se corrige los casos en que no viene pais en la data de hybris y se extrae del campo idcampanaconsultora
update lan_virtual_coach.fdethybrysdata
set country = substring(regexp_substr(idcampanaconsultora,'_[^.]*'),2,2)
where country = '';

-- se actualizan los campos de fechas al formato timestamp
UPDATE lan_virtual_coach.fdethybrysdata
SET interactiontimestamputc = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(interactiontimestamputc,'/Date(',''),'+0000)/','')::BIGINT)/1000 * interval '1 second'),
	birthdate = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(birthdate,'/Date(',''),')/','')::BIGINT)/1000 * interval '1 second'),
	interactionutcdate = convert_timezone('PET',timestamp 'epoch' + (REPLACE(REPLACE(interactionutcdate,'/Date(',''),')/','')::BIGINT)/1000 * interval '1 second');

--Se actualizan los codigos de las consultoras que llegan sin "0" adelante por temas de hybris: caso registradas

drop table if exists #Consultoras_digit0;

select country as codcountry, campanaexposicion as aniocampanacorrect,yy1_codigoebelista_mps as codebelista2,'0'+yy1_codigoebelista_mps as codconsultora
into #consultoras_digit0 from
(select distinct campanaexposicion,country,yy1_codigoebelista_mps from lan_virtual_coach.fdethybrysdata
minus
select distinct aniocampana, codpais, codebelista from fnc_analitico.dwh_fstaebecam
WHERE aniocampana >= '201906');

-- se crea otro temp para separar los casos en que el cod consultora esté correcto pero no aparezca en la fstaebecam
drop table if exists #Consultoras_digit02;

select a.*
into #consultoras_digit02
from #consultoras_digit0 a
left join fnc_analitico.dwh_debelista b on
a.codcountry = b.codpais and
a.codebelista2 = b.codebelista
where b.codebelista is null;

-- Se realiza la actualización de la tabla Lan, para que los codigos aparezcan corrrectamente

update lan_virtual_coach.fdethybrysdata
set yy1_codigoebelista_mps = codconsultora
from #consultoras_digit02
where country = codcountry
and yy1_codigoebelista_mps = codebelista2
and campanaexposicion = aniocampanacorrect;
