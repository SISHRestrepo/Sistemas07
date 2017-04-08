--use dbconventa

declare @idgrupo int, @idcadena int, @idcomercio int
declare @fecha varchar(50), @fechaproceso varchar(50), @idconciliacion int, @ruta varchar(8000), @SQL varchar(8000)

select @ruta=ruta_archivo, @idgrupo=id_grupo, @idcadena=id_cadena, @idcomercio=id_comercio from tb_def_cliente where cliente='PACIFICO'

set @fecha=convert(varchar,getdate(),112)
set @fechaproceso=convert(varchar,dateadd(dd,-1,getdate()),112)
drop table #TB_CABECERA_CONCILIACION_PACIFICO
drop table #TB_CONCILIACION_PACIFICO
CREATE TABLE #TB_CABECERA_CONCILIACION_PACIFICO  (CANTIDAD INT)
CREATE TABLE #TB_CONCILIACION_PACIFICO (LINEA VARCHAR(8000))
--CABECERA
SET @SQL = 'SELECT SUBSTRING(F1,10,10) FROM OPENROWSET(''Microsoft.ACE.OLEDB.12.0'',''Text;HDR=NO;Database='+@ruta+''',''SELECT * FROM BROADNET'+@fechaproceso+'.txt'') WHERE SUBSTRING(F1,1,1)=''1'''
INSERT INTO #TB_CABECERA_CONCILIACION_PACIFICO
EXEC(@SQL)
--DETALLE
SET @SQL = 'SELECT F1 FROM OPENROWSET(''Microsoft.ACE.OLEDB.12.0'',
				''Text;HDR=NO;Database='+@ruta+''',
				''SELECT * FROM BROADNET'+@fechaproceso+'.txt'') WHERE SUBSTRING(F1,1,1)=''2'''
INSERT INTO #TB_CONCILIACION_PACIFICO
EXEC(@SQL)

insert into tb_con_cliente(fecha_proceso,cliente,valor_archivo,cantidad_archivo)
select @fechaproceso,'PACIFICO',0,sum(convert(int,CANTIDAD)) from #TB_CABECERA_CONCILIACION_PACIFICO
set @idconciliacion=ident_current('tb_con_cliente')

--282762320170404120117000200060501892475         778911N00000000020000000000000020170404
insert into TB_DETALLE_CON_CLIENTE(idconciliacion,nombre_archivo,linea_archivo,
	codigo_merchant,numero_referencia,numero_autorizacion,valor,operadora,conciliado,fecha)
select @idconciliacion,@ruta+'BROADNET'+@fechaproceso+'.txt',linea,b.codigo_merchant,
	'',substring(linea,49,6),convert(numeric(18,2),substring(linea,56,10)+'.'+substring(linea,66,2)),
	case substring(linea,2,6) when '827623' then '01' when '827624' then '06' else '83' end,'N', @fechaproceso
from #TB_CONCILIACION_PACIFICO, DBHEPS2000.dbo.tb_comercio b where b.codigo_merchant='016010BPAC00001'


	drop table #pacifico
	select codigo_merchant into #pacifico from dbheps2000..tb_comercio where id_grupo=coalesce(@idgrupo,id_grupo) and id_cadena=coalesce(@idcadena,id_cadena) 
		and id=coalesce(@idcomercio,id)

	drop table #trx
	select b.* into #trx from  dbheps2000.dbo.tptransactionlog_diario b, 
			dbheps2000.dbo.tb_log_transaccion_diario c, dbheps2000.dbo.tb_comercio d
		where b.ctxmerchantid in (select codigo_merchant from #pacifico)
			and c.numero_autorizacion=b.ntxautorization
			and c.numero_referencia=b.ctxtxnnumber
			and c.codigo_proveedor=b.ctxbusiness
			and c.idmerchant=d.id
			and c.idchain=d.id_cadena
			and c.idgroup=d.id_grupo
			and d.codigo_merchant=b.ctxmerchantid
			and b.ftxcreatedate=c.fecha_transaccion
			and b.ctxtype='20' and b.ctxstatus='0' and ctxresultext='00'
			and b.ftxaccountingdate=@fechaproceso


	update a set a.conciliado='S' 
		from TB_DETALLE_CON_CLIENTE a, #trx b
		where a.codigo_merchant=b.ctxmerchantid
			and a.numero_autorizacion=b.ntxautorization
			and a.valor=b.ntxamount
			--and a.operadora=b.ctxbusiness
			and a.idconciliacion=@idconciliacion

	--diferencia cliente vs broadnet
	insert into tb_diferencia_conciliacion(idconciliacion,codigo_merchant,numero_referencia,numero_autorizacion,valor,operadora,ente,fecha)
	select idconciliacion, codigo_merchant, numero_referencia, numero_autorizacion, valor, operadora, 'C', fecha from TB_DETALLE_CON_CLIENTE 
		where conciliado='N' and idconciliacion=@idconciliacion
	
	--diferencia broadnet vs cliente
	insert into tb_diferencia_conciliacion(idconciliacion,codigo_merchant,numero_referencia,numero_autorizacion,valor,operadora,ente,fecha)
	select @idconciliacion,ctxmerchantid,ctxtxnnumber,ntxautorization,ntxamount,ctxbusiness,'R',ftxaccountingdate from #trx 
		where not exists (select * from  TB_DETALLE_CON_CLIENTE where idconciliacion=@idconciliacion and ctxmerchantid=codigo_merchant
			and ntxautorization=numero_autorizacion and ntxamount=valor)

	select * from tb_diferencia_conciliacion where idconciliacion=@idconciliacion
	


/*
CASO A ANALIZAR DENTRO DE PACIFICO RECARGAS
CAMPOS DE RELEVANCIA:
	CTXBUSINESS: CODIGO DEL PROVEEDOR (01 MOVISTAR, 83 TUENTI, 06 CNT)
	CTXTYPE: TIPO DE LA TRANSACCION (20 SOLICITUD, 05 REVERSAS)
	CTXRESULTEXT: RESULTADO DE LA RECARGA (00 OK, 74 DESCONOCIDO)

EN LA ACTUALIDAD SE HAN ANALIZADO 3 SUPUESTOS ESCENARIOS:
	1)EXISTE 1 TRANSACCION PERO LAS REVERSAS ARROJAN ERROR DESCONOCIDO POR PARTE DEL WS.
		CTXTYPE	CTXRESULTEXT	CTXBUSINESS
		20		00				1
		05		74				1
		05		74				1
		05		74				1

	2)LA TRANSACCION DEVUELVE COMO PRIMER UN REVERSO.
		CTXTYPE	CTXRESULTEXT	CTXBUSINESS
		05		00				1
		20		00				1
		
	2)LA TRANSACCION SE REALIZA COMO TUENTI, SIN EMBARGO, EL REVERSO SE PROCESA COMO MOVISTAR.
		CTXTYPE	CTXRESULTEXT	CTXBUSINESS
		20		00				83
		05		00				1
*/
/*


--REPORTE DE LAS TRANSACCIONES QUE HAY QUE ELEVAR LOS CASOS AL BANCO
SELECT --*
	(ftxaccountingdate) FECHA, 
	(hTxCreateTime) HORA, 
	(cTxTxnNumber) REFERENCIA, 
	(ntxAutorizationHost) AUTORIZACION, 
	(nTxAmount) VALOR, 
	(cTxTelefhoneNumber) TELEFONO, 
	(CASE cTxBusiness WHEN '01' THEN 'MOVISTAR' WHEN '83' THEN 'TUENTI' ELSE 'CNT' END) PROVEEDOR
FROM DBHEPS2000..TPTRANSACTIONLOG_DIARIO a 
WHERE cTxMerchantId = '016010BPAC00001' 
AND NTXAUTORIZATION IN (select * FROM #TMP_1)
AND ftxaccountingdate = @FECHA
--(846702,846704,846705,846864,846863,847222,847312,846738)
--846065,846069,846080,846602,846603,846652) --PONER LAS REFERENCIAS DE LAS TRANSACCIONES A ANALIZAR
AND cTxType = '20'
ORDER BY a.nTxGenCounter


SELECT
	(ftxaccountingdate) FECHA, 
	(cTxTxnNumber) REFERENCIA, 
	(ntxAutorizationHost) AUTORIZACION, 
	(nTxAmount) VALOR, 
	(cTxTelefhoneNumber) TELEFONO, 
	(CASE cTxBusiness WHEN '01' THEN 'MOVISTAR' WHEN '83' THEN 'TUENTI' ELSE 'CNT' END) PROVEEDOR
FROM DBHEPS2000..TPTRANSACTIONLOG a
WHERE cTxMerchantId = '016010BPAC00001' 
AND cTxTxnNumber IN (847390,847391,847395,847400,847401,847532,847657,848054,848092)

--(846702,846704,846705,846864,846863,847222,847312,846738)
--846065,846069,846080,846602,846603,846652) --PONER LAS REFERENCIAS DE LAS TRANSACCIONES A ANALIZAR
AND cTxType = '20'
ORDER BY a.nTxGenCounter


SELECT CTXBUSINESS, *
FROM [192.168.3.28].DBHEPS2000.DBO.TPTRANSACTIONLOG_DIARIO a
WHERE cTxMerchantId = '016010BPAC00001' 
AND cTxTxnNumber IN (855956)
--AND cTxType = '20'
ORDER BY a.nTxGenCounter

*/