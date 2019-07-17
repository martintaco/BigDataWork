from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

import json
 
# Define a subclass of StreamCallback for use in session.write()
class PyStreamCallback(StreamCallback):
    def __init__(self):
        pass

    def process(self, inputStream, outputStream):
        text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
        csv_string = self.transform(text)
        outputStream.write(csv_string.encode('utf-8'))

    def transform(self, data):
        corp_det_tactica_oferta = json.loads(data, parse_int=str, parse_float=str)
        corp_det_tactica_oferta_arr = []

        corp_det_tactica_oferta_numeric_null_columns = ['NUMOFERTA', 'UNIDADES', 'FLAGDIGITABLE', 'FACTORCUADRE', 'INDCUADRE',
				           'INDPADRE', 'NROGRUPO', 'PRECIOOFERTA',
					   'PRECIOVTAPROPUESTOMN', 'PRECIONORMALMN']

        corp_det_tactica_oferta_text_columns = ['CODCANALVENTA', 'CODPAIS', 'ANIOCAMPANA', 'CODESTRATEGIA', 'CODPRODUCTO', 'CODVENTA', 'CODCATALOGO', 'DESCATALOGO', 'CODTIPOOFERTA', 'DESTIPOOFERTA', 'DESCOMERCIALOFERTA']

        for record in corp_det_tactica_oferta:
	    for key in corp_det_tactica_oferta_numeric_null_columns:
                if not record[key]:
                    record[key] = ''

            for key in corp_det_tactica_oferta_text_columns:
                # Cadena que tiene un unico caracter, insertado por la base de datos: 0x0
		if not record[key]:
		    record[key] = ''
		elif '\0' in record[key]:
		    record[key] = record[key].replace('\0', '')
		    record[key] = '"' + record[key] + '"'
                elif record[key]:
                    record[key] = record[key].strip()
                    record[key] = record[key].replace('\\', '\\\\')
		    record[key] = '"' + record[key] + '"'

            values = [record['CODCANALVENTA'], record['CODPAIS'], record['ANIOCAMPANA'], record['NUMOFERTA'],
		      record['CODESTRATEGIA'], record['CODPRODUCTO'], record['CODVENTA'], record['CODCATALOGO'],
		      record['DESCATALOGO'], record['CODTIPOOFERTA'], record['UNIDADES'], record['DESTIPOOFERTA'],
		      record['FLAGDIGITABLE'], record['FACTORCUADRE'], record['INDCUADRE'],
		      record['INDPADRE'], record['NROGRUPO'], record['PRECIOOFERTA'], record['PRECIOVTAPROPUESTOMN'], record['PRECIONORMALMN'],
		      record['DESCOMERCIALOFERTA']]

            corp_det_tactica_oferta_arr.append('\t'.join(values))

        corp_det_tactica_oferta_arr.append('')

        return '\r\n'.join(corp_det_tactica_oferta_arr)


flowFile = session.get()

if(flowFile != None):
    flowFile = session.write(flowFile, PyStreamCallback())
    session.transfer(flowFile, REL_SUCCESS)
