#!/usr/bin/env python3
# coding=utf-8
import sys
import re
import unicodecsv as csv
import pandas
import urllib, json


url  = "https://opendata.arcgis.com/datasets/e64c741c13464d418f66bf3a5badeda2_0.geojson"


response = urllib.urlopen(url)

data = json.loads(response.read())

df = pandas.DataFrame(data)
#df.to_csv("gasolinerasPrueba.csv",encoding='utf-8')


my_List = df['features'].tolist() #Descargo la lista

dfList = pandas.DataFrame(my_List)

dfList = dfList["properties"]



#Creo header
headers = ['objectid','provincia','municipio','localidad',u'c\xf3digo_postal',u'direcci\xf3n','margen','longitud','latitud','precio_gasolina_95',u'precio_gas\xf3leo_a',u'precio_gas\xf3leo_b','precio_bioetanol',u'precio_nuevo_gas\xf3leo_a','precio_biodiesel',u'f__\xe9ster_met\xedlico','f__bioalcohol','precio_gasolina_98','precio_gas_natural_comprimido','precio_gas_natural_licuado',u'precio_gases_licuados_del_petr',u'r\xf3tulo','tipo_venta','rem_','horario', 'horario00','z','fecha']
with open('Gasolineras.csv', 'w') as f:
	w = csv.DictWriter(f, fieldnames=headers, encoding='utf-8')
	w.writeheader()
	for row in dfList:
		w.writerow(row)


