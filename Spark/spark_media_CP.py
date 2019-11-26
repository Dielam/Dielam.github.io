#!/usr/bin/python
#Diego Laguna

from pyspark import SparkConf, SparkContext
import string
import sys

#Spark configuration
conf = SparkConf().setMaster('local').setAppName('AveragePriceCP')
sc = SparkContext(conf=conf)

RDDvar = sc.textFile("Gasolineras.csv")
wanted = sys.argv[1]
#CP = 7
decode = RDDvar.map(lambda line: line.encode("ascii", "ignore"))
text = decode.filter(lambda line: wanted == line.split(',')[6]) 
#precio_gasolina_95 = 12
data = text.filter(lambda line: str(line.split(',')[12]) != '')
precio = data.map(lambda line: (int(line.split(',')[6]),float(line.split(',')[13]))) #map (CP, precio_gasolina_95)
count = data.map(lambda line: (int(line.split(',')[6]), 1)) #map (CP, contador)
aggreg1 = precio.reduceByKey(lambda a, b: a+b)
aggreg2 = count.reduceByKey(lambda a, b: a+b)
union = aggreg1.join(aggreg2)
result = union.map(lambda line: (line[1][0]/line[1][1]))

result.saveAsTextFile("media_CP_gasolina_95.txt")
# precio_gasoleo_a = 13
# precio_gasoleo_b
# precio_bioetanol
# precio_nuevo_gasoleo_a
# precio_biodiesel
# f__ester_metilico
# f__bioalcohol
# precio_gasolina_98
# precio_gas_natural_comprimido
# precio_gas_natural_licuado
# precio_gases_licuados_del_petr