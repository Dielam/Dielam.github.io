#!/usr/bin/python
#Diego Laguna

from pyspark import SparkConf, SparkContext
import string
import sys

if len(sys.argv) <= 1:
    print("Error. No ha introducido Empresa.")
else:
    #Spark configuration
    conf = SparkConf().setMaster('local').setAppName('AveragePriceCP')
    sc = SparkContext(conf=conf)

    RDDvar = sc.textFile("Gasolineras.csv")
    wanted = sys.argv[1]

    # empresa = [23]
    decode = RDDvar.map(lambda line: line.encode("ascii", "ignore"))
    text = sample.filter(lambda line: wanted == line.split(',')[23])

    # precio_gasolina_95 = [11]
    data = text.filter(lambda line: str(line.split(',')[11]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_gasolina_95.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[11]))) #map (Empresa, precio_gasolina_95)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_gasolina_95.txt")
    # precio_gasoleo_a = [12]
    data = text.filter(lambda line: str(line.split(',')[12]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_gasoleo_a.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[12]))) #map (Empresa, precio_gasoleo_a)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_gasoleo_a.txt")
    # precio_gasoleo_b = [13]
    data = text.filter(lambda line: str(line.split(',')[13]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_gasoleo_b.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[13]))) #map (Empresa, precio_gasoleo_b)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_gasoleo_b.txt")
    # precio_bioetanol = [14]
    data = text.filter(lambda line: str(line.split(',')[14]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_bioetanol.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[14]))) #map (Empresa, precio_bioetanol)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_bioetanol.txt")
    # precio_nuevo_gasoleo_a = [15]
    data = text.filter(lambda line: str(line.split(',')[15]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_nuevo_gasoleo_a.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[15]))) #map (Empresa, precio_nuevo_gasoleo_a)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_nuevo_gasoleo_a.txt")
    # precio_biodiesel = [16]
    data = text.filter(lambda line: str(line.split(',')[16]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_biodiesel.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[16]))) #map (Empresa, precio_biodiesel)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_biodiesel.txt")
    # f__ester_metilico = [17]
    data = text.filter(lambda line: str(line.split(',')[17]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_eter_metilico.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[17]))) #map (Empresa, precio_ester_metilico)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_ester_metilico.txt")
    # f__bioalcohol = [18]
    data = text.filter(lambda line: str(line.split(',')[18]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_bioalcohol.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[18]))) #map (Empresa, bioalcohol)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_bioalcohol.txt")
    # precio_gasolina_98 = [19]
    data = text.filter(lambda line: str(line.split(',')[19]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_gasolina_98.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[19]))) #map (Empresa, precio_gasolina_98)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_gasolina_98.txt")
    # precio_gas_natural_comprimido = [20]
    data = text.filter(lambda line: str(line.split(',')[20]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_gas_natural_comprimido.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[20]))) #map (Empresa, precio_gas_natural_comprimido)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_gas_natural_comprimido.txt")
    # precio_gas_natural_licuado = [21]
    data = text.filter(lambda line: str(line.split(',')[21]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_gas_natural_licuado.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[21]))) #map (Empresa, precio_gas_natural_licuado)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_gas_natural_licuado.txt")
    # precio_gases_licuados_del_petr = [22]
    data = text.filter(lambda line: str(line.split(',')[22]) != '')
    if data.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output/media_empresa_gases_licuados_del_petr.txt")
    else:
        precio = data.map(lambda line: (str(line.split(',')[23]),float(line.split(',')[22]))) #map (Empresa, precio_gases_licuados_del_petr)
        count = data.map(lambda line: (str(line.split(',')[23]), 1)) #map (Empresa, contador)
        aggreg1 = precio.reduceByKey(lambda a, b: a+b)
        aggreg2 = count.reduceByKey(lambda a, b: a+b)
        union = aggreg1.join(aggreg2)
        result = union.map(lambda line: (line[1][0]/line[1][1]))

        result.saveAsTextFile("output/media_empresa_gases_licuados_del_petr.txt")