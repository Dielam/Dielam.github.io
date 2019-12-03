#!/usr/bin/python
#Diego Laguna

from pyspark import SparkConf, SparkContext, SQLContext
import string
import sys

if len(sys.argv) <= 1:
    print("Error. No ha introducido Codigo Postal.")
else:
    if len(sys.argv[1]) > 5:
        print("Error. No ha introducido un Codigo Postal correcto.")
    else:
        #Spark configuration
        conf = SparkConf().setMaster('local').setAppName('ListCP')
        sc = SparkContext(conf=conf)

        RDDvar = sc.textFile("Gasolineras.csv")
        wanted = sys.argv[1]

        # CP = [6]
        decode = RDDvar.map(lambda line: line.encode("ascii", "ignore"))
        sample = decode.filter(lambda line: wanted == line.split(',')[6])
        data = sample.map(lambda line: line.replace('\",', "DIRECCION"))
        text = sample.map(lambda line: line.replace('\",', "DIRECCION"))
        data = data.map(lambda line: (line.split(',')[13].replace(' ',"0")))
        result2 = text.map(lambda line: (int(line.split(',')[6]),str(line.split(',')[5]),str(line.split(',')[23]),float(line.split(',')[11]),float(line.split(',')[12])))
        result = result2.union(data)
        result.saveAsTextFile("outputTest/lista_cp.txt")
        # Cambiamos los espacios vacios por 0
        # precio_gasolina_95 = [11]
        text1 = text.filter(lambda line: str(line.split(',')[11]) != '')
        if text1.isEmpty():
            data = data.map(lambda line: line.replace(line.plit[11],0))
        # precio_gasoleo_a = [12]
        text2 = text.filter(lambda line: str(line.split(',')[12]) != '')
        if text2.isEmpty():
            data = data.map(lambda line: line.replace(line.plit[12],0))
        # precio_gasoleo_b = [13]
        text3 = text.filter(lambda line: str(line.split(',')[13]) != '')
        if text3.isEmpty():
            data = data.map(lambda line: line.replace(line.plit[13],0))
        # precio_bioetanol = [14]
        text4 = text.filter(lambda line: str(line.split(',')[14]) != '')
        if text4.isEmpty():
            data = data.map(lambda line: line.replace(" ","0"))
        # precio_nuevo_gasoleo_a = [15]
        text5 = text.filter(lambda line: str(line.split(',')[15]) != '')
        if text5.isEmpty():
            data = data.map(lambda line: line.replace(line.plit[15],0))
        # precio_biodiesel = [16]
        text6 = text.filter(lambda line: str(line.split(',')[16]) != '')
        if text6.isEmpty():
            data = data.map(lambda line: line.replace(line.plit[16],0))
        # precio_ester_metilico = [17]
        text7 = text.filter(lambda line: str(line.split(',')[17]) != '')
        if text7.isEmpty():
            data = data.map(lambda line: line.replace(line.plit[17],0))
        # precio_bioalcohol = [18]
        text8 = text.filter(lambda line: str(line.split(',')[18]) != '')
        if text8.isEmpty():
            data = data.map(lambda line: line.replace(line.plit[18],0))
        # precio_gasolina_98 = [19]
        text9 = text.filter(lambda line: str(line.split(',')[19]) != '')
        if text9.isEmpty():
            data = data.map(lambda line: line.replace(line.plit[19],0))
        # precio_gas_natural_comprimido = [20]
        text10 = text.filter(lambda line: str(line.split(',')[20]) != '')
        if text10.isEmpty():
            data = data.map(lambda line: line.replace(line.plit[20],0))
        # precio_gas_natural_licuado = [21]
        text11 = text.filter(lambda line: str(line.split(',')[21]) != '')
        if text11.isEmpty():
            data = data.map(lambda line: line.replace(line.plit[21],0))
        # precio_gas_licuados_del_petr = [22]
        text12 = text.filter(lambda line: str(line.split(',')[22]) != '')
        if text12.isEmpty():
            data = data.map(lambda line: line.replace(line.plit[22],0))
        #map (Localidad, Empresa, precio_gasolina_95,precio_gasoleo_a,precio_gasoleo_b,precio_bioetanol,precio_nuevo_gasoleo_a,precio_biodiesel,f__ester_metilico,f__bioalcohol,precio_gasolina_98,precio_gas_natural_comprimido,precio_gas_natural_licuado,precio_gases_licuados_del_petr)
        result = data.map(lambda line: (str(line.split(',')[5]),str(line.split(',')[23]),float(line.split(',')[11]),float(line.split(',')[12]),float(line.split(',')[13]),float(line.split(',')[14]),float(line.split(',')[15]),float(line.split(',')[16]),float(line.split(',')[17]),float(line.split(',')[18]),float(line.split(',')[19]),float(line.split(',')[20]),float(line.split(',')[21]),float(line.split(',')[22])))

        result.saveAsTextFile("outputLista/lista_cp.txt")