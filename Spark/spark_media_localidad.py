#!/usr/bin/python

import sys
from pyspark import SparkContext
from shutil import rmtree
import os.path as path

def generar(line):
    array = []
    array.append(line[0])
    array.append(line[1])
    aux = line[2]
    ini = 2
    fin = 18

    if aux != "I" and aux != "D" and aux != "N":
        aux = line[3]
        ini = 3
        fin = 19

    array.append(aux)
    ini+=1
    
    for i in range(ini, fin):
       if line[i] == '':
           array.append("0")
       else:
           array.append(line[i])
    return array

if len(sys.argv) > 1:
    if path.exists("output"):
        rmtree("output")

    sc = SparkContext()
    localidad = sys.argv[1]
    localidadRDD = sc.textFile("Gasolineras.csv")
    localidadRDD = localidadRDD.map(lambda line: line.encode("ascii", "ignore"))
    localidadRDD = localidadRDD.map(lambda rows: rows.split(","))
    localidadRDD = localidadRDD.filter(lambda rows: localidad == rows[5])
    localidadRDD = localidadRDD.map(lambda rows: (rows[5], rows[7], rows[8], rows[9],rows[10], rows[11], rows[12], rows[13], rows[14], rows[15], rows[16], rows[17], rows[18], rows[19], rows[20], rows[21], rows[22], rows[23], rows[24]))

    datosRDD = localidadRDD.map(generar)
    if datosRDD.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output")
    else:
        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[5])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_gasolina_95.txt")

        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[6])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_gasoleo_a.txt")

        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[7])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_gasoleo_b.txt")

        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[8])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_bioetanol.txt")

        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[9])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_nuevo_gasoleo_a.txt")

        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[10])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_biodiesel.txt")

        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[11])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_ester_metilico.txt")

        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[12])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_bioalcohol.txt")

        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[13])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_gasolina_98.txt")

        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[14])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_gas_natural_comprimido.txt")

        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[15])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_gas_natural_licuado.txt")

        precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[16])]))
        precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
        tamRDD = datosRDD.count()
        mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
        mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
        mediaTotal.saveAsTextFile("output/media_localidad_gas_licuados_del_petr.txt")
else:
    print "Error no ha introducido localidad."
