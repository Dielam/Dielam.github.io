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
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[5])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[5])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]))
    media_gasolina.saveAsTextFile("output/gasolina_95")
    mediaTotal.saveAsTextFile("output/gasolina_95/total")

    datosRDD = localidadRDD.map(generar)
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[6])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[6])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/gasoleo_A")
    mediaTotal.saveAsTextFile("output/gasoleo_A/total")


    datosRDD = localidadRDD.map(generar)
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[7])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[7])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/gasoleo_B")
    mediaTotal.saveAsTextFile("output/gasoleo_B/total")


    datosRDD = localidadRDD.map(generar)
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[8])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[8])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/bioetanol")
    mediaTotal.saveAsTextFile("output/bioetanol/total")


    datosRDD = localidadRDD.map(generar)
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[9])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[9])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/nuevo_gasoleo_A")
    mediaTotal.saveAsTextFile("output/nuevo_gasoleo_A/total")


    datosRDD = localidadRDD.map(generar)
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[10])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[10])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/biodiesel")
    mediaTotal.saveAsTextFile("output/biodiesel/total")


    datosRDD = localidadRDD.map(generar)
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[11])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[11])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/ether_metilico")
    mediaTotal.saveAsTextFile("output/ether_metilico/total")


    datosRDD = localidadRDD.map(generar)
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[12])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[12])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/bioalcohol")
    mediaTotal.saveAsTextFile("output/bioalcohol/total")


    datosRDD = localidadRDD.map(generar)
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[13])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[13])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/gasolina_98")
    mediaTotal.saveAsTextFile("output/gasolina_98/total")


    datosRDD = localidadRDD.map(generar)
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[14])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[14])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/gas_n_comprimido")
    mediaTotal.saveAsTextFile("output/gas_n_comprimido/total")


    datosRDD = localidadRDD.map(generar)
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[15])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[15])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/gas_n_licuado")
    mediaTotal.saveAsTextFile("output/gas_n_licuado/total")


    datosRDD = localidadRDD.map(generar)
    precioRDD = datosRDD.map(lambda rows: ([rows[0], float(rows[16])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    combustible1RDD = datosRDD.map(lambda rows: ([rows[17], float(rows[16])]))
    tam = combustible1RDD.map(lambda x: ([x[0], int(1)]))
    tam = tam.reduceByKey(lambda x,y: x+y)
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/glp")
    mediaTotal.saveAsTextFile("output/glp/total")
else:
    print "Error no ha introducido localidad."
