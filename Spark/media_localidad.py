#!/usr/bin/python

import sys
from pyspark import SparkContext
from shutil import rmtree


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
    sc = SparkContext()

    localidad = sys.argv[1]
    localidadRDD = sc.textFile("Gasolineras.csv")
    localidadRDD = localidadRDD.map(lambda line: line.encode("ascii", "ignore"))

    localidadRDD = localidadRDD.map(lambda rows: rows.split(","))
    localidadRDD = localidadRDD.filter(lambda rows: localidad == rows[5])
    localidadRDD = localidadRDD.map(lambda rows: (rows[5], rows[7], rows[8], rows[9],rows[10], rows[11], rows[12], rows[13], rows[14], rows[15], rows[16], rows[17], rows[18], rows[19], rows[20], rows[21], rows[22], rows[23], rows[24]))

    datosRDD = localidadRDD.map(generar)
    combustible1RDD = datosRDD.map(lambda rows: (rows[17], float(rows[5])))
    combustible1RDD.saveAsTextFile("data.txt") #GUARDA LA EMPRESA QUE LO SUMINISTRA Y EL PRECIO DEL COMBUSTIBLE FUNCIONA

    tam = combustible1RDD.map(lambda x: (x[0], 1))
    tam.saveAsTextFile("map.txt") #GUARDA LA EMPRESA Y UN 1 FUNCIONA
    tam = tam.reduceByKey(lambda x,y: x+y) #NO FUNCIONA!!!! NO HACE EL REDUCE
    tam.saveAsTextFile("tamReduce.txt")
    combustible1RDD = combustible1RDD.reduceByKey(lambda x,y: x+y)
    combustible1RDD.saveAsTextFile("reduce.txt")

    join1RDD = combustible1RDD.join(tam)
    media_gasolina = join1RDD.map(lambda calc:(calc[0], calc[1][0]/calc[1][1]) )
    media_gasolina.saveAsTextFile("output/gasolina_95.txt")
else:
    print "Error no ha introducido localidad."
