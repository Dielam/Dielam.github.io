#!/usr/bin/python

import sys
from pyspark import SparkContext
from shutil import rmtree
import os
import os.path as path

def generar(line):
    array = []
    array.append(line[0]) #ID
    array.append(line[1])
    aux = line[3]
    ini = 3
    fin = 19

    if aux != "I" and aux != "D" and aux != "N":
        array.append(line[2]+','+line[3]) #direccion
        aux = line[4]
        ini = 4
        fin = 20
    else:
        array.append(line[2]) #direccion

    array.append(aux)
    ini+=1
    
    for i in range(ini, fin):
       if line[i] == '':
           array.append("0")
       else:
           array.append(line[i])
    return array

if path.exists("outputMin"):
    rmtree("outputMin")
else:
    os.mkdir("outputMin")

if len(sys.argv) > 1:


    sc = SparkContext()
    localidad = sys.argv[1]
    localidadRDD = sc.textFile("Gasolineras.csv")
    localidadRDD = localidadRDD.map(lambda line: line.encode("ascii", "ignore"))

    localidadRDD = localidadRDD.map(lambda rows: rows.split(","))
    localidadRDD = localidadRDD.filter(lambda rows: localidad == rows[5])
    localidadRDD = localidadRDD.map(lambda rows: (rows[2], rows[5], rows[7], rows[8], rows[9],rows[10], rows[11], rows[12], rows[13], rows[14], rows[15], rows[16], rows[17], rows[18], rows[19], rows[20], rows[21], rows[22], rows[23], rows[24]))

    os.mkdir("outputMin/gasolina_95")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[6])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/gasolina_95/media.txt", "w+")
        f.write(texto)
        f.close()

    os.mkdir("outputMin/gasoleo_A")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[7])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/gasoleo_A/media.txt", "w")
        f.write(texto)
        f.close()

    os.mkdir("outputMin/gasoleo_B")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[8])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/gasoleo_B/media.txt", "w")
        f.write(texto)
        f.close()

    os.mkdir("outputMin/bioetanol")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[9])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/bioetanol/media.txt", "w")
        f.write(texto)
        f.close()

    os.mkdir("outputMin/nuevo_gasoleo_A")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[10])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/nuevo_gasoleo_A/media.txt", "w")
        f.write(texto)
        f.close()

    os.mkdir("outputMin/biodiesel")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[11])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/biodiesel/media.txt", "w")
        f.write(texto)
        f.close()


    os.mkdir("outputMin/ether_metilico")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[12])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/ether_metilico/media.txt", "w")
        f.write(texto)
        f.close()


    os.mkdir("outputMin/bioalcohol")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[13])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/bioalcohol/media.txt", "w")
        f.write(texto)
        f.close()


    os.mkdir("outputMin/gasolina_98")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[14])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/gasolina_98/media.txt", "w")
        f.write(texto)
        f.close()


    os.mkdir("outputMin/gas_n_comprimido")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[15])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/gas_n_comprimido/media.txt", "w")
        f.write(texto)
        f.close()


    os.mkdir("outputMin/gas_n_licuado")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[16])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/gas_n_licuado/media.txt", "w")
        f.write(texto)
        f.close()


    os.mkdir("outputMin/glp")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[17])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/glp/media.txt", "w")
        f.write(texto)
        f.close()        
else:
    print "NO HA INTRODUCIDO LOCALIDAD"
