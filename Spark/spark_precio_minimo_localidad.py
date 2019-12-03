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

os.mkdir("outputMin")

if len(sys.argv) > 1:


    sc = SparkContext()
    localidad = sys.argv[1]
    localidadRDD = sc.textFile("Gasolineras.csv")
    localidadRDD = localidadRDD.map(lambda line: line.encode("ascii", "ignore"))

    localidadRDD = localidadRDD.map(lambda rows: rows.split(","))
    localidadRDD = localidadRDD.filter(lambda rows: localidad == rows[5])
    localidadRDD = localidadRDD.map(lambda rows: (rows[2], rows[5], rows[7], rows[8], rows[9],rows[10], rows[11], rows[12], rows[13], rows[14], rows[15], rows[16], rows[17], rows[18], rows[19], rows[20], rows[21], rows[22], rows[23], rows[24]))

    os.mkdir("outputMin/minimo_localidad_gasolina_95.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[6])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_gasolina_95.txt/media.txt", "w+")
        f.write(texto+"\n")
        f.close()

    os.mkdir("outputMin/minimo_localidad_gasoleo_a.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[7])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_gasoleo_a.txt/media.txt", "w")
        f.write(texto+"\n")
        f.close()

    os.mkdir("outputMin/minimo_localidad_gasoleo_b.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[8])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_gasoleo_b.txt/media.txt", "w")
        f.write(texto+"\n")
        f.close()

    os.mkdir("outputMin/minimo_localidad_bioetanol.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[9])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_bioetanol.txt/media.txt", "w")
        f.write(texto+"\n")
        f.close()

    os.mkdir("outputMin/minimo_localidad_nuevo_gasoleo_a.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[10])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_nuevo_gasoleo_a.txt/media.txt", "w")
        f.write(texto+"\n")
        f.close()

    os.mkdir("outputMin/minimo_localidad_biodiesel.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[11])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_biodiesel.txt/media.txt", "w")
        f.write(texto+"\n")
        f.close()


    os.mkdir("outputMin/minimo_localidad_ester_metilico.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[12])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_ester_metilico.txt/media.txt", "w")
        f.write(texto+"\n")
        f.close()

    os.mkdir("outputMin/minimo_localidad_bioalcohol.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[13])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_bioalcohol.txt/media.txt", "w")
        f.write(texto+"\n")
        f.close()


    os.mkdir("outputMin/minimo_localidad_gasolina_98.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[14])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_gasolina_98.txt/media.txt", "w")
        f.write(texto+"\n")
        f.close()


    os.mkdir("outputMin/minimo_localidad_gas_natural_comprimido.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[15])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_gas_natural_comprimido.txt/media.txt", "w")
        f.write(texto+"\n")
        f.close()


    os.mkdir("outputMin/minimo_localidad_gas_natural_licuado.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[16])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_gas_natural_licuado.txt/media.txt", "w")
        f.write(texto+"\n")
        f.close()


    os.mkdir("outputMin/minimo_localidad_gas_licuados_del_petr.txt")
    datosRDD = localidadRDD.map(generar)
    ascenRDD = datosRDD.map(lambda rows: ([rows[18], float(rows[17])]))
    ascenRDD = ascenRDD.filter(lambda rows: rows[1] != 0)
    if ascenRDD.isEmpty():
        r = sc.parallelize("0")
    else:
        ascenRDD = ascenRDD.sortBy(lambda x: x[1])
        texto = str(ascenRDD.first()[0]) + ' ' + str(ascenRDD.first()[1])
        f = open("outputMin/minimo_localidad_gas_licuados_del_petr.txt/media.txt", "w")
        f.write(texto+"\n")
        f.close()        
else:
    print "NO HA INTRODUCIDO LOCALIDAD"
