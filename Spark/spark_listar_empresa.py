#!/usr/bin/python
#Diego Laguna

from pyspark import SparkConf, SparkContext
from shutil import rmtree
import string
import sys
import os
import os.path as path

if path.exists("outputLista"):
    rmtree("outputLista")

def generar(line):
    array = []
    array.append(line[0])
    array.append(line[1])
    array.append(line[2])
    aux = line[4]
    ini = 4
    fin = 20

    if aux != "I" and aux != "D" and aux != "N":
        array.append(line[3]+','+line[4]) #direccion
        aux = line[5]
        ini = 5
        fin = 21
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

if len(sys.argv) <= 1:
    print("Error. No ha introducido Empresa.")
else:
    #Spark configuration
    conf = SparkConf().setMaster('local').setAppName('ListEmpresa')
    sc = SparkContext(conf=conf)

    RDDvar = sc.textFile("Gasolineras.csv")
    wanted = sys.argv[1]
    
    # CP = [6]
    localidadRDD = RDDvar.map(lambda line: line.encode("ascii", "ignore"))
    localidadRDD = localidadRDD.map(lambda rows: rows.split(","))
    localidadRDD = localidadRDD.map(lambda rows: (rows[2], rows[5], rows[6], rows[7], rows[8], rows[9], rows[10], rows[11], rows[12], rows[13], rows[14], rows[15], rows[16], rows[17], rows[18], rows[19], rows[20], rows[21], rows[22], rows[23], rows[24]))
    localidadRDD = localidadRDD.filter(lambda rows: wanted == rows[19])
    
    datosRDD = localidadRDD.map(generar)
    # map(Empresa,CP,Localidad,precio_gasolina_95,precio_gasoleo_a,precio_gasoleo_b,precio_bioetanol,precio_nuevo_gasoleo_a,precio_biodiesel,f__ester_metilico,f__bioalcohol,precio_gasolina_98,precio_gas_natural_comprimido,precio_gas_natural_licuado,precio_gases_licuados_del_petr)
    result = datosRDD.map(lambda rows: ([rows[19], rows[2], rows[1], float(rows[7]), float(rows[8]), float(rows[9]), float(rows[10]), float(rows[11]), float(rows[12]), float(rows[13]), float(rows[14]), float(rows[15]), float(rows[16]), float(rows[17]), float(rows[18])]))

    result.saveAsTextFile("outputLista/lista_empresa.txt")