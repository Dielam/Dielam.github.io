#!/usr/bin/python

import sys
from pyspark import SparkContext
from shutil import rmtree
import os.path as path

if path.exists("output"):
    rmtree("output")

def gasolina_95(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[0]!= 'precio_gasolina_95')
    datosRDD = datosRDD.filter(lambda x: x[0]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[0])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_gasolina_95.txt")
    return 0
def gasoleo_a(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[1]!= 'precio_gasleo_a')
    datosRDD = datosRDD.filter(lambda x: x[1]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[1])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_gasoleo_a.txt")
    return 0
def gasoleo_b(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[2]!= 'precio_gasleo_b')
    datosRDD = datosRDD.filter(lambda x: x[2]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[2])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_gasoleo_b.txt")
    return 0
def bioetanol(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[3]!= 'precio_bioetanol')
    datosRDD = datosRDD.filter(lambda x: x[3]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[3])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_bioetanol.txt")
    return 0
def nuevo_gasoleo_a(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[4]!= 'precio_nuevo_gasleo_a')
    datosRDD = datosRDD.filter(lambda x: x[4]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[4])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_nuevo_gasoleo_a.txt")
    return 0
def biodiesel(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[5]!= 'precio_biodiesel')
    datosRDD = datosRDD.filter(lambda x: x[5]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[5])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_biodiesel.txt")
    return 0
def ester_metilico(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[6]!= 'f__ster_metlico')
    datosRDD = datosRDD.filter(lambda x: x[6]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[6])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_ester_metilico.txt")
    return 0
def bioalcohol(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[7]!= 'f__bioalcohol')
    datosRDD = datosRDD.filter(lambda x: x[7]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[7])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_bioalcohol.txt")
    return 0
def precio_gasolina_98(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[8]!= 'precio_gasolina_98')
    datosRDD = datosRDD.filter(lambda x: x[8]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[8])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_gasolina_98.txt")
    return 0
def gas_natural_comprimido(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[9]!= 'precio_gas_natural_comprimido')
    datosRDD = datosRDD.filter(lambda x: x[9]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[9])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_gas_natural_comprimido.txt")
    return 0
def gas_natural_licuado(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[10]!= 'precio_gas_natural_licuado')
    datosRDD = datosRDD.filter(lambda x: x[10]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[10])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_gas_natural_licuado.txt")
    return 0
def gas_licuados_del_petr(datosRDD):
    datosRDD = datosRDD.filter(lambda x: x[11]!= 'precio_gases_licuados_del_petr')
    datosRDD = datosRDD.filter(lambda x: x[11]!= '')
    precioRDD = datosRDD.map(lambda rows: (["1", float(rows[11])]))
    precioRDD = precioRDD.reduceByKey(lambda x,y: x+y)
    tamRDD = datosRDD.count()
    mediaTotal = precioRDD.map(lambda rows: ([rows[1], int(tamRDD)]))
    mediaTotal = mediaTotal.map(lambda calc:(calc[0]/calc[1]))
    mediaTotal.saveAsTextFile("output/media_tipo_gas_licuado_del_petr.txt")
    return 0

if len(sys.argv) > 1:
    sc = SparkContext()
    tipo = sys.argv[1]
    tipoRDD = sc.textFile("Gasolineras.csv")
    tipoRDD = tipoRDD.map(lambda line: line.encode("ascii", "ignore"))
    tipoRDD = tipoRDD.filter(lambda line: str(line.split(',')[11]) != '')
    tipoRDD = tipoRDD.map(lambda line:(str(line.split(',')[11]) , str(line.split(',')[12]), str(line.split(',')[13]), str(line.split(',')[14]), str(line.split(',')[15]), str(line.split(',')[16]), str(line.split(',')[17]), str(line.split(',')[18]), str(line.split(',')[19]), str(line.split(',')[20]), str(line.split(',')[21]), str(line.split(',')[22])))
    if tipoRDD.isEmpty():
        result = sc.parallelize("0")
        result.saveAsTextFile("output")
    else:
        if tipo == "gasolina_95":
            gasolina_95(tipoRDD)
        elif tipo == "gasoleo_a":
            gasoleo_a(tipoRDD)
        elif tipo == "gasoleo_b":
            gasoleo_b(tipoRDD)
        elif tipo == "bioetanol":
            bioetanol(tipoRDD)
        elif tipo == "nuevo_gasoleo_a":
            nuevo_gasoleo_a(tipoRDD)
        elif tipo == "biodiesel":
            biodiesel(tipoRDD)
        elif tipo == "ester_metilico":
            ester_metilico(tipoRDD)
        elif tipo == "bioalcohol":
            bioalcohol(tipoRDD)
        elif tipo == "gasolina_98":
            gasolina_98(tipoRDD)
        elif tipo == "gas_natural_comprimido":
            gas_natural_comprimido(tipoRDD)
        elif tipo == "gas_natural_licuado":
            gas_natural_licuado(tipoRDD)
        elif  tipo == "glp":
            gas_licuados_del_petr(tipoRDD)
        else:
            print "ERROR DE TIPO"
else:
    print "Error no ha introducido tipo."
