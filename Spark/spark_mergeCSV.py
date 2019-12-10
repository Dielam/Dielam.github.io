#!/usr/bin/python

from pyspark import SparkConf, SparkContext
import string
import sys
import csv
import os


if len(sys.argv) <= 1:
    print("Error. No se ha introducido datos.")
else:

    #Spark configuration
    conf = SparkConf().setAppName('MergeCVS')
    sc = SparkContext(conf=conf)
    found = False;

    #compruebo que csvSemanal existe

    if os.path.exists("/csvSemanal.csv"):
        found = True
       

    csvFinal = open("csvSemanal.csv", 'a')
    for i in sys.argv[1:]:
    	csvAux = open(i)
        if found == True:
    	   csvAux.next() #ignoro el header
    	for row in csvAux:
    		csvFinal.write(row)
   	csvAux.close()
    csvFinal.close()
