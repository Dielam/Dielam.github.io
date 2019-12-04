#!/usr/bin/python

from pyspark import SparkConf, SparkContext
import string
import sys
import csv



if len(sys.argv) <= 1:
    print("Error. No se ha introducido datos.")
else:

    #Spark configuration
    conf = SparkConf().setMaster('local').setAppName('JoinCVS')
    sc = SparkContext(conf=conf)

    csvFinal = open("csvSemanal.csv", 'a')
    for i in sys.argv:
    	csvAux = open(i) 
    	csvAux.next() #ignoro el header
    	for line in csvAux:
    		csvFinal.write(line)
    	csvAux.close()
    csvFinal.close()
