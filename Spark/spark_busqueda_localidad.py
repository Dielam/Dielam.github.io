from pyspark import SparkConf, SparkContext
import pandas
import sys
import csv
with open("Gasolineras.csv", "a") as csvFile:
	df = pd.DataFrame(csvFile)
	result = df[df['Localidad'] == sys.argv[1]]
