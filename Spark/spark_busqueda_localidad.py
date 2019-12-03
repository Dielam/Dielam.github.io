import pandas
import sys
import csv
with open("Gasolineras_de_España.csv", "a") as csvFile:
	df = pd.DataFrame(csvFile)
	result = df[df['Localidad'] == sys.argv[1]]



//Localidad precio 
