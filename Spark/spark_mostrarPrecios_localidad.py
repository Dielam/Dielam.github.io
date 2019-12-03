import pandas
import csv
with open("Gasolineras_de_España.csv", "a") as csvFile:
	df = pd.DataFrame(csvFile)
	dfL = df[['Localidad','precio_gasolina_95','precio_gasoleo_a','precio_gasoleo_b','precio_bioetanol','precio_nuevo_gasóleo_a','precio_biodiesel','f__éster_metílico','f__bioalcohol','precio_gasolina_98','precio_gas_natural_comprimido','precio_gas_natural_licuado','precio_gases_licuados_del_petroleo' ]]

	print(dfL)


//Localidad precio 
