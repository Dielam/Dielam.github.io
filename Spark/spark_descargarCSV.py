#!/usr/bin/python3
# encoding: utf-8

import sys
import re
import csv
import pandas
import urllib, json 

url  = "https://opendata.arcgis.com/datasets/e64c741c13464d418f66bf3a5badeda2_0.geojson"

response = urllib.urlopen(url)
data = json.loads(response.read())

df = pandas.DataFrame(data)

df.to_csv("gasolinerasPrueba.csv")
