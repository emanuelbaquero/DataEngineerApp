print('El ejercicio termino correctamente!')## SCHEDULER

for i in range(len(l_datos_2010)-1):
	start >> crear_carpeta_temporal >> procesar_2010 >> [l_datos_2010[i], l_datos_2010[i+1]] >> procesar_2011

for i in range(len(l_datos_2011)-1):
	procesar_2011 >> [l_datos_2011[i], l_datos_2011[i+1]] >> procesar_2012

for i in range(len(l_datos_2012)-1):
	procesar_2012 >> [l_datos_2012[i], l_datos_2012[i+1]] >> procesar_2013

for i in range(len(l_datos_2013)-1):
	procesar_2013 >> [l_datos_2013[i], l_datos_2013[i+1]] >> procesar_2014

for i in range(len(l_datos_2014)-1):
	procesar_2014 >> [l_datos_2014[i], l_datos_2014[i+1]] >> procesar_2015

for i in range(len(l_datos_2015)-1):
	procesar_2015 >> [l_datos_2015[i], l_datos_2015[i+1]] >> procesar_2016

for i in range(len(l_datos_2016)-1):
	procesar_2016 >> [l_datos_2016[i], l_datos_2016[i+1]] >> procesar_2017

for i in range(len(l_datos_2017)-1):
	procesar_2017 >> [l_datos_2017[i], l_datos_2017[i+1]] >> procesar_2018

for i in range(len(l_datos_2018)-1):
	procesar_2018 >> [l_datos_2018[i], l_datos_2018[i+1]] >> procesar_2019

for i in range(len(l_datos_2019)-1):
	procesar_2019 >> [l_datos_2019[i], l_datos_2019[i+1]] >> procesar_2020

for i in range(len(l_datos_2020)-1):
	procesar_2020 >> [l_datos_2020[i], l_datos_2020[i+1]] >> procesar_2021

for i in range(len(l_datos_2021)-1):
	procesar_2021 >> [l_datos_2021[i], l_datos_2021[i+1]] >> into_DataLake_s3 >> eliminar_carpeta_temporal >> into_metrics_on_mongo >> end
