# Dielam.github.io
Proyecto curso 2019-2020
Estudio de gasolineras de España para la
asignatura Cloud y Big Data en la Universidad
Complutense de Madrid.

Alvaro Antón Garcia

Gonzalo Figueroa del Val

Joel Garcia Aparicio

John Erik Ibarra Guerrón

Diego Laguna Martin



WEB: https://dielam.github.io/WEB/




Descripción del Proyecto:

-Objetivos y Datos

Este proyecto consiste en un estudio de mercado de las gasolineras de toda España 	 	 	
permitiendo al usuario manejar diversas variables en cuanto a gasolineras se refiere, 	 	 	
desde los distintos tipos de combustible hasta la localización de estas gasolineras y otros 	 	
datos.
Los datos que manejaremos han sido obtenidos del portal opendata.esri.es, un portal de datos
abiertos que ofrece datasets de todo tipo de materias.

-Big Data

Big Data consiste en un gran volumen de datos, estructurados o no. Vamos a manejar una gran
cantidad de información que nos permitirá ofrecer unos resultados fiables y precisos respecto al
estudio que realizamos. Gracias al procesamiento paralelo a gran escala, el procesamiento de
estos datos será veloz

-Herramientas

Utilizaremos diferentes herramientas para el testeo local, la ejecución en clúster y el repositorio de
los datos.

-Repositorio

Como está explicado anteriormente en el apartado de herramientas, hemos utilizado GitHub para
el repositorio de nuestro código fuente y todo lo relacionado con el proyecto.

-Guia de uso

En el siguiente apartado encontrarás una descripción del código realizado. Además, habrá
también una guía con todo lo necesario para ejecutar cada una de las funciones de nuestro
proyecto.

-Resultados

Como está explicado anteriormente en el apartado de herramientas, hemos utilizado GitHub para
el repositorio de nuestro código fuente y todo lo relacionado con el proyecto.

-Dataset

El dataset sobre el que trabajaremos para realizar el estudio, contiene información de más de
10.000 gasolineras de toda España, además está actualizado diariamente, lo que supondrá que
quien utilice nuestra aplicación, estará siempre consultando información en vigor con datos
reales.
Opendata.esri.es es un portal de datos abiertos que ofrece datasets tratados por la empresa que
gestiona el portal y que además recopila datasets de otros portales Open Data para ofrecer así
una mayor cantidad de información.
Los principales datos del dataset que trabajaremos serán:
Localización de las gasolineras (Provincia, Municipio, Código Postal).
Horario de la gasolinera.
Empresa que gestiona la gasolinera (Cepsa, Galp, Repsol...).
Diferentes tipos de combustible (gasóleo a, gasolina sin plomo, biodiesel…).

Herramientas:

-Spark

Apache Spark es un framework de computación en clúster open-source. Fue desarrollada
originariamente en la Universidad de California, en el AMPLab de Berkeley. El código base del
proyecto Spark fue donado más tarde a la Apache Software Foundation que se encarga de su
mantenimiento desde entonces. Spark proporciona una interfaz para la programación de clusters
completos con Paralelismo de Datos implícito y tolerancia a fallos.	 
Apache Spark se puede considerar un sistema de computación en clúster de propósito general y
orientado a la velocidad. Proporciona APIs en Java, Scala, Python y R. También proporciona un
motor optimizado que soporta la ejecución de grafos en general. También soporta un conjunto
extenso y rico de herramientas de alto nivel entre las que se incluyen Spark SQL (para el
procesamiento de datos estructurados basada en SQL), MLlib para implementar machine
learning, GraphX para el procesamiento de grafos y Spark Streaming.

-Python

Python es un lenguaje de programación interpretado cuya filosofía hace hincapié en la legibilidad
de su código
Se trata de un lenguaje de programación multiparadigma, ya que soporta orientación a objetos,
programación imperativa y, en menor medida, programación funcional. Es un lenguaje
interpretado, dinámico y multiplataforma.
Es administrado por la Python Software Foundation. Posee una licencia de código abierto,
denominada Python Software Foundation License, que es compatible con la Licencia pública
general de GNU a partir de la versión 2.1.1, e incompatible en ciertas versiones anteriores.

-Amazon Web Services

Amazon Web Services (AWS abreviado) es una colección de servicios de computación en la nube
pública (también llamados servicios web) que en conjunto forman una plataforma de
computación en la nube, ofrecidas a través de Internet por Amazon.com. Es usado en
aplicaciones populares como Dropbox, Foursquare, HootSuite. Es una de las ofertas
internacionales más importantes de la computación en la nube y compite directamente contra
servicios como Microsoft Azure y Google Cloud Platform. Es considerado como un pionero en
este campo.
De los servicios que ofrece Amazon Web Services, utilizaremos EC2 para el testeo local en una
instancia con spark, en este caso ubuntu. Además utilizaremos EMR con spark en hadoop para el
procesamiento de los datos de gran volumen y respuesta de los mismos, mediante un clúster con
un nodo master y dos nodos workers.

-Hadoop

Apache Hadoop es un framework de software que soporta aplicaciones distribuidas bajo una
licencia libre. Permite a las aplicaciones trabajar con miles de nodos y petabytes de datos.
Hadoop se inspiró en los documentos Google para MapReduce y Google File System (GFS).
Hadoop es un proyecto de alto nivel Apache que está siendo construido y usado por una
comunidad global de contribuyentes, mediante el lenguaje de programación Java. Yahoo! ha sido
el mayor contribuyente al proyecto, y usa Hadoop extensivamente en su negocio.

-Github

GitHub es una forja (plataforma de desarrollo colaborativo) para alojar proyectos utilizando el
sistema de control de versiones Git. Se utiliza principalmente para la creación de código fuente
de programas de ordenador. El software que opera GitHub fue escrito en Ruby on Rails. Desde
enero de 2010, GitHub opera bajo el nombre de GitHub, Inc. Anteriormente era conocida como
Logical Awesome LLC. El código de los proyectos alojados en GitHub se almacena típicamente
de forma pública, aunque utilizando una cuenta de pago, también permite hospedar repositorios
privados.
Utilizamos Github como repositorio para todo nuestro proyecto y para el control de versiones.
También, para el host de la página web aprovecharemos Github Pages, un complemento de
Github.

Preparación del programa:

-Modo local

Si queremos ejecutar este programa en modo local, ya sea en vuestro sistema operativo Linux
Ubuntu o en Windows pero a través de una máquina virtual de Ubuntu (con VirtualBox, por
ejemplo), es necesario tener instalado en nuestro equipo Spark en modo local, si no lo tienes o no
sabes como hacerlo, aquí tienes un tutorial para conseguirlo.
Instalación Spark en nuestra máquina Ubuntu
Aunque pensemos que podemos tener todo instalado, si no estamos correctamente seguros,
realizaremos los siguientes pasos:

Primero, instalaremos Java, ya que es necesario para arrancar Apache Spark

$ sudo apt-get update

$ sudo apt install default-jdk

Después, será necesarior instalar Scala y comprobar la correcta instalación y version.

$ sudo apt-get install scala

$ scala -version
Scala code runner version 2.11.12 -- Copyright 2002-2017, LAMP/EPF

Tras esto, instalaremos Python

$ sudo apt-get install python

Para comprobar la instalación, ejecutamos:

$ python -h

Por último, instalamos Spark

$ sudo curl -o http://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz

$ sudo tar xvf ./spark-2.2.0-bin-hadoop2.7.tgz

$ sudo mkdir /usr/local/spark

$ sudo cp -r spark-2.2.0-bin-hadoop2.7/* /usr/local/spark

Puede ser que nuestra máquina no reconozca el comando curl, en este caso, procederemos a
instalarlo y a continuación, volveremos a realizar el paso anterior

$ sudo apt install curl

-Modo Amazon Web Services

Para ejecutar la aplicación mediante un clúster, será necesario iniciar un clúster con Spark en
Hadoop a través de Amazon AWS. Después de iniciar el clúster, instalaremos Pandas, Numpy y
unicodecsv igual que para la ejecución en modo local, esta vez sin sudo:

$ sudo apt-get install python-pip

$ pip install numpy

$ pip install pandas

$ pip install unicodecsv

En caso de aparecer un error con el comando pip, tendremos que actualizarlo:

$ pip install --upgrade pip

Al estar ejecutando la aplicación en un clúster, los comandos cambian, para ejecutar cualquier
opcion, lo haremos de la siguiente forma (donde N y M son los número que queramos poner):

$ spark-submit --num-executors N --executor-cores M “script"

Una vez ejecutado el script, necesitamos obtener la carpeta de salida del hadoop file system,
para ello:

$ hadoop fs -get “nombreDirectorioSalida"

Si queremos volver a ejecutar el mismo código con otras opciones habría que borrar los ficheros
de salida generados en la ejecución anterior, para ello, utilizamos los siguientes comandos:

$ hadoop fs -rm -r “nombreDirectorioSalida"

Para consultar los directorios:

$ hadoop fs -ls

Opciones de ejecución:

-Media de precio de cada combustible por Código Postal:

$ spark-submit spark_media_CP.py "codigoPostal"

En el argumento "codigoPostal" debemos escribir el número correspondiente para el que
queremos ejecutar la aplicación. Sobre entendemos que siempre escribimos un entero correcto,
ya que de no ser así, se produciría un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada output, donde existirá una carpeta
sobre cada tipo de combustible. Para consultar el precio deseado:

$ cat output/“nombreCarpetaCombustible"/*

Los nombres de las carpetas de los combustibles los podrás consultar con el siguiente comando:

$ ls output/

-Media de precio de cada combustible por Empresa:

$ spark-submit spark_media_empresa.py "nombreEmpresa"

En el argumento "nombreEmpresa" debemos escribir el nombre de la empresa que queremos
consultar, para ello el nombre tendrá que estar escrito en mayúsculas, con los espacios, símbolos
y acentos ortográficos correspondientes. En caso de querer consultar las empresas sin rótulo, el
argumento tendrá que ser "(SIN RÓTULO)". De no cumplir lo establecido, se producirá un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada output, donde existirá una carpeta
sobre cada tipo de combustible. Para consultar el precio deseado:

$ cat output/“nombreCarpetaCombustible"/*

Los nombres de las carpetas de los combustibles los podrás consultar con el siguiente comando:

$ ls output/

-Media de precio de cada combustible por Localidad

$ spark-submit spark_media_localidad.py "localidad"

En el argumento "localidad" debemos escribir el nombre de la localidad que queremos consultar,
para ello el nombre tendrá que estar escrito en mayúsculas, con los espacios, símbolos y acentos
ortográficos correspondientes. De no cumplir lo establecido, se producirá un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada output, donde existirá una carpeta
sobre cada tipo de combustible. Para consultar el precio deseado:

$ cat output/“nombreCarpetaCombustible"/*

Los nombres de las carpetas de los combustibles los podrás consultar con el siguiente comando:

$ ls output/

-Listado de gasolineras por Código Postal:

$ spark-submit spark_listar_cp.py "codigoPostal"

En el argumento "codigoPostal" debemos escribir el número correspondiente para el que
queremos ejecutar la aplicación. Sobre entendemos que siempre escribimos un entero correcto,
ya que de no ser así, se produciría un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada output, para consultar esta
información:

$ cat output/listaPorCP/*

-Listado de gasolineras en una Localidad:

$ spark-submit spark_busqueda_localidad.py "localidad"

En el argumento "localidad" debemos escribir el nombre de la localidad que queremos consultar,
para ello el nombre tendrá que estar escrito en mayúsculas, con los espacios, símbolos y acentos
ortográficos correspondientes. De no cumplir lo establecido, se producirá un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada output, para consultar esta
información:

$ cat output/listaPorCP/*

-Listado de gasolineras pertenecientes a una Empresa:

$ spark-submit spark_listar_empresa.py "empresa"

En el argumento "empresa" debemos escribir el nombre de la empresa que queremos consultar,
para ello el nombre tendrá que estar escrito en mayúsculas, con los espacios, símbolos y acentos
ortográficos correspondientes. De no cumplir lo establecido, se producirá un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada outputLista, para consultar esta
información:

$ cat outputLista/lista_empresa.txt/*

-Precio máximo de cada combustible a partir de un Código Postal:

$ spark-submit spark_precio_maximo_cp.py "codigoPostal"

En el argumento "codigoPostal" debemos escribir el número correspondiente para el que
queremos ejecutar la aplicación. Sobre entendemos que siempre escribimos un entero correcto,
ya que de no ser así, se produciría un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada outputMax, para consultar esta
información:

$ cat outputMax/“nombreCarpetaCombustible"/*

Los nombres de las carpetas de los combustibles los podrás consultar con el siguiente comando:

$ ls output/

-Precio máximo de cada combustible a partir de una Localidad:

$ spark-submit spark_precio_maximo_localidad.py "localidad"

En el argumento "localidad" debemos escribir el nombre de la localidad que queremos consultar,
para ello el nombre tendrá que estar escrito en mayúsculas, con los espacios, símbolos y acentos
ortográficos correspondientes. De no cumplir lo establecido, se producirá un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada outputMax, para consultar esta
información:

$ cat outputMax/“nombreCarpetaCombustible"/*

Los nombres de las carpetas de los combustibles los podrás consultar con el siguiente comando:

$ ls output/

-Precio máximo de cada combustible en una Empresa:

$ spark-submit spark_precio_maximo_empresa.py "empresa"

En el argumento "empresa" debemos escribir el nombre de la empresa que queremos consultar,
para ello el nombre tendrá que estar escrito en mayúsculas, con los espacios, símbolos y acentos
ortográficos correspondientes. De no cumplir lo establecido, se producirá un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada outputMax, para consultar esta
información:

$ cat outputMax/“nombreCarpetaCombustible"/*

Los nombres de las carpetas de los combustibles los podrás consultar con el siguiente comando:

$ ls output/

-Precio mínimo de cada combustible a partir de un Código Postal:

$ spark-submit spark_precio_minimo_cp.py "codigoPostal"

En el argumento "codigoPostal" debemos escribir el número correspondiente para el que
queremos ejecutar la aplicación. Sobre entendemos que siempre escribimos un entero correcto,
ya que de no ser así, se produciría un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada outputMin, para consultar esta
información:

$ cat outputMin/“nombreCarpetaCombustible"/*

Los nombres de las carpetas de los combustibles los podrás consultar con el siguiente comando:

$ ls output/

-Precio mínimo de cada combustible a partir de una Localidad:

$ spark-submit spark_precio_minimo_localidad.py "localidad"

En el argumento "localidad" debemos escribir el nombre de la localidad que queremos consultar,
para ello el nombre tendrá que estar escrito en mayúsculas, con los espacios, símbolos y acentos
ortográficos correspondientes. De no cumplir lo establecido, se producirá un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada outputMin, para consultar esta
información:

$ cat outputMin/“nombreCarpetaCombustible"/*

Los nombres de las carpetas de los combustibles los podrás consultar con el siguiente comando:

$ ls output/

-Precio máximo de cada combustible en una Empresa:

$ spark-submit spark_precio_minimo_empresa.py "empresa"

En el argumento "empresa" debemos escribir el nombre de la empresa que queremos consultar,
para ello el nombre tendrá que estar escrito en mayúsculas, con los espacios, símbolos y acentos
ortográficos correspondientes. De no cumplir lo establecido, se producirá un error.

Salida del programa

La información obtenida se encontrará en una carpeta llamada outputMin, para consultar esta
información:

$ cat outputMin/"nombreCarpetaCombustible"/*

Los nombres de las carpetas de los combustibles los podrás consultar con el siguiente comando:

$ ls output/

Conclusiones:

Con esta aplicación que hemos desarrollado podemos ver es un gran adelanto para el
consumidor, pues le permite ahorrar combustible así como economizar a la hora de tener que
repostar. Todo esto se debe a que gracias a las técnicas que hemos aprendido durante el curso,
podemos tener conocimiento en todo momento de donde hay una gasolinera, el precio que tiene
y la media de los diferentes combustibles.
De este modo facilita al consumidor el elegir el lugar donde repostar y así economizar ya que
sabrá en todo momento cual seria el lugar donde el combustible sea mas barato, las medias de
todos los precios de los combustibles y otras muchas posibilidades de las cuales ya hemos
hablado en sus respectivos apartados.

Futuras mejoras:

En futuras actualizaciones trabajaremos en mejorar nuestra aplicación con mejoras como:

-Los usuarios podrán darse de alta en nuestra BBDD y así beneficiarse de promociones
especiales en las estaciones de servicio adheridas. Para ello se creara una nueva sección en
nuestra aplicación llamada “Promociones” y en ese apartado se verán disponibles las diferentes
promociones y en qué estaciones de servicio se podrán beneficiarse de ello.

-La posibilidad de crear rutas de viaje y obtener las gasolineras en ella y asi poder seleccionar la
más conveniente antes de empezar el viaje.

-También se creara un nueva modalidad que funcionará de historial para el precio de los
diferentes combustibles para así poder saber cuando es el mejor momento para repostar

-Otra de las futuras funcionalidades de nuestra aplicación será la posibilidad de guardar tus
estaciones favoritas y así tener acceso rápido y a tiempo real de tus estaciones favoritas, para
comprobar cómodamente varias gasolineras y así ver donde te interesa repostar.
