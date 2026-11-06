Recuperación de Documentos y Análisis de Similitud con Hadoop

**Descripción**

Proyecto de recuperación de información desarrollado con Java y Apache Hadoop MapReduce, cuyo objetivo es procesar una colección de documentos de noticias, identificar los términos más relevantes y recuperar documentos relacionados mediante similitud del coseno.

**Tecnologías utilizadas**

*  Java 21 (compilación compatible con Java 11 mediante --release 11).
*  Apache Hadoop 3.4.1.
*  Hadoop Distributed File System (HDFS).
*  MapReduce.
*  Ubuntu.
*  TF (Term Frequency).
*  TF-IDF (Term Frequency-Inverse Document Frequency).
*  Similitud del coseno.

**Flujo de procesamiento**

El proyecto se compone de seis etapas:

1-Bolsa de palabras (Bag of Words): contabiliza las apariciones de cada término dentro de cada documento.
2-Frecuencia de términos (TF): calcula la frecuencia relativa de cada palabra respecto al total de términos del documento.
3-Índice invertido: agrupa los documentos en los que aparece cada palabra y almacena sus valores TF.
4-TF-IDF: calcula la importancia de cada término considerando su frecuencia en el documento y su distribución en toda la colección. Los vectores resultantes se normalizan.
5-Similitud del coseno: compara los vectores TF-IDF de los documentos para determinar qué tan similares son.
6-Consulta y recuperación: permite buscar términos, identificar el documento con mayor frecuencia relativa y recuperar los documentos más similares.

**Estructura del proyecto**

<img width="286" height="502" alt="image" src="https://github.com/user-attachments/assets/016bf1ec-b496-4b75-bcd5-5ba57a09797f" />


**Requisitos**

* Ubuntu o una distribución Linux.
* Java JDK 21 o compatible.
* Apache Hadoop 3.4.1.
* HDFS y YARN configurados.
* Una colección de documentos .txt.

**Instalación y compilación**
* Clonar el repositorio:
git clone URL_DEL_REPOSITORIO
cd RecuperacionDocumentos-Hadoop

* Compilar las clases para Java 11:
javac --release 11 -classpath "$(hadoop classpath)" *.java

* Crear el archivo:
   JAR:jar -cvf proyecto.jar *.class
* Carga de documentos en HDFS
*Iniciar los servicios de Hadoop:
start-dfs.sh
start-yarn.sh

*Crear el directorio de entrada:
hdfs dfs -mkdir -p /practica/entrada

* Copiar los documentos .txt desde el sistema local hacia HDFS:
hdfs dfs -put /ruta/a/noticias_crudas/*.txt /practica/entrada/

*Verificar los archivos cargados:
hdfs dfs -ls /practica/entrada

**Ejecución del procesamiento MapReduce**
Ejecución del procesamiento MapReduce
Ejecutar las etapas en el siguiente orden:

1. Bolsa de palabras
hadoop jar proyecto.jar BolsaPalabras /practica/entrada /practica/bolsa_palabras

2. Frecuencia de términos (TF)
hadoop jar proyecto.jar FrecuenciaTerminos /practica/bolsa_palabras /practica/tf

3. Índice invertido
hadoop jar proyecto.jar IndiceInvertido /practica/tf /practica/indice_invertido

4. TF-IDF
Se utiliza 206 como número total de documentos:
hadoop jar proyecto.jar TFIDF /practica/indice_invertido /practica/tfidf 206

5. Similitud del coseno
hadoop jar proyecto.jar SimilitudCoseno /practica/tfidf /practica/similitud

6. Consultas
Buscar un término y recuperar los cinco documentos más similares:
hadoop jar proyecto.jar Consulta economia 5

Otros ejemplos:

hadoop jar proyecto.jar Consulta dinero 10
hadoop jar proyecto.jar Consulta sociedad 15
hadoop jar proyecto.jar Consulta tecnologia 5
