**Descripción**

Este proyecto utiliza una colección de 206 documentos de noticias en formato .txt.
Cada documento se identifica mediante un nombre consecutivo:

* doc_001.txt
* doc_002.txt
* doc_003.txt
* ...
* doc_206.txt

**Preparación de los datos** 
Colocar los documentos originales en una carpeta llamada:
noticias_crudas/

**Carga en HDFS**
* Crear el directorio de entrada:
hdfs dfs -mkdir -p /practica/entrada
* Cargar los documentos:
hdfs dfs -put noticias_crudas/*.txt /practica/entrada/
* Verificar la carga:
hdfs dfs -ls /practica/entrada
