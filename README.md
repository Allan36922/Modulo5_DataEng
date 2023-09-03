
#  Datapath 
#  Programa: Data Engineering Program 13va edición
# Tema: Entrega de proyecto migración end to end Azure Datalake

Profesor: Rubén Quispe
Estudiantes: Allan Mora, Lorenzo Guerrero.


## Enunciado: 

Migracion de una base de datos MySql alojada en Google Cloud Platform a un datalake en Microsoft Azure, Databricks y synapse. 
El objetivo es realizar la migración desde el entorno local hasta el cloud, y posteriormente hacer uso de las capacidades analitIcas que ofrece Azure.


## El paso a paso en la migración de una base de datos MySql a Azure Datalake:

### Configuración previa:
- Antes de comenzar, asegúrse de tener una cuenta de Azure y acceso a Azure Data Factory, Databricks y Synapse. 

### Desarrollo:

Migración de una base de datos MySql alojada en Google Cloud Platform a un datalake en Microsoft Azure, utilizando: 
1- Data Storage para la creación de las tres áreas del Datalake:
    - Bronce: consume las tablas de MySQL y las guarda en formato avro.
             : se almacena la lectura de logs en formato csv.
    - Silver: se ingesta las tablas en formato parquet para ser analizadas por synapse.
2- Data Factory para la ingesta de datos utilizando pipelines:
    a- Creación de un pipeline Master que orquestara la ejecución de los otros pipelines.
    b- Creación de un pipeline para la migración de datos entre on-premise a bronce.
    c- Creación de un pipeline para la migración de datos entre bronce a silver.
3- Azure Databricks 
    - Leer los logs del copy data (On-prime a bronce) y dejarlos en formato csv para su consumo.
    - Agregar columnas de validación al csv como la fecha de finalización y diferencia en cantidad de registros Rows Read - Rows Write, esto como indicado de error para mostrar en un dashboard en Power BI.
    - Tratamiento de los archivos logs resumiéndolos con spark y generando un solo dataframe para ingesta en la tabla de bitácora con formato parquet.
4- Azure Synapce Analytics  
    - Synapse Analytics para el procesamiento de las tablas en formato parquet.
    - Se creo una base de datos serverless llamada retail_db.
    - generación de KPIS a partir de las tablas alojadas en la base de datos retail_db.
    - se dejaron listos las tablas, los querys de los kpi y los logs resumidos en una tabla para su consumo.


 
### Detalle de arquitectura:

![Arquitectura de la solución propuesta.](https://github.com/Allan36922/Modulo4_DataEng/blob/main/imgs/10-Corrida%20exitosa.png)


* Ambiente On-premise 
	- Servidor: MySql
	- Base Datos: retail_db
	- Linkes: ls_retaildb_mysql_dwh
	- DataSet: ds_mysql_tables_list

* Ambiente Datalake 
Resource groups | M5-ProyectoFinal 
	- Storage account : storageretaillake
	- Data factory (V2): adfretailm5
	- Azure Databricks Service : dbm5
	- Synapse workspace : m5proyfinalsynapse
	

storageretaillake | Containers
	- $logs Private 
	- storagebqbronce Container 	
	- storagebqsilver Container 
	- storagebqgold Container 

Containers | storagebqbronce | 
	- Blob type : Block blob	
	- categories.avro
	- customers.avro
	- departments.avro
	- order_items.avro
	- orders.avro
	- products.avro
outputlog : Directory
	- fileoutputlog_2023-09-02 16:02:49.961721.csv
	- fileoutputlog_2023-09-02 16:03:22.078942.csv
	- fileoutputlog_2023-09-02 16:03:50.349318.csv
	- fileoutputlog_2023-09-02 16:04:23.192486.csv
	- fileoutputlog_2023-09-02 16:04:52.514245.csv
	- fileoutputlog_2023-09-02 16:05:24.087958.csv

Containers | storagebqsilver | 
	- Blob type : Block blob	
	- categories.parquet
	- customers.parquet
	- departments.parquet
	- order_items.parquet
	- orders.parquet
	- products.parquet
	- outputlog.parquet

Containers | storagebqgold | 
	- No Files



* Pipilenes: 
	- 01-Master
	- 02-OnPrime-Bronce
	- 03-Bronce-Silver
	
Pipeline : 01-Master Pipeline
	02-OnPrime-Bronce | Pln-OnPrime-Bronce
	03-Bronce-Silver | Pin-Bronce-Silver

Pipeline : 02-OnPrime-Bronce
	Get Tables Names: (Lee la lista de tablas de on-prime)	
		Source Datasets : ds_mysql_table_list (lista de tablas)
		Query : 'SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables WHERE table_schema = 'retail_db''
	
    ForEach : () ( recibe el listado de tablas para ser almacenadas)
    Datasets : ds_mysql_table_list (Almacenar el nombre de las tablas de la base de datos on-premises)
    Parameters : @activity('Get Tables Names').output.value  (Parametro de entrada usado en el ForEach proveniente de la actividad de Get Tables Names)
        Copy data2: 
            Source:
                Source Datasets : ds_MySql_source (Dataset tipo mysql empleado para leer cada una de las tablas)
                Parameters : @item().TABLE_NAME (Parametro empleado en ds_MySQL_source para obtener cada uno de los nombres provenientes de la actividad de Get List de manera dinamica)
            Sink:
                Sink dataset : Avro1 (Cada tabla se guarda en formato avro dentro del Storage Bronce)
                Dataset properties : @concat(item().TABLE_NAME,'.avro')  (Empleado para darle el tipo de formato a cada uno de los archivos avro que corresponde a cada tabla)
        Validate data bronce_copy1 (Databricks)    
            Notebook path : /M5_Proyect/Notebook-Validation-Data (Notebook empleado para realizar las transformaciones y logs respectivos)
            Base parameters: adf_input : @string(activity('Copy data2').output) (Parametro de entrada usado en el databricks , para leer la salida generada de cada actividad de copy data)


Pipeline : 03-Pin-Bronce-Silver
	Get Data Bronce: (Lee la lista de tablas de Bronce)	
		Dataset : Avro2 (lee las tablas en formato avro dentro del Storage bronce)
		Argument : Child Items
		
    ForEach : () ( recibe el listado de tablas para ser almacenadas en formato parquet en Silver area)	
		Copy data migrate tables b-s:
			Source dataset : Avro1 (listado de tablas que se guardaran en formato parquet en Silver)
			Sink dataset  : ds_destination_silver

Linked services:
AzureDatabricks1 | Azure Databricks
ls_adls_silver | Azure Data Lake Storage Gen2
ls_retaildb_adls_temp | Azure Data Lake Storage Gen2
ls_retaildb_mysql_dwh | MySQL 

Datasets:
02-OnPrime-Bronce | Pln-OnPrime-Bronce
	ds_mysql_table_list
	ds_MySql_source
	Avro1
03-Pin-Bronce-Silver | Pin-Bronce-Silver
	Avro2
	Avro3
	ds_destination_silver





### El paso a paso en la migración de MYSql a Azure Datalake:

Ofrecemos aqui una posible guia paso a paso para migrar de una base de datos MySQL a un datalake en Microsoft Azure, utilizando Data Factory, Databricks y Synapse y sería el siguiente:

1- Crear el servicio vinculado de MySQL en Data Factory, que será el origen de la migración. 
2- Crear un conjunto de datos de MySQL en Data Factory, que definirá la estructura y el formato de los datos de origen. 
3- Crear una actividad de copia en Data Factory, que copiará los datos desde MySQL a Azure Data Lake Storage Gen2. 
4- Crear un servicio vinculado de Azure Data Lake Storage Gen2 en Data Factory, que será el destino de la migración. 
5- Crear un conjunto de datos de Azure Data Lake Storage Gen2 en Data Factory, que definirá la estructura y el formato de los datos de destino.
6- Crear un servicio vinculado de Azure Databricks en Data Factory, que se utilizará para ejecutar los notebooks de Databricks que transformarán los datos durante la migración. 
7- Crear un notebook de Databricks, que transformará los datos copiados a un formato optimizado para el análisis, como Parquet o Delta Lake. 
8- Crear una actividad de Databricks en Data Factory, que ejecutará el notebook creado en el paso anterior. 
9- Crear un servicio vinculado de Azure Synapse Analytics en Data Factory, que se utilizará para acceder al datalake desde Synapse. 
10- Conectar Azure Synapse Analytics al datalake creado en Azure Data Lake Storage Gen2, para poder acceder y analizar los datos migrados desde Synapse.
11- Crear una canalización en Data Factory, que encadenará las actividades creadas en los pasos anteriores y definirá los parámetros y las dependencias entre ellas.
12- Ejecutar y supervisar la canalización creada en Data Factory, que realizará la migración de los datos desde MySQL a Azure Data Lake Storage Gen2.

### Links de interes:
Links de interes:
### [(1) Copia y transformación de datos en Azure Data Lake Storage Gen2](https://learn.microsoft.com/es-es/azure/data-factory/connector-azure-data-lake-storage)



---------------eof