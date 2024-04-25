# Delta Lake & Apache Iceberg

#### Pré-requisitos: 
- Docker

#### Versões das tecnologias: 
- Apache Spark 3.3.1
- Apache Iceberg 1.3.1
- Delta io 2.1.0

---

## Passo a passo para subir e configurar os ambientes:

#### 1. Baixar/Clonar repositório no GitHub para o VSCode

#### 2. Abrir o terminal no VSCode dentro do projeto e executar o comando:

```sh
$ docker-compose up spark_notebook
```

> Comando irá iniciar e baixar dependências necessárias do nosso container responsável por executar o Jupyter Notebook

#### 3. Abrir um segundo terminal no VSCode e executar o comando:

```sh
$ docker-compose up minioserver
```
> Instância de object store para salvar os dados que serão gerados pelos nossos exemplos

#### 4. Abrir um terceiro terminal no VSCode e executar o comando:

```sh
$ docker-compose up dremio
```
> Barramento de dados para unificar as camadas de storage/banco de dados, útil para storage distribuídos

#### 4. Abrir um quarto terminal no VSCode e executar o comando:

```sh
$ docker-compose up nessie
```
> Ferramenta para versionamento de códigos/dados, é semelhante ao GitHub, mas voltada a Engenharia de Dados
`
#### 5. No terminal que foi iniciado o minioserver

- Acessar o link na porta 9001 e utilizar as credenciais disponíveis no arquivo .env (minio_root_user e minio_root_password) para acessar a ferramenta

    - Criando nosso object store e access key
    - Object Store: Navegar até “Bucket” e criar o nosso Object Store, forneça apenas um nome e faça criação dele
    - Com o object criado coloque o nome fornecido a ele no campo Warehouse no arquivo .env
    - Access Key: Navegar até “Access Keys” e criar a nossa chave de acesso para podermos realizar a conexão ao nosso object store
    - Copie o access key e o secret key, e coloque ele arquivo .env e substitua as informações “aws_access_key_id” e “aws_secret_access_key”, respectivamente.
    - Obs.: No arquivo .env forneça as informações corretas de acordo com a sua região para AWS_REGION e MINIO_REGION`


### CONFIGURANDO A UTILIZAÇÃO PARA APACHE ICEBERG

#### 6. No terminal que iniciamos o Jupyter Notebook (docker-compose up spark_notebook)
- Acessar o link fornecido que está rodando na porta 8888

- Criar um novo notebook e adicionar os seguintes comandos para configuração e manipulação do ambiente Apache Iceberg

```
# Imports necessários para manipulações
import pyspark
from pyspark.sql import SparkSession
import os


## DEFINE VARIAVEIS
NESSIE_URI = os.environ.get("NESSIE_URI") ## Nessie Server URI
WAREHOUSE = os.environ.get("WAREHOUSE") ## BUCKET PARA ESCREVER OS DADOS
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY") ## AWS CREDENCIAIS
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY") ## AWS CREDENCIAIS
AWS_S3_ENDPOINT= os.environ.get("AWS_S3_ENDPOINT") ## MINIO ENDPOINT


print(AWS_S3_ENDPOINT)
print(NESSIE_URI)
print(WAREHOUSE)
```
Resultado:

`http://minioserver:9000`

`http://nessie:19120/api/v1`

`s3a://warehouse/`

```
## Realiza configuração das dependencias para utilização do Apache Iceberg e ambiente para criar, alterar e deletar dados (minio e nessie)
conf = (
    pyspark.SparkConf()
        .setAppName('app_name')
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178')
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
        .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.nessie.uri', NESSIE_URI)
        .set('spark.sql.catalog.nessie.ref', 'main')
        .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
        .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
        .set('spark.sql.catalog.nessie.s3.endpoint', AWS_S3_ENDPOINT)
        .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
        .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)
        .set('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)
)


## Inicia Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Funcionado...")
```
Resultado:

`
...
Output is truncated. View as a scrollable element or open in a text editor. Adjust cell output settings...
24/04/22 23:59:12 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
24/04/22 23:59:15 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.`

`Spark Funcionado...`

```
## Cria Tabela Carros

spark.sql(
    """create or replace table nessie.carros (placa string, 
                                              modelo string, 
                                              chassi string, 
                                              marca string, 
                                              ano integer, 
                                              cor string)
    using iceberg"""
)
```

```
## Insere dados na tabela criada

spark.sql(
    """insert into nessie.carros (placa, modelo, chassi, marca, ano, cor) values
    ('ABC1234', 'Civic', '1234567890', 'Honda', 2020, 'Preto'),
    ('XYZ5678', 'Corolla', '0987654321', 'Toyota', 2019, 'Branco'),
    ('DEF9876', 'Golf', '1357902468', 'Volkswagen', 2018, 'Prata'),
    ('GHI6543', 'Fiesta', '9876543210', 'Ford', 2017, 'Azul'),
    ('JKL3210', 'Cruze', '5432167890', 'Chevrolet', 2016, 'Vermelho'),
    ('MNO0987', 'Focus', '3210987654', 'Ford', 2015, 'Verde'),
    ('PQR7654', 'Fusca', '6789012345', 'Volkswagen', 2014, 'Amarelo'),
    ('STU4321', 'Onix', '0123456789', 'Chevrolet', 2013, 'Cinza'),
    ('VWX1098', 'Fit', '2345678901', 'Honda', 2012, 'Roxo'),
    ('YZA2468', 'HB20', '4567890123', 'Hyundai', 2011, 'Laranja'),
    ('BCD8642', 'Sentra', '6789012345', 'Nissan', 2010, 'Marrom'),
    ('EFG1357', 'Ecosport', '8901234567', 'Ford', 2009, 'Prata'),
    ('HIJ7530', 'Uno', '0123456789', 'Fiat', 2008, 'Preto'),
    ('KLM3698', 'Cobalt', '2345678901', 'Chevrolet', 2007, 'Branco'),
    ('NOP8524', 'Logan', '4567890123', 'Renault', 2006, 'Azul'),
    ('QRS9513', 'Civic', '6789012345', 'Honda', 2005, 'Vermelho'),
    ('TUV2468', 'Celta', '8901234567', 'Chevrolet', 2004, 'Verde'),
    ('WXY3698', 'Palio', '0123456789', 'Fiat', 2003, 'Amarelo'),
    ('ZAB8524', 'Polo', '2345678901', 'Volkswagen', 2002, 'Cinza'),
    ('CDE9513', 'Siena', '4567890123', 'Fiat', 2001, 'Roxo');"""
)
```

```
## Consulta os dados da tabela carros

spark.sql(
    "select * from nessie.carros"
).show()
```

```
## Exemplo de update na tabela carros

spark.sql(
    "update nessie.carros set cor = 'Verde Limão' where placa = 'ABC1234'"
)
```

```
## Consulta os dados da tabela carros

spark.sql(
    "select * from nessie.carros"
).show()
```

```
## Exemplo de delete na tabela carros

spark.sql(
    "delete from nessie.carros where placa = 'XYZ5678'"
)
```

```
## Consulta os dados da tabela carros

spark.sql(
    "select * from nessie.carros"
).show()
```

### CONFIGURANDO A UTILIZAÇÃO PARA DELTA LAKE

#### 7. No Jupyter Notebook, criar um novo notebook para configuração e manipulação do ambiente Delta Lake
- Primeiramente, acessar o Docker Desktop e procurar pelo nome do projeto. Procurar pelo container que está rodando o spark_notebook, está com o nome “notebook”, abrir um terminal e navegar até a raiz “/”. Obs.: É um ambiente Linux

 - No terminal aberto rodar o comando “sudo pip install delta-spark==2.3.0” para instalar o delta-spark para utilização no nosso ambiente Delta Lake
 
 - No nosso notebook criado para o delta lake, executar os comandos necessários para funcionamento

```
# Realiza importações necessárias para manipulação
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from delta import *

import pyspark
import os # não precisa esse import no readme

## DEFINE VARIAVEIS - Não precisa colocar essa parte no readme
NESSIE_URI = os.environ.get("NESSIE_URI") ## Nessie Server URI
WAREHOUSE = os.environ.get("WAREHOUSE") ## BUCKET PARA ESCREVER OS DADOS
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY") ## AWS CREDENCIAIS
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY") ## AWS CREDENCIAIS
AWS_S3_ENDPOINT= os.environ.get("AWS_S3_ENDPOINT") ## MINIO ENDPOINT
```

```
# Create SparkSession e configura as dependencias para funcionamento do ambiente delta
    
conf = (
    pyspark.SparkConf()
        .setAppName('app_name')
        .set('spark.jars.packages', 'io.delta:delta-core_2.12:2.3.0')
        .set('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .set('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
)

## Inicia Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Funcionando...")
```
Resultado:

`
...
24/04/23 23:07:55 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).`

`Spark Funcionando...`

```
## Prepara dados para criação da tabela no formato delta, isso gera um dataframe para nós

data = [
    ("ID001", "CLIENTE_X","SP","ATIVO",   250000.00),
    ("ID002", "CLIENTE_Y","SC","INATIVO", 400000.00),
    ("ID003", "CLIENTE_Z","DF","ATIVO",   1000000.00)
]

schema = (
    StructType([
        StructField("ID_CLIENTE",     StringType(),True),
        StructField("NOME_CLIENTE",   StringType(),True),
        StructField("UF",             StringType(),True),
        StructField("STATUS",         StringType(),True),
        StructField("LIMITE_CREDITO", FloatType(), True)
    ])
)

df = spark.createDataFrame(data=data,schema=schema) # Cria o dataframe

df.show(truncate=False) # Printa o dataframe
```
Resultado:

|ID_CLIENTE|NOME_CLIENTE|UF |STATUS |LIMITE_CREDITO|
|----------|------------|---|-------|--------------|
|ID001     |CLIENTE_X   |SP |ATIVO  |250000.0      |
|ID002     |CLIENTE_Y   |SC |INATIVO|400000.0      |
|ID003     |CLIENTE_Z   |DF |ATIVO  |1000000.0     |


```
## Cria tabela no formato delta

( 
    df
    .write
    .format('delta')
    .save('/tmp/delta-table') # Aqui é passado o nosso diretório para salvar os arquivos
)
```
Resultado:

`
24/04/23 23:09:48 WARN package: Truncated the string representation of a plan since it was too large. This behavior can be adjusted by setting 'spark.sql.debug.maxToStringFields'.
`

```
df = spark.read.format("delta").load("/tmp/delta-table") # Lê os arquivos
df.show() 
```

Resultado: 

|ID_CLIENTE|NOME_CLIENTE| UF| STATUS|LIMITE_CREDITO|
|----------|------------|---|-------|--------------|
|     ID002|   CLIENTE_Y| SC|INATIVO|      400000.0|
|     ID001|   CLIENTE_X| SP|  ATIVO|      250000.0|
|     ID003|   CLIENTE_Z| DF|  ATIVO|     1000000.0|


```
## Prepara dados para upsert e merge na tabela criada

new_data = [
    ("ID001","CLIENTE_X","SP","INATIVO", 0.00),
    ("ID002","CLIENTE_Y","SC","ATIVO",   400000.00),
    ("ID004","CLIENTE_Z","DF","ATIVO",   5000000.00)
]

df_new = spark.createDataFrame(data=new_data, schema=schema)

df_new.show()
```
Resultado: 

|ID_CLIENTE|NOME_CLIENTE| UF| STATUS|LIMITE_CREDITO|
|----------|------------|---|-------|--------------|
|     ID001|   CLIENTE_X| SP|INATIVO|           0.0|
|     ID002|   CLIENTE_Y| SC|  ATIVO|      400000.0|
|     ID004|   CLIENTE_Z| DF|  ATIVO|     5000000.0|


```
## Realiza upsert e merge na tabela criada

deltaTable = DeltaTable.forPath(spark, "/tmp/delta-table")

(
    deltaTable.alias("dados_atuais")
    .merge(
        df_new.alias("novos_dados"),
        "dados_atuais.ID_CLIENTE = novos_dados.ID_CLIENTE"
    )
    .whenMatchedUpdateAll() # Atualiza os dados caso existam
    .whenNotMatchedInsertAll() # Caso o dado não exista ele insere
    .execute()
)
```

```
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()
```
Resultado:

|ID_CLIENTE|NOME_CLIENTE| UF| STATUS|LIMITE_CREDITO|
|----------|------------|---|-------|--------------|
|     ID001|   CLIENTE_X| SP|INATIVO|           0.0|
|     ID002|   CLIENTE_Y| SC|  ATIVO|      400000.0|
|     ID004|   CLIENTE_Z| DF|  ATIVO|     5000000.0|
|     ID003|   CLIENTE_Z| DF|  ATIVO|     1000000.0|


```
## Deleta registro que o limite de credito é menor que 400000.0

deltaTable.delete("LIMITE_CREDITO < 400000.0")
```

```
df = spark.read.format("delta").load("/tmp/delta-table")
df.show()
```
Resultado:

|ID_CLIENTE|NOME_CLIENTE| UF|STATUS|LIMITE_CREDITO|
|----------|------------|---|------|--------------|
|     ID002|   CLIENTE_Y| SC| ATIVO|      400000.0|
|     ID004|   CLIENTE_Z| DF| ATIVO|     5000000.0|
|     ID003|   CLIENTE_Z| DF| ATIVO|     1000000.0|
