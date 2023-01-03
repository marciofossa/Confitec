#!/usr/bin/env python
# coding: utf-8

# In[18]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import TimestampType
spark = SparkSession.builder.appName("pysparkteste").getOrCreate()


# In[19]:


parDF = spark.read.parquet('OriginaisNetflix.parquet')
parDF.printSchema()


# In[32]:


parDF.columns


# In[23]:


# 1 - Convertendo as colunas dt_inclusao e Premiere para Timestamp - apenas no resultado da consulta
parDF.select('dt_inclusao',to_timestamp('dt_inclusao',"yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),             'Premiere',to_timestamp('Premiere','d-MMM-yy')).show(truncate=False)


# In[22]:


# 1 - Convertendo as colunas dt_inclusao e Premiere para Timestamp de forma permanente em outro sparkDataFrame
parDF2 = parDF.withColumn('Premiere',to_timestamp(col("Premiere"),'d-MMM-yy'))             .withColumn('dt_inclusao',to_timestamp(col("dt_inclusao"),"yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
parDF2['Premiere','dt_inclusao'].show(truncate=False)


# In[24]:


# 2 - Ordenar os dados por ativos e genero
parDF2.createOrReplaceTempView("netflix")
spark.sql("select * from netflix order by Active desc, Genre").show(truncate=False)


# In[34]:


# 3 remover linas duplicadas e trocar o resultado da coluna Seasons de TBA para a ser anunciado
query = """
select distinct
Title,
Genre,
GenreLabels,
Premiere,
replace(Seasons,'TBA','a ser anunciado') as Seasons,
SeasonsParsed,
EpisodesParsed,
Length,
MinLength,
MaxLength,
Status,
Active,
Table,
Language,
dt_inclusao
from netflix 
"""
parDF3 = spark.sql(query)
parDF3.show(truncate=False)


# In[61]:


# 4 incluir uma colunas chamada Data de alteracao com um timestamp
parDF3.createOrReplaceTempView("netflix")
query = """
select netflix.*,current_timestamp as data_de_alteracao from netflix
"""
parDF3 = spark.sql(query)
# parDF3.show(truncate=False)
parDF3.columns


# In[63]:


# 5 Traduzir as colunas para o portugues com acentuação
parDF3.createOrReplaceTempView("netflix")
query = """
select 
Title as Título,
Genre as Gênero,
GenreLabels as Gênero_Descrição,
Premiere as Pré_estreia,
Seasons as Temporada,
SeasonsParsed as Temporadas_Analisados,
EpisodesParsed as Episódios_Analisados,
Length as Tamanho,
MinLength as Tamanho_Mínimo,
MaxLength as Tamanho_Máximo,
Status ,
Active as Ativo,
Table as Tabela,
Language as Idioma,
dt_inclusao as Data_de_Inclusão,
data_de_alteracao as Data_de_Alteração
from netflix 
"""
spark.sql(query).show(truncate=False)


# In[66]:


# 6 - Ocorreu erro por conta dos acentos, gerando a tradução sem acentos
parDF3.createOrReplaceTempView("netflix")
query = """
select
Title as Titulo,
Genre as Genero,
GenreLabels as Genero_Descricao,
Premiere as Pre_estreia,
Seasons as Temporada,
SeasonsParsed as Temporadas_Analisados,
EpisodesParsed as Episodios_Analisados,
Length as Tamanho,
MinLength as Tamanho_Minimo,
MaxLength as Tamanho_Maximo,
Status ,
Active as Ativo,
Table as Tabela,
Language as Idioma,
dt_inclusao as Data_de_Inclusao,
data_de_alteracao as Data_de_Alteracao
from netflix
"""
parDF4 = spark.sql(query)
parDF4.show(truncate=False)


# In[76]:


# Criar apenas um arquivo csv com o conteúdo do dataframe 
parDF4['Titulo',
 'Genero',
 'Temporada',
 'Pre_estreia',
 'Idioma',
 'Ativo',
 'Status',
 'Data_de_Inclusao',
 'Data_de_Alteracao'] \
.coalesce(1) \
.write.format("csv") \
.options(header="True",delimiter=";") \
.mode("overwrite") \
.save("netflix/netflix.csv")


# In[ ]:


#importar para um bucket aws s3
import boto3
s3 = boto3.resource('s3')
bucket_name = 'pysparkteste'
file_name = 'netflix/netflix.csv'
s3.Bucket(bucket_name).upload_file(file_name,file_name)

