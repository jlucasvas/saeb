
##### IMPORTANTE
##### Escrevi o código em Spark para transformar inicialmente os dados da base alunos
##### de acordo com a tabela dicionário.. Mas parei no meio do processo, pois também avaliei
##### fazer essas transformações no dbt (no caso extrair as duas tabelas e transformar em SQL)



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast


spark = SparkSession.builder \
    .appName("ETL Alunos para BigQuery") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1") \
    .getOrCreate()

GCS_BUCKET = "gs://bucket_removi_segurança"
BQ_DATASET = "dataset_removi_segurança"

# leitura dos dados do GCS
alunos_df = spark.read.parquet(f"{GCS_BUCKET}/raw/aluno_ef_9ano/")
dicionario_df = spark.read.parquet(f"{GCS_BUCKET}/raw/dicionario/")

# colunas únicas no dicionário
colunas_dicionario = dicionario_df.select("nome_coluna").distinct().rdd.flatMap(lambda x: x).collect()

# dicionário para um join único usando pivot()
dict_pivot_df = dicionario_df \
    .withColumnRenamed("valor", "descricao") \
    .groupby("chave", "cobertura_temporal") \
    .pivot("nome_coluna").agg({"descricao": "first"})

# join dinâmica para todas as colunas
join_condition = [(alunos_df["ano"] == dict_pivot_df["cobertura_temporal"])]
for col_name in colunas_dicionario:
    join_condition.append(alunos_df[col_name] == dict_pivot_df[col_name])

# join único
alunos_df = alunos_df.join(
    broadcast(dict_pivot_df),
    join_condition,
    "left"
).drop("cobertura_temporal") 

# Seleciona todas as colunas transformadas dinamicamente
colunas_transformadas = alunos_df.columns  # Agora pega todas as colunas, sem precisar especificar manualmente

alunos_transformados_df = alunos_df.select(*colunas_transformadas)

# Escreve os dados transformados no BigQuery (particionado por ano)
alunos_transformados_df.write.format("bigquery") \
    .option("table", f"{BQ_DATASET}.alunos_transformados") \
    .option("partitionField", "ano") \
    .mode("append") \
    .save()

spark.stop()