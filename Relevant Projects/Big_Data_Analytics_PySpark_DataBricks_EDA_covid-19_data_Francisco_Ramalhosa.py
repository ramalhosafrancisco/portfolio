# Databricks notebook source
# MAGIC %md
# MAGIC # Segunda Parte - Exploratory Data Analysis (EDA) - utilização de Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contexto
# MAGIC
# MAGIC #### Projeto Final - BIG DATA ANALYTICS - EDIT 2023.
# MAGIC
# MAGIC __Cenário:__  
# MAGIC Fui contratado como consultor em 'Big Data Analytics' pelo Ministério de Saúde dos Estados Unidos (EUA) para analisar os mais recentes dados da COVID-19. Os meus dois principais objetivos são: um procedimento de ingestão, transformação e carregamento dos dados ('Extract Transformation and Load - ETL'); e o outro é a análise dos dados ('Exploratory Data Analysis - EDA'). Neste notebook será abordado o segundo objetivo (EDA). O primeiro objetivo foi desenvolvido no notebook que contém a primeira parte do Projeto Final: 'francisco_ramalhosa_etl.ipynb'.  
# MAGIC
# MAGIC __EDA__:  
# MAGIC O Departamento de Saúde dos Estados Unidos quer que analise a informação que tem em Pyspark para que depois, se a analise for realmente prometedora, possam ser integrados mais dados, e sem importar o volume destes, possam ser analisados os novos dados. Se for preciso usar algum Dataset previamente carregado no Snowflake, devo fazer a ligação utilizando o desenvolvido no primeiro ponto (e não diretamente com o ficheiro CSV). O Departamento de Saúde dos EUA conta comigo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importação de bibliotecas

# COMMAND ----------

import pandas as pd
import numpy as np
import time
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classe definida no ETL

# COMMAND ----------

# A classe 'DatabricksSnowflakeConnection' definida no notebook do 'ETL', será aqui também utilizada para fazer as leituras das tabelas que foram previamente carregadas para o SF.
# As tabelas serão necessárias para a realização da 'EDA'.

class DatabricksSnowflakeConnection:
    def __init__(self, host, user, password, dw, db, schema, table):
        self.host = host
        self.user = user
        self.password = password
        self.dw = dw
        self.db = db
        self.schema = schema
        self.table = table
        options1 = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "sfWarehouse": self.dw,
            "database": self.db,
            "schema": self.schema,
        }
        try:
            connection = (
                spark.read.format("snowflake")
                .options(**options1)
                .option("dbtable", self.table)
                .load()
            )
            return print(
                "Connection between Databricks and Snowflake executed successfully"
            )
        except:
            print("Connection between Databricks and Snowflake failed")

    def read_table(self, db_read, schema_read, table_read, query=None, type=True):
        options2 = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "sfWarehouse": self.dw,
            "database": db_read,
            "schema": schema_read,
        }
        try:
            if type == True:
                df = (
                    spark.read.format("snowflake")
                    .options(**options2)
                    .option("dbtable", table_read)
                    .load()
                )
                return df
            else:
                df = (
                    spark.read.format("snowflake")
                    .options(**options2)
                    .option("query", query)
                    .load()
                )
                return df
        except Exception as e:
            print(f"Error description: {e}")

    def write_table(self, df, db_write, schema_write, table_write):
        options3 = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "sfWarehouse": self.dw,
            "database": db_write,
            "schema": schema_write,
        }
        try:
            start_time = time.time()
            df.write.format("snowflake").options(**options3).option("dbtable", table_write).save()
            end_time = time.time()
            time_total = end_time - start_time
            time_total_rounded = round(time_total, 2)
            dict_info_tabela = {
                "Tempo total transcorrido (segundos)": time_total_rounded,
                "Schema": self.schema,
                "Tabela": table_write,
                "Numero de columnas": len(df.columns),
                "nome das colunas": df.columns,
                "Numero de linhas": df.count(),
            }
            return dict_info_tabela
        except Exception as e:
            print(f"Error description: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conexão com SF com recurso ao objeto criado a partir da classe definida no ETL

# COMMAND ----------

# Objeto 'francisco' permite estabelecer e verificar a ligação entre o Databricks (DB) e o SF.
# Na eventualidade de ainda não terem sido criados novos 'databases', 'schemas' e/ou 'tabelas' no SF, a definição da função de conexão será estabelecida por intermédio da leitura de uma das tabelas de amostragem previamente forneceidas pelo SF. 
# Se a conexão for bem-sucedida, uma mensagem indicando o sucesso é impressa. Se ocorrer uma exceção (erro), uma mensagem de falha é impressa.
# Mensagem de sucesso expectável: "Connection between Databricks and Snowflake executed successfully".

francisco = DatabricksSnowflakeConnection(
    host="txdygja-lz90113.snowflakecomputing.com",
    user="RAMALHOSAFRANCISCO",
    password="Edit2023_",
    dw="COMPUTE_WH",
    db="SNOWFLAKE_SAMPLE_DATA",
    schema="TPCH_SF10",
    table="CUSTOMER",
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Qual é quantidade de pessoas do género feminino e masculino e a sua percentagem sobre o total de doentes? 
# MAGIC

# COMMAND ----------

# importação e leitura de DF_PATIENTS desde o SF, com recurso às propriedades da classe 'DatabricksSnowflakeConnection' previamente definida.

df_patients = francisco.read_table(
    "EDIT2023", "PROJETO_FINAL", "DF_PATIENTS", query=None, type=True
)

# COMMAND ----------

# Total de pacientes.
patients_total = df_patients.count()

# Contagem de pacientes por género (Masculino (M) e Feminino (F)).
df_patients_gender = df_patients.groupBy("GENDER").count()

# Percentagem (%) do género sobre o total de doentes.
df_patients_gender = df_patients_gender.withColumn(
    "GENDER_DISTRIBUTION (%)",
    F.round((df_patients_gender["count"] / patients_total) * 100, 2),
)

# O seguinte DF incluí as informações relativas à quantidade de pessoas do género feminino e masculino e a sua percentagem sobre o total de doentes
df_patients_gender.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1. Crie uma visualização com esta informação (gráfico de barras) 

# COMMAND ----------

df_patients_gender.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Identifique se existe informação de doentes com data de nascimento superior à data de morte. 

# COMMAND ----------

# Não existem doentes com data de nascimento superior à data de morte. 
# Para isso acontecer df_born_dead_dates_count.born_dead_dates_count teria de ser maior que 0, o que não é o caso.

df_born_dead_dates_count = df_patients.where(
    df_patients["BIRTHDATE"] > df_patients["DEATHDATE"]
).select(F.count("BIRTHDATE").alias("born_dead_dates_count"))
df_born_dead_dates_count.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Calcule a idade (em anos) das pessoas usando as seguintes condições:
# MAGIC #####3.1. Se o estiver morto, essa será a data final para calcular a idade.
# MAGIC #####3.2. Se estiver vivo, considere como data final, '2020-04-05' para o cálculo da idade.

# COMMAND ----------

# Criação de uma nova coluna 'AGE_DATE' que contém a data final considerada para o cálculo da idade em anos.
# Data final será: data de morte 'DEATHDATE', se estiver morto; e '2020-04-05', se estiver vivo.

df_patients = df_patients.withColumn(
    "AGE_DATE",
    F.when(
        (df_patients["DEATHDATE"].isNull()), F.to_date(F.lit("2020-04-05"))
    ).otherwise(df_patients["DEATHDATE"]),
)

# COMMAND ----------

# Cálculo da idade do paciente.
# Neste caso de determinação da idade, acho que faz mais sentido fazer o arredondamento por defeito através de '.floor()'.
# Por norma diz-se que uma pessoa tem 'x' anos apenas se ela os cumpriu (no dia ou após o dia de aniversário).

df_patients = df_patients.withColumn(
    "AGE",
    F.floor(
        F.datediff(df_patients["AGE_DATE"], df_patients["BIRTHDATE"]) / 365.25,
    ),
)

# COMMAND ----------

# Alguns pacientes tem data de nascimento superior à data final considerada no enunciado ("data final, '2020-04-05'").
# Nesses casos a idade ('AGE') obtida foi de -1. 
# Vou alterar esses valores de idade '-1' para '0' (correspondendo a 0 anos de idade ('AGE')).

df_patients = df_patients.withColumn(
    "AGE", F.when(df_patients["AGE"] == -1, F.lit(0)).otherwise(df_patients["AGE"])
)

# COMMAND ----------

df_patients.select("ID", "AGE").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Identifique a idade máxima, idade mínima, média, mediana e máximo. 
# MAGIC

# COMMAND ----------

# Idade máxima.

df_patients_max = df_patients.select(
    F.round(F.max(df_patients["AGE"])).alias("max_age")
)
df_patients_max.display()

# COMMAND ----------

# Idade mínima.

df_patients_min = df_patients.select(
    F.round(F.min(df_patients["AGE"])).alias("min_age")
)
df_patients_min.display()

# COMMAND ----------

# Média das idades.

df_patients_mean = df_patients.select(
    F.round(F.avg(df_patients["AGE"])).alias("avg_age")
)
df_patients_mean.display()

# COMMAND ----------

# Mediana das idades.

median_age = df_patients.approxQuantile("AGE", [0.5], 0.0001)[0]
display(median_age)

# Neste contexto o resultado não é um Dataframe, mas sim um valor escalar 'float'.
# Por esse motivo para 'mostrar' o resultado ou fazer output tenho de usar o método print() ou colocar a variável dentro de display().

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Faça um histograma com 100 bins (intervalos) da idade das pessoas

# COMMAND ----------

display(df_patients.select('ID','AGE'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5.1. Encontra alguma situação estranha com a distribuição? Comente 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC No gráfico em questão, os 'bins' que representam as idades compreendidades entre: 0-1.10 11-12.1, 22-23.1, 31.9-33, 44-45.1, 53.9-55, 64.9-66, 77-78.1, 88-89.1, 97.9-99, 109-110 anos, tem claramente picos de contagem, estando por isso mais representados do que os bins anteriores ou posteriores ("vizinhos").  
# MAGIC Ao criar um histograma, os 'bins' geralmente são intervalos de valores, e a cada observação (paciente) é atribuída a um desses 'bins' com base no valor da sua idade. Se houver sobreposição entre os 'bins' ou se houver uma escolha específica na forma como os 'bins' são definidos, a contagem pode-se acumular em determinadas faixas etárias. O fato de haver 100 bins para representar 110 idades sugere que pode haver sobreposição ou acumulação de contagens em alguns dos 'bins'. Isso pode criar picos aparentes em certas faixas etárias. 
# MAGIC Aliado a isto, os picos observados podem também ser influênciados por eventos demográficos, históricos (como guerras e pandemias), ou outras influências específicas em determinadas faixas etárias (aumento natural de nascimentos, concentração de pessoas que atingiram uma idade bastante avançada, etc.).  
# MAGIC Ajustar a largura dos 'bins' ou escolher uma abordagem diferente na construção do histograma pode fornecer uma representação mais fiel da distribuição de idades. Esta acumulação de contagem em determinados 'bins', poderia eventualmente ser minimizada se o número de 'bins' fosse igual ou maior (e não menor) em relação ao numero de idades representadas (110 neste caso específico).

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Como estão distribuídas cada umas das etnias sobre o total dos doentes?

# COMMAND ----------

# Total de pacientes
patients_total = df_patients.count()

# Contagem de pacientes por etnia
df_patients_ethnicity_count = df_patients.groupBy("ETHNICITY").count()

# Coluna para determinar a distribuição das etnias
df_patients_ethnicity_count = df_patients_ethnicity_count.withColumn(
    "ETHNICITY_DISTRIBUTION",
    F.round((df_patients_ethnicity_count["count"] / patients_total) * 100, 1),
)

# O seguinte DF incluí as informações relativas à distribuídas cada umas das etnias sobre o total dos doentes.
df_patients_ethnicity_count.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Qual é a raça com maior e menor número de doentes e qual é a % sobre o total da população?

# COMMAND ----------

# Contagem de pacientes por raça
df_patients_race_count = df_patients.groupBy("RACE").count()

# Coluna para determinar a percentagem das raças sobre o total da população por ordem decrescente da distribuição
df_patients_race_count = df_patients_race_count.withColumn(
    "RACE_DISTRIBUTION",
    F.round((df_patients_race_count["count"] / patients_total) * 100, 1),
).orderBy("RACE_DISTRIBUTION", ascending=False)

df_patients_race_count.display()

# A seguinte tabela fornece as informações relativas ao número de doentes por raça e as suas percentagens sobre o total da população.
# Através da análise da tabela podemos verificar que a raça mais representada é 'white' com 10328 doentes e 83.6% do total da população.
# No sentido inverso a raça menos representada é 'other' com 9 doentes e 0.1% do total da população.
# No caso de não consideramos 'other' como uma raça mas sim como um conjunto de outras raças, as quais são desconhecidas ou não identificadas. 
# Podemos neste caso específico, considerar 'native' como a raça com menor número de doentes, 79 e com 0.6% do total da população.

# COMMAND ----------

# Podemos retirar os valores máximos e mínimos diretamente da tabela anterior.

# Valor máximo (raça com maior número de doentes e a sua percentagem no total de doentes):

df_patients_race_count_max = df_patients_race_count.orderBy(
    "RACE_DISTRIBUTION", ascending=False
).first()
print(df_patients_race_count_max)

# Utilizo o método 'print()' porque o resultado anterior deixa de ser um Dataframe e passa a ser 'Row'. 
# Para usar 'display()' ou 'show()' teria de converter de 'Row' para 'DataFrame': df_patients_race_count_max = spark.createDataFrame([df_patients_race_count_max])

# COMMAND ----------

# Valor mínimo (raça com menor número de doentes e a sua percentagem no total de doentes):

df_patients_race_count_min = df_patients_race_count.orderBy(
    "RACE_DISTRIBUTION", ascending=True
).first()
print(df_patients_race_count_min)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. Quais são 15 condições mais detetadas?

# COMMAND ----------

# importação e leitura de DF_CONDITIONS desde o SF, com recurso às propriedades da classe 'DatabricksSnowflakeConnection' previamente definida.

df_conditions = francisco.read_table(
    "EDIT2023", "PROJETO_FINAL", "DF_CONDITIONS", query=None, type=True
)
df_conditions.display()

# COMMAND ----------

df_conditions_count = (
    df_conditions.groupBy("CODE", "DESCRIPTION")
    .count()
    .orderBy("count", ascending=False)
    .limit(15)
)

df_conditions_count.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8.1. Faça um horizontal barplot com esta informação?

# COMMAND ----------

df_conditions_count.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 9. Identifique quantos códigos nas condições estão repetidos?

# COMMAND ----------

# Identificação dos códigos ('CODE') com mais de uma ocorrência (repetidos).

df_code_count = df_conditions.groupBy("CODE").count().orderBy("count", ascending=False)
df_code_count_repeated = df_code_count.filter(df_code_count["count"] > 1)

df_code_count_repeated.display()

# COMMAND ----------

# Quantificação dos códigos ('CODE') com mais de uma ocorrência (repetidos).

df_code_count_repeated.count()

# 172 códigos de doenças ('CODE') tem mais de uma ocorrência (repetidos).

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 9.1. Quantas descrições diferentes tem cada um dos códigos identificados?

# COMMAND ----------

df_code_descripton = df_conditions.select("CODE", "DESCRIPTION")

df_code_descripton_repeated = df_code_descripton.join(
    df_code_count_repeated, on="CODE", how="inner"
)

different_description_per_code = df_code_descripton_repeated.groupBy("CODE").agg(
    F.count_distinct("DESCRIPTION")
)

different_description_per_code.display()

# Cada um dos códigos identificados tem apenas uma descrição diferente (à exceção de '233604007' e '427089005').
# Os códigos '233604007' e '427089005' tem duas descrições diferentes, respetivamente.

# COMMAND ----------

# Identificação dos códigos ('CODE') com mais de uma descrição diferente.

different_description_per_code = different_description_per_code.filter(
    different_description_per_code["count(DESCRIPTION)"] > 1
)
different_description_per_code.display()

# Os códigos '233604007' e '427089005' tem duas descrições diferentes, ou seja mais do que uma descrição.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 9.2. Proponha uma forma de unificar os códigos e a suas descrições. 

# COMMAND ----------

# Concatenar as duas colunas com separador ' - '.

df_code_description_unified = df_conditions.withColumn(
    "UNIFIED_CODE_DESCRIPTION",
    F.concat(df_conditions["CODE"], F.lit(" - "), df_conditions["DESCRIPTION"]),
)
df_code_description_unified.select(
    "PATIENT", "CODE", "DESCRIPTION", "UNIFIED_CODE_DESCRIPTION"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 10. Calcule a duração das condições (doenças) que os doentes padecem, desde a primeira vez que foram diagnosticados. 

# COMMAND ----------

# Filtrar o DF apenas com as colunas de interesse.

df_patients_filtered = df_patients.select(
    "ID", "BIRTHDATE", "DEATHDATE", "AGE_DATE", "AGE"
)

# COMMAND ----------

# Mudar nome da coluna 'PATIENTS' para 'ID' de 'DF_CONDITIONS', para conseguir fazer o '.join()' com a tabela 'df_patients'.

df_conditions_renamed = df_conditions.withColumnRenamed("PATIENT", "ID")
df_condition_duration = df_conditions_renamed.join(
    df_patients_filtered, on="ID", how="inner"
)

# COMMAND ----------

# Cálculo direto da duração das doenças em cada um dos pacientes.
# Naturalmente irá conter valores nulos, porque em alguns casos 'STOP' é nulo ('null').
# 'STOP' é nulo em alguns casos porque ou o doente ainda está vivo e ainda padece da doença, ou o doente morreu com a doença.

df_condition_duration = df_condition_duration.withColumn(
    "CONDITION_DURATION_DAYS",
    F.floor(
        F.datediff(df_condition_duration["STOP"], df_condition_duration["START"]),
    ),
)
df_condition_duration.select(
    "ID", "CODE", "DESCRIPTION", "START", "STOP",  "CONDITION_DURATION_DAYS"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 10.1. Considere que para as pessoas mortas, a data de finalização da condição é o dia da morte específico para cada um dos doentes.

# COMMAND ----------

# Data final da doença ('STOP') em pessoas que já morreram, será a data de morte dessas pessoas ('DEATHDATE').
# De salientar que neste cálculo existem pessoas que não morreram e que ainda não tem data final de doença ('STOP').
# Pelo que nesses casos específicos a duração da doença/condição ('CONDITION_DURATION_DAYS_2') continuará a ser valor nulo.

df_condition_duration_2 = df_condition_duration.withColumn(
    "STOP",
    F.when(
        df_condition_duration["DEATHDATE"].isNotNull(),
        df_condition_duration["DEATHDATE"],
    ).otherwise(df_condition_duration["STOP"]),
)
df_condition_duration_2 = df_condition_duration_2.withColumn(
    "CONDITION_DURATION_DAYS_2",
    F.floor(
        F.datediff(df_condition_duration_2["STOP"], df_condition_duration_2["START"]),
    ),
)

df_condition_duration_2.select(
    "ID",
    "START",
    "STOP",
    "DEATHDATE",
    "CODE",
    "DESCRIPTION",
    "CONDITION_DURATION_DAYS_2",
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 10.2. Calcule a média em dias e anos, se for mais de 365 dias transforme a anos.

# COMMAND ----------

# Retirar os valores nulos, ou seja os pacientes que estão vivos e ainda tem a doença/condição.
# Uma hipótese para tratar estes valores nulos, seria eventualmente substituir a data final ('STOP') por '2020-04-05'. 
# No entanto poderia estar a condicionar a análise, no sentido de estar a considerar uma duração para determinadas doenças, que poderia não se aproximar da realidade e desta forma enviesar o cálculo da duração média.
# Este DF filtrado será utilizado nas duas respostas apresentadas. 

df_condition_duration_2 = df_condition_duration_2.na.drop(
    subset=["CONDITION_DURATION_DAYS_2"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Como não tinha total certeza da interpretação da pergunta - se seria o cálculo médio da duranção por doença ou por pacientes, resolvi responder para ambos os cenários.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Calculo da média em dias e anos (se for mais de 365 dias) __por condição (doença)__.

# COMMAND ----------

# Cálculo média dos dias e/ou anos por condição (doença).

df_condition_duration_mean = df_condition_duration_2.groupBy("DESCRIPTION").agg(
    F.round(F.avg("CONDITION_DURATION_DAYS_2"), 0).alias(
        "DESCRIPTION_DURATION_DAYS_MEAN"
    )
)
df_condition_duration_mean = df_condition_duration_mean.withColumn(
    "DESCRIPTION_DURATION_YEARS_MEAN",
    F.when(
        col("DESCRIPTION_DURATION_DAYS_MEAN") > 365,
        F.floor(col("DESCRIPTION_DURATION_DAYS_MEAN") / 365.25),
    ).otherwise("less than 1 year"),
)

df_condition_duration_mean.display()

# Por exemplo: a duração média da condição num paciente que contrai 'Atrial Fibrillation' é de 4214 dias ou 11 anos.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Cálculo da média em dias e anos (se for mais de 365 dias) por pacientes/doentes.

# COMMAND ----------

# Cálculo média dos dias e/ou anos por pacientes (doentes).

df_condition_duration_3 = df_condition_duration_2.withColumn(
    "DESCRIPTION_DURATION_YEARS_2",
    F.when(
        col("CONDITION_DURATION_DAYS_2") > 365,
        F.floor(col("CONDITION_DURATION_DAYS_2") / 365.25),
    ).otherwise("less than 1 year"),
)
df_condition_duration_3 = df_condition_duration_3.select(
    "ID", "DESCRIPTION", "CONDITION_DURATION_DAYS_2", "DESCRIPTION_DURATION_YEARS_2"
)

df_condition_duration_3.display()

# COMMAND ----------

# Cálculo da duração média em dias da condição/doença por paciente.

df_condition_duration_3_days_mean = df_condition_duration_3.select(
    F.round(F.avg(df_condition_duration_3["CONDITION_DURATION_DAYS_2"])).alias(
        "mean_condition_days"
    )
)

df_condition_duration_3_days_mean.display()

# Em média um paciente contrai uma (qualquer) condição ou doença durante 1672 dias.

# COMMAND ----------

# Cálculo da duração média em anos da condição/doença por paciente.

df_condition_duration_3_years_mean = df_condition_duration_3.select(
    F.round(
        F.avg(df_condition_duration_3["CONDITION_DURATION_DAYS_2"] / 365.25), 1
    ).alias("mean_condition_years")
)

df_condition_duration_3_years_mean.display()

# Em média um paciente contrai uma condição ou doença durante 4.6 anos.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 11. O Dr Anthony Fauci recebeu informação afirmando que o número de doenças crónicas está relacionado diretamente com estádios mais severos do covid-19. A indicação dele é que toda condição detetada que tiver mais de 1 ano será considerada como uma doença crónica. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Doenças com mais de 1 ano de duração tendo por base o cálculo médio da duranção por doença.

# COMMAND ----------

df_condition_chronic = df_condition_duration_mean.filter(
    df_condition_duration_mean["DESCRIPTION_DURATION_YEARS_MEAN"] > 1
)

df_condition_chronic.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Doenças com mais de 1 ano de duração tendo por base o cálculo médio da duranção por paciente.

# COMMAND ----------

df_condition_chronic_2 = df_condition_duration_3.filter(
    df_condition_duration_3["DESCRIPTION_DURATION_YEARS_2"] > 1
)

df_condition_chronic_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 12. Quantas doenças/condições foram classificadas como crónicas segundo a conceito do Dr. Fauci. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Número de doenças detetadas classificadas como potencialmente crónicas

# COMMAND ----------

df_condition_chronic.count()

# 85 doenças/condições diferentes foram classificadas como potencialmente crónicas segundo a conceito do Dr. Fauci.

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Número de doentes classificados com doenças crónicas.

# COMMAND ----------

df_condition_chronic_2.count()

# 13174 doentes foram classificados com doenças potencialmente crónicas segundo o conceito do Dr. Fauci.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 13. Identifique a duração mínima, máxima e média (em anos) das doenças que crónicas

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Cáculo tendo em consideração a duração média por condição.

# COMMAND ----------

# mudar o 'datatype' da variável para inteiro de forma a facilitar os cálculos pretendidos.

df_condition_chronic = df_condition_chronic.withColumn(
    "DESCRIPTION_DURATION_YEARS_MEAN",
    df_condition_chronic["DESCRIPTION_DURATION_YEARS_MEAN"].cast(IntegerType()),
)

df_condition_chronic.printSchema()

# COMMAND ----------

# Duranção máxima (consideração a duração média por condição).

max_chronic = df_condition_chronic.select(
    F.max("DESCRIPTION_DURATION_YEARS_MEAN").alias("max_chronic")
)

max_chronic.display()

# COMMAND ----------

# Duração mínima (consideração a duração média por condição).

df_condition_chronic_min = df_condition_chronic.select(
    F.round(F.min(df_condition_chronic["DESCRIPTION_DURATION_YEARS_MEAN"])).alias(
        "min_chronic"
    )
)

df_condition_chronic_min.display()

# COMMAND ----------

# Duração média (consideração a duração média por condição).


df_condition_chronic_mean = df_condition_chronic.select(
    F.round(F.avg(df_condition_chronic["DESCRIPTION_DURATION_YEARS_MEAN"])).alias(
        "mean_chronic"
    )
)

df_condition_chronic_mean.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Cáculo tendo em consideração a duração da condição por paciente.

# COMMAND ----------

# Mudar o datatype da variável para inteiro de forma a facilitar os cálculos pretendidos.

df_condition_chronic_2 = df_condition_chronic_2.withColumn(
    "DESCRIPTION_DURATION_YEARS_2",
    df_condition_chronic_2["DESCRIPTION_DURATION_YEARS_2"].cast(IntegerType()),
)

df_condition_chronic_2.printSchema()

# COMMAND ----------

# Duranção máxima (consideração a duração da condição por paciente).

max_chronic_2 = df_condition_chronic_2.select(
    F.max("DESCRIPTION_DURATION_YEARS_2").alias("max2_chronic")
)

max_chronic_2.display()

# COMMAND ----------

# Duranção mínima (consideração a duração da condição por paciente).

df_condition_chronic_min_2 = df_condition_chronic_2.select(
    F.round(F.min("DESCRIPTION_DURATION_YEARS_2")).alias("min2_chronic")
)

df_condition_chronic_min_2.display()

# COMMAND ----------

# Duranção média (consideração a duração da condição por paciente).

df_condition_chronic_mean_2 = df_condition_chronic_2.select(
    F.round(F.avg("DESCRIPTION_DURATION_YEARS_2")).alias("mean2_chronic")
)

df_condition_chronic_mean_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 14. Qual é o nome das 10 pessoas com mais doenças crónicas. 

# COMMAND ----------

# Através de '.join()', uni a tabela com a duração da condição por paciente ('df_condition_duration_3') e a tabela que contém os nomes dos pacientes ('df_patients').
# Selecionei apenas as colunas de interesse para a análise.
# Criei uma nova coluna através da concatenação das colunas com o primeiro e o último nome dos pacientes, respetivamente.
# Filtrei o Dataframe para incluir apenas os doentes com doenças crónicas, ou seja doenças com duração superior a 1 ano (segundo o Dr. Fauci).

top10_chronic = df_condition_duration_3.join(df_patients, on="ID", how="inner")

top10_chronic = top10_chronic.select(
    "ID",
    "FIRST",
    "LAST",
    "DESCRIPTION",
    "CONDITION_DURATION_DAYS_2",
    "DESCRIPTION_DURATION_YEARS_2",
)
top10_chronic = top10_chronic.withColumn(
    "FULL_NAME", F.concat(top10_chronic["FIRST"], F.lit(" "), top10_chronic["LAST"])
)
top10_chronic = top10_chronic.drop("FIRST", "LAST")

top10_chronic = top10_chronic.filter(top10_chronic["DESCRIPTION_DURATION_YEARS_2"] >= 2)

# COMMAND ----------

# Fiz um agrupamento dos doentes ('FULL_NAME') por contagem do número de doenças distintas.
# Ordenei de forma decrescente de contagem e limitei a pesqueisa para os 10 primeiros valores.
# Desta forma obtenho os nomes (primeiro e último) do 'Top 10 pessoas com mais doenças crónicas'.

top10_chronic_count = top10_chronic.groupBy("FULL_NAME").agg(
    F.count_distinct("DESCRIPTION").alias("TOP10_CHRONIC_COUNT")
)
top10_chronic_count = top10_chronic_count.orderBy(
    "TOP10_CHRONIC_COUNT", ascending=False
).limit(10)

top10_chronic_count.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 15. Identifique qual é o código que indica o peso do doente. 

# COMMAND ----------

# importação e leitura de 'df_observations' desde o SF, com recurso às propriedades da classe 'DatabricksSnowflakeConnection' previamente definida.

df_observations = francisco.read_table(
    "EDIT2023", "PROJETO_FINAL", "DF_OBSERVATIONS", query=None, type=True
)
df_observations.display()

# COMMAND ----------

# A descrição 'Body Weight' indica o peso do doente.

df_body_weight = df_observations.filter(df_observations["DESCRIPTION"] == "Body Weight")
df_body_weight.count()

# COMMAND ----------

# As unidades 'kg' podem referir-se ao peso do doente mas também a outras descrições além desta.
# Fiz também a verificação de outras unidades de peso, como 'lb' e 'oz', mas não obtive resultados.

df_body_weight_2 = df_observations.filter(df_observations['UNITS'] == 'kg')
df_body_weight_2.count()

# COMMAND ----------

# Fiz também a verificação de outras unidades de peso corporal, como 'lb', 'oz', e 'st', mas não obtive resultados.

df_body_weight_lb_oz = df_observations.filter(df_observations['UNITS'].isin('lb', 'oz', 'st'))
df_body_weight_lb_oz.count()

# COMMAND ----------

# Pelos motivos referidos nas células anteriores, filtrei o Dataframe por: ['DESCRIPTION'] == 'Body Weight' & (['UNITS'] == 'kg'.
# De forma a ter a certeza quue obtenho apenas os dados relativos ao peso dos pacientes em kg.

df_weight = df_observations.filter(
    (df_observations["DESCRIPTION"] == "Body Weight")
    & (df_observations["UNITS"] == "kg")
)
df_weight.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 16. Calcule o BMI (Indíce de Massa Corporal - IMC).

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Fórmula do IMC = Peso (kg) / Altura^2 (m)
# MAGIC ​
# MAGIC

# COMMAND ----------

# Para o cálculo do 'BMI' é necessário onter a altura do paciente.

df_body_height = df_observations.filter(df_observations["DESCRIPTION"] == "Body Height")
df_body_height.count()

# COMMAND ----------

# Mantive no DF apenas as colunas de interesse para o cálculo.
# Alterei o nome da coluna que contém o valor do peso para 'WEIGHT(KG)'.
# Desta forma já não necessito de manter as colunas 'DESCRIPTION' e 'UNITS', porque o novo nome da coluna já cumpre essa 'função'.

df_body_weight = df_body_weight.drop(
    "DESCRIPTION", "UNITS", "ENCOUNTER", "TYPE", "CODE"
)
df_body_weight_renamed = df_body_weight.withColumnRenamed("VALUE", "WEIGHT(KG)")

df_body_weight_renamed.display()

# COMMAND ----------

# Mantive no DF apenas as colunas de interesse para o cálculo.
# Alterei o nome da coluna que contém o valor da altura para 'HEIGHT(CM)'.
# Desta forma já não necessito de manter as colunas 'DESCRIPTION' e 'UNITS', porque o novo nome da coluna já cumpre essa 'função'.

df_body_height = df_body_height.drop(
    "DESCRIPTION", "UNITS", "ENCOUNTER", "TYPE", "CODE", "DATE"
)
df_body_height_renamed = df_body_height.withColumnRenamed("VALUE", "HEIGHT(CM)")

df_body_height_renamed.display()

# COMMAND ----------

# Para facilitar o cálculo do BMI, juntei os dois DFs do peso e da altura obtidos anterioemente, através de '.join()'.

df_bmi = df_body_height_renamed.join(df_body_weight_renamed, on="PATIENT", how="inner")

df_bmi.display()

# COMMAND ----------

# Para facilitar o cáluclo do BMI mudei os 'datatypes' para inteiros.
df_bmi = df_bmi.withColumn("WEIGHT(KG)", df_bmi["WEIGHT(KG)"].cast(IntegerType()))
df_bmi = df_bmi.withColumn("HEIGHT(CM)", df_bmi["HEIGHT(CM)"].cast(IntegerType()))

# Cálculo do BMI a partir da fórmula (tive de dividir 'HEIGHT(CM)' por 100 para passar de 'cm' para 'm', de acordo com a fórmula).
df_bmi = df_bmi.withColumn(
    "BMI", F.round(df_bmi["WEIGHT(KG)"] / (df_bmi["HEIGHT(CM)"] / 100) ** 2, 1)
)

df_bmi.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 17. Cria uma classificação do BMI segundo a seguintes condições:
# MAGIC ###### - Menos de 18,5: Abaixo do peso ('Underweight');
# MAGIC ###### - 18,5 a menos de 25: Peso saudável ('Healthy Weight');
# MAGIC ###### - 25 a menos de 30: Sobrepeso ('Overweight');
# MAGIC ###### - 30 a menos de 40: Obesidade ('Obesity');
# MAGIC ###### - 40 ou mais: Obesidade de Classe 3 ('Class 3 Obesity')).

# COMMAND ----------

# Classificação do BMI segundo as condições definidas acima.

df_bmi = df_bmi.withColumn(
    "BMI_CLASS",
    F.when(df_bmi["BMI"] < 18.5, "Underweight")
    .when((18.5 <= df_bmi["BMI"]) & (df_bmi["BMI"] < 25), "Healthy Weight")
    .when((25 <= df_bmi["BMI"]) & (df_bmi["BMI"] < 30), "Overweight")
    .when((30 <= df_bmi["BMI"]) & (df_bmi["BMI"] < 40), "Obesity")
    .when(df_bmi["BMI"] >= 40, "Class 3 Obesity")
    .otherwise(df_bmi["BMI"]),
)

df_bmi.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 18. Detecte os doentes que presentam anomalias no seu peso.

# COMMAND ----------

# Contagem de doentes com anomalias de peso.
# Ou seja, todos os doentes em que 'BMI_CLASS' não seja igual a 'Healthy Weight'.

df_bmi_anomaly = df_bmi.filter(df_bmi["BMI_CLASS"] != "Healthy Weight")
df_bmi_anomaly.count()

# 69493 doentes tem anomalias de peso.

# COMMAND ----------

# Na seguinte tabela é possivel verificar quais os pacientes/doentes ('PATIENT') que apresentam anomalias no peso.

df_bmi_anomaly.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 18.1. Use as seguintes formulas com os valores calculados no ponto 17 (16?).

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 18.1.1 𝑙𝑖𝑚𝑖𝑡𝑒 𝑠𝑢𝑝𝑒𝑟𝑖𝑜𝑟 = 𝑎𝑣𝑔(𝐵𝑀𝐼) + (3 × 𝑠𝑡𝑑𝑒𝑣)

# COMMAND ----------

# De forma a facilmente aceder ao resultado de 'avg_bmi' e de 'stddev_bmi', para assim determinar o "limite superior", utilizei o método '.collect()'.
# Método '.collect()' converte o resultado do cálculo numa lista de linhas ('Rows'). Neste caso, teríamos uma lista com uma única 'Row'.

avg_bmi = df_bmi.agg(F.avg("BMI").alias("avg_bmi")).collect()[0]["avg_bmi"]
stddev_bmi = df_bmi.agg(F.stddev("BMI").alias("stddev_bmi")).collect()[0]["stddev_bmi"]


superior_limit = avg_bmi + (3 * stddev_bmi)

print(superior_limit)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 18.1.3. 𝑙𝑖𝑚𝑖𝑡𝑒 𝑖𝑛𝑓𝑒𝑟𝑖𝑜𝑟 = 𝑎𝑣𝑔(𝐵𝑀𝐼) − (3 × 𝑠𝑡𝑑𝑒𝑣)

# COMMAND ----------

inferior_limit = avg_bmi - (3 * stddev_bmi)

print(inferior_limit)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 18.4. Crie una nova coluna, boolean, onde represente se cada uma das observações calculadas no ponto 17 fica dentro ou fora do intervalo 

# COMMAND ----------

# Neste caso as classificações 'Underweight' e 'Class 3 Obesity' estão fora dos limites calculados em 18.1.
# Por isso tem o valor booleano de 'false', pois os seus valores de 'BMI' estão a baixo do limite inferior e acima do limite superior, respetivamente.
# As restantes classificações tem o valor booleano 'true', uma vez que os seus valores de 'BMI' estão dentro dos limites definidos.

df_bmi_anomaly = df_bmi_anomaly.withColumn(
    "WITHIN_INTERVALS(?)",
    F.when(
        (inferior_limit <= df_bmi["BMI"]) & (df_bmi["BMI"] <= superior_limit), True
    ).otherwise(False),
)

df_bmi_anomaly.display()
