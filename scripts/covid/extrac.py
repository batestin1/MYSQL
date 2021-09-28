#!/usr/local/bin/python
# coding: latin-1
#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: COVID
#     Reposit�rio: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, overlay, concat, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType
from pyspark.sql.functions import desc, expr
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct, map_values
from pyspark.sql.functions import variance,var_samp,  var_pop
from pyspark.sql import SQLContext
import logging
import sys

# Cria objeto da Spark Session
spark = (SparkSession.builder.appName("DeltaExercise")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('COVID')
logger.setLevel(logging.DEBUG)


class Extrac():
    def __init__(self):
        self.dados = spark.read.csv("../../dataset/mmsa-icu-beds.csv", header= True, inferSchema=True, encoding='cp1252')




logger.info("EXTRAÇÃO DOS DADOS BRUTOS")