#!/usr/local/bin/python
# coding: latin-1
#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Drogas
#     Reposit�rio: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructField,IntegerType, StructType,StringType, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('DROGAS')
logger.setLevel(logging.DEBUG)


class Extrac():
    def __init__(self):
        df = [StructField('age', StringType(), True),
                 StructField('n', DoubleType(), True),
                 StructField('alcohol-use', DoubleType(), True),
                 StructField('alcohol-frequency', DoubleType(), True),
                 StructField('marijuana-use', DoubleType(), True),
                 StructField('marijuana-frequency', DoubleType(), True),
                 StructField('cocaine-use', DoubleType(), True),
              StructField('cocaine-frequency', DoubleType(), True),
              StructField('crack-use', DoubleType(), True),
              StructField('crack-frequency', DoubleType(), True),
              StructField('heroin-use', DoubleType(), True),
              StructField('heroin-frequency', DoubleType(), True),
              StructField('hallucinogen-use', DoubleType(), True),
              StructField('hallucinogen-frequency', DoubleType(), True),
              StructField('inhalant-use', DoubleType(), True),
              StructField('inhalant-frequency', DoubleType(), True),
              StructField('pain-releiver-use', DoubleType(), True),
              StructField('pain-releiver-frequency', DoubleType(), True),
              StructField('oxycontin-use', DoubleType(), True),
              StructField('oxycontin-frequency', DoubleType(), True),
              StructField('tranquilizer-use', DoubleType(), True),
              StructField('tranquilizer-frequency', DoubleType(), True),
              StructField('stimulant-use', DoubleType(), True),
              StructField('stimulant-frequency', DoubleType(), True),
              StructField('meth-use', DoubleType(), True),
              StructField('meth-frequency', DoubleType(), True),
              StructField('sedative-use', DoubleType(), True),
              StructField('sedative-frequency', DoubleType(), True)
                 ]
        finalStruct = StructType(fields=df)
        self.dados = sql_c.read.csv("../../dataset/drug-use-by-age.csv", header= True, schema= finalStruct, encoding='cp1252')




logger.info("EXTRAÇÃO DOS DADOS BRUTOS")