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
#imports
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('COVID')
logger.setLevel(logging.DEBUG)


class Extrac():
    def __init__(self):
        self.dados = sql_c.read.csv("../../dataset/mmsa-icu-beds.csv", header= True, inferSchema=True, encoding='cp1252')




logger.info("EXTRAÇÃO DOS DADOS BRUTOS")