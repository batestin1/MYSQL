#!/usr/local/bin/python
# coding: latin-1
#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Herois
#     Reposit�rio: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('herois').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('HEROES')
logger.setLevel(logging.DEBUG)


class Extrac():
    def __init__(self):
        self.dados = sql_c.read.csv("../../dataset/dc-wikia-data.csv", sep=',', header= True, inferSchema=True, encoding='UTF-8')
        self.dados2 = sql_c.read.csv("../../dataset/marvel.csv", sep=',', header=True, inferSchema=True, encoding='UTF-8')


logger.info("EXTRAÇÃO DOS DADOS BRUTOS")