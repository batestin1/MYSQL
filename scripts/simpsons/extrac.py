#!/usr/local/bin/python
# coding: latin-1
#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Simpsons
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
logger = logging.getLogger('SIMPSONS')
logger.setLevel(logging.DEBUG)

class Extrac():
    def __init__(self):
        self.dados = sql_c.read.csv("../../dataset/simpsons_characters.csv",
                                    sep=',', header=True,
                                    inferSchema=True,
                                    encoding='UTF-8')
        self.dados2 = sql_c.read.csv("../../dataset/simpsons_episodes.csv",
                                    sep=',', header=True,
                                    inferSchema=True,
                                    encoding='UTF-8')
        self.dados3 = sql_c.read.csv("../../dataset/simpsons_locations.csv",
                                     sep=',', header=True,
                                     inferSchema=True,
                                     encoding='UTF-8')


logger.info("EXTRAÇÃO DOS DADOS BRUTOS")