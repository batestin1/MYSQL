#!/usr/local/bin/python
# coding: latin-1
#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: BASQUETBALL
#     Reposit�rio: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
from pyspark.sql import SQLContext
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('NBA')
logger.setLevel(logging.DEBUG)

class Extrac():
    def __init__(self):
        self.dados = sql_c.read.csv("../../dataset/allstar_player_talent.csv", header= True, inferSchema=True, encoding='cp1252')




logger.info("EXTRAÇÃO DOS DADOS BRUTOS")
