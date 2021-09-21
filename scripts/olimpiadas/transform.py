#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Olimpiadas
#     Reposit�rio: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pandas as pd
import spark
from extrac import Extrac
from pyspark.python.pyspark.shell import sc
from pyspark.sql.functions import desc
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import sum,avg,max,min,mean,count,col
from pyspark.sql.functions import rand
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('OLIMPIADAS')
logger.setLevel(logging.DEBUG)




class Transform():
    def __init__(self):
        dados = Extrac().dados
        dados2 = Extrac().dados2

        self.dados2 = dados2.drop('ID')


logger.info("TRANSFORMANDO E CONSOLIDANDO OS DADOS")