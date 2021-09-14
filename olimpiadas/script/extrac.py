#!/usr/local/bin/python
# coding: latin-1
#extrac

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
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('olimpiadas').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from typewriter import MaquinadeEscrever

class Extrac():
    def __init__(self):
        self.dados = sql_c.read.csv("../dataset/noc_regions.csv", sep=',', header= True, inferSchema=True, encoding='UTF-8')
        self.dados2 = sql_c.read.csv("../dataset/olimpiadas.csv", sep=',', header=True, inferSchema=True, encoding='UTF-8')


print('*'*100)
MaquinadeEscrever('Seus dados est�o sendo EXTRAIDOS! \n')
print('*'*100)