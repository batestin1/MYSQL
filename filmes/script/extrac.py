#!/usr/local/bin/python
# coding: latin-1
#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Filme
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('filme').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from typewriter import MaquinadeEscrever

class Extrac():
    def __init__(self):
        self.dados = sql_c.read.csv("../filmes/fandango_score_comparison.csv", header= True, inferSchema=True, encoding='cp1252')
        self.dados2 = sql_c.read.csv("../filmes/fandango_scrape.csv", header= True, inferSchema=True, encoding='cp1252')




print('*'*100)
MaquinadeEscrever('Seus dados estão sendo EXTRAIDOS! \n')
print('*'*100)