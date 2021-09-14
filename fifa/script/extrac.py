#!/usr/local/bin/python
# coding: latin-1
#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Fifa
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('fifa').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from typewriter import MaquinadeEscrever

class Extrac():
    def __init__(self):
        self.dados = sql_c.read.csv("../dataset/fifa_countries_audience.csv", header= True, inferSchema=True, encoding='cp1252')




print('*'*100)
MaquinadeEscrever('Seus dados estão sendo EXTRAIDOS! \n')
print('*'*100)