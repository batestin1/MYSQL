#!/usr/local/bin/python
# coding: latin-1
#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: HarryPotter
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = pyspark.SparkConf().setAppName('harry').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)
from typewriter import MaquinadeEscrever

class Extrac():
    def __init__(self):
        self.dados = sql_c.read.csv("../dataset/Characters.csv", sep=';', header= True, inferSchema=True, encoding='UTF-8')
        self.dados2 = sql_c.read.csv("../dataset/Potions.csv", sep=';', header=True, inferSchema=True, encoding='UTF-8')
        self.dados3 = sql_c.read.csv("../dataset/Spells.csv", sep=';', header=True, inferSchema=True, encoding='UTF-8')


print('*'*100)
MaquinadeEscrever('Seus dados estão sendo EXTRAIDOS! \n')
print('*'*100)