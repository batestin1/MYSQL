#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: madmen
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pandas as pd
import spark
from extrac import Extrac
from pyspark.python.pyspark.shell import sc
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import sum,avg,max,min,mean,count,col, lit, desc, asc
from typewriter import MaquinadeEscrever

dados = Extrac().dados
dados2 = Extrac().dados2




class Transform():
    def __init__(self):
        dados = Extrac().dados
        dados2 = Extrac().dados2

        # step 1 = normalize
        dados = dados.select(col("Performer").alias("nome"),
                             col("Score per year").alias("votos"),
                             col("Show").alias("serie"), lit("tv_one").alias("show")).sort(asc("nome"))
        dados2 = dados2.select(col("Performer").alias("nome"),
                               col("Show").alias("serie"),
                               col("Show Start").alias("start"), lit("tv_two").alias("show")).sort(asc("nome"))
        # step2 = join

        self.full = dados.union(dados2).sort(asc("nome"))





print('*'*100)
MaquinadeEscrever('Seus dados estão sendo TRANSFORMADOS! \n')
print('*'*100)