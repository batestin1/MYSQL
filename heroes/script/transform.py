#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Herois
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



class Transform():
    def __init__(self):
        dados = Extrac().dados
        dados2 = Extrac().dados2
        # step1 = normaliza
        dc = dados.select(col("name").alias("nome"),
                          col("ID").alias("identidade"),
                          col("ALIGN").alias("carater"),
                          col("EYE").alias("olhos"),
                          col("HAIR").alias("cabelo"),
                          col("SEX").alias("genero"),
                          col("ALIVE").alias("situacao"),
                          col("FIRST APPEARANCE").alias("primeira_aparicao"),
                          col("Year").alias("ano"),
                          lit("dc_comics").alias("editora"))

        marvel = dados2.select(col("name").alias("nome"),
                               col("ID").alias("identidade"),
                               col("ALIGN").alias("carater"),
                               col("EYE").alias("olhos"),
                               col("HAIR").alias("cabelo"),
                               col("SEX").alias("genero"),
                               col("ALIVE").alias("situacao"),
                               col("FIRST APPEARANCE").alias("primeira_aparicao"),
                               col("YEAR").alias("ano"),
                               lit("marvel_comics").alias("editora"))

        # step2 - join

        # full = dc.join(marvel, dc['editora'] == marvel['editora'], 'cross').show()
        full = dc.union(marvel).sort(asc("nome"))
        
        #step3 - rename
        self.full = full
        self.marvel = marvel
        self.dc = dc


print('*'*100)
MaquinadeEscrever('Seus dados estão sendo TRANSFORMADOS! \n')
print('*'*100)