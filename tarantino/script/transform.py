#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Tarantino
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
from pyspark.sql.functions import desc
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import sum,avg,max,min,mean,count,col
from pyspark.sql.functions import rand
from typewriter import MaquinadeEscrever



class Transform():
    def __init__(self):
        dados = Extrac().dados


        self.pulp = dados.filter((dados.movie == "Pulp Fiction")).sort(asc('word'))
        self.jango = dados.filter((dados.movie == "Django Unchained")).sort(asc("word"))
        self.dog = dados.filter((dados.movie == "Reservoir Dogs")).sort(asc("word"))
        self.kill1 = dados.filter((dados.movie == "Kill Bill: Vol. 1")).sort(asc("word"))
        self.kill2 = dados.filter((dados.movie == "Kill Bill: Vol. 2")).sort(asc("word"))
        self.brown = dados.filter((dados.movie == "Jackie Brown")).sort(asc("word"))
        self.bast = dados.filter((dados.movie == "Inglorious Basterds")).sort(asc("word"))
        



print('*'*100)
MaquinadeEscrever('Seus dados estão sendo TRANSFORMADOS! \n')
print('*'*100)