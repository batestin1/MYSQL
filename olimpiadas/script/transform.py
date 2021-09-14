#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Olimpiadas
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
        dados2 = Extrac().dados2

        self.dados2 = dados2.drop('ID')


print('*'*100)
MaquinadeEscrever('Seus dados estão sendo TRANSFORMADOS! \n')
print('*'*100)