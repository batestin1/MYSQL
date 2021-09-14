#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: COVID
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pandas as pd
from extrac import Extrac
from pyspark.sql.functions import desc
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import sum, col, desc, mean
from typewriter import MaquinadeEscrever
dados = Extrac().dados



class Transform():
    def __init__(self):
        self.dados = dados.na.fill(value=0)
        risco = dados.groupBy("MMSA").agg(sum("total_at_risk").alias("risco_total"))
        hospitals = dados.groupBy('MMSA').agg(sum('hospitals').alias('media_por_hospital'))
        dados2 = risco.join(hospitals, risco.MMSA == hospitals.MMSA, "left")
        self.dados2 = dados2.select('risco_total', 'media_por_hospital')



print('*'*100)
MaquinadeEscrever('Seus dados estão sendo TRANSFORMADOS! \n')
print('*'*100)