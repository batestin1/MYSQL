#!/usr/local/bin/python
# coding: latin-1
#transform

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
import pandas as pd
from extrac import Extrac
from pyspark.sql.functions import desc
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from typewriter import MaquinadeEscrever
from pyspark.sql import SparkSession
from pyspark.sql.functions import col



class Transform():
    def __init__(self):
        self.dados = Extrac().dados

        self.concaf = self.dados.filter("confederation == 'CONCACAF'").select('country', 'population_share', 'tv_audience_share',
                                                                    'gdp_weighted_share')
        self.afc = self.dados.filter("confederation == 'AFC'").select('country', 'population_share', 'tv_audience_share',
                                                            'gdp_weighted_share')
        self.uefa = self.dados.filter("confederation == 'UEFA'").select('country', 'population_share', 'tv_audience_share',
                                                              'gdp_weighted_share')
        self.commebol = self.dados.filter("confederation == 'CONMEBOL'").select('country', 'population_share',
                                                                      'tv_audience_share', 'gdp_weighted_share')


print('*'*100)
MaquinadeEscrever('Seus dados estão sendo TRANSFORMADOS! \n')
print('*'*100)