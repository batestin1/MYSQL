#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: GOOSE
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pandas as pd
from extrac import Extrac
from pyspark.shell import sqlContext, spark
from pyspark.sql.functions import desc
from pyspark.sql.functions import *
from pyspark.sql.functions import countDistinct
from typewriter import MaquinadeEscrever


class Transform():
    def __init__(self, ):
        dados = Extrac().dados

        # step1 - reverse infra, sep dataframe
        nl_1 = dados.filter("'league' == 'NL'")
        self.nl = nl_1.select('name', 'year', 'team', 'goose_eggs', 'broken_eggs')
        al1 = dados.filter(dados.league != 'NL')
        self.al = al1.select('name', 'year', 'team', 'goose_eggs', 'broken_eggs')
        self.dados = dados


print('*'*100)
MaquinadeEscrever('Seus dados estão sendo TRANSFORMADOS! \n')
print('*'*100)