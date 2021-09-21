#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Simpsons
#     Reposit�rio: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pandas as pd

from extrac import Extrac
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance,var_samp,  var_pop
from pyspark.sql.functions import sum, col, desc, asc, lit, collect_set, column
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('SIMPSONS')
logger.setLevel(logging.DEBUG)






class Transform():
    def __init__(self):
        dados = Extrac().dados
        dados2 = Extrac().dados2
        dados3 = Extrac().dados3

        dados = dados.select(col("name"), lit("1").alias("lit_value1")).sort(asc('name'))


        dados2 = dados2.select(col("imdb_votes").alias("votes"), col("original_air_date").alias("date_air"),
                               col("original_air_year").alias("year"), col("season").alias("season"),
                               col("title").alias("episode"), lit("1").alias("lit_value1")).sort(asc("episode"))


        dados3 = dados3.select(col("name").alias('locations'), lit("1").alias("lit_value1")).sort(asc('locations'))


        full = dados2.join(dados, dados2.lit_value1 == dados.lit_value1, 'left')
        self.full = full.drop("lit_value1")



logger.info("TRANSFORMANDO E CONSOLIDANDO OS DADOS")