#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: crimes
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
from extrac import Extrac
from typewriter import MaquinadeEscrever
from pyspark.sql import SparkSession, Row
from pyspark.sql import Row
from pyspark.sql.functions import collect_set,avg,max,countDistinct,count
from pyspark.sql.functions import sum, col, desc, asc

class Transform():
    def __init__(self):
        dados = Extrac().dados
        #step1 = creating new dataframes by groupby and aggregations functions


        popu = dados.groupBy("state").agg(sum("share_population_in_metro_areas").alias("population_in_metro"),
                                          sum("share_population_with_high_school_degree").alias(
                                              "population_with_high_school"),
                                          sum("share_non_citizen").alias("no_citizen")).sort(asc("state"))

        white = dados.groupBy('state').agg(avg('share_white_poverty').alias('white_poverty'),
                                           avg('share_non_white').alias('no_white'),
                                           avg('share_voters_voted_trump').alias('voted_trump')).sort(asc('state'))

        violenc = dados.groupBy('state').agg(max('avg_hatecrimes_per_100k_fbi').alias('hatecrimes'),
                                             max('hate_crimes_per_100k_splc').alias('crimes')).sort(asc('state'))

        #topandas
        self.dados = dados
        self.dados2 = popu
        self.dados3 = white
        self.dados4 = violenc




print('*'*100)
MaquinadeEscrever('Seus dados estão sendo TRANSFORMADOS!\n')
print('*'*100)