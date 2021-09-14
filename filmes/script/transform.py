#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Fillme
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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import concat, col, lit




class Transform():
    def __init__(self):
        dados = Extrac().dados
        dados2 = Extrac().dados2

        # step 1 - lefjoin

        # step2 - REPARTITION DATAFRAME
        self.tomatoes = dados.groupBy('FILM').agg(sum('RottenTomatoes').alias('ROTTENTOMATOES'),
                                             sum('RottenTomatoes_User').alias('USUARIOS_ROTTENTOMATOES')).sort(asc('FILM'))

        self.imdb = dados.groupBy('FILM').agg(sum('IMDB').alias('IMDB'),
                                         sum('IMDB_norm').alias('NORMAL_IMDB'),
                                         sum('IMDB_user_vote_count').alias('USUARIOS_IMDB')).sort(asc('FILM'))
        self.fandango = dados.groupBy('FILM').agg(sum('Fandango_Stars').alias('FANDANGO_STARS'),
                                             sum('Fandango_votes').alias('FANDANGO_VOTOS')).sort(asc('FILM'))
        self.dados = dados

print('*'*100)
MaquinadeEscrever('Seus dados estão sendo TRANSFORMADOS! \n')
print('*'*100)''