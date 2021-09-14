#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Bebidas
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
from extrac import Extrac
from typewriter import MaquinadeEscrever
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import row_number

class Transform():
    def __init__(self):
        dados = Extrac().dados
        # step1 select
        dados = dados.select('beer_name', 'brewery_name', 'beer_style',
                             'review_time', 'review_overall', 'review_aroma',
                             'review_appearance', 'review_palate', 'review_taste', 'beer_abv')
        # step2 - fillno
        dados = dados.na.fill(value=0)
        # step3 - windows
        windowSpec = Window.partitionBy("beer_name").orderBy("brewery_name")
        beer = dados.withColumn('number_unique_beer', row_number().over(windowSpec))
        # step4- window with rank
        brewery = dados.withColumn("rank", rank().over(windowSpec))

        self.dados = dados
        self.beer = beer
        self.brewery = brewery






print('*'*100)
MaquinadeEscrever('Seus dados estão sendo TRANSFORMADOS!\n')
print('*'*100)