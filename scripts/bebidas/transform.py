#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Bebidas
#     Reposit�rio: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
from extrac import Extrac

from pyspark.sql.window import Window
from pyspark.sql.functions import rank
from pyspark.sql.functions import row_number
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('BEBIDAS')
logger.setLevel(logging.DEBUG)

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




logger.info("TRANSFORMANDO E CONSOLIDANDO OS DADOS")