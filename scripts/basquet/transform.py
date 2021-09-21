#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: BASQUETBALL
#     Reposit�rio: MYSQL
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
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('NBA')
logger.setLevel(logging.DEBUG)


class Transform():
    def __init__(self):
        dados = Extrac().dados
           # step1 - select
        dados = dados.select('Player', 'Pos', 'Age', 'Tm', 'G', 'MP', 'PER', 'TS%', '3PAr', 'FTr', 'ORB%', 'DRB%',
                                'TRB%', 'AST%', 'STL%', 'BLK%', 'OWS', 'DWS')


           # step2 - new_dataframes
        self.ofensivo = dados.groupBy("PLayer").agg(min(dados['DRB%']).alias("MinRebotDef"),
                                                  max(dados['TS%']).alias("MaxPontosPart"))
        self.defensivo = dados.groupBy("PLayer").agg(min(dados['ORB%']).alias("MinRebotOfe"),
                                                   max(dados['DWS']).alias("Derrotas"))
        self.dados = dados


logger.info("TRANSFORMANDO E CONSOLIDANDO OS DADOS")
