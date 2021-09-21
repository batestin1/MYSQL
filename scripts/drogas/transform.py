#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Drogas
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

from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import col,lit
from pyspark.sql.functions import map_keys
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import col,struct,when
from pyspark.sql.functions import concat_ws,col,lit
from pyspark.sql.functions import collect_set,avg,max,countDistinct,count
from pyspark.sql.functions import sum, col, desc, asc
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('DROGAS')
logger.setLevel(logging.DEBUG)



class Transform():
    def __init__(self):
        dados = Extrac().dados
        # step 1 - Cleaning
        dados = dados.na.fill(value=0)

        # step 2 - Reverse Engeering

        self.dados2 = dados.groupBy('age').agg(avg('alcohol-use').alias('media_usario_alcool'),
                                          avg('alcohol-frequency').alias('media_alcohol-frequency'),
                                          sum('alcohol-use').alias('soma_usario_alcool'),
                                          sum('alcohol-frequency').alias('soma_alcohol-frequency')).sort(asc('age'))
        self.dados3 = dados.groupBy('age').agg(avg('marijuana-use').alias('media_marijuana-use'),
                                             avg('marijuana-frequency').alias('media_marijuana-frequency'),
                                             sum('marijuana-use').alias('soma_marijuana-use'),
                                             sum('marijuana-frequency').alias('soma_marijuana-frequency')).sort(asc('age'))
        self.dados4 = dados.groupBy('age').agg(avg('cocaine-use').alias('media_cocaine-use'),
                                           avg('cocaine-frequency').alias('media_cocaine-frequency'),
                                           sum('cocaine-use').alias('soma_cocaine-use'),
                                           sum('cocaine-frequency').alias('soma_cocaine-frequency')).sort(asc('age'))
        self.dados5 = dados.groupBy('age').agg(avg('crack-use').alias('media_crack-use'),
                                         avg('crack-frequency').alias('media_crack-frequency'),
                                         sum('crack-use').alias('soma_crack-use'),
                                         sum('crack-frequency').alias('soma_crack-frequency')).sort(asc('age'))
        self.dados6 = dados.groupBy('age').agg(avg('heroin-use').alias('media_heroin-use'),
                                          avg('heroin-frequency').alias('media_heroin-frequency'),
                                          sum('heroin-use').alias('soma_heroin-use'),
                                          sum('heroin-frequency').alias('soma_heroin-frequency')).sort(asc('age'))
        self.dados7 = dados.groupBy('age').agg(avg('hallucinogen-use').alias('media_hallucinogen-use'),
                                                avg('hallucinogen-frequency').alias('media_hallucinogen-frequency'),
                                                sum('hallucinogen-use').alias('soma_hallucinogen-use'),
                                                sum('hallucinogen-frequency').alias(
                                                    'soma_hallucinogen-frequency')).sort(asc('age'))
        self.dados8 = dados.groupBy('age').agg(avg('inhalant-use').alias('media_inhalant-use'),
                                            avg('inhalant-frequency').alias('media_inhalant-frequency'),
                                            sum('inhalant-use').alias('soma_inhalant-use'),
                                            sum('inhalant-frequency').alias('soma_inhalant-frequency')).sort(asc('age'))

        #step3 - variables
        self.dados = dados



logger.info("TRANSFORMANDO E CONSOLIDANDO OS DADOS")