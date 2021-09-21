#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: married
#     Reposit�rio: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
from extrac import Extrac
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('CASAMENTO')
logger.setLevel(logging.DEBUG)


class Transform():
    def __init__(self):
        dados = Extrac().dados
        dados2 = Extrac().dados2

        # step1 = dropColum
        dados = dados.drop("_c0")
        dados2 = dados2.drop('_c0')

        # setp2 - join
        dados3 = dados.join(dados2, dados.year == dados2.year, 'leftsemi')

        #topandas
        self.dados = dados
        self.dados2 = dados2
        self.dados3 = dados3


logger.info("TRANSFORMANDO E CONSOLIDANDO OS DADOS")