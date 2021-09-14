#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Assassinatos
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
from extrac import Extrac
from typewriter import MaquinadeEscrever




class Transform():
    def __init__(self):
        dados = Extrac().dados
        dados2 = Extrac().dados2

        # union dataframes
        murders_2014 = dados.select('2014_murders')
        dados3 = dados2.join(dados, dados.city == dados2.city, 'leftsemi')
        dados4 = murders_2014.join(dados3, dados3['2015_murders'] == murders_2014['2014_murders'], 'full')
        df = dados4.select("city", "state", "2014_murders", "2015_murders", "2016_murders", "change", 'source', "as_of")

        # step1
        # limpeza dos dados
        df = df.na.fill({"city": "unknown", "state": "unknow", "source": "not informed", "as_of": "not informed"})
        df = df.na.fill(value=0)

        # step2
        # normalizando
        df2 = df.dropDuplicates(
            ["city", "state", "2014_murders", "2015_murders", "2016_murders", "change", "source", "as_of"])
        #step3
        #topandas
        self.dados = dados
        self.dados2 = dados2
        self.df2 = df2






print('*'*100)
MaquinadeEscrever('Seus dados estão sendo TRANSFORMADOS!\n')
print('*'*100)