#!/usr/local/bin/python
# coding: latin-1
#transform

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: HarryPotter
#     Reposit�rio: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pandas as pd
import spark
from extrac import Extrac
from pyspark.python.pyspark.shell import sc
from pyspark.sql.functions import desc
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.functions import sum,avg,max,min,mean,count,col
from pyspark.sql.functions import rand
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('HARRY POTTER')
logger.setLevel(logging.DEBUG)



class Transform():
    def __init__(self):
        dados = Extrac().dados
        dados2 = Extrac().dados2
        self.dados3 = Extrac().dados3


        # step1 = clean
        dados = dados.na.fill(value=0)

        # step2 = grouby houses
        sly = dados.filter((dados.House == "Slytherin") & (dados.Job == "Student")).sort(asc('Name'))
        sly = sly.select("Name", "Gender", "Wand", "Patronus", "Species",
                         "Skills", "Birth", "Death")
        huf = dados.filter((dados.House == "Hufflepuff") & (dados.Job == "Student")).sort(asc('Name'))
        huf = huf.select("Name", "Gender", "Wand", "Patronus", "Species",
                         "Skills", "Birth", "Death")
        gry = dados.filter((dados.House == "Gryffindor") & (dados.Job == "Student")).sort(asc('Name'))
        gry = gry.select("Name", "Gender", "Wand", "Patronus", "Species",
                         "Skills", "Birth", "Death")
        rav = dados.filter((dados.House == "Ravenclaw") & (dados.Job == "Student")).sort(asc('Name'))
        rav = rav.select("Name", "Gender", "Wand", "Patronus", "Species",
                         "Skills", "Birth", "Death")
        stud = dados.filter(dados.Job == "Student").sort(asc('Name'))
        stud = stud.select("Name", "House", "Gender", "Wand", "Patronus", "Species",
                           "Skills", "Birth", "Death")
        notstud = dados.filter(dados.Job != "Student").sort(asc('Name'))
        notstud = notstud.select("Name", "House", "Job", "Gender", "Wand", "Patronus", "Species",
                                                         "Skills", "Birth", "Death")
        #step3 - potion
        dados2 = dados2.select(col("Name").alias("potion_name"),
                       col("Known ingredients").alias("ingredients"),
                       col("Difficulty level").alias("level"))
        
        #step3 - rename
        self.sly = sly
        self.huf = huf
        self.gry = gry
        self.rav = rav
        self.notstud = notstud
        self.stud = stud
        self.dados2 = dados2

logger.info("TRANSFORMANDO E CONSOLIDANDO OS DADOS")