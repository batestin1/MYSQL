#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Simpsons
#     Reposit�rio: Mysql
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################

from transform import Transform
import mysql.connector
from sqlalchemy import create_engine
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('SIMPSONS')
logger.setLevel(logging.DEBUG)

#connection
banco = mysql.connector.connect(
    host = "mysql",
    user= "root",
    password = ""
)

cursor = banco.cursor()

cursor = banco.cursor()
cursor.execute('CREATE DATABASE simpsons')
my_conn = create_engine('mysql+mysqldb://root:@mysql/simpsons')


def simpsons():

    tab1 = Transform().full


    # step 1 - to output
    (tab1.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('year')
     .save('../../output/simpsons/simpsons')
     )
    # step 2 -to mysql
    tab1 = tab1.toPandas()

    tab1.to_sql(con=my_conn, name='simpsons', if_exists='append', index=False)
    logger.info("DADOS CARREGADOS COM SUCESSO!")




logger.info("CARREGANDO OS DADOS NO BANCO MYSQL")