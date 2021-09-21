#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Covid
#     Reposit�rio: MYSQL
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
logger = logging.getLogger('COVID')
logger.setLevel(logging.DEBUG)

#connection
banco = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = ""
)

cursor = banco.cursor()

cursor = banco.cursor()
cursor.execute('CREATE DATABASE covid')
my_conn = create_engine('mysql+mysqldb://root:@localhost/covid')


def covid():
    tab1 = Transform().dados
    tab2 = Transform().dados2


    # step 1 - to output
    (tab1.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('hospitals')
     .save('../../output/covid/covid')
     )
    (tab2.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('media_por_hospital')
     .save('../../output/covid/riscos')
     )
    # step 2 -to mysql
    tab1 = tab1.toPandas()
    tab2 = tab2.toPandas()

    tab1.to_sql(con=my_conn, name='covid', if_exists='append', index=False)
    tab2.to_sql(con=my_conn, name='riscos', if_exists='append', index=False)


    
    logger.info("DADOS CARREGADOS COM SUCESSO!")




logger.info("CARREGANDO OS DADOS NO BANCO MYSQL")