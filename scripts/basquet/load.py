#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: BASQUETBALL
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
logger = logging.getLogger('NBA')
logger.setLevel(logging.DEBUG)

#connection
banco = mysql.connector.connect(
    host = "mysql",
    user= "root",
    password = ""
)

cursor = banco.cursor()

cursor = banco.cursor()
cursor.execute('CREATE DATABASE nba')
my_conn = create_engine('mysql+mysqldb://root:@mysql/nba')


def nba():
    tab1 = Transform().dados
    tab2 = Transform().ofensivo
    tab3 = Transform().defensivo

    # step 1 - to output
    (tab1.
     write
     .mode('overwrite')
     .format('parquet')
     .save('../../output/nba/nba_stats')
     )
    (tab2.
     write
     .mode('overwrite')
     .format('parquet')
     .save('../../output/nba/nba_ofe')
     )
    (tab3.
     write
     .mode('overwrite')
     .format('parquet')
     .save('../../output/nba/nba_def')
     )
    # step 2 -to mysql
    tab1 = tab1.toPandas()
    tab2 = tab2.toPandas()
    tab3 = tab3.toPandas()
    tab1.to_sql(con=my_conn, name='nba_stats', if_exists='append', index=False)
    tab2.to_sql(con=my_conn, name='nba_ofe', if_exists='append', index=False)
    tab3.to_sql(con=my_conn, name='nba_def', if_exists='append', index=False)
    logger.info("DADOS CARREGADOS COM SUCESSO!")




logger.info("CARREGANDO OS DADOS NO BANCO MYSQL")