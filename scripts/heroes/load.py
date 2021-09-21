#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Herois
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
logger = logging.getLogger('HEROES')
logger.setLevel(logging.DEBUG)


#connection
banco = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = ""
)

cursor = banco.cursor()

cursor = banco.cursor()
cursor.execute('CREATE DATABASE herois')
my_conn = create_engine('mysql+mysqldb://root:@localhost/herois')


def herois():

    tab1 = Transform().full
    tab2 = Transform().marvel
    tab3 = Transform().dc

    # step 1 - to output
    (tab1.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('ano')
     .save('../../output/herois/full')
     )
    (tab2.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('ano')
     .save('../../output/herois/marvel')
     )
    (tab3.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('ano')
     .save('../../output/herois/dc')
     )

    # step 2 -to mysql
    tab1 = tab1.toPandas()
    tab2 = tab2.toPandas()
    tab3 = tab3.toPandas()

    tab1.to_sql(con=my_conn, name='full', if_exists='append', index=False)
    tab2.to_sql(con=my_conn, name='marvel', if_exists='append', index=False)
    tab3.to_sql(con=my_conn, name='dc', if_exists='append', index=False)
    logger.info("DADOS CARREGADOS COM SUCESSO!")




logger.info("CARREGANDO OS DADOS NO BANCO MYSQL")