#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Assassinatos
#     Reposit�rio: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#import
import mysql.connector
from sqlalchemy import create_engine
from transform import Transform

import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('ASSSASSINATOS')
logger.setLevel(logging.DEBUG)

#connection
banco = mysql.connector.connect(
    host = "mysql",
    user= "root",
    password = ""
)

cursor = banco.cursor()

cursor = banco.cursor()
cursor.execute('CREATE DATABASE assassinatos')
my_conn = create_engine('mysql+mysqldb://root:@mysql/assassinatos')



#function

def assassinato():

        tab1 = Transform().dados
        tab2 = Transform().dados2
        tab3 = Transform().df2


        # step 1 - to output
        (tab1.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('2014_murders')
         .save('../../../output/assassinato/ass_2014')
         )
        (tab2.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('2015_murders')
         .save('../../../output/assassinato/ass_2015')
         )
        (tab3.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('2016_murders')
         .save('../../../output/assassinato/ass_final')
         )

        # step 2 -to mysql
        tab1 = tab1.toPandas()
        tab2 = tab2.toPandas()
        tab3 = tab3.toPandas()
        tab1.to_sql(con=my_conn, name='ass_2014', if_exists='append', index=False)
        tab2.to_sql(con=my_conn, name='ass_2015', if_exists='append', index=False)
        tab3.to_sql(con=my_conn, name='ass_final', if_exists='append', index=False)

        logger.info("DADOS CARREGADOS COM SUCESSO!")




logger.info("CARREGANDO OS DADOS NO BANCO MYSQL")