#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Married
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
logger = logging.getLogger('CASAMENTO')
logger.setLevel(logging.DEBUG)

#connection
banco = mysql.connector.connect(
    host = "mysql",
    user= "root",
    password = ""
)

cursor = banco.cursor()

cursor = banco.cursor()
cursor.execute('CREATE DATABASE married')
my_conn = create_engine('mysql+mysqldb://root:@mysql/married')



#function

def married():

        tab1 = Transform().dados
        tab2 = Transform().dados2
        tab3 = Transform().dados3


        # step 1 - to output
        (tab1.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('year')
         .save('../../output/casamentos/divorce')
         )
        (tab2.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('year')
         .save('../../output/casamentos/married')
         )
        (tab3.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('year')
         .save('../../output/casamentos/final')
         )

        # step 2 -to mysql
        tab1 = tab1.toPandas()
        tab2 = tab2.toPandas()
        tab3 = tab3.toPandas()
        tab1.to_sql(con=my_conn, name='divorce', if_exists='append', index=False)
        tab2.to_sql(con=my_conn, name='married', if_exists='append', index=False)
        tab3.to_sql(con=my_conn, name='final', if_exists='append', index=False)
        logger.info("DADOS CARREGADOS COM SUCESSO!")




logger.info("CARREGANDO OS DADOS NO BANCO MYSQL")