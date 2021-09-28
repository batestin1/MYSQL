#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Senhordosaneis
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
logger = logging.getLogger('SENHOR DOS ANÉIS')
logger.setLevel(logging.DEBUG)

#connection
banco = mysql.connector.connect(
    host = "mysql",
    user= "root",
    password = ""
)

cursor = banco.cursor()

cursor = banco.cursor()
cursor.execute('CREATE DATABASE senhordosaneis')
my_conn = create_engine('mysql+mysqldb://root:@mysql/senhordosaneis')


def senhor():

    tab1 = Transform().dados
    tab2 = Transform().human
    tab3 = Transform().elf
    tab4 = Transform().hobbit
    tab5 = Transform().anao
    tab6 = Transform().maiar


    # step 1 - to output
    (tab1.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/senhordosaneis/full')
     )
    (tab2.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/senhordosaneis/human')
     )
    (tab3.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/senhordosaneis/elfo')
     )
    (tab4.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/senhordosaneis/hobbit')
     )
    (tab5.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/senhordosaneis/anao')
     )
    (tab6.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/senhordosaneis/maiar')
     )
    # step 2 -to mysql

    tab1 = tab1.toPandas()
    tab2 = tab2.toPandas()
    tab3 = tab3.toPandas()
    tab4 = tab4.toPandas()
    tab5 = tab5.toPandas()
    tab6 = tab6.toPandas()

    tab1.to_sql(con=my_conn, name='full', if_exists='append', index=False)
    tab2.to_sql(con=my_conn, name='homens', if_exists='append', index=False)
    tab3.to_sql(con=my_conn, name='elfos', if_exists='append', index=False)
    tab4.to_sql(con=my_conn, name='hobbits', if_exists='append', index=False)
    tab5.to_sql(con=my_conn, name='anao', if_exists='append', index=False)
    tab6.to_sql(con=my_conn, name='maiar', if_exists='append', index=False)
    logger.info("DADOS CARREGADOS COM SUCESSO!")




logger.info("CARREGANDO OS DADOS NO BANCO MYSQL")