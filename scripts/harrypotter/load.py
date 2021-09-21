#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: HarryPotter
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
logger = logging.getLogger('HARRY POTTER')
logger.setLevel(logging.DEBUG)

#connection
banco = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = ""
)

cursor = banco.cursor()

cursor = banco.cursor()
cursor.execute('CREATE DATABASE harrypotter')
my_conn = create_engine('mysql+mysqldb://root:@localhost/harrypotter')


def harry():

    tab1 = Transform().sly
    tab2 = Transform().huf
    tab3 = Transform().gry
    tab4 = Transform().rav
    tab5 = Transform().dados2
    tab6 = Transform().dados3
    tab7 = Transform().stud
    tab8 = Transform().notstud

    # step 1 - to output
    (tab1.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/harrypotter/sonserina')
     )
    (tab2.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/harrypotter/lufa')
     )
    (tab3.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/harrypotter/grifinoria')
     )
    (tab4.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/harrypotter/corvinal')
     )
    (tab5.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('potion_name')
     .save('../../output/harrypotter/potion')
     )
    (tab6.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/harrypotter/spells')
     )
    (tab7.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/harrypotter/stud')
     )
    (tab8.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Name')
     .save('../../output/harrypotter/notstud')
     )
    tab1 = tab1.toPandas()
    tab2 = tab2.toPandas()
    tab3 = tab3.toPandas()
    tab4 = tab4.toPandas()
    tab5 = tab5.toPandas()
    tab6 = tab6.toPandas()
    tab7 = tab7.toPandas()
    tab8 = tab8.toPandas()
    tab1.to_sql(con=my_conn, name='sonserina', if_exists='append', index=False)
    tab2.to_sql(con=my_conn, name='lufa', if_exists='append', index=False)
    tab3.to_sql(con=my_conn, name='grifinoria', if_exists='append', index=False)
    tab4.to_sql(con=my_conn, name='corvinal', if_exists='append', index=False)
    tab5.to_sql(con=my_conn, name='potion', if_exists='append', index=False)
    tab6.to_sql(con=my_conn, name='spells', if_exists='append', index=False)
    tab7.to_sql(con=my_conn, name='stud', if_exists='append', index=False)
    tab8.to_sql(con=my_conn, name='notstud', if_exists='append', index=False)
    logger.info("DADOS CARREGADOS COM SUCESSO!")




logger.info("CARREGANDO OS DADOS NO BANCO MYSQL")