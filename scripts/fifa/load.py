#!/usr/local/bin/python
# coding: latin-1
# LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: fifa
#     Reposit�rio: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# import
import mysql.connector
from sqlalchemy import create_engine
from transform import Transform
import logging
import sys
# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('FIFA')
logger.setLevel(logging.DEBUG)



# connection
banco = mysql.connector.connect(
    host="localhost",
    user="root",
    password=""
)

cursor = banco.cursor()

cursor = banco.cursor()
cursor.execute('CREATE DATABASE fifa')
my_conn = create_engine('mysql+mysqldb://root:@localhost/fifa')


# function

def fifa():
    tab1 = Transform().dados
    tab2 = Transform().concaf
    tab3 = Transform().afc
    tab4 = Transform().uefa
    tab5 = Transform().commebol

    # step 1 - to output
    (tab1.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('country')
     .save('../../output/fifa/fifa_all')
     )
    (tab2.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('country')
     .save('../../output/fifa/concaf')
     )
    (tab3.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('country')
     .save('../../output/fifa/afc')
     )
    (tab4.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('country')
     .save('../../output/fifa/uefa')
     )
    (tab5.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('country')
     .save('../../output/fifa/commebol')
     )

    # step 2 -to mysql
    tab1 = tab1.toPandas()
    tab2 = tab2.toPandas()
    tab3 = tab3.toPandas()
    tab4 = tab4.toPandas()
    tab5 = tab5.toPandas()

    tab1.to_sql(con=my_conn, name='fifa_full', if_exists='append', index=False)
    tab2.to_sql(con=my_conn, name='concaf', if_exists='append', index=False)
    tab3.to_sql(con=my_conn, name='afc', if_exists='append', index=False)
    tab4.to_sql(con=my_conn, name='uefa', if_exists='append', index=False)
    tab5.to_sql(con=my_conn, name='commebol', if_exists='append', index=False)
    logger.info("DADOS CARREGADOS COM SUCESSO!")




logger.info("CARREGANDO OS DADOS NO BANCO MYSQL")