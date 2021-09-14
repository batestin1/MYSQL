#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Covid
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
from typewriter import MaquinadeEscrever
from transform import Transform
import mysql.connector
from sqlalchemy import create_engine

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
     .save('../output/covid')
     )
    (tab2.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('media_por_hospital')
     .save('../output/riscos')
     )

    tab1 = tab1.toPandas()
    tab2 = tab2.toPandas()

    tab1.to_sql(con=my_conn, name='covid', if_exists='append', index=False)
    tab2.to_sql(con=my_conn, name='riscos', if_exists='append', index=False)


    # step 2 -to mysql




print('*'*100)
print('Seus dados foram CARREGADOS!')