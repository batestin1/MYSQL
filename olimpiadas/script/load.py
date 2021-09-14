#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Olimpiadas
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
cursor.execute('CREATE DATABASE olimpiadas')
my_conn = create_engine('mysql+mysqldb://root:@localhost/olimpiadas')


def olimpiadas():

    tab1 = Transform().dados2


    # step 1 - to output
    (tab1.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('Year')
     .save('../output/olimpiadas')
     )
    # step 2 -to mysql
    tab1 = tab1.toPandas()

    tab1.to_sql(con=my_conn, name='olimpiadas', if_exists='append', index=False)







print('*'*100)
print('Seus dados foram CARREGADOS!')