#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Married
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#import
import mysql.connector
from sqlalchemy import create_engine
from transform import Transform
from typewriter import MaquinadeEscrever


#connection
banco = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = ""
)

cursor = banco.cursor()

cursor = banco.cursor()
cursor.execute('CREATE DATABASE married')
my_conn = create_engine('mysql+mysqldb://root:@localhost/married')



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
         .save('../output/divorce')
         )
        (tab2.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('year')
         .save('../output/married')
         )
        (tab3.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('year')
         .save('../output/final')
         )

        # step 2 -to mysql
        tab1 = tab1.toPandas()
        tab2 = tab2.toPandas()
        tab3 = tab3.toPandas()
        tab1.to_sql(con=my_conn, name='divorce', if_exists='append', index=False)
        tab2.to_sql(con=my_conn, name='married', if_exists='append', index=False)
        tab3.to_sql(con=my_conn, name='final', if_exists='append', index=False)




print('*'*100)
MaquinadeEscrever('Seus dados foram CARREGADOS! \n')
print('*'*100)
