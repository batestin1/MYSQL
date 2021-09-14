#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: crimes
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
cursor.execute('CREATE DATABASE crimes')
my_conn = create_engine('mysql+mysqldb://root:@localhost/crimes')



#function

def crime():

        tab1 = Transform().dados
        tab2 = Transform().dados2
        tab3 = Transform().dados3
        tab4 = Transform().dados4




        # step 1 - to output
        (tab1.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('state')
         .save('../output/crimes_full')
         )
        (tab2.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('state')
         .save('../output/crimes_de_odio')
         )
        (tab3.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('state')
         .save('../output/crimes_por_cor')
         )
        (tab4.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('state')
         .save('../output/crimes_por_populacao')
         )

        # step 2 -to mysql
        tab1 = tab1.toPandas()
        tab2 = tab2.toPandas()
        tab3 = tab3.toPandas()
        tab4 = tab4.toPandas()
        tab1.to_sql(con=my_conn, name='crimes_full', if_exists='append', index=False)
        tab2.to_sql(con=my_conn, name='crimes_por_populacao', if_exists='append', index=False)
        tab3.to_sql(con=my_conn, name='crimes_por_cor', if_exists='append', index=False)
        tab4.to_sql(con=my_conn, name='crimes_de_odio', if_exists='append', index=False)




print('*'*100)
MaquinadeEscrever('Seus dados foram CARREGADOS! \n')
print('*'*100)
