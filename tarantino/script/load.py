#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: Tarantino
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
cursor.execute('CREATE DATABASE tarantino')
my_conn = create_engine('mysql+mysqldb://root:@localhost/tarantino')


def tarantino():

    tab1 = Transform().pulp
    tab2 = Transform().jango
    tab3 = Transform().dog
    tab4 = Transform().kill1
    tab5 = Transform().kill2
    tab6 = Transform().brown
    tab7 = Transform().bast


    # step 1 - to output
    (tab1.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('word')
     .save('../output/pulp_fiction')
     )
    (tab2.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('word')
     .save('../output/django')
     )
    (tab3.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('word')
     .save('../output/reservoir_dogs')
     )
    (tab4.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('word')
     .save('../output/killbill1')
     )
    (tab5.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('word')
     .save('../output/killbill2')
     )
    (tab6.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('word')
     .save('../output/jackie_brown')
     )
    (tab7.
     write
     .mode('overwrite')
     .format('parquet')
     .partitionBy('word')
     .save('../output/inglorious')
     )
    # step 2 -to mysql
    tab1 = tab1.toPandas()
    tab2 = tab2.toPandas()
    tab3 = tab3.toPandas()
    tab4 = tab4.toPandas()
    tab5 = tab5.toPandas()
    tab6 = tab6.toPandas()
    tab7 = tab7.toPandas()

    tab1.to_sql(con=my_conn, name='pulp_fiction', if_exists='append', index=False)
    tab2.to_sql(con=my_conn, name='django', if_exists='append', index=False)
    tab3.to_sql(con=my_conn, name='reservoir_dogs', if_exists='append', index=False)
    tab4.to_sql(con=my_conn, name='killbill1', if_exists='append', index=False)
    tab5.to_sql(con=my_conn, name='killbill2', if_exists='append', index=False)
    tab6.to_sql(con=my_conn, name='jackie_brown', if_exists='append', index=False)
    tab7.to_sql(con=my_conn, name='inglorious', if_exists='append', index=False)





print('*'*100)
print('Seus dados foram CARREGADOS!')