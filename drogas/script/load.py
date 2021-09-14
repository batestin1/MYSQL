#!/usr/local/bin/python
# coding: latin-1
#LOAD

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: drogas
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
cursor.execute('CREATE DATABASE drogas')
my_conn = create_engine('mysql+mysqldb://root:@localhost/drogas')



#function

def drogas():

        tab1 = Transform().dados
        tab2 = Transform().dados2
        tab3 = Transform().dados3
        tab4 = Transform().dados4
        tab5 = Transform().dados5
        tab6 = Transform().dados6
        tab7 = Transform().dados7
        tab8 = Transform().dados8



        # step 1 - to output
        (tab1.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('age')
         .save('../output/drogas_full')
         )
        (tab2.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('age')
         .save('../output/alcool')
         )
        (tab3.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('age')
         .save('../output/marijuana')
         )
        (tab4.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('age')
         .save('../output/cocaine')
         )
        (tab5.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('age')
         .save('../output/crack')
         )
        (tab6.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('age')
         .save('../output/heroin')
         )
        (tab7.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('age')
         .save('../output/hallucinogen')
         )
        (tab8.
         write
         .mode('overwrite')
         .format('parquet')
         .partitionBy('age')
         .save('../output/inhalant')
         )

        # step 2 -to mysql
        tab1 = tab1.toPandas()
        tab2 = tab2.toPandas()
        tab3 = tab3.toPandas()
        tab4 = tab4.toPandas()
        tab5 = tab5.toPandas()
        tab6 = tab6.toPandas()
        tab7 = tab7.toPandas()
        tab8 = tab8.toPandas()
        tab1.to_sql(con=my_conn, name='drogas_full', if_exists='append', index=False)
        tab2.to_sql(con=my_conn, name='alcool', if_exists='append', index=False)
        tab3.to_sql(con=my_conn, name='marijuana', if_exists='append', index=False)
        tab4.to_sql(con=my_conn, name='cocaine', if_exists='append', index=False)
        tab5.to_sql(con=my_conn, name='crack', if_exists='append', index=False)
        tab6.to_sql(con=my_conn, name='heroin', if_exists='append', index=False)
        tab7.to_sql(con=my_conn, name='hallucinogen', if_exists='append', index=False)
        tab8.to_sql(con=my_conn, name='inhalant', if_exists='append', index=False)



print('*'*100)
MaquinadeEscrever('Seus dados foram CARREGADOS! \n')
print('*'*100)
