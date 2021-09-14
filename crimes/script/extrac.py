
#!/usr/local/bin/python
# coding: latin-1
#extrac

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: crimes
#     Repositório: MYSQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports
import pyspark
from pyspark.sql import SparkSession

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, overlay, concat, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType
from pyspark.sql.functions import desc, expr
from pyspark.sql.functions import approx_count_distinct,collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct, map_values
from pyspark.sql.functions import variance,var_samp,  var_pop
from typewriter import MaquinadeEscrever
conf = pyspark.SparkConf().setAppName('crimes').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
from pyspark.sql import SQLContext
sql_c = SQLContext(sc)

class Extrac():
    def __init__(self):
        #extract
        self.dados = sql_c.read.csv("../dataset/hate_crimes.csv", header= True, inferSchema=True)


print('*'*100)
MaquinadeEscrever("Seus dados estão sendo EXTRAIDOS! \n")
print('*'*100)