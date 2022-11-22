#!/usr/bin/env python
# coding: utf-8

# In[3]:


#importing pyspark
import pyspark
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# In[4]:


#importing sparksession
from pyspark.sql import SparkSession


# In[5]:


#creating a sparkSession object and providing appName
spark=SparkSession.builder.appName("airline").getOrCreate()


# In[ ]:


spark.stop()


# In[6]:


#To create dataframe form External datasets
df=spark.read.option("header","true").csv("airlines1.csv")


# In[7]:


#for few columns 
df.columns[:5]


# In[8]:


#for all columns
df.columns


# In[9]:


display(df)


# <h1> Create multiple dataframes as per need </h1>

# In[10]:


Flight_Details = df.select("_c0", "Year", "Month", "DayofMonth", "FlightDate", "Tail_Number", "Flight_Number_Reporting_Airline")


# In[11]:


#count total no of flights in each years 
Flight_Details.select('Year').groupBy('Year').count().show()


# In[12]:


#count total no of flights in each months
Flight_Details.select('Month').groupBy('Month').count().show()


# In[13]:


#count total no of cancelled flights in each years 
df.select('Year').filter("Cancelled = 1").groupBy('Year').count().show()


# In[14]:


# count total no cancelled flights in each month
df.select('Month').filter('Cancelled = 1').groupBy('Month').count().show()


# In[15]:


#count total no of flights By departure airports
df.select('Origin').groupBy('Origin').count().show()


# In[16]:


#Calculate years wise on time departure flights
df.select('Year').filter('DepDelay <= 0').groupBy('Year').count().show()


# In[17]:


#calculate years wise departure delay flights
df.select('Year').filter('DepDelay > 0').groupBy('Year').count().show()


# In[18]:


#Caclulate years wise performance of flight like on-time departure, on-time arrival
df.select('Year').filter((df.DepDelay <=0) & (df.ArrDelay <=0)).groupBy('Year').count().show()


# In[34]:


Flight_Origin = df.select("_c0","OriginAirportID", "OriginAirportSeqID", "OriginCityMarketID", "Origin", "OriginCityMarketID","OriginState")


# In[22]:


Flight_Destination = df.select("_c0", "DestAirportID", "DestAirportSeqID", "Dest", "DestCityName", "DestState")


# In[23]:


Flight_arr_dep = df.select("_c0", "CRSArrTime", "ArrTime", "CRSDepTime", "DepTime")


# In[24]:


DelayDF=df.select("_c0","CarrierDelay","WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay")


# In[25]:


Flight_cancel = df.select("_c0", "Cancelled", "CancellationCode", "Diverted", "Flights")


# In[26]:


#modify column name
#to modify multiple column name
#Flight_cancel.withColumnRenamed("_c0","ID").withColumnRenamed("CancellationCode","Code")


# In[27]:


Flight_cancel = Flight_cancel.withColumnRenamed("_c0","ID")


# <h1> Data Cleaning </h1>
# 
# <h1> handling null values </h1>

# In[28]:


Flight_cancel.show()


# In[29]:


Flight_cancel.na.drop().show()


# In[30]:


Flight_cancel.na.drop(how='any').show()


# In[31]:


Flight_cancel.na.fill('yes').show()


# <h1> apply pyspark windows function </h1>

# In[32]:


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# In[35]:


ndf = Window.partitionBy('OriginState').orderBy('OriginAirportID')
Flight_Origin.withColumn('row_number',row_number().over(ndf)).show()


# In[36]:


#Use rank function
from pyspark.sql.functions import rank 
Flight_Origin.withColumn("rank", rank().over(ndf)).show()


# In[38]:


#sorting dataframe by using sort 
Flight_Origin.sort("Origin", "OriginState").show()


# In[40]:


#sorting dataframe by orderby
Flight_Origin.orderBy('Origin','OriginState').show()


# In[41]:


#sort by ascending 
#Flight Origin.sort(Flight_Origin, Origin.asc(),df.OriginState.asc()).show()
#Flight_Origin.orderBy(col("Origin").asc(), col("OriginState").asc()).show()


# In[42]:


#sort by descending 
#Flight_Origin.sort(Flight_Origin.Origin.asc(), df.OriginSate.asc().show())
#Flight_Origin.orderBy(col("Origin").desc(),col("OriginState").desc().show())


# In[45]:


from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col, array_contains


# In[48]:


Flight_Destination.show()


# <h1> how to use UDF in pyspark DataFrame </h1>

# In[47]:


from pyspark.sql.functions import udf 


# In[49]:


from pyspark.sql.functions import col


# In[50]:


#from pyspark.sql.types import StructType, IntegerType, StringType
#as per datatype StructType, IntegerType, StringType in columns we need to import these library
#otherwise bydefault it will take StringType


# In[51]:


#create function in Python


# In[55]:


def destination(strl):
    resStr=""
    arr = strl.split(",")
    for x in arr:
        resStr=arr[0]
    return resStr


# In[53]:


#Converting python function to UDF
destinationUDF = udf(lambda z: destination(z))


# In[54]:


df.select(col("Dest"),destinationUDF(col("DestCityName")).alias("DestCityName")).show(truncate=False)


# In[56]:


Flight_Destination.show()


# In[ ]:




