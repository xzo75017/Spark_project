#!/usr/bin/env python
# coding: utf-8

# In[27]:


#importing pyspark
import pyspark
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# In[28]:


#importing sparksession
from pyspark.sql import SparkSession 


# In[29]:


#creating a sparksession object and providing appName
spark=SparkSession.builder.appName("dataframe1").getOrCreate()


# <h1> Ways of Creating dataframe in pyspark </h1>
# <h1> Create Dataframe from Tuples </h1>

# In[30]:


data = [(1, 'Dhoni', 'Male', 38, 10),
        (2, 'Virat', 'Male', 30, 5),
        (3, 'Smirit', 'Female', 25, 9),
        (4, 'Sachin', 'Male', 45, 50),
        (5, 'Mitali', 'Female', 35, 7)
        ]
columns = ["ID", "Cricketer Name", "Gender", "Age", "Century"]


# In[31]:


df = spark.createDataFrame(data,columns)


# In[32]:


display(df)


# In[33]:


df.show()


# In[63]:


spark.stop()


# <h1> Create Dataframe from Dictionary </h1>

# In[42]:


data = [{'Flower':'Rose', 'price': 100, 'quantity':10},
        {'Flower':'Tulip', 'price': 500, 'quantity':10},
        {'Flower':'Sunflower', 'price': 400, 'quantity':6},
        {'Flower':'Jasmine', 'price': 100, 'quantity':12}]


# In[43]:


#Creating a dataframe
df1 = spark.createDataFrame(data)


# In[44]:


type(df1)


# In[45]:


df1.show()


# In[46]:


#importing pandas library
import pandas as pd


# <h1> Create Pandas Dataframe </h1>

# In[48]:


pandasDF = pd.read_csv("airlines1.csv")


# In[49]:


pandasDF.shape


# In[50]:


pandasDF


# <h1> Create spark Dataframe from Pandas Dataframe </h1>
# 
# <h1> we need to cast all the columns in the pandas df to string type to overcome this datatype issue while converting pandas df to spark df </h1>

# In[51]:


sparkDF = spark.createDataFrame(pandasDF.astype(str))


# In[52]:


sparkDF.printSchema()


# In[54]:


display(sparkDF)


# In[53]:


sparkDF.show()


# In[56]:


sparknewdf = sparkDF.select("Year", "Month", "DayofMonth", "FlightDate", "Tail_Number", "Flight_Number_Reporting_Airline")


# In[57]:


sparknewdf.printSchema()


# <h1> Create Pandas Dataframe from Spark Dataframe </h1>

# In[59]:


pdf = sparknewdf.toPandas()


# In[60]:


pdf


# In[61]:


#To create dataframe form External datasets
df = spark.read.option("header", "true").csv("airlines1.csv")


# In[62]:


df.show()


# In[ ]:




