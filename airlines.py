#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd


# In[6]:


df = pd.read_csv("airlines1.csv")


# In[7]:


df.head()


# In[11]:


df.columns.values


# In[3]:


#importing pyspark
import pyspark
import os
import sys
#importing sparksession
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# In[ ]:





# In[12]:


#creating a sparksession object and provinding appName
spark = SparkSession.builder.appName('airline').getOrCreate()


# In[ ]:


spark.stop()


# In[13]:


#To create dataframe form External datasets 
df = spark.read.option("header", "true").csv("airlines1.csv")


# In[14]:


df.show()


# In[15]:


df1 = df.select("Year", "Month", "DayofMonth", "FlightDate", "Tail_Number", "Flight_Number_Reporting_Airline" )


# In[16]:


#To display dataframe use show() function it display 20 rows
df1.show()


# In[17]:


df1.collect()


# In[18]:


#show all columns names 
df1.columns


# In[19]:


#to count total no for rows in dataframe
df1.count()


# In[20]:


#to display the statistical properties of all the columns in the dataset
df1.describe().show()


# In[21]:


#group data by using group() function 
df1.select('Year').groupBy('Year').count().show()


# In[22]:


d1 = df1.select('Year')
d2 = df1.groupBy('Year')
d3 = d2.count()


# In[23]:


d3.show(25)


# In[24]:


#group by month
df1.select('Month').groupBy('Month').count().show()


# In[27]:


#Filter data using filter() function
df1.select('Year').filter('Month = 6').groupBy('Year').count().show()


# In[28]:


#to print schema of columns use printSchema() function
df1.printSchema()


# In[ ]:




