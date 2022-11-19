#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install pyspark


# In[2]:


import pyspark


# In[3]:


from pyspark.sql import SparkSession 


# In[4]:


spark = SparkSession.builder.master("local[1]").appName("sessionname1").getOrCreate()


# In[ ]:





# In[5]:


spark.stop()


# In[10]:


sc.stop()


# In[1]:


from pyspark import SparkContext


# In[2]:


sc = SparkContext()


# In[3]:


#How to create RDDs
#Method of RDDs


# In[4]:


#From passing variable use parallelize
x = [1, 2 ,3]
y = sc.parallelize(x)


# In[5]:


y


# In[6]:


y.collect()


# In[16]:





# In[17]:





# In[19]:





# In[7]:


#Transformations Functions 
#map()

x = sc.parallelize(["b", "a", "c"])
y = x.map(lambda z :(z, 1))
print(x.collect())
print(y.collect())


# In[ ]:




