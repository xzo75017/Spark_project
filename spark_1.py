#!/usr/bin/env python
# coding: utf-8

# <h1> How to create RDDs </h1>
# 
# <h2> Methods of RDDs </h2>
# 
# 1.from variable
# 2.from RDD
# 3.from External Datasets

# In[19]:


import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# In[22]:


sc = SparkContext()


# In[21]:


sc.stop()


# In[23]:


a1 = ["b", "a", "c"]
x = sc.parallelize(a1)


# <h1> map() </h1>

# In[24]:


y2 = x.map(lambda z: (z, 1))


# In[25]:


y2.collect()


# <h1> flatMap() </h1>

# In[ ]:


# It applies to each element of RDD and it returns the result as new RDD
#It is similar to Map but FlatMap allows returning 0, 1 or more elements from map function 


# In[30]:


x = sc.parallelize([1, 2, 3])


# In[31]:


y = x.flatMap(lambda x: (x, x*100, 42))


# In[32]:


y.collect()


# In[33]:


d1 = ["This is a flatMap operation in Pyspark"]
rdd1 = sc.parallelize(d1)
rdd2 = rdd1.flatMap(lambda x: x.split(" "))


# In[34]:


rdd2.collect()


# In[35]:


y = rdd2.map(lambda z: (z, 1))


# In[36]:


y.collect()


# <h1> filter() </h1>

# In[37]:


#It returns an RDD that only has element that pass the condition


# In[43]:


x = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
y = x.filter(lambda x: x%2 == 1) #keep odd values


# In[44]:


y.collect()


# In[40]:


rdd2.collect()


# In[45]:


y = rdd2.filter(lambda x: x == 'operation')


# In[46]:


y.collect()


# <h1> distinct() </h1>

# In[47]:


#It returns a new dataset that contains the distinct elements of the source dataset. It is helpful to remove duplicate data 
#For example, if RDD has eements (Spark, Spark, Hadoop, Flink), then rdd.distinct() will five elements (Spark, Hadoop, Flink)


# In[48]:


sc.parallelize(('a','r','a','h','h')).distinct().collect()


# <h1> Actions Functions </h1>
# 
# <h1> count() </h1>

# In[49]:


#Action count() return the number of elements in RDD. 
sc.parallelize((1, 2, 3, 4)).count()


# <h1> sum() </h1>

# In[50]:


#It adds up the value in an RDD.
sc.parallelize((1, 2, 3, 4)).sum()


# <h1> max() </h1>

# In[51]:


#Return the maximum value from the dataset.
x = sc.parallelize((2, 4, 1))


# In[52]:


x.max()


# In[53]:


x.min()


# <h1> mean() </h1>

# In[55]:


#Alias for Avg. Return the average of the values in a colum.
x = sc.parallelize([2, 4, 1])
x.mean()


# In[56]:


sc.stop()


# In[ ]:




