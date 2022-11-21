#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pyspark
import os
import sys
from pyspark import SparkContext
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# In[2]:


sc = SparkContext()


# In[24]:


text_file = sc.textFile("sample.txt")


# In[25]:


text_file.collect()


# In[26]:


new1 = text_file.flatMap(lambda line: line.split('@'))


# In[27]:


new1.collect()


# In[31]:


text_file = sc.textFile("sample.txt")


# In[32]:


counts = text_file.flatMap(lambda line: line.split(" "))


# In[33]:


counts.collect()


# In[34]:


counts1 = counts.map(lambda word: (word, 1))


# In[37]:


counts1.collect()


# In[35]:


counts3 = counts1.reduceByKey(lambda x, y: x + y)


# In[36]:


counts3.collect()


# In[41]:


counts3.filter(lambda x: x[0] not in ["for", "and", "not", "on", "the", "as", "of", "is"]).collect()


# In[42]:


sc.stop()


# In[ ]:




