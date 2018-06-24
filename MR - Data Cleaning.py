
# coding: utf-8

# In[20]:


import tables
import pandas as pd
import csv
import copy
import datetime as dt

#Read raw file
raw_mindright_df = pd.read_csv(r"C:\Users\Rasiga\Documents\MindRight\mindright-csv-export-staging-2018-05-08.csv")
