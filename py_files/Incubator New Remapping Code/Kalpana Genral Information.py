#!/usr/bin/env python
# coding: utf-8

# In[1]:


# importing required modules
import pandas as pd
import mysql.connector as msql
import math
import os


# In[2]:


directory_path =(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File\01_Source_Kalpana General Information")

csv_files = [file for file in os.listdir(directory_path) if file.endswith(('.xlsx'))]

for file in csv_files:
    file_path = os.path.join(directory_path, file)
    df = pd.read_excel(file_path)
    print(f"Data from {file}:")


# In[3]:


#df= pd.read_excel('C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 2.0\\Kalpana Source File\\Incubator 2.0 General Information Source.xlsx')
#df= pd.read_excel('C:\\Users\\HP\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 2.0\\Kalpana Source File\\01_Source_Kalpana General Information\\Incubator 2.0 General Information Source.xlsx')


# In[4]:


df.head()


# In[5]:


df.columns


# In[6]:


df.columns =['Email', 'Name', 'Phone', 'Gender', 'Currently_pursuing_degree',
       'Subject_Area', 'Name_of_College_University', 'Country',
       'State_Union_Territory', 'District', 'City_category', 'Caste_Category',
       'Annual_Family_Income', 'Assigned_Through']


# In[7]:


#df.to_csv('General_Info.csv', mode='a',index=False)


# In[8]:


df.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\General_Info.csv",mode='w',index=False)


# In[9]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')


# In[10]:


cursor =conn.cursor()


# In[11]:


for i, row in df.iterrows():
    row = [None if isinstance(val, float) and math.isnan(val) else val for val in row] # replace "nan" values with None
    columns = ','.join(df.columns)
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.01_general_information ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    query += ", ".join([f"{col}=VALUES({col})" for col in df.columns if col != 'Email'])
    # Execute the query
    cursor.execute(query, tuple(row))


# In[12]:


# Inserting records from data file, if already present ignoring it.
for i,row in df.iterrows():
    row = [None if isinstance(val, float) and math.isnan(val) else val for val in row] # replace "nan" values with None
    cursor.execute("INSERT IGNORE INTO Kalpana.01_general_information(Email, Name, Phone, Gender, Currently_pursuing_degree, Subject_Area, Name_of_College_University, Country, State_Union_Territory, District, City_category, Caste_Category, Annual_Family_Income, Assigned_Through) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))


# In[13]:


conn.commit()


# In[ ]:





# In[ ]:




