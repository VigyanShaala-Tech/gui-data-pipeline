#!/usr/bin/env python
# coding: utf-8

# In[9]:


# importing required modules
import pandas as pd
import mysql.connector as msql
import math


# In[10]:


#df= pd.read_excel('C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 2.0\\Kalpana Source File\\Incubator 2.0 General Information Source.xlsx')
df= pd.read_excel('C:\\Users\\HP\\OneDrive - VigyanShaala\\02 Products  Initiatives\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 2.0\\Kalpana Source File\\01_Source_Kalpana General Information\\Incubator 2.0 General Information Source.xlsx')


# In[11]:


# Display the first 10 rows
df.head()


# In[12]:


# Get the column names
df.columns


# In[13]:


# Rename the columns
df.columns =['Email', 'Name', 'Phone', 'Gender', 'Currently_pursuing_degree',
       'Subject_Area', 'Name_of_College_University', 'Country',
       'State_Union_Territory', 'District', 'City_category', 'Caste_Category',
       'Annual_Family_Income', 'Assigned_Through']


# In[14]:


#df.to_csv('General_Info.csv', mode='a',index=False)


# In[15]:


# Use the df.to_csv() method to save the DataFrame to a CSV file
df.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Output\General_Info.csv",index=False)


# In[16]:


# Use the data.to_csv() method to save the DataFrame to a CSV file
#data.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Incubator_and_attendence_monitoring_WithoutNotation.csv",mode='w',index=False)


# In[17]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')


# In[18]:


# Create a cursor object
cursor =conn.cursor()


# In[19]:


for i, row in df.iterrows():
    # Replace "nan" values with None
    row = [None if isinstance(val, float) and math.isnan(val) else val for val in row]
    # Create a comma-separated list of column names
    columns = ','.join(df.columns)
    # Create a comma-separated list of placeholders (%s) for values
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.01_general_information ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    query += ", ".join([f"{col}=VALUES({col})" for col in df.columns if col != 'Email'])
    # Execute the query
    cursor.execute(query, tuple(row))


# In[20]:


# Inserting records from data file, if already present ignoring it.
for i,row in df.iterrows():
    row = [None if isinstance(val, float) and math.isnan(val) else val for val in row] # replace "nan" values with None
    cursor.execute("INSERT IGNORE INTO Kalpana.01_general_information(Email, Name, Phone, Gender, Currently_pursuing_degree, Subject_Area, Name_of_College_University, Country, State_Union_Territory, District, City_category, Caste_Category, Annual_Family_Income, Assigned_Through) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))


# In[21]:


# Commit the changes to the database
conn.commit()


# In[ ]:





# In[ ]:




