#!/usr/bin/env python
# coding: utf-8

# In[1]:


# importing required modules
import pandas as pd
import mysql.connector as msql


# In[2]:


# Reading Kalpana She for STEM incubator file from source files
df = pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Kalpana SHE for STEM Incubator_July 22 Batch.csv",usecols=["Email","Name",'What is your Date of Birth (Strictly for Internal Purposes Only, used to match fellows with internships, jobs, experienced jobs etc.)','What is your Subject Area?','Where do you live now (for internal purposes)?','Annual Family Income','Introduce yourself - 62b2b7da0cf26c3ef0388612'])


# In[3]:


# Reading wati file present in 01_general_info code
wati = pd.read_excel("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Wati Contact Management.xlsx", usecols=['Email','Phone','Assigned Through'])


# In[4]:


# Adding Phone to genral info
data = pd.merge(left=df, right = wati, on = 'Email',how ='left')


# In[5]:


# Extracting document link from introdue_yourself column 
data[['Introduce yourself - 62b2b7da0cf26c3ef0388612','Status','Intro_Name','Date','Document_Link']]=data['Introduce yourself - 62b2b7da0cf26c3ef0388612'].str.split('|',expand=True)
data['Document_Link']=data['Document_Link'].str.replace('Q3-','')


# In[6]:


# Dropping unwanted columns
data.drop(['Introduce yourself - 62b2b7da0cf26c3ef0388612','Status','Intro_Name','Date'],axis=1,inplace=True)


# # Storing data to MySQL

# In[7]:


# Connecting to MySQL change the password or database here
conn= msql.connect(host='localhost',user='root',password="VS@123",database="Kalpana",auth_plugin='mysql_native_password',charset="utf8mb4")
cursor =conn.cursor()


# In[8]:


# Many of the headers come from google forms
new_cols=['Email','Name', 'Phone', 'Where do you live now (for internal purposes)?', 'Annual Family Income',
       'Assigned Through','Document_Link']
data=data.reindex(columns=new_cols)


# In[9]:


# Filling Null values with blank values # Without it SQL insert showing error
data=data.fillna('')


# In[10]:


# Inserting records from data file, if already present ignoring it.
for i,row in data.iterrows():
    cursor.execute("INSERT IGNORE INTO Kalpana.01_general_information(Email,Name,Phone,City_category,Annual_Family_Income,Assigned_Through,Photo_ID) VALUES (%s,%s,%s,%s,%s,%s,%s)", tuple(row))


# In[11]:


# Inserting records from data file, if already present ignoring it.
for i,row in data.iterrows():
    cursor.execute("REPLACE INTO Kalpana.01_general_information(Email,Name,Phone,City_category,Annual_Family_Income,Assigned_Through,Photo_ID) VALUES (%s,%s,%s,%s,%s,%s,%s)", tuple(row))


# In[12]:


# Commit the changes and close the connection
conn.commit()
cursor.close()


# In[13]:


# Exporting the genral information to Output files
data.to_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Output\\General_Information.csv",index=False)


# In[ ]:




