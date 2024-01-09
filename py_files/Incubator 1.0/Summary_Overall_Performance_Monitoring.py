#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing reequired modules
import pandas as pd
import mysql.connector as msql


# In[2]:


# Reading data from incubator attendence monitoring sheet present on output folder
df=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Output\\Incubator_and_attendence_monitoring.csv")


# In[3]:


# Reading Quiz score from Quiz Information sheet present on output folder
Quiz=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Output\\Quiz_information.csv",usecols=["Email",'Overall_Average'])


# In[4]:


# Reading Assignment Score from Assignment sheet present on output folder
Assignment=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Output\\Assignment_Review.csv",usecols=["Email","Assignment_Score"])


# In[5]:


df.columns[0]


# In[6]:


df1 = df[['Email', 'Recorded_Percentage', 'SUK_Live_Percentage', 'SUK_Recorded_Percentage', 'Masterclass_Percentage', 'Program_Total', 'Program_percentage']].copy()


# In[7]:


df1


# In[8]:


# Adding Quiz score
df1=pd.merge(df1,Quiz, how ='left', on='Email')


# In[9]:


# Adding Assignment Score
df1=pd.merge(df1,Assignment, how ='left', on='Email')


# In[10]:


# Adding Time spend quizzes columns
df1["Time Spent on quizzes  Quizzes"]=6*0.167*df1["Overall_Average"]/100


# In[11]:


# Adding Time spent on assignments
df1["Time Spent on Assignments"]=9*2.67*df1["Assignment_Score"]/100


# In[12]:


df1["Program_Overall_Time_Spent_Hours"]=df1[["Program_Total","Time Spent on quizzes  Quizzes","Time Spent on Assignments"]].sum(axis=1)


# In[13]:


# Changing name of columns
df1.columns=[['Email', 'Recorded Videos- Percentage WatchTime per Week (%)',
       'SUK Live - Percentage WatchTime per Week (%)',
       'SUK Sessions - Percentage WatchTime per Week (%)',
       'MasterClass - Percentage WatchTime per Week (%) ','Time Spent on Video content  (Hours)',
       'Program Percentage WatchTime per Week (%', 'Quiz Score Average Score (%)',
       'Assignment Completion Average Score (%)', 'Time Spent on quizzes  Quizzes',
       'Time Spent on Assignments',"Program_Overall_Time_Spent_Hours"]]


# In[14]:


# Rearranging columns of Final Report
cols=[['Email', 'Recorded Videos- Percentage WatchTime per Week (%)',
       'SUK Live - Percentage WatchTime per Week (%)',
       'SUK Sessions - Percentage WatchTime per Week (%)',
       'MasterClass - Percentage WatchTime per Week (%) ',
       'Program Percentage WatchTime per Week (%','Time Spent on Video content  (Hours)' ,'Time Spent on quizzes  Quizzes',
       'Time Spent on Assignments' ,"Program_Overall_Time_Spent_Hours",'Quiz Score Average Score (%)',
       'Assignment Completion Average Score (%)']]
df1 = df1.reindex(cols, axis=1)


# In[15]:


# Filling Null values with zero and rounding off upto 2 decimal points
df1=df1.fillna(0)
df1=df1.round(2)


# In[16]:


# Exporting final sheet to Output folder
df1.to_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Output\\summary_overall_performance_monitoring.csv",index=False)


# In[17]:


# Connecting to Database select database and password here
conn= msql.connect(host='localhost',user='root',password="VS@123",database="Kalpana",auth_plugin='mysql_native_password')


# In[18]:


cursor =conn.cursor()


# In[19]:


# Inserting New records to database only
for i,row in df1.iterrows():
    cursor.execute("INSERT IGNORE INTO Kalpana.07_summary_overall_performance_monitoring VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))


# In[20]:


# Replacing database values with new values
for i,row in df1.iterrows():
    cursor.execute("REPLACE INTO Kalpana.07_summary_overall_performance_monitoring VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))


# In[21]:


conn.commit()


# In[22]:


conn.close()


# In[ ]:




