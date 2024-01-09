#!/usr/bin/env python
# coding: utf-8

# In[1]:


# importing required modules
import pandas as pd
import mysql.connector as msql


# # Source file should be named as "Incubator Assignment Review"

# In[2]:


#Reading Incubator Assignment Review file
df= pd.read_excel("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Incubator Assignment Review.xlsx",sheet_name='Assignment Review')


# In[3]:


# Dispalying All columns
pd.set_option('display.max_columns',None)
pd.options.display.float_format='{:.0f}'.format


# In[4]:


#Selecting desired columns
df=df.iloc[:,:30]


# In[5]:


# Dropping unwanted columns here
df.drop(df.columns[[5,8,11,14,17,20,23,26,29]],axis=1, inplace=True)


# In[6]:


# dropping the Assignment score % column of Assignment review sheeet
# df.drop('Assignment Score %',axis=1,inplace=True)


# In[7]:


# Assigning the different values of score
assign=['WK_1_SWOT Analysis','WK_2_RIASEC','WK_2_SMART goal','MC_Planning Masters','WK_3_Career Action Plan ','MC_LinkedIn Profile ','MC_CV/Resume ','WK_6_Critical thinking','WK_7_Problem solving']
df[assign] = df[assign].replace(['Accepted'],100)
df[assign] = df[assign].replace(['Submit 1'],30)
df[assign] = df[assign].replace(['Review 1'],30)
df[assign] = df[assign].replace(['Submit 2'],80)
df[assign] = df[assign].replace(['Rejected '],80)


# In[8]:


# Filling NaN Values with zero
df[assign]=df[assign].fillna(0)


# In[9]:


# Calculating the average score
df['Assignment_Score']=df[['WK_1_SWOT Analysis','WK_2_RIASEC','WK_2_SMART goal','MC_Planning Masters','WK_3_Career Action Plan ','MC_LinkedIn Profile ','MC_CV/Resume ','WK_6_Critical thinking','WK_7_Problem solving']].mean(axis=1)


# In[10]:


# Changing the headers of dataset
df.columns=['Email', 'Name', 'Phone', 'WK_1_SWOT_Analysis', 'Comments',
       'WK_2_RIASEC', 'Comments_1', 'WK_2_SMART_goal', 'Comments_2',
       'MC_Planning_Masters', 'Comments_3', 'WK_3_Career_Action_Plan',
       'Comments_4', 'MC_LinkedIn_Profile ', 'Comments_5', 'MC_CV_Resume',
       'Comments_6', 'WK_6_Critical_thinking', 'Comments_7',
       'WK_7_Problem_solving', 'Comments_8', 'Assignment_Score']


# In[11]:


# Rounding Off values of Assignment score to two digits
df['Assignment_Score']=df['Assignment_Score'].round(2)


# In[12]:


df=df.fillna('')


# In[13]:


# Exporting the dataset to output folder
df.to_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Output\\Assignment_Review.csv",index=False)


# In[14]:


df.columns


# # Storing data on MySQL

# In[15]:


# Connecting MySQL Kalpana Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="Kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[16]:


for i, row in df.iterrows():
    columns = ','.join(df.columns)
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.11_incubator_assignment_monitoring ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    query += ", ".join([f"{col}=VALUES({col})" for col in df.columns if col != 'Email'])
    # Execute the query
    cursor.execute(query, tuple(row))


# In[17]:


conn.commit()


# In[18]:


#(Email,Name,Phone,WK_1_SWOT Analysis,Comments,WK_2_RIASEC,Comments_1,WK_2_SMART_goal,Comments_2,MC_Planning Masters,Comments_3,WK_3_Career_Action_Plan,Comments_4,MC_LinkedIn Profile,Comments_5,MC_CV_Resume,Comments_6,WK_6_Critical_thinking,Comments_7,WK_7_Problem_solving,Comments_8,Assignment_Score)


# In[ ]:




