#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# # Change the file of graphy sheet  and all Quizzes sheet on Source Files

# In[2]:


# Reading Kalpana SHE for STEM Incubator file from Source files
Kalpana = pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Kalpana SHE for STEM Incubator_July 22 Batch.csv",usecols=['Email'])


# # Number of attempts need to be done mannualy Will have to check manually to run the code Marks1,Marks2....etc

# In[3]:


# Reading all quizzes files from Source Files
SWOT_review = pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Quiz_ Strategic planning tools for STEM (SWOT).csv",usecols=["Email","Marks","Marks.1","Marks.2","Marks.3"])
IDP_review = pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Quiz_ Individual Development Plan (IDP).csv",usecols=["Email","Marks","Marks.1","Marks.2","Marks.3","Marks.4","Marks.5"])
Communication_review= pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Quiz_ 21st Century skills - Communication.csv",usecols=["Email","Marks","Marks.1","Marks.2","Marks.3","Marks.4","Marks.5","Marks.6","Marks.7"])
Critical_Thinking=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Quiz_ 21st Century skills - Critical thinking.csv",usecols=["Email","Marks","Marks.1","Marks.2"])
Creativity_Growth=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Quiz_ 21st Century skills - Creativity & growth mindset.csv",usecols=["Email","Marks","Marks.1","Marks.2","Marks.3","Marks.4","Marks.5"])
Collabaration=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Quiz_ 21st Century skills - Collaboration and problem solving - Copy.csv",usecols=["Email","Marks","Marks.1","Marks.2","Marks.3","Marks.4"])


# In[4]:


# pd.set_option("display.max_columns",None)
# pd.set_option("display.max_rows",None)


# # SWOT

# In[5]:


# Merging Emails and SWOT Marks
df1 = Kalpana.merge(SWOT_review,on= 'Email', how ='left')


# In[6]:


# Taking Max of all attempt marks and assigning it into Marks_SWOT Column
Marks_max = df1.loc[:,'Marks':'Marks3'].max(axis=1)
df1["Marks_SWOT"] = Marks_max


# In[7]:


# Dropping different attempt Marks of SWOT
df1.drop(["Marks","Marks.1","Marks.2","Marks.3"],axis=1,inplace=True)


# # IDP

# In[8]:


# Adding IDP Marks
df2 = df1.merge(IDP_review,on= 'Email', how ='left')


# In[9]:


# Taking Max of all attempt marks and assigning it into Marks_IDP Column
IDP_Marks_max = df2.loc[:,'Marks':'Marks.5'].max(axis=1)
df2["Marks_IDP"] = IDP_Marks_max


# In[10]:


# Dropping different attempt Marks of IDP
df2.drop(["Marks","Marks.1","Marks.2","Marks.3","Marks.4","Marks.5"],axis=1,inplace=True)


# # Communication

# In[11]:


# Adding Communication marks
df3 = df2.merge(Communication_review,on= 'Email', how ='left')


# In[12]:


# Taking Max of all attempt marks and assigning it into Marks_Communication Column
Communication_Marks_max = df3.loc[:,'Marks':'Marks.7'].max(axis=1)
df3["Marks_Communication"] = Communication_Marks_max


# In[13]:


# Dropping different attempt Marks of Communication
df3.drop(["Marks","Marks.1", 'Marks.2', 'Marks.3', 'Marks.4',
       'Marks.5', 'Marks.6', 'Marks.7'],axis=1,inplace=True)


# # Critical Thinking

# In[14]:


# Adding Critical Thinking marks
df4 = df3.merge(Critical_Thinking,on= 'Email', how ='left')


# In[15]:


# Taking Max of all attempt marks and assigning it into Marks_Critical_Thinking Column
Critical_Thinking_Marks_max = df4.loc[:,'Marks':'Marks.2'].max(axis=1)
df4["Marks_Critical_Thinking"] = Critical_Thinking_Marks_max


# In[16]:


# Dropping different attempt Marks of Critical Thinking
df4.drop(["Marks","Marks.1","Marks.2"],axis=1,inplace=True)


# # Collabaration

# In[17]:


# Adding Collabartion marks
df5 = df4.merge(Collabaration,on= 'Email', how ='left')


# In[18]:


# Taking Max of all attempt marks and assigning it into Marks_Collabration Column
Collabaration_Marks_max = df5.loc[:,'Marks':'Marks.4'].max(axis=1)
df5["Marks_Collabaration"] = Collabaration_Marks_max


# In[19]:


# Dropping different attempt Marks of Collabration
df5.drop(["Marks","Marks.1","Marks.2","Marks.3","Marks.4"],axis=1,inplace=True)


# # Creativity and Growth Mindset

# In[20]:


# Adding Creativity & Growth marks
df6 = df5.merge(Creativity_Growth,on= 'Email', how ='left')


# In[21]:


# Taking Max of all attempt marks and assigning it into Marks_Creativiity Growth Column
Creativity_Growth_Marks_max = df6.loc[:,'Marks':'Marks.5'].max(axis=1)
df6["Marks_Creativity_Growth"] = Creativity_Growth_Marks_max


# In[22]:


# Dropping different attempt Marks of Creativity and Growth Mindset
df6.drop(["Marks","Marks.1","Marks.2","Marks.3","Marks.4","Marks.5"],axis=1,inplace=True)


# In[23]:


# Function for conveerting marks into percentage marks
def percent(value):
    return value*10


# In[24]:


# Converting all quizzes marks to percentage
df6["Marks_SWOT"]= df6["Marks_SWOT"].apply(percent)
df6["Marks_IDP"]= df6["Marks_IDP"].apply(percent)
df6["Marks_Communication"]= df6["Marks_Communication"].apply(percent)
df6["Marks_Critical_Thinking"]= df6["Marks_Critical_Thinking"].apply(percent)
df6["Marks_Collabaration"]= df6["Marks_Collabaration"].apply(percent)
df6["Marks_Creativity_Growth"]= df6["Marks_Creativity_Growth"].apply(percent)


# In[25]:


# Filling NaN values with Zeros
fillna = df6.iloc[:,1:]
fillna0=fillna.columns
df6[fillna0]=df6[fillna0].fillna(0)


# In[26]:


# Creating Overall_Score column which is average of columns of all quizzes
df6["Overall_Average"]=df6.iloc[:,1:].mean(axis=1)


# In[27]:


df6["Overall_Average"]=df6["Overall_Average"].round(2)


# In[28]:


# Exporting Quiz_Review file to Output Files
df6.to_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Output\\Quiz_Information.csv",index=False)


# # Storing data on MySQL

# In[29]:


import mysql.connector as msql


# In[30]:


#Connecting to MySQL Kalpana database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="Kalpana",auth_plugin='mysql_native_password')


# In[31]:


cursor =conn.cursor()


# In[32]:


# cursor.execute("CREATE TABLE 10_Incubator_quiz_monitoring(Email  varchar(255) Primary Key, Marks_SWOT varchar(255), Marks_IDP varchar(255), Marks_Communication varchar(255),Marks_Critical_Thinking varchar(255),Marks_Collabaration varchar(255),Marks_Creativity_Growth varchar(255), Overall_Average varchar(255))")


# In[33]:


# Inserting data on 10_incubator_quiz_monitoring
for i,row in df6.iterrows():
    cursor.execute("INSERT IGNORE INTO Kalpana.10_incubator_quiz_monitoring VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))


# In[34]:


# Replacing previous records with new records into INTO 10_incubator_quiz_monitoring
for i,row in df6.iterrows():
    cursor.execute("REPLACE INTO Kalpana.10_incubator_quiz_monitoring VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))


# In[35]:


conn.commit()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




