#!/usr/bin/env python
# coding: utf-8

# In[5]:


# Import all necessary libraries
import pandas as pd
import os
import warnings
warnings.filterwarnings('ignore')


# # Change the file of graphy sheet  and all Quizzes sheet on Source Files

# In[6]:


# Reading Kalpana SHE for STEM Incubator file from Source files
# Reading Kalpana SHE for STEM Incubator file from Source files
directory_path =(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\02_Source_Kalpana incubator")
csv_files = [file for file in os.listdir(directory_path) if file.endswith('.csv')]

for file in csv_files:
    # Create the full file path by joining the directory path and file name
    file_path = os.path.join(directory_path, file)
    # Read the CSV file into the 'Kalpana' DataFrame
    Kalpana = pd.read_csv(file_path)
    # Print a message indicating the data source
    print(f"Data from {file}:")


# In[35]:


# Reading quizze files from Source Files
IDP_review = pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\03_Source_Kalpana Quiz Review\Individual Development Plan (IDP).csv")
# Creating new List to just take 'Email' and Column which contain 'Marks' word in it
cols_to_keep = ['Email'] + [col for col in IDP_review.columns if 'marks' in col.lower()]
# Reassigning the "IDP_review" DataFrame with only the columns in the "cols_to_keep" list,
IDP_review = IDP_review[cols_to_keep]


# In[36]:


IDP_review.dtypes


# # Number of attempts need to be done mannualy Will have to check manually to run the code Marks1,Marks2....etc

# # SWOT

# In[32]:


IDP_review = pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\03_Source_Kalpana Quiz Review\Individual Development Plan (IDP).csv")


# In[19]:


# Reading quizze files from Source Files
SWOT_review = pd.read_csv(r'C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\03_Source_Kalpana Quiz Review\Strategic planning tools for STEM (SWOT).csv')
# Creating new List to just take 'Email' and Column which contain 'Marks' word in it
cols_to_keep = ['Email'] + [col for col in SWOT_review.columns if 'marks' in col.lower()]
# Reassigning the "SWOT_review" DataFrame with only the columns in the "cols_to_keep" list,
SWOT_review = SWOT_review[cols_to_keep]
# Merging Emails and SWOT Marks
df1 = Kalpana.merge(SWOT_review,on= 'Email', how ='left')

# Create a list of columns to use for the maximum calculation, excluding 'Email'
cols_to_max = [col for col in cols_to_keep if col != 'Email']

# Calculate the maximum value for each row based on the specified columns
df1['Marks_SWOT'] = df1[cols_to_max].max(axis=1, skipna=True)

# Drop all other columns except 'email' and 'max_marks'
df1 = df1[['Email', 'Marks_SWOT']]


# In[ ]:





# In[6]:


Collabaration=pd.read_csv(r"C:\Users\spjay\Desktop\VigyanShaala\OneDrive_2023-03-17\INC 2.0 Source Data_Shunham DS\Kalpana Incubator Jan - Mar 2023 Batch-Quiz_ 21st Century skills - Collaboration and problem solving-1679893027645.csv")


# In[7]:


Collabaration.head()


# # IDP

# In[50]:


# Reading quizze files from Source Files
IDP_review = pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\03_Source_Kalpana Quiz Review\Individual Development Plan (IDP).csv")
# Creating new List to just take 'Email' and Column which contain 'Marks' word in it
cols_to_keep = ['Email'] + [col for col in IDP_review.columns if 'marks' in col.lower()]
# Reassigning the "IDP_review" DataFrame with only the columns in the "cols_to_keep" list,
IDP_review = IDP_review[cols_to_keep]
# Merging Emails and IDP Marks
df2 = df1.merge(IDP_review,on= 'Email', how ='left')
# Drop all columns except 'Email' and 'Marks_SWOT'
df2 = df2[['Email', 'Marks_SWOT']]

# Calculate the maximum value for all columns except 'Email' and store it in 'Marks_IDP' column
df2['Marks_IDP'] = IDP_review[cols_to_keep[1:]].max(axis=1, skipna=True)

# Drop all columns except 'Email' and 'Marks_IDP'
df2 = df2[['Email', 'Marks_SWOT', 'Marks_IDP']]


# # Cmmunication

# In[51]:


# Reading quizze files from Source Files
Communication_review= pd.read_csv(r'C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\03_Source_Kalpana Quiz Review\Communication.csv')
# Creating new List to just take 'Email' and Column which contain 'Marks' word in it
cols_to_keep = ['Email'] + [col for col in Communication_review.columns if 'marks' in col.lower()]
# Reassigning the "Communication_review" DataFrame with only the columns in the "cols_to_keep" list,
Communication_review = Communication_review[cols_to_keep]
# Merging Emails and Communication Marks
df3 = df2.merge(Communication_review,on= 'Email', how ='left')

# Calculate the maximum value for all columns except 'Email' and store it in 'Marks_Communication' column
df3['Marks_Communication'] = df3[cols_to_keep[1:]].max(axis=1, skipna=True)

# Drop all columns except 'Email', 'Marks_SWOT', 'Marks_IDP', and 'Marks_Communication'
df3 = df3[['Email', 'Marks_SWOT', 'Marks_IDP', 'Marks_Communication']]


# In[55]:


df3.head()


# # Critical Thinking

# In[57]:


# Reading quizze files from Source Files
Critical_Thinking=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\03_Source_Kalpana Quiz Review\Critical thinking.csv")
# Creating new List to just take 'Email' and Column which contain 'Marks' word in it
cols_to_keep = ['Email'] + [col for col in Critical_Thinking.columns if 'marks' in col.lower()]
# Reassigning the "Critical_Thinking" DataFrame with only the columns in the "cols_to_keep" list,
Critical_Thinking = Critical_Thinking[cols_to_keep]
# Merging Emails and Communication Marks
df4 = df3.merge(Critical_Thinking,on= 'Email', how ='left')
# Take the maximum of each row for the selected columns
df4['Marks_Critical_Thinking'] = df4[cols_to_keep[1:]].max(axis=1, skipna=True)
# Drop all other columns except 'email' and 'max_marks'
df4 = df4[['Email','Marks_SWOT', 'Marks_IDP','Marks_Communication','Marks_Critical_Thinking']]


# # Collabaration

# In[58]:


# Reading quizze files from Source Files
Collabaration=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\03_Source_Kalpana Quiz Review\Collaboration and problem solving.csv")
# Creating new List to just take 'Email' and Column which contain 'Marks' word in it
cols_to_keep = ['Email'] + [col for col in Collabaration.columns if 'marks' in col.lower()]
# Reassigning the "Collabaration" DataFrame with only the columns in the "cols_to_keep" list,
Collabaration = Collabaration[cols_to_keep]
# Merging Emails and Collabaration Marks
df5 = df4.merge(Collabaration,on= 'Email', how ='left')
# Take the maximum of each row for the selected columns
df5['Marks_Collabaration'] = df5[cols_to_keep[1:]].max(axis=1, skipna=True)
# Drop all other columns except 'email' and 'max_marks'
df5 = df5[['Email','Marks_SWOT', 'Marks_IDP','Marks_Communication','Marks_Critical_Thinking','Marks_Collabaration']]


# # Creativity and Growth Mindset

# In[60]:


# Reading quizze files from Source Files
Creativity_Growth=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\03_Source_Kalpana Quiz Review\Creativity & growth mindset.csv")
# Creating new List to just take 'Email' and Column which contain 'Marks' word in it
cols_to_keep = ['Email'] + [col for col in Creativity_Growth.columns if 'marks' in col.lower()]
# Reassigning the "SWOT_review" DataFrame with only the columns in the "cols_to_keep" list,
Creativity_Growth = Creativity_Growth[cols_to_keep]
# Merging Emails and Collabaration Marks
df6 = df5.merge(Creativity_Growth,on= 'Email', how ='left')
# Take the maximum of each row for the selected columns
df6['Marks_Creativity_Growth'] = df6[cols_to_keep[1:]].max(axis=1, skipna=True)
# Drop all other columns except 'email' and 'max_marks'
df6 = df6[['Email','Marks_SWOT', 'Marks_IDP','Marks_Communication','Marks_Critical_Thinking','Marks_Collabaration','Marks_Creativity_Growth']]


# In[61]:


# Function for conveerting marks into percentage marks
def percent(value):
    return value*10


# In[62]:


# Converting all quizzes marks to percentage
df6["Marks_SWOT"]= df6["Marks_SWOT"].apply(percent)
df6["Marks_IDP"]= df6["Marks_IDP"].apply(percent)
df6["Marks_Communication"]= df6["Marks_Communication"].apply(percent)
df6["Marks_Critical_Thinking"]= df6["Marks_Critical_Thinking"].apply(percent)
df6["Marks_Collabaration"]= df6["Marks_Collabaration"].apply(percent)
df6["Marks_Creativity_Growth"]= df6["Marks_Creativity_Growth"].apply(percent)


# In[63]:


# Filling NaN values with Zeros
fillna = df6.iloc[:,1:]
fillna0=fillna.columns
df6[fillna0]=df6[fillna0].fillna(0)


# In[64]:


# Creating Overall_Score column which is average of columns of all quizzes
df6["Overall_Average"]=df6.iloc[:,1:].mean(axis=1)


# In[65]:


# DataFrame df6 to 2 decimal places using the round() 
df6["Overall_Average"]=df6["Overall_Average"].round(2)


# In[66]:


df6.to_csv('Quiz_Information_Final.csv', mode='a',index=False)


# In[69]:


# Exporting Quiz_Review file to Output Files
df6.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Output\Quiz_Information.csv",mode='w',index=False)


# In[70]:


df6.head()


# # Storing data on MySQL

# import mysql.connector as msql

# #Connecting to MySQL Kalpana database
# conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')

# In[73]:


cursor =conn.cursor()


# # Inserting data on 10_incubator_quiz_monitoring
# for i,row in df6.iterrows():
#     cursor.execute("INSERT IGNORE INTO Kalpana.10_incubator_quiz_monitoring VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))

# In[32]:


# cursor.execute("CREATE TABLE 10_Incubator_quiz_monitoring(Email  varchar(255) Primary Key, Marks_SWOT varchar(255), Marks_IDP varchar(255), Marks_Communication varchar(255),Marks_Critical_Thinking varchar(255),Marks_Collabaration varchar(255),Marks_Creativity_Growth varchar(255), Overall_Average varchar(255))")


# # Replacing previous records with new records into INTO 10_incubator_quiz_monitoring
# for i,row in df6.iterrows():
#     cursor.execute("REPLACE INTO Kalpana.10_incubator_quiz_monitoring VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))

# conn.commit()
