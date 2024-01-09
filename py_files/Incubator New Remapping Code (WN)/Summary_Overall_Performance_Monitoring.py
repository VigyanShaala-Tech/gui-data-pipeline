#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing reequired modules
import pandas as pd
import mysql.connector as msql
import math


# In[2]:


pd.__version__


# In[3]:


# Displaying all columns and rows
pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)


# In[4]:


# Reading data from incubator attendence monitoring sheet present on output folder
df=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Incubator_and_attendence_monitoring_WithoutNotation.csv")


# In[5]:


# Reading Quiz score from Quiz Information sheet present on output folder
Quiz=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Quiz_Information.csv",usecols=["Email",'Overall_Average'])


# In[6]:


# Reading Assignment Score from Assignment sheet present on output folder
Assignment=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Assignment_Review.csv",usecols=["Email","Assignment_Score"])


# In[7]:


# Cheaking the first column of datafame 'df'
df.columns[0]


# In[8]:


# Taking Email and % of all Recorded, SUK Live,SUK sessions, Master
df1 = df[['Email', 'Recorded_Percentage', 'SUK_Recorded_Percentage', 'Masterclass_Percentage', 'Program_Total', 'Program_percentage']].copy()


# In[9]:


# Adding Quiz score
df1=pd.merge(df1,Quiz, how ='left', on='Email')


# In[10]:


# Adding Assignment Score
df1=pd.merge(df1,Assignment, how ='left', on='Email')


# In[11]:


# Adding Time spend quizzes columns
df1["Time_Spent_On_Quizzes"]=6*0.167*df1["Overall_Average"]/100


# In[12]:


# Filling null values with '0.0'
df1=df1.fillna('0.0')


# In[13]:


# Making 'Assignment_Score' column data type as 'float'
df1['Assignment_Score'] = df1['Assignment_Score'].astype(float)


# In[14]:


# Adding Time spent on assignments
df1["Time Spent on Assignments"]=9*2.67*df1["Assignment_Score"]/100


# In[15]:


# Creating 'Program_Overall_Time_Spent_Hours' by adding all necessary columns
df1["Program_Overall_Time_Spent_Hours"]=df1[["Program_Total","Time_Spent_On_Quizzes","Time Spent on Assignments"]].sum(axis=1)


# In[16]:


# Filling Null values with zero and rounding off upto 2 decimal points
df1=df1.fillna(0)
df1=df1.round(2)


# In[17]:


df1.columns


# In[18]:


# Define a dictionary that maps column names to their desired new names
new_names = {
    'Recorded_Percentage': 'Recorded_Videos_Percentage_WatchTime_Per_Week',
    'SUK_Recorded_Percentage': 'SUK_Sessions_Percentage_WatchTime_Per_Week',
    'Masterclass_Percentage': 'MasterClass_Total_WatchTime_Per_Week',
    'Program_Total': 'Program_Percentage_WatchTime_Per_Week',
    'Program_percentage': 'Program_Total_WatchTime_Per_Week',
    'Time Spent on Assignments': 'Time_Spent_On_Assignments',
    'Program_Overall_Time_Spent_Hours': 'Program_Overall_Time_Spent_Hours'
}


# Iterate over the columns in Kalpana DataFrame
for col in df1.columns:
    # Check if the column name is in the new_names dictionary
    if col in new_names:
        # Rename the column using the corresponding new name from the dictionary
        df1.rename(columns={col: new_names[col]}, inplace=True)


# In[ ]:





# # Summary Date MySQL Table

# In[19]:


# Connecting to Database select database and password here
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')


# In[20]:


cursor =conn.cursor()


# In[21]:


# Get the existing columns in the database
cursor.execute("SHOW COLUMNS FROM kalpana.07_summary_overall_performance_monitoring")
existing_columns = [col[0] for col in cursor.fetchall()]

# Define the column name before which the new column should be added
target_column = 'Assignment_Completion_Average_Score'

# Check if any new columns exist in the dataframe but not in the database
new_columns = [col for col in df1.columns if col not in existing_columns]
if new_columns:
    # Add new columns to the database before the target column
    for col in reversed(new_columns):
        if col not in existing_columns:
            # Get the index of the target column
            target_column_index = existing_columns.index(target_column)
            # Set the data type based on whether the column name starts with Comment
            data_type = "INT" 
            alter_query = f"ALTER TABLE kalpana.07_summary_overall_performance_monitoring ADD COLUMN {col} {data_type} AFTER {existing_columns[target_column_index - 1]}"
            cursor.execute(alter_query)
            existing_columns.insert(target_column_index - 1, col)


# In[22]:


# Your existing code for inserting data into the database table
for i, row in df1.iterrows():
    row = [None if isinstance(val, float) and math.isnan(val) else val for val in row] # replace "nan" values with None
    columns = ','.join(df1.columns)
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.07_summary_overall_performance_monitoring ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    query += ", ".join([f"{col}=VALUES({col})" for col in df1.columns if col != 'Email'])
    # Execute the query
    cursor.execute(query, tuple(row))


# In[23]:


conn.commit()


# In[ ]:





# # Summary data in CSV

# In[24]:


# Checking columns name in table
df1.columns


# In[25]:


# Changing name of columns
df1.columns=[['Email', 'Recorded Videos- Percentage WatchTime per Week (%)',
       'SUK Sessions - Percentage WatchTime per Week (%)',
       'MasterClass - Percentage WatchTime per Week (%) ','Time Spent on Video content  (Hours)',
       'Program Percentage WatchTime per Week (%', 'Quiz Score Average Score (%)',
       'Assignment Completion Average Score (%)', 'Time Spent on quizzes  Quizzes',
       'Time Spent on Assignments',"Program_Overall_Time_Spent_Hours"]]


# In[26]:


# Rearranging columns of Final Report
cols=[['Email', 'Recorded Videos- Percentage WatchTime per Week (%)',
       'SUK Live - Percentage WatchTime per Week (%)',
       'SUK Sessions - Percentage WatchTime per Week (%)',
       'MasterClass - Percentage WatchTime per Week (%) ',
       'Program Percentage WatchTime per Week (%)','Time Spent on Video content  (Hours)' ,'Time Spent on quizzes  Quizzes',
       'Time Spent on Assignments' ,"Program_Overall_Time_Spent_Hours",'Quiz Score Average Score (%)',
       'Assignment Completion Average Score (%)']]
df1 = df1.reindex(cols, axis=1)


# In[27]:


# Filling Null values with zero and rounding off upto 2 decimal points
df1=df1.fillna(0)
df1=df1.round(2)


# In[28]:


df1.head()


# In[29]:


#df1.to_csv('summary_overall_performance_monitoring_Final.csv', mode='a',index=False)


# In[30]:


# Exporting final sheet to Output folder
df1.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\summary_overall_performance_monitoring.csv",mode='w',index=False)

