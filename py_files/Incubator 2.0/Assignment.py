#!/usr/bin/env python
# coding: utf-8

# In[18]:


# importing required modules
import pandas as pd
import mysql.connector as msql
import os


# # Source file should be named as "Incubator Assignment Review"

# In[19]:


# Define the directory path containing Excel files
directory_path = (r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\04_Source_Kalpana Assignment Review")

# Get a list of Excel files with the '.xlsx' extension in the directory
csv_files = [file for file in os.listdir(directory_path) if file.endswith('.xlsx')]

# Iterate through the list of Excel files
for file in csv_files:
    # Create the full file path by joining the directory path and file name
    file_path = os.path.join(directory_path, file)
    
    # Read the Excel file into the 'df' DataFrame, specifying the sheet name 'Assignment Review'
    df = pd.read_excel(file_path, sheet_name='Assignment Review')
    
    # Print a message indicating the data source
    print(f"Data from {file}:")


# In[20]:


# Dispalying All columns
pd.set_option('display.max_columns',None)
pd.options.display.float_format='{:.0f}'.format


# In[21]:


#Selecting desired columns
df=df.iloc[:,:33]


# In[22]:


# Dropping columns containing 'Current Status' in their column name
cols_to_drop = [col for col in df.columns if 'Current Status' in col]
df.drop(cols_to_drop, axis=1, inplace=True)


# In[23]:


# Assigning the different values of score
assign=['MC_Career Exploration','WK_1_SWOT Analysis','WK_2_RIASEC','WK_2_SMART goal','MC_Planning Masters','WK_3_Career Action Plan ','MC_LinkedIn Profile ','MC_CV/Resume ','WK_6_Critical thinking','WK_7_Problem solving']
df[assign] = df[assign].replace(['Accepted'],100)
df[assign] = df[assign].replace(['Submit 1'],30)
df[assign] = df[assign].replace(['Review 1'],30)
df[assign] = df[assign].replace(['Submit 2'],80)
df[assign] = df[assign].replace(['Rejected '],80)


# In[24]:


# Filling NaN Values with zero
df[assign]=df[assign].fillna(0)


# In[25]:


# Calculating the average score
df['Assignment_Score']=df[['MC_Career Exploration','WK_1_SWOT Analysis','WK_2_RIASEC','WK_2_SMART goal','MC_Planning Masters','WK_3_Career Action Plan ','MC_LinkedIn Profile ','MC_CV/Resume ','WK_6_Critical thinking']].mean(axis=1)


# In[26]:


# Changing the headers of dataset
df.columns=['Email', 'Name ','Category','MC_Career_Exploration', 'Comments_9', 'WK_1_SWOT_Analysis', 'Comments',
       'WK_2_RIASEC', 'Comments_1', 'WK_2_SMART_goal', 'Comments_2',
       'MC_Planning_Masters', 'Comments_3', 'WK_3_Career_Action_Plan ',
       'Comments_4', 'MC_LinkedIn_Profile ', 'Comments_5', 'MC_CV_Resume ',
       'Comments_6', 'WK_6_Critical_thinking', 'Comments_7',
       'WK_7_Problem_solving', 'Comments_8', 'Assignment_Score']


# In[27]:


df = df.drop('Category', axis=1)


# In[28]:


# Rounding Off values of Assignment score to two digits
df['Assignment_Score']=df['Assignment_Score'].round(2)


# In[29]:


# Check for and count the number of null (missing) values in the 'Assignment_Score' column of the DataFrame 'df'
df['Assignment_Score'].isnull().sum()


# In[30]:


# Fill all the null (missing) values in the DataFrame 'df' with empty strings ('')
df = df.fillna('')


# In[31]:


# Export the 'df' DataFrame to a CSV file named 'Assignment_Review.csv'
df.to_csv('Assignment_Review.csv', mode='a',index=False)


# In[32]:


# Exporting the dataset to output folder
df.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Output\Assignment_Review.csv",mode='w',index=False)


# # Storing data on MySQL

# In[33]:


# Connecting MySQL Kalpana Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[34]:


for i, row in df.iterrows():
    columns = ','.join(df.columns)
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.11_incubator_assignment_monitoring ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    query += ", ".join([f"{col}=VALUES({col})" for col in df.columns if col != 'Email'])
    # Execute the query
    cursor.execute(query, tuple(row))


# In[35]:


conn.commit()


# In[ ]:




