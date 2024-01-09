#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing Required modules
import pandas as pd
import mysql.connector as msql
import math
import os
import datetime


# In[2]:


# Displaying all columns and rows
pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)


# In[3]:


#Reading She for STEM Incubator file present on source files
directory_path =(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File\02_Source_Kalpana incubator")

csv_files = [file for file in os.listdir(directory_path) if file.endswith('.csv')]

for file in csv_files:
    file_path = os.path.join(directory_path, file)
    data = pd.read_csv(file_path)
    print(f"Data from {file}:")


# In[4]:


data.head()


# In[5]:


# Define a list of column names to extract for putting into database 
columns_to_extract = ['Name', 'Email', 'Segment', 'Mobile', 'Enroll Date', 'Assigned Through']

# Use the loc method to extract the specified columns
df = data.loc[:, columns_to_extract]


# In[6]:


# Create a new dataframe with 'email' column and columns that start with 'Week' 'Video', 'Recording', and 'Master class'
data = data[['Email'] + [col for col in data.columns if col.startswith(('Week','Video', 'SUK', 'Master Class'))]]


# In[7]:


# Cheaking shape of our dataset
data.shape


# In[8]:


# Initialize variables to keep track of the current week, video count, recording count, and master class count
week_col = None
video_count = 0
recording_count = 0
master_count = 0

# Create an empty list to store the new column names
new_cols = []

# Iterate over each column in the data
for col in data.columns:
    # If the column starts with 'Week'
    if col.startswith('Week'):
        # Split the column name by space and get the second element (the week number)
        week_col = col.split()[1]
        # Reset the video, recording, and master class counts for the new week
        video_count = 0
        recording_count = 0
    # If the column starts with 'Video'
    elif col.startswith('Video'):
        # Increment the video count for the current week
        video_count += 1
        # Append a new column name to the list using f-string formatting
        new_cols.append(f'WK{week_col}_V{video_count}')
    # If the column starts with 'Recording'
    elif col.startswith('SUK'):
        # Increment the recording count for the current week
        recording_count += 1
        # Append a new column name to the list using f-string formatting
        new_cols.append(f'WK{week_col}_SUK_V')
    # If the column starts with 'Master Class'
    elif col.startswith('Master Class'):
        # Increment the master class count for the current week
        master_count += 1
        # Append a new column name to the list using f-string formatting
        new_cols.append(f'WK{week_col}_Master{master_count}')
    # If the column doesn't start with any of the above prefixes
    else:
        # Append the original column name to the list
        new_cols.append(col)

# Remove all columns that start with 'Week' from the data
data = data.loc[:, ~data.columns.str.startswith('Week')]
# Assign the new column names to the data
data.columns = new_cols


# In[9]:


# Rechecking the shape of dataframe
data.shape


# In[10]:


# Cheaking columns name
data.columns


# In[11]:


# This condition happen beacause we dont have week number before it so it doesnt have week number 
if 'WKNone_SUK_V' in data.columns:
    data = data.rename(columns={'WKNone_SUK_V': 'WK0_SUK_V'})


# In[12]:


# Cheaking for duplicate data
data.duplicated().sum()


# In[13]:


# Locating columns for extracting only numbers
col1 = data.iloc[:,1:]


# In[14]:


# Code for extracting only numbers from dataset
for column in [i for i in col1.columns if col1[i].dtype == 'object']:
    data[column] = data[column].astype(str).str.extract('(\d+)').astype(float)


# In[15]:


# Filling Null values with zero
fillna = data.iloc[:,1:]
fillnacol=fillna.columns
data[fillnacol]=data[fillnacol].fillna(0)


# In[16]:


data.head()


# In[ ]:





# # Enroll Date MySQL Table

# In[17]:


# Creating new table which for Enroll_Dates which is taken from incubator graphy sheet
Enroll=pd.DataFrame(df[["Email", 'Enroll Date']])


# In[18]:


# Extracting only Enroll_Date
Enroll[['Enroll Date','Time']]=Enroll['Enroll Date'].str.split(' ',expand=True)
Enroll=Enroll.drop(["Time"],axis=1)


# In[19]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[20]:


# Inserting data to 03_enroll_date table
for i,row in Enroll.iterrows():
    cursor.execute("insert IGNORE into 03_enroll_date (Email,Incubator) values(%s,%s)",tuple(row))


# In[21]:


# Replacing data  with new data to 03_enroll_date table
for i,row in Enroll.iterrows():
    cursor.execute("REPLACE into 03_enroll_date (Email,incubator) values(%s,%s)",tuple(row))


# In[22]:


conn.commit()


# # Payment details MySQL table

# In[23]:


# Creating new table which for Payment which is taken from incubator graphy sheet
Payment=pd.DataFrame(df[["Email","Assigned Through"]])


# In[24]:


# Extracting only Payment removing Order Id
Payment[["Assigned Through","Order_ID"]]=Payment["Assigned Through"].str.split("-",expand=True)
Payment.drop(["Order_ID"],axis=1,inplace=True)


# In[25]:


# Assigning fee cost to different cathegory of enrollment
Payment["Assigned Through"]=Payment["Assigned Through"].replace(['Admin'],'0')
Payment["Assigned Through"]=Payment["Assigned Through"].replace(['Excel Upload'],'0')
Payment["Assigned Through"]=Payment["Assigned Through"].replace(['Paid Transaction '],'1899')


# In[26]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[27]:


# Inserting data into 02_payment_details 
for i,row in Payment.iterrows():
    cursor.execute("insert Ignore into 02_payment_details (Email,Incubator_Fee) values(%s,%s)",tuple(row))


# In[28]:


# Replacing data with new data into 02_payment_details 
for i,row in Payment.iterrows():
    cursor.execute("REPLACE into 02_payment_details (Email,Incubator_Fee) values(%s,%s)",tuple(row))


# In[29]:


conn.commit()


# # Changing seconds to Percentage

# In[30]:


# Reading Excel file form our source
excel_file  = pd.read_excel(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File\00_Source_Kalpana Video Details\Video Time Period.xlsx")


# In[31]:


data1=pd.DataFrame()


# In[32]:


# Iterate over each row in the Excel file
for index, row in excel_file.iterrows():
    # Get the value in the "Name" column 
    column_name = row['Name']
    
    # Get the value in the "Time" column 
    value = row['Time']
    
    # Calculate the percentage value by dividing the value in the corresponding column of the DataFrame by the "Time" value
    percentage_value = (data[column_name] * 100) / value
    
    # Assign the calculated percentage value to a new column in the "data1" DataFrame with the same name as the "Name" column
    data1[column_name] = percentage_value


# In[33]:


data1.head()


# In[34]:


# Creating a function "Max_Value" for converting % max 100 if values are above 100
def Max_Value(value):
    if value >=100:
        return 100
    else:
        return value


# In[35]:


# Applying Max_Value function
per1=data1.iloc[:,0:]
per1c=per1.columns
data1[per1c]=data1[per1c].applymap(Max_Value)


# # Code for converting seconds to hours

# In[36]:


# Creating a function for converting seconds to hours
def Convert_Hours(seconds):
    hours = seconds / (3600)
    return hours
n = 5400
print(Convert_Hours(n))


# In[37]:


# Applying Convert_Hours function
col1 = data.iloc[:,1:]
col=col1.columns
data[col]=data[col].apply(Convert_Hours)


# # Code for Recorded Videos: Total hours, Average & Percentage

# ## Input Weeks Here ↓ 

# In[38]:


# Define the week number up to which you want to include columns
end_week = 8


# In[39]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    for video in range(1, 10):
        col_name = f'WK{week}_V{video}'
        if col_name in data.columns:
            cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
Recorded_Total = data[cols_to_select]


# In[40]:


Recorded_Total.head()


# In[41]:


data["Recorded_Total"]=Recorded_Total.sum(axis=1)


# In[42]:


data["Recorded_Average"]=Recorded_Total.mean(axis=1)


# In[43]:


# Create a list of all column names up to the end week
cols_to_select_per = []
for week in range(end_week + 1):
    for video in range(1, 10):
        col_name = f'WK{week}_V{video}'
        if col_name in data1.columns:
            cols_to_select_per.append(col_name)

# Select the desired columns and create a new DataFrame
Recorded_Percent = data1[cols_to_select_per]


# In[44]:


Recorded_Percent.head()


# In[45]:


data["Recorded_Percentage"]=Recorded_Percent.mean(axis=1)


# # Code for SUK Recorded Videos: Total hours, Average & Percentage

# In[46]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    if col_name in data.columns:
        cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
SUK_Recorded_Total = data[cols_to_select]


# In[47]:


SUK_Recorded_Total.head()


# In[48]:


data["SUK_Recorded_Total"]=SUK_Recorded_Total.sum(axis=1)


# In[49]:


data["SUK_Recorded_Average"]=SUK_Recorded_Total.mean(axis=1)


# In[50]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    if col_name in data1.columns:
        cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
SUK_Recorded_Percent = data1[cols_to_select]


# In[51]:


data["SUK_Recorded_Percentage"]=SUK_Recorded_Percent.mean(axis=1)


# # Code for Master Class: Total hours, Average & Percentage

# In[52]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 2):
    for masterclass in range(1, 10):
        col_name = f'WK{week}_Master{masterclass}'
        if col_name in data.columns:
            cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
Masterclass_Total = data[cols_to_select]


# In[53]:


data["Masterclass_Total"]=Masterclass_Total.sum(axis=1)


# In[54]:


data["Masterclass_Average"]=Masterclass_Total.mean(axis=1)


# In[55]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    for masterclass in range(1, 10):
        col_name = f'WK{week}_Master{masterclass}'
        if col_name in data1.columns:
            cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
Masterclass_Percent = data1[cols_to_select]


# In[56]:


Masterclass_Percent.head()


# In[57]:


data["Masterclass_Percentage"]=Masterclass_Percent.mean(axis=1)


# # Code for Program: Total hours, Average & Percentage

# In[58]:


# select columns from data where the column names start with "WK"
selected_cols = [col for col in data.columns if col.startswith('WK')]

# create a new dataframe called Whole_Program_Total with the selected columns
Whole_Program_Total = data[selected_cols]


# In[59]:


Whole_Program_Total.head()


# In[60]:


data["Program_Total"]=Whole_Program_Total.sum(axis=1)


# In[61]:


data["Program_Average"]=Whole_Program_Total.mean(axis=1)


# In[62]:


# select columns from data where the column names start with "WK"
selected_cols = [col for col in data1.columns if col.startswith('WK')]

# create a new dataframe called Whole_Program_Total with the selected columns
Whole_Program_Percent = data1[selected_cols]


# In[63]:


data["Program_percentage"]=Whole_Program_Percent.mean(axis=1)


# In[64]:


data=data.round(2)


# # Storing data on MySQL

# In[65]:


merged = df.merge(data, on ='Email', how = 'inner') 


# In[66]:


merged = merged.drop('Enroll Date', axis=1)


# In[67]:


merged = merged.rename(columns={'Assigned Through': 'Assigned_Through'})
merged = merged.rename(columns={'Mobile': 'Phone'})


# In[68]:


conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor() 


# In[69]:


# Get the existing columns in the database
cursor.execute("SHOW COLUMNS FROM kalpana.08_incubator_and_attendence_monitoring")
existing_columns = [col[0] for col in cursor.fetchall()]

# Define the column name before which the new column should be added
target_column = 'Recorded_Total'

# Check if any new columns exist in the dataframe but not in the database
new_columns = [col for col in merged.columns if col not in existing_columns]
if new_columns:
    # Add new columns to the database before the target column
    for col in reversed(new_columns):
        if col not in existing_columns:
            # Get the index of the target column
            target_column_index = existing_columns.index(target_column)
            # Set the data type based on whether the column name starts with Comment
            data_type = "INT" 
            alter_query = f"ALTER TABLE kalpana.08_incubator_and_attendence_monitoring ADD COLUMN {col} {data_type} AFTER {existing_columns[target_column_index - 1]}"
            cursor.execute(alter_query)
            existing_columns.insert(target_column_index - 1, col)


# In[70]:


conn.commit()


# # Extracting desire output to excel sheet

# In[71]:


data.head()


# In[72]:


data.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Incubator_and_attendence_monitoring_WithoutNotation.csv",mode='w',index=False)


# In[ ]:





# In[ ]:




