#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing Required modules
import pandas as pd
import math
import os
import datetime
import mysql.connector as msql


# In[2]:


# Displaying all columns and rows
pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)


# In[3]:


# Define the directory path where the CSV files are located
directory_path =(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\02_Source_Kalpana incubator")

# Use os.listdir() to get a list of all files in the directory
# Filter the list to include only files ending with '.csv'
csv_files = [file for file in os.listdir(directory_path) if file.endswith('.csv')]

# Iterate through the CSV files
for file in csv_files:
    # Create the full file path by joining the directory path and file name
    file_path = os.path.join(directory_path, file)
    # Read the CSV file into a DataFrame
    data = pd.read_csv(file_path)
    # Print information about the data
    print(f"Data from {file}:")


# In[ ]:


# Display the first 10 rows
data.head()


# In[ ]:


# Dropping unwanted columns
data.drop(data.columns[[0, 6, 7, 9, 10, 11, 14, 17, 18, 19, 20, 22, 23, 25, 27, 29, 30, 31, 32, 33, 34, 36, 38, 40, 42, 43, 44, 45, 46, 48, 49, 50, 51, 53, 54, 56, 58, 60, 62, 63, 64, 65, 67, 68, 69, 71, 73, 75, 77, 79, 81, 82, 83, 84, 85, 86, 88, 89, 90, 92, 94, 96, 98, 100, 102, 104, 106, 108, 109, 110, 111, 113, 114, 115, 117, 119, 121, 122, 123, 124, 126, 127, 129, 131, 133, 135, 136, 137, 138, 139, 140, 142, 143, 144, 145, 147, 149, 150, 152, 154, 156, 157, 158, 160, 161, 162, 163, 164, 165, 166, 168, 169,171,172,173,174,176,177]],axis=1,inplace=True)


# In[ ]:


# Display the first 10 rows
data.head()


# In[ ]:


# Changing headers of columns
data.columns=['Last Login','Course Name','Name', 'Email', 'Segment', 'Mobile', 'Name of College', 'Enroll Date', 'Assigned Through','Start Date', 'WK0_SUK_V', 'WK0_V1', 'WK0_V2', 'WK0_Master1', 'WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4', 'WK1_SUK_V', 'WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4', 'WK2_V5', 'WK2_SUK_V', 'WK3_V1', 'WK3_V2', 'WK3_V3', 'WK3_V4', 'WK3_V5', 'WK3_V6', 'WK3_SUK_V', 'WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5', 'WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9', 'WK4_SUK_V', 'WK5_V1', 'WK5_V2', 'WK5_V3', 'WK5_SUK_V', 'WK6_V1', 'WK6_V2', 'WK6_V3', 'WK6_V4', 'WK6_Master2', 'WK7_V1', 'WK7_V2', 'WK7_V3', 'WK7_V4', 'WK7_V5', 'WK7_Master3', 'WK8_Master4', 'WK8_Master5','WK8_SUK_V']


# In[ ]:


# Count the number of duplicate rows
data.duplicated().sum()


# In[ ]:


# Locating columns for extracting only numbers
col1 = data.iloc[:,10:]


# In[ ]:


# Code for extracting only numbers from dataset
for column in [i for i in col1.columns if col1[i].dtype == 'object']:
    data[column] = data[column].astype(str).str.extract('(\d+)').astype(float)


# In[ ]:


#import libraries
import pandas as pd
import re

# Replace characters between two word characters with a space in the 'Name' column
data['Name'] = data['Name'].str.replace(r'(\w)[^\w\s](\w)', r'\1 \2')

# Create a new column 'Name_lower' with lowercase letters and cleaned text
data['Name_lower'] = data['Name'].str.lower().str.replace(r'[^a-z\s]', '').str.replace(r'\s+', ' ').str.replace('guest', '').str.replace('student', '').str.strip()


# In[ ]:


# Reading SUK Live sheets from SUK_Teams present on Kalpana Source Files
W0=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\SUK\W0 (1).csv")
w1=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\SUK\W1 (1).csv")
w2=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\SUK\W2 (1).csv")
w3=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\SUK\W3 (1).csv")
w4=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\SUK\W4 (1).csv")
w8=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\SUK\W8.csv")
#w6=pd.read_csv("C:\\Users\\Vigyanshaala\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Kalpana Source File\\SUK_Teams\\W6.csv")
#w7=pd.read_csv("C:\\Users\\Vigyanshaala\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Kalpana Source File\\SUK_Teams\\W7.csv")


# In[ ]:


# Adding SUK live data
data=pd.merge(data,W0,on='Name_lower',how='left')
data=pd.merge(data,w1,on='Name_lower',how='left')
data=pd.merge(data,w2,on='Name_lower',how='left')
data=pd.merge(data,w3,on='Name_lower',how='left')
data=pd.merge(data,w4,on='Name_lower',how='left')
data=pd.merge(data,w8,on='Name_lower',how='left')
#data=pd.merge(data,w6,on='Name_lower',how='left')
#data=pd.merge(data,w7,on='Name_lower',how='left')


# In[ ]:


# dropping the Name_lower column that was previously added for SUK_Live data adding key
data=data.drop("Name_lower",axis=1)


# In[ ]:


# Checking column location for filling Null values with zero
data.columns[21]


# In[ ]:


# Filling Null values with zero
fillna = data.iloc[:,10:]
fillnacol=fillna.columns
data[fillnacol]=data[fillnacol].fillna(0)


# In[ ]:


# Display the first 10 rows
data.head()


# # Enroll Date MySQL Table

# In[ ]:


# Creating new table which for Enroll_Dates which is taken from incubator graphy sheet
#Enroll=pd.DataFrame(data[["Email", 'Enroll Date','Course Name','Last Login','Start Date']])


# In[ ]:


# Creating new table which for Enroll_Dates which is taken from incubator graphy sheet
Enroll=pd.DataFrame(data[["Email", 'Enroll Date']])


# In[ ]:


# Extracting only Enroll_Date
Enroll[['Enroll Date','Time']]=Enroll['Enroll Date'].str.split(' ',expand=True)
Enroll=Enroll.drop(["Time"],axis=1)


# In[ ]:


# rename the 'Enroll Date' column to 'Incubator'
Enroll = Enroll.rename(columns={'Enroll Date': 'Incubator'})
#Enroll = Enroll.rename(columns={'Course Name': 'Batch'})
#Enroll = Enroll.rename(columns={'Last Login': 'Course_Last_Login'})
#Enroll = Enroll.rename(columns={'Start Date': 'Course_Start_Date'})


# In[ ]:


# Get the column names
Enroll.columns


# In[ ]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[ ]:


# Iterate over rows in the 'Enroll' DataFrame
for i, row in Enroll.iterrows():
    # Create a comma-separated list of column names
    columns = ','.join(Enroll.columns)
    # Create a comma-separated list of placeholders (%s) for values
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.03_enroll_date ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    # Add column-specific update clauses for columns other than 'Email'
    query += ", ".join([f"{col}=VALUES({col})" for col in Enroll.columns if col != 'Email'])
    # Execute the query with the values from the current row
    cursor.execute(query, tuple(row))


# In[ ]:


# Commit the changes to the database to make them permanent
conn.commit()


# # Batch Login StartDate

# In[ ]:


# Creating new table which for Batch_Login which is taken from incubator graphy sheet
Batch = pd.DataFrame(data[["Email", 'Course Name','Last Login','Start Date']])


# In[ ]:


# Extracting only Last_Login
Batch[['Last Login','Time']]=Batch['Last Login'].str.split(' ',expand=True)
Batch=Batch.drop(["Time"],axis=1)


# In[ ]:


# Renaming in the DataFrame 'Batch'
Batch = Batch.rename(columns={'Course Name': 'Batch'})
Batch = Batch.rename(columns={'Last Login': 'Course_last_login'})
Batch = Batch.rename(columns={'Start Date': 'Course_start_date'})


# In[ ]:


# Connecting to MySQL change database and password here
conn= msql.connect(host='localhost',user='root',password="VS@123",database="Kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[ ]:


# Iterate over rows in the 'Batch' DataFrame
for i, row in Batch.iterrows():
    # Create a comma-separated list of column names
    columns = ','.join(Batch.columns)
    # Create a comma-separated list of placeholders (%s) for values
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.16_batch_lastlogin_startdate({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    query += ", ".join([f"{col}=VALUES({col})" for col in Batch.columns if col != 'Email'])
    # Execute the query
    cursor.execute(query, tuple(row))


# In[ ]:


# Commit the changes to the database to make them permanent
conn.commit()


# # Payment details MySQL table

# In[ ]:


# Creating new table which for Payment which is taken from incubator graphy sheet
Payment=pd.DataFrame(data[["Email","Assigned Through"]])


# In[ ]:


# Extracting only Payment removing Order Id
Payment[["Assigned Through","Order_ID"]]=Payment["Assigned Through"].str.split("-",expand=True)
Payment.drop(["Order_ID"],axis=1,inplace=True)


# In[ ]:


# Assigning fee cost to different cathegory of enrollment
Payment["Assigned Through"]=Payment["Assigned Through"].replace(['Admin'],'0')
Payment["Assigned Through"]=Payment["Assigned Through"].replace(['Excel Upload'],'0')
Payment["Assigned Through"]=Payment["Assigned Through"].replace(['Paid Transaction '],'1899')


# In[ ]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[ ]:


# Inserting data into 02_payment_details 
for i,row in Payment.iterrows():
    cursor.execute("insert Ignore into 02_payment_details (Email,Incubator_Fee) values(%s,%s)",tuple(row))


# In[ ]:


# Replacing data with new data into 02_payment_details 
for i,row in Payment.iterrows():
    cursor.execute("REPLACE into 02_payment_details (Email,Incubator_Fee) values(%s,%s)",tuple(row))


# In[ ]:


# Commit the changes to the database to make them permanent
conn.commit()


# # Changing seconds to Percentage

# In[ ]:


# Create an empty DataFrame named 'data1'
data1=pd.DataFrame()


# In[ ]:


# Create a new column 'WK0_SUK_V' in the DataFrame 'data1' by performing a calculation
# The new column is calculated as a transformation of the 'WK0_SUK_V' column in 'data'
# The original values in 'WK0_SUK_V' are multiplied by 100 and divided by 3600
data1['WK0_SUK_V'] = (data['WK0_SUK_V']*100)/3600


# In[ ]:


data1["WK0_SUK_Live"] = (data["WK0_SUK_Live"]*100)/3600


# In[ ]:


data1['WK0_V1'] = (data['WK0_V1']*100)/291


# In[ ]:


data1["WK0_V2"]= (data["WK0_V2"])*100/287


# In[ ]:


data1["WK0_Master1"] = (data["WK0_Master1"]*100)/3600


# In[ ]:


data1["WK1_V1"] = (data["WK1_V1"]*100)/141


# In[ ]:


data1["WK1_V2"] = (data["WK1_V2"]*100)/453


# In[ ]:


data1["WK1_V3"] = (data["WK1_V3"]*100)/347


# In[ ]:


data1["WK1_V4"] = (data["WK1_V4"]*100)/441


# In[ ]:


data1["WK1_SUK_V"] = (data["WK1_SUK_V"]*100)/3600


# In[ ]:


data1["WK1_SUK_Live"] = (data["WK1_SUK_Live"]*100)/3600


# In[ ]:


data1["WK2_V1"] = (data["WK2_V1"]*100)/345


# In[ ]:


data1["WK2_V2"] = (data["WK2_V2"]*100)/239


# In[ ]:


data1["WK2_V3"] = (data["WK2_V3"]*100)/290


# In[ ]:


data1["WK2_V4"] = (data["WK2_V4"]*100)/296


# In[ ]:


data1["WK2_V5"] = (data["WK2_V5"]*100)/291


# In[ ]:


data1["WK2_SUK_V"] = (data["WK2_SUK_V"]*100)/3600


# In[ ]:


data1["WK2_SUK_Live"] = (data["WK2_SUK_Live"]*100)/3600


# In[ ]:


data1["WK3_V1"] = (data["WK3_V1"]*100)/428


# In[ ]:


data1["WK3_V2"] = (data["WK3_V2"]*100)/650


# In[ ]:


data1["WK3_V3"] = (data["WK3_V3"]*100)/309


# In[ ]:


data1["WK3_V4"] = (data["WK3_V4"]*100)/463


# In[ ]:


data1["WK3_V5"] = (data["WK3_V5"]*100)/560


# In[ ]:


data1["WK3_V6"] = (data["WK3_V6"]*100)/496


# In[ ]:


data1["WK3_SUK_V"] = (data["WK3_SUK_V"]*100)/3600


# In[ ]:


data1["WK3_SUK_Live"] = (data["WK3_SUK_Live"]*100)/3600


# In[ ]:


data1["WK4_V1"] = (data["WK4_V1"]*100)/286


# In[ ]:


data1["WK4_V2"] = (data["WK4_V2"]*100)/265


# In[ ]:


data1["WK4_V3"] = (data["WK4_V3"]*100)/321


# In[ ]:


data1["WK4_V4"] = (data["WK4_V4"]*100)/120


# In[ ]:


data1["WK4_V5"] = (data["WK4_V5"]*100)/154


# In[ ]:


data1["WK4_V6"] = (data["WK4_V6"]*100)/115


# In[ ]:


data1["WK4_V7"] = (data["WK4_V7"]*100)/154


# In[ ]:


data1["WK4_V8"] = (data["WK4_V8"]*100)/124


# In[ ]:


data1["WK4_V9"] = (data["WK4_V9"]*100)/203


# In[ ]:


data1["WK4_SUK_V"] = (data["WK4_SUK_V"]*100)/3600


# In[ ]:


data1["WK4_SUK_Live"] = (data["WK4_SUK_Live"]*100)/3600


# In[ ]:


data1["WK5_V1"] = (data["WK5_V1"]*100)/114


# In[ ]:


data1["WK5_V2"] = (data["WK5_V2"]*100)/327


# In[ ]:


data1["WK5_V3"] = (data["WK5_V3"]*100)/131


# In[ ]:


data1["WK5_SUK_V"] = (data["WK5_SUK_V"]*100)/3600


# In[ ]:


data1["WK6_V1"] = (data["WK6_V1"]*100)/259


# In[ ]:


data1["WK6_V2"] = (data["WK6_V2"]*100)/432


# In[ ]:


data1["WK6_V3"] = (data["WK6_V3"]*100)/261


# In[ ]:


data1["WK6_V4"] = (data["WK6_V4"]*100)/515


# In[ ]:


data1["WK6_Master2"] = (data["WK6_Master2"]*100)/3600


# In[ ]:


data1["WK7_V1"] = (data["WK7_V1"]*100)/228


# In[ ]:


data1["WK7_V2"] = (data["WK7_V2"]*100)/214


# In[ ]:


data1["WK7_V3"] = (data["WK7_V3"]*100)/110


# In[ ]:


data1["WK7_V4"] = (data["WK7_V4"]*100)/170


# In[ ]:


data1["WK7_V5"] = (data["WK7_V5"]*100)/561


# In[ ]:


data1["WK7_Master3"] = (data["WK7_Master3"]*100)/3600


# In[ ]:


data1["WK8_Master4"] = (data["WK8_Master4"]*100)/3600


# In[ ]:


data1["WK8_Master5"] = (data["WK8_Master5"]*100)/3600


# In[ ]:


data1["WK8_SUK_V"] = (data["WK8_SUK_V"]*100)/3600


# In[ ]:


# Creating a function "Max_Value" for converting % max 100 if values are above 100
def Max_Value(value):
    if value >=100:
        return 100
    else:
        return value


# In[ ]:


# Applying Max_Value function
per1=data1.iloc[:,0:]
per1c=per1.columns
data1[per1c]=data1[per1c].applymap(Max_Value)


# # Code for converting seconds to hours

# In[ ]:


def Convert_Hours(value):
    try:
        # Convert the value to a float or integer if possible
        hours = float(value)
        return hours
    except ValueError:
        # Handle the case where the value cannot be converted
        return 0  # or any other appropriate default value

# Apply the Convert_Hours function to the columns in the DataFrame
col1 = data.iloc[:, 7:]
col = col1.columns
data[col] = data[col].applymap(Convert_Hours)


# In[ ]:


# Creating a function for converting seconds to hours
def Convert_Hours(seconds):
    hours = seconds / (3600)
    return hours
n = 5400
print(Convert_Hours(n))


# In[ ]:


# Get the column names starting from index 7
col = data.columns[7:] 
data[col] = data[col].applymap(Convert_Hours)


# In[ ]:


# Append the data from DataFrame 'data1' to the CSV file 'data299.csv' without including the index
data1.to_csv('data299.csv', mode='a',index=False)


# # Code for Recorded Videos: Total hours, Average & Percentage

# ## Input Weeks Here ↓ 

# In[ ]:


# Define the week number up to which you want to include columns
end_week = 8


# In[ ]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    for video in range(1, 10):
        col_name = f'WK{week}_V{video}'
        if col_name in data.columns:
            cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
Recorded_Total = data[cols_to_select]


# In[ ]:


# Create a new column 'Recorded_Total' in 'data' by summing rows in 'Recorded_Total' along the columns
data["Recorded_Total"]=Recorded_Total.sum(axis=1)


# In[ ]:


# Create a new column 'Recorded_Average' in 'data' by calculating the mean of rows in 'Recorded_Total' along the columns
data["Recorded_Average"]=Recorded_Total.mean(axis=1)


# In[ ]:


# Create a list of all column names up to the end week
cols_to_select_per = []
for week in range(end_week + 1):
    for video in range(1, 10):
        col_name = f'WK{week}_V{video}'
        if col_name in data1.columns:
            cols_to_select_per.append(col_name)

# Select the desired columns and create a new DataFrame
Recorded_Percent = data1[cols_to_select_per]


# In[ ]:


# Create a new column 'Recorded_Percentage' in 'data' by calculating the mean of rows in 'Recorded_Percent' along the columns
data["Recorded_Percentage"]=Recorded_Percent.mean(axis=1)


# # Code for SUK Live: Total hours, Average & Percentage

# In[ ]:


# Create a new DataFrame 'SUK_Live' by selecting specific columns from 'data'
SUK_Live=data[['WK0_SUK_Live','WK1_SUK_Live', 'WK2_SUK_Live', 'WK3_SUK_Live', 'WK4_SUK_Live']]


# In[ ]:


# Create a new column 'SUK_Live_Total' in 'data' by summing rows in 'SUK_Live' along the columns
data["SUK_Live_Total"]=SUK_Live.sum(axis=1)


# In[ ]:


# Create a new column 'SUK_Live_Average' in 'data' by calculating the mean of rows in 'SUK_Live' along the columns
data["SUK_Live_Average"]=SUK_Live.mean(axis=1)


# In[ ]:


# Create a new DataFrame 'SUK_Live_Percent' by selecting specific columns from 'data1'
SUK_Live_Percent=data1[[ 'WK0_SUK_Live', 'WK1_SUK_Live', 'WK2_SUK_Live', 'WK3_SUK_Live', 'WK4_SUK_Live']]


# In[ ]:


# Create a new column 'SUK_Live_Percentage' in 'data' by calculating the mean of rows in 'SUK_Live_Percent' along the columns
data["SUK_Live_Percentage"]=SUK_Live_Percent.mean(axis=1)


# In[ ]:


#SUK_MAX_Percent=data[[ 'WK0_SUK', 'WK1_SUK', 'WK2_SUK', 'WK3_SUK', 'WK4_SUK']]


# In[ ]:


#data["SUK_Max_Percentage"]=SUK_MAX_Percent.mean(axis=1)


# # Code for SUK Recorded Videos: Total hours, Average & Percentage

# In[ ]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    if col_name in data.columns:
        cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
SUK_Recorded_Total = data[cols_to_select]


# In[ ]:


# Create a new column 'SUK_Recorded_Total' in 'data' by summing rows in 'SUK_Recorded_Total' along the columns
data["SUK_Recorded_Total"]=SUK_Recorded_Total.sum(axis=1)


# In[ ]:


# Create a new column 'SUK_Recorded_Average' in 'data' by calculating the mean of rows in 'SUK_Recorded_Total' along the columns
data["SUK_Recorded_Average"]=SUK_Recorded_Total.mean(axis=1)


# In[ ]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    if col_name in data1.columns:
        cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
SUK_Recorded_Percent = data1[cols_to_select]


# In[ ]:


# Create a new column 'SUK_Recorded_Percentage' in 'data' by calculating the mean of rows in 'SUK_Recorded_Percent' along the columns
data["SUK_Recorded_Percentage"]=SUK_Recorded_Percent.mean(axis=1)


# # Code for Master Class: Total hours, Average & Percentage

# In[ ]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    for masterclass in range(1, 10):
        col_name = f'WK{week}_Master{masterclass}'
        if col_name in data.columns:
            cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
Masterclass_Total = data[cols_to_select]


# In[ ]:


# Calculating the Sum
data["Masterclass_Total"]=Masterclass_Total.sum(axis=1)


# In[ ]:


# Calculating the Mean
data["Masterclass_Average"]=Masterclass_Total.mean(axis=1)


# In[ ]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    for masterclass in range(1, 10):
        col_name = f'WK{week}_Master{masterclass}'
        if col_name in data1.columns:
            cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
Masterclass_Percent = data1[cols_to_select]


# In[ ]:


# Calculating the mean
data["Masterclass_Percentage"]=Masterclass_Percent.mean(axis=1)


# # Code for Program: Total hours, Average & Percentage

# In[ ]:


# Create a new DataFrame 'Whole_Program_Total' by selecting specific columns from 'data'
Whole_Program_Total=data[['WK0_SUK_V', 'WK0_V1', 'WK0_V2', 'WK0_Master1', 
                    'WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4', 'WK1_SUK_V', 
                     'WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4', 'WK2_V5', 'WK2_SUK_V', 
                     'WK3_V1', 'WK3_V2', 'WK3_V3', 'WK3_V4', 'WK3_V5', 'WK3_V6', 'WK3_SUK_V', 
                     'WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5', 'WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9', 'WK4_SUK_V', 
                    'WK5_V1', 'WK5_V2', 'WK5_V3', 'WK5_SUK_V',
                     'WK6_V1', 'WK6_V2', 'WK6_V3', 'WK6_V4', 'WK6_Master2', 
                     'WK7_V1', 'WK7_V2', 'WK7_V3', 'WK7_V4', 'WK7_V5', 'WK7_Master3', 
                     'WK8_Master4', 'WK8_Master5','WK8_SUK_V','WK0_SUK_Live','WK1_SUK_Live', 'WK2_SUK_Live', 'WK3_SUK_Live', 'WK4_SUK_Live']]


# In[ ]:


# Calculating the sum
data["Program_Total"]=Whole_Program_Total.sum(axis=1)


# In[ ]:


# Create a new DataFrame 'Whole_Program_Average' by selecting specific columns from 'data'
Whole_Program_Average=data[['WK0_SUK_V', 'WK0_V1', 'WK0_V2', 'WK0_Master1', 
                    'WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4', 'WK1_SUK_V', 
                     'WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4', 'WK2_V5', 'WK2_SUK_V', 
                     'WK3_V1', 'WK3_V2', 'WK3_V3', 'WK3_V4', 'WK3_V5', 'WK3_V6', 'WK3_SUK_V', 
                     'WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5', 'WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9', 'WK4_SUK_V', 
                    'WK5_V1', 'WK5_V2', 'WK5_V3', 'WK5_SUK_V',
                     'WK6_V1', 'WK6_V2', 'WK6_V3', 'WK6_V4', 'WK6_Master2', 
                     'WK7_V1', 'WK7_V2', 'WK7_V3', 'WK7_V4', 'WK7_V5', 'WK7_Master3', 
                     'WK8_Master4', 'WK8_Master5','WK8_SUK_V','WK0_SUK_Live','WK1_SUK_Live', 'WK2_SUK_Live', 'WK3_SUK_Live', 'WK4_SUK_Live']]


# In[ ]:


# Calculating the mean
data["Program_Average"]=Whole_Program_Average.mean(axis=1)


# In[ ]:


# Create a new DataFrame 'Whole_Program_Percent' by selecting specific columns from 'data'
Whole_Program_Percent=data1[['WK0_SUK_V', 'WK0_V1', 'WK0_V2', 'WK0_Master1', 
                    'WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4', 'WK1_SUK_V', 
                     'WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4', 'WK2_V5', 'WK2_SUK_V', 
                     'WK3_V1', 'WK3_V2', 'WK3_V3', 'WK3_V4', 'WK3_V5', 'WK3_V6', 'WK3_SUK_V', 
                     'WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5', 'WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9', 'WK4_SUK_V', 
                    'WK5_V1', 'WK5_V2', 'WK5_V3', 'WK5_SUK_V',
                     'WK6_V1', 'WK6_V2', 'WK6_V3', 'WK6_V4', 'WK6_Master2', 
                     'WK7_V1', 'WK7_V2', 'WK7_V3', 'WK7_V4', 'WK7_V5', 'WK7_Master3', 
                     'WK8_Master4', 'WK8_Master5','WK8_SUK_V','WK0_SUK_Live','WK1_SUK_Live', 'WK2_SUK_Live', 'WK3_SUK_Live', 'WK4_SUK_Live']]


# In[ ]:


# Calculating the mean
data["Program_percentage"]=Whole_Program_Percent.mean(axis=1)


# In[ ]:


# Round all the values in the DataFrame 'data' to two decimal places
data=data.round(2)


# # Storing data on MySQL

# In[ ]:


# print rows
data.head() 


# In[ ]:


# print columns
data.columns


# In[ ]:


# Remove the 'Enroll Date' column from the DataFrame 'data'
data = data.drop('Enroll Date', axis=1)


# In[ ]:


# Rename columns in the DataFrame 'data' 
data = data.rename(columns={'Name of College': 'Name_of_College'})
data = data.rename(columns={'Assigned Through': 'Assigned_Through'})
data = data.rename(columns={'Mobile': 'Phone'})


# In[ ]:


# Establish a connection to a MySQL database using specified connection parameters
conn= msql.connect(host='localhost',user='root',password="VS@123",database="Kalpana",auth_plugin='mysql_native_password')


# In[ ]:


# Create a cursor object to interact with the MySQL database through the established connection
cursor =conn.cursor()


# In[ ]:


# Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
query = f"INSERT INTO kalpana.08_incubator_and_attendence_monitoring ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
query += ", ".join([f"`{col}`=VALUES(`{col}`)" for col in data.columns if col != 'Email'])
   


# In[ ]:


# Commit the changes made through the database
conn.commit()


# # extracing desire output to excel sheet

# In[ ]:


#  Selecting desired columns
Kalpana=data[['Email','WK0_SUK_V', 'WK0_SUK_Live','WK0_V1', 'WK0_V2', 'WK0_Master1', 
                    'WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4', 'WK1_SUK_V', 'WK1_SUK_Live', 
                     'WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4', 'WK2_V5', 'WK2_SUK_V','WK2_SUK_Live',
                     'WK3_V1', 'WK3_V2', 'WK3_V3', 'WK3_V4', 'WK3_V5', 'WK3_V6', 'WK3_SUK_V', 'WK3_SUK_Live',
                     'WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5', 'WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9', 'WK4_SUK_V', 'WK4_SUK_Live',
                    'WK5_V1', 'WK5_V2', 'WK5_V3', 'WK5_SUK_V', 
                     'WK6_V1', 'WK6_V2', 'WK6_V3', 'WK6_V4', 'WK6_Master2', 
                     'WK7_V1', 'WK7_V2', 'WK7_V3', 'WK7_V4', 'WK7_V5', 'WK7_Master3', 
                     'WK8_Master4', 'WK8_Master5','WK8_SUK_V','WK8_SUK_Live','Recorded_Total','Recorded_Average','Recorded_Percentage','SUK_Live_Total',"SUK_Live_Average",'SUK_Live_Percentage',
        "SUK_Recorded_Total" ,"SUK_Recorded_Average",'SUK_Recorded_Percentage',
        'Masterclass_Total',"Masterclass_Average",'Masterclass_Percentage',
        'Program_Total','Program_Average' ,'Program_percentage']]


# In[ ]:


# Export the 'Kalpana' DataFrame to a CSV file named 'Incubator_and_attendance_monitoring.csv'
# 'mode='w'' overwrites the file if it already exists (creates a new file if not)
# 'index=False' ensures that the DataFrame's index is not written to the CSV file
Kalpana.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Output\Incubator_and_attendence_monitoring.csv",mode='w',index=False)

