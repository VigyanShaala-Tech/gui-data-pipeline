#!/usr/bin/env python
# coding: utf-8

# In[84]:


# Importing Required modules
import pandas as pd
import mysql.connector as msql
import math
import os
import datetime


# In[85]:


# Displaying all columns and rows
pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)


# In[86]:


#directory_path =(r"C:\Users\Lenovo\OneDrive - VigyanShaala\02 Products  Initiatives-LAPTOP-D2TFS89H\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File\Incubator_and_attendence_monitoring")
directory_path =(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File\02_Source_Kalpana incubator")

csv_files = [file for file in os.listdir(directory_path) if file.endswith('.csv')]

for file in csv_files:
    file_path = os.path.join(directory_path, file)
    data = pd.read_csv(file_path)
    print(f"Data from {file}:")


# In[87]:


# Use the loc method to extract the specified columns
data2 = data.loc[:]


# In[88]:


# Define a list of column names to extract for putting into database 
columns_to_extract = ['Name', 'Email', 'Segment', 'Mobile', 'Enroll Date', 'Assigned Through']

# Use the loc method to extract the specified columns
df = data.loc[:, columns_to_extract]


# In[89]:


# Create a new dataframe with 'email' column and columns that start with 'Video', 'SUK', and 'Master class'
data = data[['Email'] + [col for col in data.columns if col.startswith(('Video', 'SUK', 'Master Class'))]]


# In[90]:


data.shape


# In[91]:


# Define a dictionary to map special characters to their respective numbers
special_chars = {'!': '1', '@': '2', '#': '3', '$': '4', '%': '5', '^': '6', '&': '7', '*': '8', '?': '9', '+': '0'}


# In[92]:


# Define a function to convert column names 
def convert_column_name(column_name):
    try:
        if column_name.startswith('SUK'):
            # Get the special character from the column name
            special_char = column_name.split(':')[0].split(' ')[1].strip()
            # Get the corresponding number from the dictionary
            number = special_chars[special_char]
            # Return the converted column name
            return f'WK{number}_SUK_V'
        else:
            return column_name
    # If We have any Index or Key Error it will ignore the column    
    except IndexError:
        return column_name
    except KeyError:
        return column_name

# Renaming columns with new name
for column in data.columns:
    new_column_name = convert_column_name(column)
    data.rename(columns={column: new_column_name}, inplace=True)


# In[93]:


# Define a function to convert column names 
def convert_column_name(column_name):
    try:
        if column_name.startswith('Video'):
            # Get the special character from the column name
            special_char = column_name.split(':')[0].split(' ')[1].strip()
            # Get the corresponding number from the dictionary
            number = ''.join([special_chars[char] for char in special_char])
        # Return the converted column name
            return 'WK{}_V{}'.format(number[0], number[1:3])
        else:
            return column_name
    # If We have any Index or Key Error it will ignore the column    
    except IndexError:
        return column_name
    except KeyError:
        return column_name

# Renaming columns with new name
for column in data.columns:
    new_column_name = convert_column_name(column)
    data.rename(columns={column: new_column_name}, inplace=True)


# In[94]:


### Define a function to convert column names
def convert_column_name(column_name):
    try:
        if column_name.startswith('Master'):
            # Get the special character from the column name
            split_name = column_name.split(':')
            special_char = split_name[0].split(' ')[2].strip()
            # Get the corresponding number from the dictionary
            number = ''.join([special_chars[char] for char in special_char])
            # Return the converted column name
            return 'WK{}_Master{}'.format(number[0], number[1:3])
        else:
            return column_name
    # If We have any Index or Key Error it will ignore the column    
    except IndexError:
        return column_name
    except KeyError:
        return column_name

# Renaming columns with new name
for column in data.columns:
    new_column_name = convert_column_name(column)
    data.rename(columns={column: new_column_name}, inplace=True)


# In[95]:


# Getting the list of columns who have wrong name
cols = [col for col in data.columns if not col.startswith('WK') and not col.startswith('Email')]
print(cols)


# If there are columns in above list than we should check for number of notation we are giving,is there extra space is given or not,etc. If we have anything above in list than we will get error in below code.

# In[96]:


# Sort the column names in chronological order
sorted_columns = sorted(data.columns[1:], key=lambda x: (int(x.split('_')[0][2:]), 'Master' in x, 'SUK' in x, x))

# Generate new column names in the specified format
new_columns = ['Email'] + sorted_columns

# Update the column names of the DataFrame
data.columns = new_columns


# In[97]:


# Dropping duplicates from data
data = data.drop_duplicates()


# In[98]:


data.columns


# In[99]:


# Locating columns for extracting only numbers
col1 = data.iloc[:,1:]


# In[100]:


# Code for extracting only numbers from dataset
for column in [i for i in col1.columns if col1[i].dtype == 'object']:
    data[column] = data[column].astype(str).str.extract('(\d+)').astype(float)


# In[101]:


# Filling Null values with zero
fillna = data.iloc[:,1:]
fillnacol=fillna.columns
data[fillnacol]=data[fillnacol].fillna(0)


# In[102]:


data.head()


# In[103]:


# Strip whitespace characters from the right end of column names
data.columns = data.columns.str.rstrip()


# # Enroll Date MySQL Table

# In[104]:


# Creating new table which for Enroll_Dates which is taken from incubator graphy sheet
Enroll=pd.DataFrame(df[["Email", 'Enroll Date']])


# In[105]:


# Extracting only Enroll_Date
Enroll[['Enroll Date','Time']]=Enroll['Enroll Date'].str.split(' ',expand=True)
Enroll=Enroll.drop(["Time"],axis=1)


# In[106]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[107]:


# Inserting data to 03_enroll_date table
for i,row in Enroll.iterrows():
    cursor.execute("insert IGNORE into 03_enroll_date (Email,Incubator) values(%s,%s)",tuple(row))


# In[108]:


# Replacing data  with new data to 03_enroll_date table
for i,row in Enroll.iterrows():
    cursor.execute("REPLACE into 03_enroll_date (Email,incubator) values(%s,%s)",tuple(row))


# In[109]:


conn.commit()


# # Payment details MySQL table

# In[110]:


# Creating new table which for Payment which is taken from incubator graphy sheet
Payment=pd.DataFrame(data2[["Email","Assigned Through"]])


# In[111]:


# Extracting only Payment removing Order Id
Payment[["Assigned Through","Order_ID"]]=Payment["Assigned Through"].str.split("-",expand=True)
Payment.drop(["Order_ID"],axis=1,inplace=True)


# In[112]:


# Assigning fee cost to different cathegory of enrollment
Payment["Assigned Through"]=Payment["Assigned Through"].replace(['Admin'],'0')
Payment["Assigned Through"]=Payment["Assigned Through"].replace(['Excel Upload'],'0')
Payment["Assigned Through"]=Payment["Assigned Through"].replace(['Paid Transaction '],'1899')


# In[113]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[114]:


# Inserting data into 02_payment_details 
for i,row in Payment.iterrows():
    cursor.execute("insert Ignore into 02_payment_details (Email,Incubator_Fee) values(%s,%s)",tuple(row))


# In[115]:


# Replacing data with new data into 02_payment_details 
for i,row in Payment.iterrows():
    cursor.execute("REPLACE into 02_payment_details (Email,Incubator_Fee) values(%s,%s)",tuple(row))


# In[116]:


conn.commit()


# # Changing seconds to Percentage

# In[117]:


# Reading Excel file form our source
excel_file  = pd.read_excel(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File\00_Source_Kalpana Video Details\Video Time Period.xlsx")


# In[118]:


excel_file.head()


# In[119]:


# Understanding the datatype of files
excel_file.dtypes


# In[120]:


# Creating empty dataframe to store data
data1=pd.DataFrame()


# In[121]:


data.head()


# In[122]:


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


# In[123]:


data1.head()


# In[124]:


# Creating a function "Max_Value" for converting % max 100 if values are above 100
def Max_Value(value):
    if value >=100:
        return 100
    else:
        return value


# In[125]:


# Applying Max_Value function
per1=data1.iloc[:,0:]
per1c=per1.columns
data1[per1c]=data1[per1c].applymap(Max_Value)


# # Code for converting seconds to hours

# In[126]:


# Creating a function for converting seconds to hours
def Convert_Hours(seconds):
    hours = seconds / (3600)
    return hours
n = 5400
print(Convert_Hours(n))


# In[127]:


# Applying Convert_Hours function
col1 = data.iloc[:,1:]
col=col1.columns
data[col]=data[col].apply(Convert_Hours)


# # Code for Recorded Videos: Total hours, Average & Percentage

# ## Input Weeks Here ↓ 

# In[128]:


# Define the week number up to which you want to include columns
end_week = 7


# In[129]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    for video in range(1, 10):
        col_name = f'WK{week}_V{video}'
        if col_name in data.columns:
            cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
Recorded_Total = data[cols_to_select]


# In[130]:


Recorded_Total.head()


# In[131]:


data["Recorded_Total"]=Recorded_Total.sum(axis=1)


# In[132]:


data["Recorded_Average"]=Recorded_Total.mean(axis=1)


# In[133]:


# Create a list of all column names up to the end week
cols_to_select_per = []
for week in range(end_week + 1):
    for video in range(1, 10):
        col_name = f'WK{week}_V{video}'
        if col_name in data1.columns:
            cols_to_select_per.append(col_name)

# Select the desired columns and create a new DataFrame
Recorded_Percent = data1[cols_to_select_per]


# In[134]:


Recorded_Percent.head()


# In[135]:


data["Recorded_Percentage"]=Recorded_Percent.mean(axis=1)


# # Code for SUK Recorded Videos: Total hours, Average & Percentage

# In[136]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    if col_name in data.columns:
        cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
SUK_Recorded_Total = data[cols_to_select]


# In[137]:


SUK_Recorded_Total.head()


# In[138]:


data["SUK_Recorded_Total"]=SUK_Recorded_Total.sum(axis=1)


# In[139]:


data["SUK_Recorded_Average"]=SUK_Recorded_Total.mean(axis=1)


# In[140]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    if col_name in data1.columns:
        cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
SUK_Recorded_Percent = data1[cols_to_select]


# In[141]:


data["SUK_Recorded_Percentage"]=SUK_Recorded_Percent.mean(axis=1)


# # Code for Master Class: Total hours, Average & Percentage

# In[142]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 2):
    for masterclass in range(1, 10):
        col_name = f'WK{week}_Master{masterclass}'
        if col_name in data.columns:
            cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
Masterclass_Total = data[cols_to_select]


# In[143]:


data["Masterclass_Total"]=Masterclass_Total.sum(axis=1)


# In[144]:


data["Masterclass_Average"]=Masterclass_Total.mean(axis=1)


# In[145]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    for masterclass in range(1, 10):
        col_name = f'WK{week}_Master{masterclass}'
        if col_name in data1.columns:
            cols_to_select.append(col_name)

# Select the desired columns and create a new DataFrame
Masterclass_Percent = data1[cols_to_select]


# In[146]:


Masterclass_Percent.head()


# In[147]:


data["Masterclass_Percentage"]=Masterclass_Percent.mean(axis=1)


# # Code for Program: Total hours, Average & Percentage

# In[148]:


# select columns from data where the column names start with "WK"
selected_cols = [col for col in data.columns if col.startswith('WK')]

# create a new dataframe called Whole_Program_Total with the selected columns
Whole_Program_Total = data[selected_cols]


# In[149]:


Whole_Program_Total.head()


# In[150]:


data["Program_Total"]=Whole_Program_Total.sum(axis=1)


# In[151]:


data["Program_Average"]=Whole_Program_Total.mean(axis=1)


# In[152]:


# select columns from data where the column names start with "WK"
selected_cols = [col for col in data1.columns if col.startswith('WK')]

# create a new dataframe called Whole_Program_Total with the selected columns
Whole_Program_Percent = data1[selected_cols]


# In[153]:


data["Program_percentage"]=Whole_Program_Percent.mean(axis=1)


# In[154]:


data=data.round(2)


# # Storing data on MySQL

# In[155]:


merged = df.merge(data, on ='Email', how = 'inner') 


# In[156]:


merged = merged.drop('Enroll Date', axis=1)


# In[157]:


merged = merged.rename(columns={'Assigned Through': 'Assigned_Through'})
merged = merged.rename(columns={'Mobile': 'Phone'})


# In[158]:


merged.head()


# In[159]:


conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')


# In[160]:


cursor =conn.cursor() 


# In[161]:


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


# In[162]:


# Your existing code for inserting data into the database table
for i, row in merged.iterrows():
    row = [None if isinstance(val, float) and math.isnan(val) else val for val in row] # replace "nan" values with None
    columns = ','.join(merged.columns)
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.08_incubator_and_attendence_monitoring ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    query += ", ".join([f"{col}=VALUES({col})" for col in merged.columns if col != 'Email'])
    # Execute the query
    cursor.execute(query, tuple(row))


# In[163]:


conn.commit()


# # Extracting desire output to excel sheet

# In[164]:


data.head()


# In[165]:


data.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Incubator_and_attendence_monitoring_notation.csv",mode='w',index=False)


# In[166]:


#rdata.to_csv("C:\\Users\\spjay\\Desktop\\VigyanShaala\\Trial\\Incubator_and_attendence_monitoring_Final.csv",index=False)


# In[ ]:




