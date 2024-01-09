#!/usr/bin/env python
# coding: utf-8

# In[41]:


# importing required modules
import pandas as pd
import mysql.connector as msql
import math
import os
import datetime


# In[42]:


#display all columns and rows
pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)


# In[43]:


# Reading Incubator_and_attendence_monitoring from Outout files
data=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Incubator_and_attendence_monitoring_WithoutNotation.csv")


# In[44]:


# Reading Incubator_and_attendence_monitoring from Outout files
#data=pd.read_csv(r"C:\Users\Lenovo\OneDrive - VigyanShaala\02 Products  Initiatives-LAPTOP-D2TFS89H\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Output\Incubator_and_attendence_monitoring_Final.csv")


# In[45]:


df=pd.DataFrame(data["Email"])


# In[46]:


excel_file  = pd.read_excel(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File\00_Source_Kalpana Video Details\Video Time Period.xlsx")


# In[47]:


for index, row in excel_file.iterrows():
    column_name = row['Name']
    value = row['Time']
    percentage_value = (data[column_name] * 100) / (value/3600)
    df[column_name] = percentage_value


# In[48]:


# Changing percent values greater than 100 to 100
def Max_Value(value):
    if value >=100:
        return 100
    else:
        return value


# In[49]:


col=df.iloc[:,1:]
df1=col.columns
df[df1]=df[df1].applymap(Max_Value)


# # Define Week Here  ↓

# In[50]:


end_week = 7


# # Weekly Recorded Total Hours

# In[51]:


cols_to_select = ['Email']  # Start with 'email' column as the first column
for week in range(end_week + 1):
    video_cols = [f'WK{week}_V{video}' for video in range(1, 10)]
    video_cols = [col for col in video_cols if col in data.columns and col != 'email']
    cols_to_select.extend(video_cols)

# Select the desired columns and create a new DataFrame
Weekly = data[cols_to_select].copy()

# Calculate total for each week and add to Weekly dataframe
for week in range(end_week + 1):
    video_cols = [f'WK{week}_V{video}' for video in range(1, 10)]
    video_cols = [col for col in video_cols if col in Weekly.columns]
    Weekly[f'WK{week}_Recorded_Total'] = Weekly[video_cols].sum(axis=1, skipna=True)

# Calculate total for all weeks and add to Weekly dataframe
total_cols = [f'WK{week}_Recorded_Total' for week in range(end_week + 1) if f'WK{week}_Recorded_Total' in Weekly.columns]
Weekly['All_Week_Total'] = Weekly[total_cols].sum(axis=1, skipna=True)

# Select the desired columns in the Weekly dataframe
cols_to_select = ['Email'] + total_cols + ['All_Week_Total']
Weekly = Weekly[cols_to_select]


# # Weekly average of Recorded Videos

# In[52]:


cols_to_select = ['Email']  # Start with 'email' column as the first column
for week in range(end_week + 1):
    video_cols = [f'WK{week}_V{video}' for video in range(1, 10)]
    video_cols = [col for col in video_cols if col in data.columns and col != 'email']
    cols_to_select.extend(video_cols)

# Select the desired columns and create a new DataFrame
Weekly_AVG = data[cols_to_select].copy()

# Calculate total for each week and add to Weekly dataframe
for week in range(end_week + 1):
    video_cols = [f'WK{week}_V{video}' for video in range(1, 10)]
    video_cols = [col for col in video_cols if col in Weekly_AVG.columns]
    Weekly_AVG[f'WK{week}_Average'] = Weekly_AVG[video_cols].mean(axis=1, skipna=True)

# Calculate total for all weeks and add to Weekly dataframe
total_cols = [f'WK{week}_Average' for week in range(end_week + 1) if f'WK{week}_Average' in Weekly_AVG.columns]
Weekly_AVG['All_Week_Average'] = Weekly_AVG[total_cols].mean(axis=1, skipna=True)

# Select the desired columns in the Weekly dataframe
cols_to_select = ['Email'] + total_cols + ['All_Week_Average']
Weekly_AVG = Weekly_AVG[cols_to_select]


# In[53]:


# Merging Dataframe
Weekly = Weekly.merge(Weekly_AVG,on= 'Email', how ='left')


# # Recorded weekly Percentage

# In[54]:


cols_to_select = ['Email']  # Start with 'email' column as the first column
for week in range(end_week + 1):
    video_cols = [f'WK{week}_V{video}' for video in range(1, 10)]
    video_cols = [col for col in video_cols if col in df.columns and col != 'email']
    cols_to_select.extend(video_cols)

# Select the desired columns and create a new DataFrame
Weekly_Per = df[cols_to_select].copy()

# Calculate total for each week
for week in range(end_week + 1):
    video_cols = [f'WK{week}_V{video}' for video in range(1, 10)]
    video_cols = [col for col in video_cols if col in Weekly_Per.columns]
    Weekly_Per[f'Recorded_WK{week}_percent'] = Weekly_Per[video_cols].mean(axis=1, skipna=True)

# Calculate total for all weeks
total_cols = [f'Recorded_WK{week}_percent' for week in range(end_week + 1) if f'Recorded_WK{week}_percent' in Weekly_Per.columns]
Weekly_Per['Recorded_All_Week_percent'] = Weekly_Per[total_cols].mean(axis=1, skipna=True)

# Select the desired columns in the Weekly dataframe
cols_to_select = ['Email'] + total_cols + ['Recorded_All_Week_percent']
Weekly_Per = Weekly_Per[cols_to_select]


# In[55]:


# Merging Dataframe
Weekly = Weekly.merge(Weekly_Per,on= 'Email', how ='left')


# # SUK Weekly Total Hours

# In[56]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    if col_name in data.columns:
        cols_to_select.append(col_name)

# Add the 'email' column to the list of selected columns
cols_to_select.append('Email')

# Select the desired columns and create a new DataFrame
SUK_Recorded_Total = data.loc[:, cols_to_select]

# Create new columns with 'SUK' prefix for each week
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    new_col_name = f'SUK{week + 1}'
    if col_name in SUK_Recorded_Total.columns:
        SUK_Recorded_Total[new_col_name] = SUK_Recorded_Total[col_name]

# Drop the original columns
SUK_Recorded_Total.drop(cols_to_select[:-1], axis=1, inplace=True)
# Select only the columns of float data type

float_cols = SUK_Recorded_Total.select_dtypes(include='float')

# Calculate the sum of hours for each row and add as a new column
SUK_Recorded_Total['SUK_Total_Hours'] = float_cols.iloc[:, :-1].sum(axis=1)




# In[57]:


# Merging Dataframe
Weekly = Weekly.merge(SUK_Recorded_Total,on= 'Email', how ='left')


# # SUK Recorded Hours for Weekly Average

# In[58]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    if col_name in data.columns:
        cols_to_select.append(col_name)

# Add the 'email' column to the list of selected columns
cols_to_select.append('Email')

# Select the desired columns and create a new DataFrame
SUK_Recorded_Average = data.loc[:, cols_to_select]

# Create new columns with 'SUK' prefix for each week
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    new_col_name = f'SUK{week+1}_Average'
    if col_name in SUK_Recorded_Average.columns:
        SUK_Recorded_Average[new_col_name] = SUK_Recorded_Average[col_name]

# Drop the original columns
SUK_Recorded_Average.drop(cols_to_select[:-1], axis=1, inplace=True)

# Select only the columns of float data type
float_cols = SUK_Recorded_Average.select_dtypes(include='float')

# Calculate the mean of the float columns
SUK_Recorded_Average['SUK_Average'] = float_cols.mean(axis=1)


# In[59]:


# Merging Dataframe
Weekly = Weekly.merge(SUK_Recorded_Average,on= 'Email', how ='left')


# # SUK Average Percentage

# In[60]:


# Create a list of all column names up to the end week
cols_to_select = []
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    if col_name in df.columns:
        cols_to_select.append(col_name)

# Add the 'email' column to the list of selected columns
cols_to_select.append('Email')

# Select the desired columns and create a new DataFrame
SUK_Recorded_percent = df.loc[:, cols_to_select]

# Create new columns with 'SUK' prefix for each week
for week in range(end_week + 1):
    col_name = f'WK{week}_SUK_V'
    new_col_name = f'SUK{week+1}_percent'
    if col_name in SUK_Recorded_percent.columns:
        SUK_Recorded_percent[new_col_name] = SUK_Recorded_percent[col_name]

# Drop the original columns
SUK_Recorded_percent.drop(cols_to_select[:-1], axis=1, inplace=True)

# Select only the columns of float data type
float_cols = SUK_Recorded_percent.select_dtypes(include='float')

# Calculate the mean of the float columns
SUK_Recorded_percent['SUK_Percentage'] = float_cols.mean(axis=1)


# In[61]:


# Merging Dataframe
Weekly = Weekly.merge(SUK_Recorded_percent,on= 'Email', how ='left')


# # Weekly Masterclass total hours

# In[62]:


# Create a list of all column names up to the end week
cols_to_select = [f'WK{i}_Master{j}' for i in range(end_week + 2) for j in range(1, 10) if f'WK{i}_Master{j}' in data.columns]

# Add the 'email' column to the list of selected columns
cols_to_select.append('Email')

# Select the desired columns and create a new DataFrame
Masterclass_Total = data.loc[:, cols_to_select]

# Rename columns as "Master{number}"
Masterclass_Total = Masterclass_Total.rename(columns={col_name: f'Master{i+1}' for i, col_name in enumerate(Masterclass_Total.columns) if col_name != 'Email'})

# Create the "Total_Hours" column
Masterclass_Total['Total_Hours'] = Masterclass_Total.select_dtypes(include=['float']).sum(axis=1)


# In[63]:


# Merging Dataframe
Weekly = Weekly.merge(Masterclass_Total,on= 'Email', how ='left')


# # Weekly Master Average

# In[64]:


# Create a list of all column names up to the end week
cols_to_select = [f'WK{i}_Master{j}' for i in range(end_week + 2) for j in range(1, 10) if f'WK{i}_Master{j}' in data.columns]

# Add the 'email' column to the list of selected columns
cols_to_select.append('Email')

# Select the desired columns and create a new DataFrame
Masterclass_Avg = data.loc[:, cols_to_select]

# Rename columns as "Master{number}"
Masterclass_Avg = Masterclass_Avg.rename(columns={col_name: f'Master{i +1}_Average' for i, col_name in enumerate(Masterclass_Avg.columns) if col_name != 'Email'})

# Create the "Master_Average" column
Masterclass_Avg['Master_Average'] = Masterclass_Avg.select_dtypes(include=['float']).mean(axis=1)


# In[65]:


# Merging Dataframe
Weekly = Weekly.merge(Masterclass_Avg,on= 'Email', how ='left')


# # Masterclass Average Percent
# 

# In[66]:


# Create a list of all column names up to the end week
cols_to_select = [f'WK{i}_Master{j}' for i in range(end_week + 2) for j in range(1, 10) if f'WK{i}_Master{j}' in df.columns]

# Add the 'email' column to the list of selected columns
cols_to_select.append('Email')

# Select the desired columns and create a new DataFrame
Masterclass_Per = df.loc[:, cols_to_select]

# Rename columns as "Master{number}"
Masterclass_Per = Masterclass_Per.rename(columns={col_name: f'Master{i +1}_percent' for i, col_name in enumerate(Masterclass_Per.columns) if col_name != 'Email'})

# Create the "Master_Percentage" column
Masterclass_Per['Master_Percentage'] = Masterclass_Per.select_dtypes(include=['float']).mean(axis=1)


# In[67]:


# Merging Dataframe
Weekly = Weekly.merge(Masterclass_Per,on= 'Email', how ='left')


# In[68]:


Weekly.head(10)


# In[69]:


Weekly.columns


# In[70]:


#Weekly.to_csv('Weekly_Attendence.csv', mode='a',index=False)


# In[71]:


# Exporting data to Output files
Weekly.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Weekly_Attendence.csv",mode='w',index=False)


# # Storing data to MySQL

# In[72]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')


# In[73]:


cursor =conn.cursor()


# In[74]:


# Get the existing columns in the database
cursor.execute("SHOW COLUMNS FROM kalpana.09_weekly_attendence")
existing_columns = [col[0] for col in cursor.fetchall()]

# Define the column name before which the new column should be added
target_column = 'Master_Percentage'

# Check if any new columns exist in the dataframe but not in the database
new_columns = [col for col in Weekly.columns if col not in existing_columns]
if new_columns:
    # Add new columns to the database before the target column
    for col in reversed(new_columns):
        if col not in existing_columns:
            # Get the index of the target column
            target_column_index = existing_columns.index(target_column)
            # Set the data type based on whether the column name starts with Comment
            data_type = "INT" 
            alter_query = f"ALTER TABLE kalpana.09_weekly_attendence ADD COLUMN {col} {data_type} AFTER {existing_columns[target_column_index]}"
            cursor.execute(alter_query)
            existing_columns.insert(target_column_index - 1, col)


# In[75]:


# Your existing code for inserting data into the database table
for i, row in Weekly.iterrows():
    row = [None if isinstance(val, float) and math.isnan(val) else val for val in row] # replace "nan" values with None
    columns = ','.join(Weekly.columns)
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.09_weekly_attendence ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    query += ", ".join([f"{col}=VALUES({col})" for col in Weekly.columns if col != 'Email'])
    # Execute the query
    cursor.execute(query, tuple(row))


# In[76]:


conn.commit()


# In[ ]:





# 
