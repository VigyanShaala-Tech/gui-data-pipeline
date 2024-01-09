#!/usr/bin/env python
# coding: utf-8

# In[1]:


# importing required modules
import pandas as pd
import mysql.connector as msql
import math
import os


# # Source file should be named as "Incubator Assignment Review"

# In[2]:


directory_path = r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File\04_Source_Kalpana Assignment Review"
sheet_name = "Assignment Review"

excel_files = [file for file in os.listdir(directory_path) if file.endswith('.xlsx')]

for file in excel_files:
    file_path = os.path.join(directory_path, file)
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    print(f"Data from {file}:")


# In[3]:


df.head()


# In[4]:


# Dispalying All columns
pd.set_option('display.max_columns',None)
pd.options.display.float_format='{:.0f}'.format


# In[5]:


# Selecting desired columns
df = df.iloc[:, :df.columns.get_loc('Assignment Score %')]


# In[6]:


# Dropping columns containing 'Current Status' in their column name
cols_to_drop = [col for col in df.columns if 'Current Status' in col]
df.drop(cols_to_drop, axis=1, inplace=True)


# In[7]:


# Create a list of columns that start with 'MC' or 'WK'
assign = [col for col in df.columns if col.startswith('MC') or col.startswith('WK')]


# In[8]:


assign


# In[9]:


# Remove leading/trailing spaces in the selected columns
df[assign] = df[assign].applymap(lambda x: x.strip() if isinstance(x, str) else x)


# In[10]:


# Replace values in selected columns
replace_dict = {'Accepted': 100, 'Submit 1': 30, 'Review 1': 30, 'Submit 2': 80, 'Rejected': 80}
df[assign] = df[assign].replace(replace_dict)


# In[11]:


# Convert the selected columns to float
df[assign] = df[assign].astype(float)


# In[12]:


# Filling NaN Values with zero
df[assign]=df[assign].fillna(0)


# In[13]:


df.head()


# In[14]:


# Calculate the sum of selected columns
sum_of_scores = df[assign].sum(axis=1)

# Count the number of selected columns
num_of_columns = len(assign)

# Calculate the average score
df['Assignment_Score'] = sum_of_scores / num_of_columns


# In[15]:


# Iterate through all columns in the dataframe
for col in df.columns:
    # Check if column name contains '.'
    if '.' in col:
        # Replace '.' with '_' and remove ' ' only where '.' is present in the column name
        new_col = col.replace('.', '_').replace(' ', '') if '.' in col else col
        # Rename the column
        df.rename(columns={col: new_col}, inplace=True)


# In[16]:


# Rounding Off values of Assignment score to two digits
df['Assignment_Score']=df['Assignment_Score'].round(2)


# In[17]:


df=df.fillna('')


# In[18]:


df.head()


# In[19]:


#df.to_csv('Assignment_Review.csv', mode='a',index=False)


# In[20]:


# Exporting the dataset to output folder
df.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Assignment_Review.csv",mode='w',index=False)


# In[21]:


df.head(1)


# In[22]:


df.columns


# In[23]:


new_names = {
    'Name ': "Name",
    'Email': "Email",
    'Category': "Phone",
    'MC_Career Exploration': "MC_Career_Exploration",
    'Comments ': "Comments",
    'WK_1_SWOT Analysis': "WK_1_SWOT_Analysis",
    'Comments_1': "Comments_1",
    'WK_2_RIASEC': "WK_2_RIASEC",
    'Comments_2': "Comments_2",
    'WK_2_SMART goal': "WK_2_SMART_Goal",
    'Comments_3': "Comments_3",
    'MC_Planning Masters': "MC_Planning_Masters",
    'Comments_4': "Comments_4",
    'WK_3_Career Action Plan ': "WK_3_Career_Action_Plan",
    'Comments_5': "Comments_5",
    'MC_LinkedIn Profile ': "MC_LinkedIn_Profile",
    'Comments_6': "Comments_6",
    'MC_CV/Resume ': "MC_CV_Resume",
    'Comments_7': "Comments_7",
    'WK_6_Critical thinking': "WK_6_Critical_thinking",
    'Comments_8': "Comments_8",
    'WK_7_Problem solving': "WK_7_Problem_solving",
    'Comments_9': "Comments_9",
    'Assignment_Score': "Assignment_Score"
}

for col in df.columns:
    if col in new_names:
        df.rename(columns={col: new_names[col]}, inplace=True)


# # Storing data on MySQL

# In[24]:


# Connecting MySQL Kalpana Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[25]:


# Get the existing columns in the database
cursor.execute("SHOW COLUMNS FROM kalpana.11_incubator_assignment_monitoring")
existing_columns = [col[0] for col in cursor.fetchall()]

# Define the column name before which the new column should be added
target_column = 'Assignment_Score'

# Check if any new columns exist in the dataframe but not in the database
new_columns = [col for col in df.columns if col not in existing_columns]
if new_columns:
    # Add new columns to the database before the target column
    for col in reversed(new_columns):
        if col not in existing_columns:
            # Get the index of the target column
            target_column_index = existing_columns.index(target_column)
            # Set the data type based on whether the column name starts with Comment
            data_type = "INT" if not col.startswith("Comment") else "TEXT"
            alter_query = f"ALTER TABLE kalpana.11_incubator_assignment_monitoring ADD COLUMN {col} {data_type} AFTER {existing_columns[target_column_index - 1]}"
            cursor.execute(alter_query)
            existing_columns.insert(target_column_index - 1, col)


# In[26]:


# Your existing code for inserting data into the database table
for i, row in df.iterrows():
    row = [None if isinstance(val, float) and math.isnan(val) else val for val in row] # replace "nan" values with None
    columns = ','.join(df.columns)
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.11_incubator_assignment_monitoring ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    query += ", ".join([f"{col}=VALUES({col})" for col in df.columns if col != 'Email'])
    # Execute the query
    cursor.execute(query, tuple(row))


# In[27]:


conn.commit()

