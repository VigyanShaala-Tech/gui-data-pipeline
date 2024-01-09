#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing libraries
import os
import glob
import pandas as pd 
import mysql.connector as msql
import warnings
warnings.filterwarnings('ignore')
import math 


# In[2]:


# Specify the directory path where the CSV files are located
path = r'C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File\03_Source_Kalpana Quiz Review'
 
# Change the current working directory to the specified path
os.chdir(path)

# Use glob to get a list of file paths for all CSV files in the current directory
file_paths = glob.glob('*.csv')


# In[ ]:





# In[3]:


# Creating empty list to store data
keywords = []

# Iterate over each file path in the file_paths list
for file in file_paths:
    # Find the starting index of the keyword by locating the position of 'Quiz_' in the file name and adding 5 to skip the 'Quiz_' prefix
    #start = file.find('Quiz') + 5
    
    # Find the ending index of the keyword by locating the last occurrence of '-' in the file name
    #end = file.rfind('-')
    
    # Extract the keyword from the file name by slicing the string between the start and end indices, and remove any leading/trailing whitespaces
    keyword = os.path.splitext(os.path.basename(file))[0]
    
    # Append the modified keyword to the keywords list
    keywords.append(keyword)


# In[4]:


# List of all keywords present in our folder 
keywords


# In[5]:


#Kalpana = pd.read_csv(r'C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives-LAPTOP-D2TFS89H\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File\02_Source_Kalpana incubator\Kalpana Incubator Jan - Mar 2023 Batch-1690281131766.csv',usecols=['Email'])


# In[6]:


# Reading Kalpana SHE for STEM Incubator file from Directory 
directory_path =(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File\02_Source_Kalpana incubator")

csv_files = [file for file in os.listdir(directory_path) if file.endswith('.csv')]

for file in csv_files:
    file_path = os.path.join(directory_path, file)
    Kalpana = pd.read_csv(file_path,usecols=['Email'])
    print(f"Data from {file}:")


# In[7]:


# Iterate over file_paths and keywords simultaneously using zip
for file, keyword in zip(file_paths, keywords):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(file)
    
    # Identify the columns containing 'marks' (case-insensitive) in their names
    marks_cols = [col for col in df.columns if 'marks' in col.lower()]
    
    # Calculate the maximum value across the marks columns for each row
    df[f'Marks_{keyword}'] = df[marks_cols].max(axis=1, skipna=True)
    
    # Select only the 'Email' column and the newly created Marks column
    df = df[['Email', f'Marks_{keyword}']]
    
    # Merge the DataFrame with Kalpana on the 'Email' column using an outer join
    Kalpana = Kalpana.merge(df, on='Email', how='outer')


# In[8]:


# Define percent function
def percent(value):
    # Check if the value is NaN (missing value)
    if pd.isna(value):
        # Return 0 if the value is NaN
        return 0
    else:
        # Multiply the value by 10 if it's not NaN
        return value * 10


# In[9]:


# Create a list of column names in Kalpana DataFrame that start with 'Marks'
marks_cols = [col for col in Kalpana.columns if col.startswith('Marks')]

# Apply the percent function to each element in the selected columns using applymap()
Kalpana[marks_cols] = Kalpana[marks_cols].applymap(percent)


# In[10]:


# Creating Overall_Score column which is average of columns of all quizzes
Kalpana["Overall_Average"]=Kalpana.iloc[:,1:].mean(axis=1)


# In[11]:


# DataFrame df6 to 2 decimal places using the round() 
Kalpana["Overall_Average"]=Kalpana["Overall_Average"].round(2)


# In[12]:


Kalpana.columns = Kalpana.columns.str.replace(' ', '_')


# In[13]:


Kalpana.columns


# In[14]:


# Define a dictionary that maps column names to their desired new names
new_names = {
    'Marks_21st_Century_skills_-_Collaboration_and_problem_solving_x': 'Marks_Collaboration',
    'Marks_Collaboration_and_problem_solving': 'Marks_Collaboration',
    'Marks_Communication': 'Marks_Communication_review',
    'Marks_Creativity_&_growth_mindset': 'Marks_Creativity_Growth',
    'Marks_Critical_thinking': 'Marks_Critical_Thinking',
    'Marks_Individual_Development_Plan_(IDP)': 'Marks_IDP_review',
    'Marks_Strategic_planning_tools_for_STEM_(SWOT)': 'Marks_SWOT_review'
}

# Iterate over the columns in Kalpana DataFrame
for col in Kalpana.columns:
    # Check if the column name is in the new_names dictionary
    if col in new_names:
        # Rename the column using the corresponding new name from the dictionary
        Kalpana.rename(columns={col: new_names[col]}, inplace=True)


# In[15]:


# Check columns name
Kalpana.columns


# In[16]:


Kalpana.head()


# In[17]:


Kalpana[Kalpana['Email'] == 'ankithaguguloth46@gmail.com']


# At last we have to just create a loop which will tell that if that file name doest prsent in mysql create a file name and add to mysql
# 

# In[18]:


#Kalpana.to_csv('Quiz_Information_Final.csv', mode='a',index=False)


# In[19]:


Kalpana.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Quiz_Information.csv",mode='w',index=False)


# # Storing data on MySQL

# In[20]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[21]:


# Get the existing columns in the database
cursor.execute("SHOW COLUMNS FROM kalpana.10_incubator_quiz_monitoring")
existing_columns = [col[0] for col in cursor.fetchall()]

# Define the column name before which the new column should be added
target_column = 'Overall_Average'

# Check if any new columns exist in the dataframe but not in the database
new_columns = [col for col in Kalpana.columns if col not in existing_columns]
if new_columns:
    # Add new columns to the database before the target column
    for col in reversed(new_columns):
        if col not in existing_columns:
            # Get the index of the target column
            target_column_index = existing_columns.index(target_column)
            # Set the data type based on whether the column name starts with Comment
            data_type = "INT" 
            alter_query = f"ALTER TABLE kalpana.10_incubator_quiz_monitoring ADD COLUMN {col} {data_type} AFTER {existing_columns[target_column_index - 1]}"
            cursor.execute(alter_query)
            existing_columns.insert(target_column_index - 1, col)


# In[22]:


# Your existing code for inserting data into the database table
for i, row in Kalpana.iterrows():
    row = [None if isinstance(val, float) and math.isnan(val) else val for val in row] # replace "nan" values with None
    columns = ','.join(Kalpana.columns)
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.10_incubator_quiz_monitoring ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    query += ", ".join([f"{col}=VALUES({col})" for col in Kalpana.columns if col != 'Email'])
    # Execute the query
    cursor.execute(query, tuple(row))


# In[23]:


conn.commit()

