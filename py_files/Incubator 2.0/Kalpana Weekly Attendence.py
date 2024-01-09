#!/usr/bin/env python
# coding: utf-8

# In[1]:


# importing required modules
import pandas as pd
import mysql.connector as msql
import math


# In[2]:


#display all columns and rows
pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)


# In[3]:


# Reading Incubator_and_attendence_monitoring from Outout files
data=pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Output\Incubator_and_attendence_monitoring.csv")


# # Calculating each video Percentage and storing it into df

# In[4]:


# Converting to numeric
for i in data.columns:
    if i != 'Email':
        data[i]=pd.to_numeric(data[i], errors='coerce')


# In[5]:


# Check datatypes
data.dtypes


# In[6]:


# Create a new DataFrame 'df' containing only the 'Email' column from the original data
df=pd.DataFrame(data["Email"])


# In[7]:


# Create a new column 'WK0_SUK_V%' in the DataFrame 'df' by calculating the percentage
# The calculation is based on the 'WK0_SUK_V' column from the original data
# The denominator (3600/3600) is used to ensure the value is multiplied by 100
df["WK0_SUK_V%"] = (data["WK0_SUK_V"] * 100) / (3600 / 3600)


# In[8]:


df['WK0_V1%'] = (data['WK0_V1']*100)/(291/3600)


# In[9]:


df["WK0_V2%"]= (data["WK0_V2"])*100/(287/3600)


# In[10]:


df["WK0_Master1%"] = (data["WK0_Master1"]*100)/(3600/3600)


# In[11]:


df["WK1_V1%"] = (data["WK1_V1"]*100)/(141/3600)


# In[12]:


df["WK1_V2%"] = (data["WK1_V2"]*100)/(453/3600)


# In[13]:


df["WK1_V3%"] = (data["WK1_V3"]*100)/(347/3600)


# In[14]:


df["WK1_V4%"] = (data["WK1_V4"]*100)/(441/3600)


# In[15]:


df["WK1_SUK_V%"] = (data["WK1_SUK_V"]*100)/(5629/3600)


# In[16]:


df["WK2_V1%"] = (data["WK2_V1"]*100)/(345/3600)


# In[17]:


df["WK2_V2%"] = (data["WK2_V2"]*100)/(239/3600)


# In[18]:


df["WK2_V3%"] = (data["WK2_V3"]*100)/(290/3600)


# In[19]:


df["WK2_V4%"] = (data["WK2_V4"]*100)/(296/3600)


# In[20]:


df["WK2_V5%"] = (data["WK2_V5"]*100)/(291/3600)


# In[21]:


df["WK2_SUK_V%"] = (data["WK2_SUK_V"]*100)/(3600/3600)


# In[22]:


df["WK3_V1%"] = (data["WK3_V1"]*100)/(428/3600)


# In[23]:


df["WK3_V2%"] = (data["WK3_V2"]*100)/(650/3600)


# In[24]:


df["WK3_V3%"] = (data["WK3_V3"]*100)/(309/3600)


# In[25]:


df["WK3_V4%"] = (data["WK3_V4"]*100)/(463/3600)


# In[26]:


df["WK3_V5%"] = (data["WK3_V5"]*100)/(560/3600)


# In[27]:


df["WK3_V6%"] = (data["WK3_V6"]*100)/(496/3600)


# In[28]:


df["WK3_SUK_V%"] = (data["WK3_SUK_V"]*100)/(3600/3600)


# In[29]:


df["WK4_V1%"] = (data["WK4_V1"]*100)/(286/3600)


# In[30]:


df["WK4_V2%"] = (data["WK4_V2"]*100)/(265/3600)


# In[31]:


df["WK4_V3%"] = (data["WK4_V3"]*100)/(321/3600)


# In[32]:


df["WK4_V4%"] = (data["WK4_V4"]*100)/(120/3600)


# In[33]:


df["WK4_V5%"] = (data["WK4_V5"]*100)/(154/3600)


# In[34]:


df["WK4_V6%"] = (data["WK4_V6"]*100)/(115/3600)


# In[35]:


df["WK4_V7%"] = (data["WK4_V7"]*100)/(154/3600)


# In[36]:


df["WK4_V8%"] = (data["WK4_V8"]*100)/(124/3600)


# In[37]:


df["WK4_V9%"] = (data["WK4_V9"]*100)/(203/3600)


# In[38]:


df["WK4_SUK_V%"] = (data["WK4_SUK_V"]*100)/(3600/3600)


# In[39]:


df["WK5_V1%"] = (data["WK5_V1"]*100)/(114/3600)


# In[40]:


df["WK5_V2%"] = (data["WK5_V2"]*100)/(327/3600)


# In[41]:


df["WK5_V3%"] = (data["WK5_V3"]*100)/(131/3600)


# In[42]:


df["WK5_SUK_V%"] = (data["WK5_SUK_V"]*100)/(3600/3600)


# In[43]:


df["WK6_V1%"] = (data["WK6_V1"]*100)/(259/3600)


# In[44]:


df["WK6_V2%"] = (data["WK6_V2"]*100)/(432/3600)


# In[45]:


df["WK6_V3%"] = (data["WK6_V3"]*100)/(261/3600)


# In[46]:


df["WK6_V4%"] = (data["WK6_V4"]*100)/(515/3600)


# In[47]:


df["WK6_Master2%"] = (data["WK6_Master2"]*100)/(3600/3600)


# In[48]:


df["WK7_V1%"] = (data["WK7_V1"]*100)/(228/3600)


# In[49]:


df["WK7_V2%"] = (data["WK7_V2"]*100)/(214/3600)


# In[50]:


df["WK7_V3%"] = (data["WK7_V3"]*100)/(110/3600)


# In[51]:


df["WK7_V4%"] = (data["WK7_V4"]*100)/(170/3600)


# In[52]:


df["WK7_V5%"] = (data["WK7_V5"]*100)/(561/3600)


# In[53]:


df["WK7_Master3%"] = (data["WK7_Master3"]*100)/(3600/3600)


# In[54]:


df["WK8_Master4%"] = (data["WK8_Master4"]*100)/(3600/3600)


# In[55]:


df["WK8_Master5%"] = (data["WK8_Master5"]*100)/(3600/3600)


# In[56]:


df["WK8_SUK_V%"] = (data["WK8_SUK_V"]*100)/(3600/3600)


# In[57]:


df["WK0_SUK_Live%"] = (data["WK0_SUK_Live"]*100)/(3600/3600)


# In[58]:


df["WK1_SUK_Live%"] = (data["WK1_SUK_Live"]*100)/(3600/3600)


# In[59]:


df["WK2_SUK_Live%"] = (data["WK2_SUK_Live"]*100)/(3600/3600)


# In[60]:


df["WK3_SUK_Live%"] = (data["WK3_SUK_Live"]*100)/(3600/3600)


# In[61]:


df["WK4_SUK_Live%"] = (data["WK4_SUK_Live"]*100)/(3600/3600)


# In[62]:


df["WK8_SUK_Live%"] = (data["WK8_SUK_Live"]*100)/(3600/3600)


# In[63]:


# Changing percent values greater than 100 to 100
def Max_Value(value):
    if value >=100:
        return 100
    else:
        return value


# In[64]:


# Extract all columns except the first one into the 'col' DataFrame
col=df.iloc[:,1:]
# Get the column names from 'col'
df1=col.columns
# Apply the 'Max_Value' function to each element in 'col'
df[df1]=df[df1].applymap(Max_Value)


# # Creating Weekly_Attendence Table

# In[65]:


# Create a new DataFrame 'Weekly' containing only the 'Email' column from the original data
Weekly=pd.DataFrame(data["Email"])


# # Weekly Recorded Total Hours

# In[66]:


# Create a new DataFrame 'WK0_Total' by selecting specific columns from 'data'
WK0_Total = data[['WK0_V1', 'WK0_V2']]

# Calculate the sum of values in 'WK0_Total' along the rows and assign it to 'WK0_Recorded_Total' in 'Weekly'
Weekly["WK0_Recorded_Total"] = WK0_Total.sum(axis=1, skipna=True)


# In[67]:


WK1_Total=data[['WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4']]
Weekly["WK1_Recorded_Total"]=WK1_Total.sum(axis=1,skipna=True)


# In[68]:


WK2_Total=data[['WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4', 'WK2_V5']]
Weekly["WK2_Recorded_Total"]=WK2_Total.sum(axis=1,skipna=True)


# In[69]:


WK3_Total=data[['WK3_V1', 'WK3_V2', 'WK3_V3', 'WK3_V4', 'WK3_V5', 'WK3_V6']]
Weekly["WK3_Recorded_Total"]=WK3_Total.sum(axis=1,skipna=True)


# In[70]:


WK4_Total=data[['WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5', 'WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9']]
Weekly["WK4_Recorded_Total"]=WK4_Total.sum(axis=1,skipna=True)


# In[71]:


WK5_Total=data[[ 'WK5_V1',  'WK5_V2', 'WK5_V3',]]
Weekly["WK5_Recorded_Total"]=WK5_Total.sum(axis=1,skipna=True)


# In[72]:


WK6_Total=data[['WK6_V1', 'WK6_V2', 'WK6_V3', 'WK6_V4']]
Weekly["WK6_Recorded_Total"]=WK6_Total.sum(axis=1,skipna=True)


# In[73]:


WK7_Total=data[['WK7_V1','WK7_V2', 'WK7_V3', 'WK7_V4', 'WK7_V5',]]
Weekly["WK7_Recorded_Total"]=WK7_Total.sum(axis=1,skipna=True)


# In[74]:


# Create a new DataFrame 'Total' by selecting specific columns from 'Weekly'
Total = Weekly[[ 'WK0_Recorded_Total', 'WK1_Recorded_Total', 'WK2_Recorded_Total',
                'WK3_Recorded_Total', 'WK4_Recorded_Total', 'WK5_Recorded_Total',
                'WK6_Recorded_Total', 'WK7_Recorded_Total']]

# Calculate the sum of values in 'Total' along the rows and assign it to 'All_Week_Total' in 'Weekly'
Weekly["All_Week_Total"] = Total.sum(axis=1, skipna=True)


# # Weekly average of Recorded Videos

# In[75]:


# Calculate the mean of values in 'WK0_Total' along the rows and assign it to 'WK0_Average' in 'Weekly'
Weekly["WK0_Average"]=WK0_Total.mean(axis=1,skipna=True)


# In[76]:


Weekly["WK1_Average"]=WK1_Total.mean(axis=1,skipna=True)


# In[77]:


Weekly["WK2_Average"]=WK2_Total.mean(axis=1,skipna=True)


# In[78]:


Weekly["WK3_Average"]=WK3_Total.mean(axis=1,skipna=True)


# In[79]:


Weekly["WK4_Average"]=WK4_Total.mean(axis=1,skipna=True)


# In[80]:


Weekly["WK5_Average"]=WK5_Total.mean(axis=1,skipna=True)


# In[81]:


Weekly["WK6_Average"]=WK6_Total.mean(axis=1,skipna=True)


# In[82]:


Weekly["WK7_Average"]=WK7_Total.mean(axis=1,skipna=True)


# In[83]:


Total=Weekly[[ 'WK0_Average', 'WK1_Average', 'WK2_Average',
       'WK3_Average', 'WK4_Average', 'WK5_Average',
       'WK6_Average', 'WK7_Average']]
Weekly["All_WK_Average"]=Total.mean(axis=1,skipna=True)


# # Recorded weekly Percentage

# In[84]:


# Calculate the mean of values in columns 'WK0_V1%' and 'WK0_V2%' from 'df' along the rows
# Assign the mean to 'Recorded_WK0_percent' in 'Weekly'
Weekly["Recorded_WK0_percent"] = df[[ 'WK0_V1%', 'WK0_V2%']].mean(axis=1, skipna=True)


# In[85]:


Weekly["Recorded_WK1_percent"]=df[[ 'WK1_V1%', 'WK1_V2%', 'WK1_V3%', 'WK1_V4%']].mean(axis=1,skipna=True)


# In[86]:


Weekly["Recorded_WK2_percent"]=df[[ 'WK2_V1%', 'WK2_V2%', 'WK2_V3%', 'WK2_V4%', 'WK2_V5%']].mean(axis=1,skipna=True)


# In[87]:


Weekly["Recorded_WK3_percent"]=df[[ 'WK3_V1%','WK3_V2%', 'WK3_V3%', 'WK3_V4%', 'WK3_V5%', 'WK3_V6%']].mean(axis=1,skipna=True)


# In[88]:


Weekly["Recorded_WK4_percent"]=df[[ 'WK4_V1%',
       'WK4_V2%', 'WK4_V3%', 'WK4_V4%', 'WK4_V5%', 'WK4_V6%', 'WK4_V7%',
       'WK4_V8%', 'WK4_V9%',]].mean(axis=1,skipna=True)


# In[89]:


Weekly["Recorded_WK5_percent"]=df[[ 'WK5_V1%', 'WK5_V2%', 'WK5_V3%']].mean(axis=1,skipna=True)


# In[90]:


Weekly["Recorded_WK6_percent"]=df[['WK6_V1%',
       'WK6_V2%', 'WK6_V3%', 'WK6_V4%',]].mean(axis=1,skipna=True)


# In[91]:


Weekly["Recorded_WK7_percent"]=df[[ 'WK7_V1%', 'WK7_V2%', 'WK7_V3%',
       'WK7_V4%', 'WK7_V5%',]].mean(axis=1,skipna=True)


# In[92]:


All_Week_percent=Weekly[['Recorded_WK0_percent','Recorded_WK1_percent','Recorded_WK2_percent','Recorded_WK3_percent','Recorded_WK4_percent','Recorded_WK5_percent','Recorded_WK6_percent','Recorded_WK7_percent']]


# In[93]:


Weekly["Recorded_All_Week_percent"]=All_Week_percent.mean(axis=1,skipna=True)


# # SUK Weekly Total Hours

# In[94]:


# Create a new column 'SUK1' in 'Weekly' by calculating the sum of values in 'WK0_SUK_V' and 'WK0_SUK_Live' from 'data'
Weekly["SUK1"]=data[['WK0_SUK_V','WK0_SUK_Live']].sum(axis=1)


# In[95]:


Weekly["SUK2"]=data[['WK1_SUK_V','WK1_SUK_Live']].sum(axis=1)


# In[96]:


Weekly["SUK3"]=data[['WK2_SUK_V','WK2_SUK_Live']].sum(axis=1)


# In[97]:


Weekly["SUK4"]=data[['WK3_SUK_V','WK3_SUK_Live']].sum(axis=1)


# In[98]:


Weekly["SUK5"]=data[['WK4_SUK_V','WK4_SUK_Live']].sum(axis=1)


# In[99]:


Weekly["SUK6"]=data[['WK5_SUK_V']].sum(axis=1)


# In[100]:


Weekly["SUK7"]=data[['WK8_SUK_V','WK8_SUK_Live']].sum(axis=1)


# In[101]:


Weekly["SUK_Total_Hours"]=Weekly[['SUK1', 'SUK2', 'SUK3', 'SUK4',
       'SUK5', 'SUK6','SUK7']].sum(axis=1)


# # Max of SUK Live & SUK Recorded Hours for Weekly Average 

# In[102]:


# Calculate the maximum value between 'WK0_SUK_Live' and 'WK0_SUK_V' from 'data' along the rows
# Assign the maximum value to 'SUK1_Average' in 'Weekly'
Weekly["SUK1_Average"]=data[["WK0_SUK_Live","WK0_SUK_V"]].max(axis=1)


# In[103]:


Weekly["SUK2_Average"]=data[["WK1_SUK_Live","WK1_SUK_V"]].max(axis=1)


# In[104]:


Weekly["SUK3_Average"]=data[["WK2_SUK_Live","WK2_SUK_V"]].max(axis=1)


# In[105]:


Weekly["SUK4_Average"]=data[["WK3_SUK_Live","WK3_SUK_V"]].max(axis=1)


# In[106]:


Weekly["SUK5_Average"]=data[["WK4_SUK_Live","WK4_SUK_V"]].max(axis=1)


# In[107]:


Weekly["SUK6_Average"]=data[["WK5_SUK_V"]]


# In[108]:


Weekly["SUK7_Average"]=data[["WK8_SUK_Live","WK8_SUK_V"]].max(axis=1)


# In[109]:


Weekly["SUK_Average"]=Weekly[["SUK1_Average","SUK2_Average","SUK3_Average","SUK4_Average","SUK5_Average","SUK6_Average",'SUK7_Average']].mean(axis=1)


# # SUK Average Percentage

# In[110]:


# Calculate the maximum value between 'WK0_SUK_Live%' and 'WK0_SUK_V%' from 'df' along the rows
# Assign the maximum value to 'SUK1_percent' in 'Weekly'
Weekly['SUK1_percent']=df[["WK0_SUK_Live%","WK0_SUK_V%"]].max(axis=1)


# In[111]:


Weekly['SUK2_percent']=df[["WK1_SUK_Live%","WK1_SUK_V%"]].max(axis=1)


# In[112]:


Weekly['SUK3_percent']=df[["WK2_SUK_Live%","WK2_SUK_V%"]].max(axis=1)


# In[113]:


Weekly['SUK4_percent']=df[["WK3_SUK_Live%","WK3_SUK_V%"]].max(axis=1)


# In[114]:


Weekly['SUK5_percent']=df[["WK4_SUK_Live%","WK4_SUK_V%"]].max(axis=1)


# In[115]:


Weekly['SUK6_percent']=df[["WK5_SUK_V%"]]


# In[116]:


Weekly['SUK7_percent']=df[["WK8_SUK_V%",'WK8_SUK_Live%']].max(axis=1)


# In[117]:


Weekly["SUK_Percentage"]=Weekly[[ 'SUK1_percent', 'SUK2_percent', 'SUK3_percent','SUK4_percent', 'SUK5_percent','SUK6_percent','SUK7_percent']].mean(axis=1)


# # Weekly Masterclass total hours

# In[118]:


# Create a new column 'Master1' in 'Weekly' by copying values from 'WK0_Master1' in 'data'
Weekly["Master1"] = data["WK0_Master1"]


# In[119]:


Weekly["Master2"]=data["WK6_Master2"]


# In[120]:


Weekly["Master3"]=data["WK7_Master3"]


# In[121]:


Weekly["Master4"]=data["WK8_Master4"]


# In[122]:


Weekly["Master5"]=data["WK8_Master5"]


# In[123]:


Weekly['Total_Hours']=Weekly[["Master1","Master2","Master3","Master4","Master5"]].sum(axis=1)


# # Weekly Master Average

# In[124]:


# Calculate the mean of values in 'WK0_Master1' column from 'data' along the rows
# Assign the mean to 'Master1_Average' in 'Weekly'
Weekly["Master1_Average"] = data[['WK0_Master1']].mean(axis=1)


# In[125]:


Weekly["Master2_Average"]=data[['WK6_Master2']].mean(axis=1)


# In[126]:


Weekly["Master3_Average"]=data[['WK7_Master3']].mean(axis=1)


# In[127]:


Weekly["Master4_Average"]=data[['WK8_Master4']].mean(axis=1)


# In[128]:


Weekly["Master5_Average"]=data[['WK8_Master5']].mean(axis=1)


# In[129]:


Weekly["Master_Average"]=Weekly[["Master1_Average","Master2_Average","Master3_Average","Master4_Average","Master5_Average"]].mean(axis=1)


# # Percentage live Masterclass recorded max

# In[130]:


# Assign the values from the 'WK0_Master1%' column in 'df' to 'Master1_percent' in 'Weekly'
Weekly["Master1_percent"] = df['WK0_Master1%']


# In[131]:


Weekly["Master2_percent"]=df['WK6_Master2%']


# In[132]:


Weekly["Master3_percent"]=df['WK7_Master3%']


# In[133]:


Weekly["Master4_percent"]=df['WK8_Master4%']


# In[134]:


Weekly["Master5_percent"]=df['WK8_Master5%']


# # Masterclass Average Percent

# In[135]:


# Calculate the mean of values from columns 'WK0_Master1%', 'WK6_Master2%', 'WK7_Master3%', 'WK8_Master4%', 'WK8_Master5%' in 'df' along the rows
# Assign the mean to 'Master_Percentage' in 'Weekly'
Weekly["Master_Percentage"] = df[['WK0_Master1%', 'WK6_Master2%', 'WK7_Master3%', 'WK8_Master4%', 'WK8_Master5%']].mean(axis=1)


# In[136]:


# Round all the values in the 'Weekly' DataFrame to two decimal places
Weekly = Weekly.round(2)


# In[137]:


# Assuming that 'Weekly' is the name of your DataFrame
Weekly.drop_duplicates(subset='Email', keep='first', inplace=True)


# In[138]:


# Assuming that 'Weekly' is the name of your DataFrame
specific_email = 'abhiakhilabhiakhil505@gmail.com'
rows_with_email = Weekly[Weekly['Email'] == specific_email]
print(rows_with_email.head())


# In[139]:


#Assuming that 'Weekly' is the name of your DataFrame
duplicated_emails = Weekly[Weekly['Email'].duplicated(keep=False)]
print(duplicated_emails)



# In[140]:


# Export the 'Weekly' DataFrame to a CSV file named 'Weekly_Attendance.csv'
Weekly.to_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Output\Weekly_Attendence.csv",mode='w',index=False)


# # Storing data to MySQL

# In[141]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="kalpana",auth_plugin='mysql_native_password')


# In[142]:


# Create a cursor object to interact with the MySQL database through the established connection
cursor = conn.cursor()


# In[143]:


for i, row in Weekly.iterrows():
    row = [None if isinstance(val, float) and math.isnan(val) else val for val in row] # replace "nan" values with None
    columns = ','.join(Weekly.columns)
    placeholders = ','.join(['%s']*len(row))
    # Construct the INSERT query with ON DUPLICATE KEY UPDATE clause
    query = f"INSERT INTO kalpana.09_weekly_attendence ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE "
    query += ", ".join([f"{col}=VALUES({col})" for col in Weekly.columns if col != 'email'])
    # Execute the query
    cursor.execute(query, tuple(row))


# In[144]:


# Commit the changes made through the database connection to make them permanent
conn.commit()

