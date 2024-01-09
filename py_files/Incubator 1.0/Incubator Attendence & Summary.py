#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing Required modules
import pandas as pd
import mysql.connector as msql
import math
import datetime


# In[2]:


# Displaying all columns and rows
pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)


# In[3]:


#Reading She for STEM Incubator file present on source files
data= pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Kalpana SHE for STEM Incubator_July 22 Batch.csv")


# In[4]:


data.head()


# In[5]:


# Dropping unwanted columns
data.drop(data.columns[[0,23,24,26,28,31,33,34,36,38,40,42,43,44,47,48,50,51,53,55,57,59,60,63,64,65,67,69,71,73,75,77,78,79,82,83,84,87,88,89,90,92,94,96,98,100,102,104,107,109,111,113,115,116,117,119,121,123,125,127,128,129,130,133,134,136,138,139,141,143,145,146,147,148,149,151,152,153,154,156,157,158]],axis=1,inplace=True)


# In[6]:


# Changing headers of columns
data.columns=['Last Login','Course Name', 'Name', 'Email', 'Segment', 'Credits',
       'Assigned via Package', 'Mobile', 'Annual Family Income',
       'How did you hear about us ?', 'Gender',
       'Where do you live now (for internal purposes)?', 'Date_of_birth',
       'What is your Subject Area?', 'Enroll Date', 'Valid Till',
       'Assigned Through', 'Start Date', 'Time Spent(mins)', 'Progress',
       'Introduce_yourself', 'Kalpana_3_Live', 'WK0_V1', 'WK0_V2', 'WK0_SUK_V',
       'WK0_Master1', 'WK0_Master2', 'WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4',
       'WK1_SUK_V', 'WK1_Master', 'WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4',
       'WK2_V5', 'WK2_SUK_V', 'WK2_Master', 'WK3_V1', 'WK3_V2', 'WK3_V3',
       'WK3_V4', 'WK3_V5', 'WK3_V6', 'WK3_SUK_V', 'WK3_Master', 'WK4_SUK_V',
       'WK4_Master', 'WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5',
       'WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9', 'WK5_SUK_V', 'WK5_V1', 'WK5_V2',
       'WK5_V3', 'WK6_SUK_V', 'WK6_V1', 'WK6_V2', 'WK6_V3',
       'WK6_V4', 'WK7_SUK_V', "WK7_Master",'WK7_V1', 'WK7_V2','WK7_V3','WK7_V4','WK7_V5','WK8_Master1','WK8_Master2'
      ]


# In[7]:


# Checking column location for extracting only numbers
data.columns[21]


# In[8]:


# Locating columns for extracting only numbers
col1 = data.iloc[:,21:]


# In[9]:


# Code for extracting only numbers from dataset
for column in [i for i in col1.columns if col1[i].dtype == 'object']:
    data[column] = data[column].astype(str).str.extract('(\d+)').astype(float)


# In[10]:


# Adding a new column Name_lower which contain lowercase Name for all students. This is for adding SUK Live time. 
#Lowercase within Graphy sheet is done, so that matching with the Teams Attendance sheet is perfect. Team Attendance
#sheet is made lowercase manually for every live session.
data["Name_lower"]=data["Name"].str.lower()


# In[11]:


data.columns


# In[12]:


# Reading SUK Live sheets from SUK_Teams present on Kalpana Source Files
W0=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\SUK_Teams\\W0.csv")
W1=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\SUK_Teams\\W1.csv")
W2=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\SUK_Teams\\W2.csv")
W3=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\SUK_Teams\\W3.csv")
W4=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\SUK_Teams\\W4.csv")
W5=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\SUK_Teams\\W5.csv")
W6=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\SUK_Teams\\W6.csv")
W7=pd.read_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\SUK_Teams\\W7.csv")


# In[13]:


# Adding SUK live data
data=pd.merge(data,W0,on='Name_lower',how='left')
data=pd.merge(data,W1,on='Name_lower',how='left')
data=pd.merge(data,W2,on='Name_lower',how='left')
data=pd.merge(data,W3,on='Name_lower',how='left')
data=pd.merge(data,W4,on='Name_lower',how='left')
data=pd.merge(data,W5,on='Name_lower',how='left')
data=pd.merge(data,W6,on='Name_lower',how='left')
data=pd.merge(data,W7,on='Name_lower',how='left')


# In[14]:


# dropping the Name_lower column that was previously added for SUK_Live data adding key
data=data.drop("Name_lower",axis=1)


# In[15]:


# Checking column location for filling Null values with zero
data.columns[76]


# In[16]:


# Filling Null values with zero
fillna = data.iloc[:,21:]
fillnacol=fillna.columns
data[fillnacol]=data[fillnacol].fillna(0)


# # Enroll Date MySQL Table

# In[17]:


# Creating new table which for Enroll_Dates which is taken from incubator graphy sheet
Enroll=pd.DataFrame(data[['Email', 'Enroll Date']])


# In[18]:


# Extracting only Enroll_Date
Enroll[['Enroll Date','Time']]=Enroll['Enroll Date'].str.split(' ',expand=True)
Enroll=Enroll.drop(["Time"],axis=1)


# In[19]:


# rename the 'Enroll Date' column to 'Incubator'
Enroll = Enroll.rename(columns={'Enroll Date': 'Incubator'})
#Enroll = Enroll.rename(columns={'Course Name': 'Batch'})
#Enroll = Enroll.rename(columns={'Last Login': 'Course_Last_Login'})
#Enroll = Enroll.rename(columns={'Start Date': 'Course_Start_Date'})


# In[20]:


# Connecting to MySQL change database and password here
conn= msql.connect(host='localhost',user='root',password="VS@123",database="Kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[21]:


# Inserting data to 03_enroll_date table
for i,row in Enroll.iterrows():
    cursor.execute("insert IGNORE into 03_enroll_date (Email,incubator) values(%s,%s)",tuple(row))


# In[22]:


# Replacing data  with new data to 03_enroll_date table
for i,row in Enroll.iterrows():
    cursor.execute("REPLACE into 03_enroll_date (Email,incubator) values(%s,%s)",tuple(row))


# In[23]:


conn.commit()


# # Batch Login StartDate

# In[24]:


# Creating new table which for Batch_Login which is taken from incubator graphy sheet
Batch = pd.DataFrame(data[["Email", 'Course Name','Last Login','Start Date']])


# In[25]:


# Extracting only Last_Login
Batch[['Last Login','Time']]=Batch['Last Login'].str.split(' ',expand=True)
Batch=Batch.drop(["Time"],axis=1)


# In[26]:


Batch = Batch.rename(columns={'Course Name': 'Batch'})
Batch = Batch.rename(columns={'Last Login': 'Course_last_login'})
Batch = Batch.rename(columns={'Start Date': 'Course_start_date'})


# In[27]:


Batch.head()


# In[28]:


# Connecting to MySQL change database and password here
conn= msql.connect(host='localhost',user='root',password="VS@123",database="Kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[29]:


# Inserting data to 03_enroll_date table
for i,row in Batch.iterrows():
    cursor.execute("insert IGNORE into 16_batch_lastlogin_startdate (Email,Batch,Course_last_login,Course_start_date) values(%s,%s,%s,%s)",tuple(row))


# In[30]:


# Replacing data  with new data to 03_enroll_date table
for i,row in Batch.iterrows():
    cursor.execute("REPLACE into 16_batch_lastlogin_startdate (Email,Batch,Course_last_login,Course_start_date) values(%s,%s,%s,%s)",tuple(row))


# In[31]:


conn.commit()


# # Payment details MySQL table

# In[32]:


# Creating new table which for Payment which is taken from incubator graphy sheet
Payment=pd.DataFrame(data[["Email","Assigned Through"]])


# In[33]:


# Extracting only Payment removing Order Id
Payment[["Assigned Through","Order_ID"]]=Payment["Assigned Through"].str.split("-",expand=True)
Payment.drop(["Order_ID"],axis=1,inplace=True)


# In[34]:


# Assigning fee cost to different category of enrollment
Payment["Assigned Through"]=Payment["Assigned Through"].replace(['Excel Upload'],'0')
Payment["Assigned Through"]=Payment["Assigned Through"].replace(['Paid Transaction '],'1899')


# In[35]:


# Connecting to MySQL Database
conn= msql.connect(host='localhost',user='root',password="VS@123",database="Kalpana",auth_plugin='mysql_native_password')
cursor =conn.cursor()


# In[36]:


# Inserting data into 02_payment_details 
for i,row in Payment.iterrows():
    cursor.execute("insert Ignore into 02_payment_details (Email,Incubator_Fee) values(%s,%s)",tuple(row))


# In[37]:


# Replacing data with new data into 02_payment_details 
for i,row in Payment.iterrows():
    cursor.execute("REPLACE into 02_payment_details (Email,Incubator_Fee) values(%s,%s)",tuple(row))


# In[38]:


conn.commit()


# # Changing seconds to Percentage

# In[39]:


data1=pd.DataFrame()
data.head()


# In[40]:


data1['WK0_V1'] = (data['WK0_V1']*100)/291


# In[41]:


data1["WK0_V2"]= (data["WK0_V2"])*100/287


# In[42]:


data1["WK0_SUK_V"] = (data["WK0_SUK_V"]*100)/3600


# In[43]:


data1["WK0_SUK_Live"] = (data["WK0_SUK_Live"]*100)/3600


# In[44]:


data1["WK0_Master1"] = (data["WK0_Master1"]*100)/3600


# In[45]:


data1["WK0_Master2"] = (data["WK0_Master2"]*100)/3600


# In[46]:


data1["WK1_V1"] = (data["WK1_V1"]*100)/141


# In[47]:


data1["WK1_V2"] = (data["WK1_V2"]*100)/453


# In[48]:


data1["WK1_V3"] = (data["WK1_V3"]*100)/347


# In[49]:


data1["WK1_V4"] = (data["WK1_V4"]*100)/441


# In[50]:


data1["WK1_SUK_V"] = (data["WK1_SUK_V"]*100)/3600


# In[51]:


data1["WK1_SUK_Live"] = (data["WK1_SUK_Live"]*100)/3600


# In[52]:


data1["WK1_Master"] = (data["WK1_Master"]*100)/3600


# In[53]:


data1["WK2_V1"] = (data["WK2_V1"]*100)/345


# In[54]:


data1["WK2_V2"] = (data["WK2_V2"]*100)/239


# In[55]:


data1["WK2_V3"] = (data["WK2_V3"]*100)/290


# In[56]:


data1["WK2_V4"] = (data["WK2_V4"]*100)/296


# In[57]:


data1["WK2_V5"] = (data["WK2_V5"]*100)/291


# In[58]:


data1["WK2_SUK_V"] = (data["WK2_SUK_V"]*100)/3600


# In[59]:


data1["WK2_SUK_Live"] = (data["WK2_SUK_Live"]*100)/3600


# In[60]:


data1["WK2_Master"] = (data["WK2_Master"]*100)/3600


# In[61]:


data1["WK3_V1"] = (data["WK3_V1"]*100)/428


# In[62]:


data1["WK3_V2"] = (data["WK3_V2"]*100)/650


# In[63]:


data1["WK3_V3"] = (data["WK3_V3"]*100)/309


# In[64]:


data1["WK3_V4"] = (data["WK3_V4"]*100)/463


# In[65]:


data1["WK3_V5"] = (data["WK3_V5"]*100)/560


# In[66]:


data1["WK3_V6"] = (data["WK3_V6"]*100)/496


# In[67]:


data1["WK3_SUK_V"] = (data["WK3_SUK_V"]*100)/3600


# In[68]:


data1["WK3_SUK_Live"] = (data["WK3_SUK_Live"]*100)/3600


# In[69]:


data1["WK3_Master"] = (data["WK3_Master"]*100)/3600


# In[70]:


data1["WK4_SUK_V"] = (data["WK4_SUK_V"]*100)/3600


# In[71]:


data1["WK4_SUK_Live"] = (data["WK4_SUK_Live"]*100)/3600


# In[72]:


data1["WK4_Master"] = (data["WK4_Master"]*100)/3600


# In[73]:


data1["WK4_V1"] = (data["WK4_V1"]*100)/286


# In[74]:


data1["WK4_V2"] = (data["WK4_V2"]*100)/265


# In[75]:


data1["WK4_V3"] = (data["WK4_V3"]*100)/321


# In[76]:


data1["WK4_V4"] = (data["WK4_V4"]*100)/120


# In[77]:


data1["WK4_V5"] = (data["WK4_V5"]*100)/154


# In[78]:


data1["WK4_V6"] = (data["WK4_V6"]*100)/115


# In[79]:


data1["WK4_V7"] = (data["WK4_V7"]*100)/154


# In[80]:


data1["WK4_V8"] = (data["WK4_V8"]*100)/124


# In[81]:


data1["WK4_V9"] = (data["WK4_V9"]*100)/203


# In[82]:


data1["WK5_SUK_V"] = (data["WK5_SUK_V"]*100)/3600


# In[83]:


data1["WK5_SUK_Live"] = (data["WK5_SUK_Live"]*100)/3600


# In[84]:


data1["WK5_V1"] = (data["WK5_V1"]*100)/114


# In[85]:


data1["WK5_V2"] = (data["WK5_V2"]*100)/327


# In[86]:


data1["WK5_V3"] = (data["WK5_V3"]*100)/131


# In[87]:


data1["WK6_SUK_V"] = (data["WK6_SUK_V"]*100)/3600


# In[88]:


data1["WK6_SUK_Live"] = (data["WK6_SUK_Live"]*100)/3600


# In[89]:


data1["WK6_V1"] = (data["WK6_V1"]*100)/259


# In[90]:


data1["WK6_V2"] = (data["WK6_V2"]*100)/432


# In[91]:


data1["WK6_V3"] = (data["WK6_V3"]*100)/261


# In[92]:


data1["WK6_V4"] = (data["WK6_V4"]*100)/515


# In[93]:


data1["WK7_SUK_V"] = (data["WK7_SUK_V"]*100)/3600


# In[94]:


data1["WK7_SUK_Live"] = (data["WK7_SUK_Live"]*100)/3600


# In[95]:


data1["WK7_Master"] = (data["WK7_Master"]*100)/3600


# In[96]:


data1["WK7_V1"] = (data["WK7_V1"]*100)/228


# In[97]:


data1["WK7_V2"] = (data["WK7_V2"]*100)/214


# In[98]:


data1["WK7_V3"] = (data["WK7_V3"]*100)/110


# In[99]:


data1["WK7_V4"] = (data["WK7_V4"]*100)/170


# In[100]:


data1["WK7_V5"] = (data["WK7_V5"]*100)/561


# In[101]:


data1["WK8_Master1"] = (data["WK8_Master1"]*100)/3600


# In[102]:


data1["WK8_Master2"] = (data["WK8_Master2"]*100)/3600


# In[103]:


data1["WK0_SUK"]=data1[["WK0_SUK_Live","WK0_SUK_V"]].max(axis=1)


# In[104]:


data1["WK1_SUK"]=data1[["WK1_SUK_Live","WK1_SUK_V"]].max(axis=1)


# In[105]:


data1["WK2_SUK"]=data1[["WK2_SUK_Live","WK2_SUK_V"]].max(axis=1)


# In[106]:


data1["WK3_SUK"]=data1[["WK3_SUK_Live","WK3_SUK_V"]].max(axis=1)


# In[107]:


data1["WK4_SUK"]=data1[["WK4_SUK_Live","WK4_SUK_V"]].max(axis=1)


# In[108]:


data1["WK5_SUK"]=data1[["WK5_SUK_Live","WK5_SUK_V"]].max(axis=1)


# In[109]:


data1["WK6_SUK"]=data1[["WK6_SUK_Live","WK6_SUK_V"]].max(axis=1)


# In[110]:


data1["WK7_SUK"]=data1[["WK7_SUK_Live","WK7_SUK_V"]].max(axis=1)


# In[111]:


# Max of percent in week0 master
data1["WK0_Master"]=data1[["WK0_Master1","WK0_Master2"]].max(axis=1)


# In[112]:


# Creating a function "Max_Value" for converting % max 100 if values are above 100
def Max_Value(value):
    if value >=100:
        return 100
    else:
        return value


# In[113]:


# Applying Max_Value function
per1=data1.iloc[:,0:]
per1c=per1.columns
data1[per1c]=data1[per1c].applymap(Max_Value)


# # Code for converting seconds to hours

# In[114]:


# Creating a function for converting seconds to hours
def Convert_Hours(seconds):
    hours = seconds / (3600)
    return hours
n = 5400
print(Convert_Hours(n))


# In[115]:


data.columns[21]


# In[116]:


# Applying Convert_Hours function
col1 = data.iloc[:,21:]
col=col1.columns
data[col]=data[col].apply(Convert_Hours)


# # Max of WK0 Master and SUK Live & SUK Master

# In[117]:


data["WK0_Master"]=data[["WK0_Master1","WK0_Master2"]].max(axis=1)


# In[118]:


data["WK0_SUK"]=data[["WK0_SUK_Live","WK0_SUK_V"]].max(axis=1)


# In[119]:


data["WK1_SUK"]=data[["WK1_SUK_Live","WK1_SUK_V"]].max(axis=1)


# In[120]:


data["WK2_SUK"]=data[["WK2_SUK_Live","WK2_SUK_V"]].max(axis=1)


# In[121]:


data["WK3_SUK"]=data[["WK3_SUK_Live","WK3_SUK_V"]].max(axis=1)


# In[122]:


data["WK4_SUK"]=data[["WK4_SUK_Live","WK4_SUK_V"]].max(axis=1)


# In[123]:


data["WK5_SUK"]=data[["WK5_SUK_Live","WK5_SUK_V"]].max(axis=1)


# In[124]:


data["WK6_SUK"]=data[["WK6_SUK_Live","WK6_SUK_V"]].max(axis=1)


# In[125]:


data["WK7_SUK"]=data[["WK7_SUK_Live","WK7_SUK_V"]].max(axis=1)


# # Code for Recorded Videos: Total hours, Average & Percentage

# In[126]:


Recorded_Total=data[['WK0_V1', 'WK0_V2',
        'WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4',
        'WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4','WK2_V5', 
        'WK3_V1', 'WK3_V2', 'WK3_V3','WK3_V4', 'WK3_V5', 'WK3_V6',
        'WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5','WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9',
        'WK5_V1', 'WK5_V2','WK5_V3', 
        'WK6_V1', 'WK6_V2', 'WK6_V3','WK6_V4',
        'WK7_V1', 'WK7_V2', 'WK7_V3','WK7_V4', 'WK7_V5']]


# In[127]:


data["Recorded_Total"]=Recorded_Total.sum(axis=1)


# In[128]:


data["Recorded_Average"]=Recorded_Total.mean(axis=1)


# In[129]:


Recorded_Percent=data1[['WK0_V1', 'WK0_V2',
        'WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4',
        'WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4','WK2_V5', 
        'WK3_V1', 'WK3_V2', 'WK3_V3','WK3_V4', 'WK3_V5', 'WK3_V6',
        'WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5','WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9',
        'WK5_V1', 'WK5_V2','WK5_V3', 
        'WK6_V1', 'WK6_V2', 'WK6_V3','WK6_V4',
        'WK7_V1', 'WK7_V2', 'WK7_V3','WK7_V4', 'WK7_V5']]


# In[130]:


data["Recorded_Percentage"]=Recorded_Percent.mean(axis=1)


# # Code for SUK Live: Total hours, Average & Percentage

# In[131]:


SUK_Live=data[['WK0_SUK_Live','WK1_SUK_Live', 'WK2_SUK_Live', 'WK3_SUK_Live',
       'WK4_SUK_Live', 'WK5_SUK_Live','WK6_SUK_Live','WK7_SUK_Live']]


# In[132]:


data["SUK_Live_Total"]=SUK_Live.sum(axis=1)


# In[133]:


data["SUK_Live_Average"]=SUK_Live.mean(axis=1)


# In[134]:


SUK_Live_Percent=data1[[ 'WK0_SUK_Live', 'WK1_SUK_Live', 'WK2_SUK_Live', 'WK3_SUK_Live',
       'WK4_SUK_Live', 'WK5_SUK_Live','WK6_SUK_Live','WK7_SUK_Live']]


# In[135]:


data["SUK_Live_Percentage"]=SUK_Live_Percent.mean(axis=1)


# In[136]:


SUK_MAX_Percent=data[[ 'WK0_SUK', 'WK1_SUK', 'WK2_SUK', 'WK3_SUK',
       'WK4_SUK', 'WK5_SUK','WK6_SUK','WK7_SUK']]


# In[137]:


data["SUK_Max_Percentage"]=SUK_MAX_Percent.mean(axis=1)


# # Code for SUK Recorded Videos: Total hours, Average & Percentage

# In[138]:


SUK_Recorded_Total=data[['WK0_SUK_V','WK1_SUK_V',"WK2_SUK_V",'WK3_SUK_V','WK4_SUK_V','WK5_SUK_V','WK6_SUK_V','WK7_SUK_V']]


# In[139]:


data["SUK_Recorded"]=SUK_Recorded_Total.sum(axis=1)


# In[140]:


data["SUK_Recorded_Average"]=SUK_Recorded_Total.mean(axis=1)


# In[141]:


SUK_Recorded_Percent=data1[['WK0_SUK_V','WK1_SUK_V',"WK2_SUK_V",'WK3_SUK_V','WK4_SUK_V','WK5_SUK_V','WK6_SUK_V','WK7_SUK_V']]


# In[142]:


data["SUK_Recorded_Percentage"]=SUK_Recorded_Percent.mean(axis=1)


# # Code for Master Class: Total hours, Average & Percentage

# In[143]:


Masterclass_Total = data[["WK0_Master1",'WK0_Master2',"WK1_Master",'WK2_Master','WK3_Master',
                          'WK4_Master','WK7_Master','WK8_Master1','WK8_Master2']]


# In[144]:


data["Masterclass_Total"]=Masterclass_Total.sum(axis=1)


# In[145]:


Masterclass_Average = data[["WK0_Master","WK1_Master",'WK2_Master','WK3_Master',
                          'WK4_Master','WK7_Master','WK8_Master1','WK8_Master2']]


# In[146]:


data["Masterclass_Average"]=Masterclass_Average.mean(axis=1)


# In[147]:


Masterclass_Percent=data1[["WK0_Master","WK1_Master",'WK2_Master','WK3_Master','WK4_Master','WK7_Master','WK8_Master1','WK8_Master2']]


# In[148]:


data["Masterclass_Percentage"]=Masterclass_Percent.mean(axis=1)


# # Code for Program: Total hours, Average & Percentage

# In[149]:


Whole_Program_Total=data[['WK0_V1', 'WK0_V2',
        'WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4',
        'WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4','WK2_V5',
        'WK3_V1', 'WK3_V2', 'WK3_V3','WK3_V4', 'WK3_V5', 'WK3_V6',
        'WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5','WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9', 
        'WK5_V1', 'WK5_V2','WK5_V3',
        'WK6_V1', 'WK6_V2', 'WK6_V3','WK6_V4',
        'WK7_V1', 'WK7_V2', 'WK7_V3','WK7_V4', 'WK7_V5',
        'WK0_SUK_V',"WK0_SUK_Live",'WK1_SUK_V',"WK1_SUK_Live","WK2_SUK_V","WK2_SUK_Live",
        'WK3_SUK_V','WK3_SUK_Live','WK4_SUK_V','WK4_SUK_Live','WK5_SUK_V','WK5_SUK_Live',
        'WK6_SUK_V','WK6_SUK_Live','WK7_SUK_V','WK7_SUK_Live',
        "WK0_Master1",'WK0_Master2',"WK1_Master",'WK2_Master','WK3_Master','WK4_Master','WK7_Master','WK8_Master1','WK8_Master2']]


# In[150]:


data["Program_Total"]=Whole_Program_Total.sum(axis=1)


# In[151]:


Whole_Program_Average=data[['WK0_V1', 'WK0_V2',
        'WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4',
        'WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4','WK2_V5', 
        'WK3_V1', 'WK3_V2', 'WK3_V3','WK3_V4', 'WK3_V5', 'WK3_V6',
        'WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5','WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9',
        'WK5_V1', 'WK5_V2','WK5_V3', 
        'WK6_V1', 'WK6_V2', 'WK6_V3','WK6_V4',
        'WK7_V1', 'WK7_V2', 'WK7_V3','WK7_V4', 'WK7_V5',
        'WK0_SUK','WK1_SUK',"WK2_SUK",'WK3_SUK','WK4_SUK','WK5_SUK','WK6_SUK','WK7_SUK',
        "WK0_Master",'WK1_Master','WK2_Master','WK3_Master','WK4_Master','WK7_Master','WK8_Master1','WK8_Master2']]


# In[152]:


data["Program_Average"]=Whole_Program_Average.mean(axis=1)


# In[153]:


Whole_Program_Percent=data1[['WK0_V1', 'WK0_V2',
        'WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4',
        'WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4','WK2_V5', 
        'WK3_V1', 'WK3_V2', 'WK3_V3','WK3_V4', 'WK3_V5', 'WK3_V6',
        'WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5','WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9',
        'WK5_V1', 'WK5_V2','WK5_V3', 
        'WK6_V1', 'WK6_V2', 'WK6_V3','WK6_V4',
        'WK7_V1', 'WK7_V2', 'WK7_V3','WK7_V4', 'WK7_V5',
        'WK0_SUK','WK1_SUK',"WK2_SUK",'WK3_SUK','WK4_SUK','WK5_SUK','WK6_SUK','WK7_SUK',
        "WK0_Master",'WK1_Master','WK2_Master','WK3_Master','WK4_Master','WK7_Master','WK8_Master1','WK8_Master2']]


# In[154]:


data["Program_percentage"]=Whole_Program_Percent.mean(axis=1)


# In[155]:


data=data.round(2)


# # Adding phone numbers and student status of students from wati sheet

# In[156]:


Wati=pd.read_excel("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Source File\\Wati Contact Management.xlsx",usecols=["Email","Phone",'Student Status'])


# In[157]:


Wat=Wati.loc[Wati["Student Status"]=='Incubator']


# In[158]:


data=pd.merge(data,Wat,on='Email',how='left')


# In[159]:


data.head()


# In[160]:


#  Selecting desired columns
Kalpana=data[['Email','Name','Segment','Mobile','Annual Family Income','How did you hear about us ?','Gender','Where do you live now (for internal purposes)?','Date_of_birth','What is your Subject Area?', 'Assigned Through','Start Date','WK0_SUK_V', 'WK0_SUK_Live','WK0_V1', 'WK0_V2', 'WK0_Master1','WK0_Master2','WK1_V1', 'WK1_V2','WK1_V3', 'WK1_V4', 'WK1_SUK_V', 'WK1_SUK_Live','WK1_Master','WK2_V1', 'WK2_V2','WK2_V3','WK2_V4', 'WK2_V5', 'WK2_SUK_V','WK2_SUK_Live','WK2_Master','WK3_V1', 'WK3_V2', 'WK3_V3', 'WK3_V4','WK3_V5','WK3_V6', 'WK3_SUK_V','WK3_SUK_Live','WK3_Master', 'WK4_V1','WK4_V2', 'WK4_V3', 'WK4_V4','WK4_V5', 'WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9', 'WK4_SUK_V', 'WK4_SUK_Live','WK4_Master','WK5_V1','WK5_V2', 'WK5_V3', 'WK5_SUK_V','WK5_SUK_Live', 'WK6_V1', 'WK6_V2', 'WK6_V3', 'WK6_V4','WK6_SUK_V','WK6_SUK_Live','WK7_V1', 'WK7_V2', 'WK7_V3', 'WK7_V4','WK7_V5','WK7_SUK_V','WK7_SUK_Live', 'WK7_Master', 'WK8_Master1', 'WK8_Master2','Recorded_Total','Recorded_Average','Recorded_Percentage','SUK_Live_Total','SUK_Live_Average','SUK_Live_Percentage', 'SUK_Recorded_Average','SUK_Recorded_Percentage','Masterclass_Total','Masterclass_Average', 'Masterclass_Percentage','Program_Total','Program_Average' ,'Program_percentage']]


# In[161]:


# Changing columns name
Kalpana.columns=[['Email','Name','Segment','Phone','Annual_Family_Income','How_did_you_hear_about_us','Gender','Where_do_you_live_now','Date_of_birth','What_is_your_Subject_Area','Assigned_Through','Start_Date','WK0_SUK_V', 'WK0_SUK_Live','WK0_V1', 'WK0_V2', 'WK0_Master1','WK0_Master2', 'WK1_V1', 'WK1_V2','WK1_V3', 'WK1_V4', 'WK1_SUK_V', 'WK1_SUK_Live', 'WK1_Master','WK2_V1', 'WK2_V2','WK2_V3','WK2_V4', 'WK2_V5', 'WK2_SUK_V','WK2_SUK_Live','WK2_Master','WK3_V1', 'WK3_V2', 'WK3_V3', 'WK3_V4','WK3_V5','WK3_V6', 'WK3_SUK_V','WK3_SUK_Live','WK3_Master', 'WK4_V1','WK4_V2', 'WK4_V3', 'WK4_V4','WK4_V5', 'WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9', 'WK4_SUK_V', 'WK4_SUK_Live','WK4_Master', 'WK5_V1','WK5_V2', 'WK5_V3', 'WK5_SUK_V','WK5_SUK_Live','WK6_V1', 'WK6_V2', 'WK6_V3', 'WK6_V4','WK6_SUK_V','WK6_SUK_Live','WK7_V1', 'WK7_V2', 'WK7_V3', 'WK7_V4','WK7_V5','WK7_SUK_V','WK7_SUK_Live', 'WK7_Master', 'WK8_Master1', 'WK8_Master2','Recorded_Total','Recorded_Average','Recorded_Percentage','SUK_Live_Total','SUK_Live_Average','SUK_Live_Percentage', 'SUK_Recorded_Average','SUK_Recorded_Percentage','Masterclass_Total','Masterclass_Average', 'Masterclass_Percentage','Program_Total','Program_Average' ,'Program_percentage']]


# In[162]:


Kalpana.to_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Output\\Incubator_and_attendence_monitoring.csv",index=False)


# In[163]:


Kalpana=Kalpana.fillna('')


# # Storing data on MySQL

# In[164]:


conn= msql.connect(host='localhost',user='root',password="VS@123",database="Kalpana",auth_plugin='mysql_native_password')


# In[165]:


cursor =conn.cursor()


# In[166]:


# insert data into 08_Incubator_and_attendence_monitoring
for i,row in Kalpana.iterrows():
    cursor.execute("insert ignore into 08_Incubator_and_attendence_monitoring(Email,Name,Segment,Phone,Annual_Family_Income,How_did_you_hear_about_us,Gender,Where_do_you_live_now,Date_of_birth,What_is_your_Subject_Area,Assigned_Through,Start_Date,WK0_SUK_V, WK0_SUK_Live,WK0_V1, WK0_V2, WK0_Master1, WK0_Master2, WK1_V1, WK1_V2, WK1_V3, WK1_V4, WK1_SUK_V, WK1_SUK_Live,WK1_Master, WK2_V1, WK2_V2, WK2_V3,WK2_V4, WK2_V5, WK2_SUK_V,WK2_SUK_Live,WK2_Master,WK3_V1, WK3_V2, WK3_V3, WK3_V4,WK3_V5,WK3_V6, WK3_SUK_V, WK3_SUK_Live,WK3_Master, WK4_V1, WK4_V2, WK4_V3, WK4_V4,WK4_V5, WK4_V6, WK4_V7, WK4_V8, WK4_V9, WK4_SUK_V, WK4_SUK_Live, WK4_Master, WK5_V1,WK5_V2, WK5_V3, WK5_SUK_V, WK5_SUK_Live, WK6_V1, WK6_V2, WK6_V3, WK6_V4, WK6_SUK_V, WK6_SUK_Live, WK7_V1, WK7_V2, WK7_V3, WK7_V4, WK7_V5,WK7_SUK_V,WK7_SUK_Live, WK7_Master, WK8_Master1, WK8_Master2,Recorded_Total,Recorded_Average,Recorded_Percentage,SUK_Live_Total,SUK_Live_Average,SUK_Live_Percentage, SUK_Recorded_Average,SUK_Recorded_Percentage,Masterclass_Total,Masterclass_Average, Masterclass_Percentage,Program_Total,Program_Average ,Program_percentage)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s, %s, %s, %s, %s,%s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s,%s, %s,%s, %s, %s, %s, %s,%s,%s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s,%s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s)",tuple(row))


# In[167]:


# Replacing data into 08_Incubator_and_attendence_monitoring with new data
for i,row in Kalpana.iterrows():
    cursor.execute("Replace into 08_Incubator_and_attendence_monitoring(Email,Name,Segment,Phone,Annual_Family_Income,How_did_you_hear_about_us,Gender,Where_do_you_live_now,Date_of_birth,What_is_your_Subject_Area,Assigned_Through,Start_Date,WK0_SUK_V, WK0_SUK_Live,WK0_V1, WK0_V2, WK0_Master1, WK0_Master2, WK1_V1, WK1_V2, WK1_V3, WK1_V4, WK1_SUK_V, WK1_SUK_Live,WK1_Master, WK2_V1, WK2_V2, WK2_V3,WK2_V4, WK2_V5, WK2_SUK_V,WK2_SUK_Live,WK2_Master,WK3_V1, WK3_V2, WK3_V3, WK3_V4,WK3_V5,WK3_V6, WK3_SUK_V, WK3_SUK_Live,WK3_Master, WK4_V1, WK4_V2, WK4_V3, WK4_V4,WK4_V5, WK4_V6, WK4_V7, WK4_V8, WK4_V9, WK4_SUK_V, WK4_SUK_Live,WK4_Master, WK5_V1,WK5_V2, WK5_V3, WK5_SUK_V, WK5_SUK_Live, WK6_V1, WK6_V2, WK6_V3, WK6_V4, WK6_SUK_V, WK6_SUK_Live,WK7_V1, WK7_V2, WK7_V3, WK7_V4, WK7_V5,WK7_SUK_V,WK7_SUK_Live, WK7_Master, WK8_Master1, WK8_Master2,Recorded_Total,Recorded_Average,Recorded_Percentage,SUK_Live_Total,SUK_Live_Average,SUK_Live_Percentage, SUK_Recorded_Average,SUK_Recorded_Percentage,Masterclass_Total,Masterclass_Average, Masterclass_Percentage,Program_Total,Program_Average ,Program_percentage)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s,%s,%s, %s, %s, %s, %s,%s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s,%s, %s,%s, %s, %s, %s, %s,%s,%s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s,%s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s)",tuple(row))


# In[168]:


conn.commit()


# In[169]:


'''# Inserting data into 08_Incubator_and_attendence_monitoring
for i,row in Kalpana.iterrows():
    cursor.execute("insert Ignore into 08_Incubator_and_attendence_monitoring(Email,Name,Segment,Batch,Phone,Annual_Family_Income,How_did_you_hear_about_us,Gender,Where_do_you_live_now,Date_of_birth,What_is_your_Subject_Area,Assigned_Through,Start_Date,WK0_V1,WK0_V2,WK0_SUK_V,WK0_SUK_Live,WK0_Master1,WK0_Master2,WK1_V1,WK1_V2,WK1_V3,WK1_V4,WK1_SUK_V,WK1_SUK_Live,WK1_Master,WK2_V1,WK2_V2,WK2_V3,WK2_V4,WK2_V5,WK2_SUK_V,WK2_SUK_Live,WK2_Master,WK3_V1,WK3_V2,WK3_V3,WK3_V4,WK3_V5,WK3_V6,WK3_SUK_V,WK3_SUK_Live,WK3_Master,WK4_SUK_V,WK4_SUK_Live,WK4_Master,WK4_V1,WK4_V2,WK4_V3,WK4_V4,WK4_V5,WK4_V6,WK4_V7,WK4_V8,WK4_V9,WK5_SUK_V,WK5_SUK_Live,WK5_V1,WK5_V2,WK5_V3,WK6_SUK_V,WK6_SUK_Live,WK6_V1,WK6_V2,WK6_V3,WK6_V4,WK7_SUK_V,WK7_SUK_Live,WK7_Master,WK7_V1,WK7_V2,WK7_V3,WK7_V4,WK7_V5,WK8_Master1,WK8_Master2,Recorded_Videos_Total_WatchTime_Per_Week_Hours,Recorded_Videos_Average_WatchTime_per_Week_Hours_per_Week,Recorded_Videos_Percentage_WatchTime_per_Week,SUK_Live_Total_WatchTime_Per_Week_Hour,SUK_Live_Average_WatchTime_per_Week,SUK_Live_Percentage_WatchTime_per_Week,SUK_Sessions_Total_WatchTime_Per_Week_Hours,SUK_Sessions_Average_WatchTime_per_Week_Hours,SUK_Sessions_Percentage_WatchTime_per_Week,MasterClass_Total_Watch_Time_Per_Week_Hours,MasterClass_Average_WatchTime_per_Week_Hours,MasterClass_Percentage_WatchTime_per_Week,Program_Total_WatchTime_Per_Week_Hours,Program_Average_WatchTime_per_Week_Hours,Program_Percentage_WatchTime_per_Week)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s,%s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s,%s)",tuple(row))'''


# In[170]:


'''# Replacing data into 08_Incubator_and_attendence_monitoring with new data
for i,row in Kalpana.iterrows():
    cursor.execute("Replace into 08_Incubator_and_attendence_monitoring(Email,Name,Segment,Batch,Phone,Annual_Family_Income,How_did_you_hear_about_us,Gender,Where_do_you_live_now,Date_of_birth,What_is_your_Subject_Area,Assigned_Through,Start_Date,WK0_V1,WK0_V2,WK0_SUK_V,WK0_SUK_Live,WK0_Master1,WK0_Master2,WK1_V1,WK1_V2,WK1_V3,WK1_V4,WK1_SUK_V,WK1_SUK_Live,WK1_Master,WK2_V1,WK2_V2,WK2_V3,WK2_V4,WK2_V5,WK2_SUK_V,WK2_SUK_Live,WK2_Master,WK3_V1,WK3_V2,WK3_V3,WK3_V4,WK3_V5,WK3_V6,WK3_SUK_V,WK3_SUK_Live,WK3_Master,WK4_SUK_V,WK4_SUK_Live,WK4_Master,WK4_V1,WK4_V2,WK4_V3,WK4_V4,WK4_V5,WK4_V6,WK4_V7,WK4_V8,WK4_V9,WK5_SUK_V,WK5_SUK_Live,WK5_V1,WK5_V2,WK5_V3,WK6_SUK_V,WK6_SUK_Live,WK6_V1,WK6_V2,WK6_V3,WK6_V4,WK7_SUK_V,WK7_SUK_Live,WK7_Master,WK7_V1,WK7_V2,WK7_V3,WK7_V4,WK7_V5,WK8_Master1,WK8_Master2,Recorded_Videos_Total_WatchTime_Per_Week_Hours,Recorded_Videos_Average_WatchTime_per_Week_Hours_per_Week,Recorded_Videos_Percentage_WatchTime_per_Week,SUK_Live_Total_WatchTime_Per_Week_Hour,SUK_Live_Average_WatchTime_per_Week,SUK_Live_Percentage_WatchTime_per_Week,SUK_Sessions_Total_WatchTime_Per_Week_Hours,SUK_Sessions_Average_WatchTime_per_Week_Hours,SUK_Sessions_Percentage_WatchTime_per_Week,MasterClass_Total_Watch_Time_Per_Week_Hours,MasterClass_Average_WatchTime_per_Week_Hours,MasterClass_Percentage_WatchTime_per_Week,Program_Total_WatchTime_Per_Week_Hours,Program_Average_WatchTime_per_Week_Hours,Program_Percentage_WatchTime_per_Week)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s,%s, %s, %s,%s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s,%s, %s, %s, %s,%s,%s)",tuple(row))'''


# In[ ]:





# In[ ]:





# In[ ]:




