#!/usr/bin/env python
# coding: utf-8

# In[1]:


# importing required modules
import pandas as pd
import mysql.connector as msql


# In[2]:


#display all columns and rows
pd.set_option('display.max_columns',None)
pd.set_option('display.max_rows',None)


# In[3]:


# Reading Incubator_and_attendence_monitoring from Outout files
data=pd.read_csv("C:\\Users\\HP\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Output\\Incubator_and_attendence_monitoring.csv")


# # Calculating each video Percentage and storing it into df

# In[4]:


df=pd.DataFrame(data["Email"])


# In[5]:


df['WK0_V1%'] = (data['WK0_V1']*100)/(291/3600)


# In[6]:


df["WK0_V2%"]= (data["WK0_V2"])*100/(287/3600)


# In[7]:


df["WK0_SUK_V%"] = (data["WK0_SUK_V"]*100)/(3600/3600)


# In[8]:


df["WK0_SUK_Live%"] = (data["WK0_SUK_Live"]*100)/(3600/3600)


# In[9]:


df["WK0_Master1%"] = (data["WK0_Master1"]*100)/(3600/3600)


# In[10]:


df["WK0_Master2%"] = (data["WK0_Master2"]*100)/(3600/3600)


# In[11]:


df["WK1_V1%"] = (data["WK1_V1"]*100)/(141/3600)


# In[12]:


df["WK1_V2%"] = (data["WK1_V2"]*100)/(453/3600)


# In[13]:


df["WK1_V3%"] = (data["WK1_V3"]*100)/(347/3600)


# In[14]:


df["WK1_V4%"] = (data["WK1_V4"]*100)/(441/3600)


# In[15]:


df["WK1_SUK_V%"] = (data["WK1_SUK_V"]*100)/(3600/3600)


# In[16]:


df["WK1_SUK_Live%"] = (data["WK1_SUK_Live"]*100)/(3600/3600)


# In[17]:


df["WK1_Master%"] = (data["WK1_Master"]*100)/(3600/3600)


# In[18]:


df["WK2_V1%"] = (data["WK2_V1"]*100)/(345/3600)


# In[19]:


df["WK2_V2%"] = (data["WK2_V2"]*100)/(239/3600)


# In[20]:


df["WK2_V3%"] = (data["WK2_V3"]*100)/(290/3600)


# In[21]:


df["WK2_V4%"] = (data["WK2_V4"]*100)/(296/3600)


# In[22]:


df["WK2_V5%"] = (data["WK2_V5"]*100)/(291/3600)


# In[23]:


df["WK2_SUK_V%"] = (data["WK2_SUK_V"]*100)/(3600/3600)


# In[24]:


df["WK2_SUK_Live%"] = (data["WK2_SUK_Live"]*100)/(3600/3600)


# In[25]:


df["WK2_Master%"] = (data["WK2_Master"]*100)/(3600/3600)


# In[26]:


df["WK3_V1%"] = (data["WK3_V1"]*100)/(428/3600)


# In[27]:


df["WK3_V2%"] = (data["WK3_V2"]*100)/(650/3600)


# In[28]:


df["WK3_V3%"] = (data["WK3_V3"]*100)/(309/3600)


# In[29]:


df["WK3_V4%"] = (data["WK3_V4"]*100)/(463/3600)


# In[30]:


df["WK3_V5%"] = (data["WK3_V5"]*100)/(560/3600)


# In[31]:


df["WK3_V6%"] = (data["WK3_V6"]*100)/(496/3600)


# In[32]:


df["WK3_SUK_V%"] = (data["WK3_SUK_V"]*100)/(3600/3600)


# In[33]:


df["WK3_SUK_Live%"] = (data["WK3_SUK_Live"]*100)/(3600/3600)


# In[34]:


df["WK3_Master%"] = (data["WK3_Master"]*100)/(3600/3600)


# In[35]:


df["WK4_SUK_V%"] = (data["WK4_SUK_V"]*100)/(3600/3600)


# In[36]:


df["WK4_SUK_Live%"] = (data["WK4_SUK_Live"]*100)/(3600/3600)


# In[37]:


df["WK4_Master%"] = (data["WK4_Master"]*100)/(3600/3600)


# In[38]:


df["WK4_V1%"] = (data["WK4_V1"]*100)/(286/3600)


# In[39]:


df["WK4_V2%"] = (data["WK4_V2"]*100)/(265/3600)


# In[40]:


df["WK4_V3%"] = (data["WK4_V3"]*100)/(321/3600)


# In[41]:


df["WK4_V4%"] = (data["WK4_V4"]*100)/(120/3600)


# In[42]:


df["WK4_V5%"] = (data["WK4_V5"]*100)/(154/3600)


# In[43]:


df["WK4_V6%"] = (data["WK4_V6"]*100)/(115/3600)


# In[44]:


df["WK4_V7%"] = (data["WK4_V7"]*100)/(154/3600)


# In[45]:


df["WK4_V8%"] = (data["WK4_V8"]*100)/(124/3600)


# In[46]:


df["WK4_V9%"] = (data["WK4_V9"]*100)/(203/3600)


# In[47]:


df["WK5_SUK_V%"] = (data["WK5_SUK_V"]*100)/(3600/3600)


# In[48]:


df["WK5_SUK_Live%"] = (data["WK5_SUK_Live"]*100)/(3600/3600)


# In[49]:


df["WK5_V1%"] = (data["WK5_V1"]*100)/(114/3600)


# In[50]:


df["WK5_V2%"] = (data["WK5_V2"]*100)/(327/3600)


# In[51]:


df["WK5_V3%"] = (data["WK5_V3"]*100)/(131/3600)


# In[52]:


df["WK6_SUK_V%"] = (data["WK6_SUK_V"]*100)/(3600/3600)


# In[53]:


df["WK6_SUK_Live%"] = (data["WK6_SUK_Live"]*100)/(3600/3600)


# In[54]:


df["WK6_V1%"] = (data["WK6_V1"]*100)/(259/3600)


# In[55]:


df["WK6_V2%"] = (data["WK6_V2"]*100)/(432/3600)


# In[56]:


df["WK6_V3%"] = (data["WK6_V3"]*100)/(261/3600)


# In[57]:


df["WK6_V4%"] = (data["WK6_V4"]*100)/(515/3600)


# In[58]:


df["WK7_SUK_V%"] = (data["WK7_SUK_V"]*100)/(3600/3600)


# In[59]:


df["WK7_SUK_Live%"] = (data["WK7_SUK_Live"]*100)/(3600/3600)


# In[60]:


df["WK7_Master%"] = (data["WK7_Master"]*100)/(3600/3600)


# In[61]:


df["WK7_V1%"] = (data["WK7_V1"]*100)/(228/3600)


# In[62]:


df["WK7_V2%"] = (data["WK7_V2"]*100)/(214/3600)


# In[63]:


df["WK7_V3%"] = (data["WK7_V3"]*100)/(110/3600)


# In[64]:


df["WK7_V4%"] = (data["WK7_V4"]*100)/(170/3600)


# In[65]:


df["WK7_V5%"] = (data["WK7_V5"]*100)/(561/3600)


# In[66]:


df["WK8_Master1%"] = (data["WK8_Master1"]*100)/(3600/3600)


# In[67]:


df["WK8_Master2%"] = (data["WK8_Master2"]*100)/(3600/3600)


# In[68]:


# Changing percent values greater than 100 to 100
def Max_Value(value):
    if value >=100:
        return 100
    else:
        return value


# In[69]:


col=df.iloc[:,1:]
df1=col.columns
df[df1]=df[df1].applymap(Max_Value)


# # Creating Weekly_Attendence Table

# In[70]:


Weekly=pd.DataFrame(data["Email"])


# # Weekly Recorded Total Hours

# In[71]:


WK0_Total=data[['WK0_V1','WK0_V2']]
Weekly["WK0_Recorded_Total"]=WK0_Total.sum(axis=1,skipna=True)


# In[72]:


WK1_Total=data[['WK1_V1', 'WK1_V2', 'WK1_V3', 'WK1_V4']]
Weekly["WK1_Recorded_Total"]=WK1_Total.sum(axis=1,skipna=True)


# In[73]:


WK2_Total=data[['WK2_V1', 'WK2_V2', 'WK2_V3', 'WK2_V4', 'WK2_V5']]
Weekly["WK2_Recorded_Total"]=WK2_Total.sum(axis=1,skipna=True)


# In[74]:


WK3_Total=data[['WK3_V1', 'WK3_V2', 'WK3_V3', 'WK3_V4', 'WK3_V5', 'WK3_V6']]
Weekly["WK3_Recorded_Total"]=WK3_Total.sum(axis=1,skipna=True)


# In[75]:


WK4_Total=data[['WK4_V1', 'WK4_V2', 'WK4_V3', 'WK4_V4', 'WK4_V5', 'WK4_V6', 'WK4_V7', 'WK4_V8', 'WK4_V9']]
Weekly["WK4_Recorded_Total"]=WK4_Total.sum(axis=1,skipna=True)


# In[76]:


WK5_Total=data[[ 'WK5_V1',  'WK5_V2', 'WK5_V3',]]
Weekly["WK5_Recorded_Total"]=WK5_Total.sum(axis=1,skipna=True)


# In[77]:


WK6_Total=data[['WK6_V1', 'WK6_V2', 'WK6_V3', 'WK6_V4']]
Weekly["WK6_Recorded_Total"]=WK6_Total.sum(axis=1,skipna=True)


# In[78]:


WK7_Total=data[['WK7_V1','WK7_V2', 'WK7_V3', 'WK7_V4', 'WK7_V5',]]
Weekly["WK7_Recorded_Total"]=WK7_Total.sum(axis=1,skipna=True)


# In[79]:


Total=Weekly[[ 'WK0_Recorded_Total', 'WK1_Recorded_Total', 'WK2_Recorded_Total',
       'WK3_Recorded_Total', 'WK4_Recorded_Total', 'WK5_Recorded_Total',
       'WK6_Recorded_Total', 'WK7_Recorded_Total']]
Weekly["All_Week_Total"]=Total.sum(axis=1,skipna=True)


# # Weekly average of Recorded Videos

# In[80]:


Weekly["WK0_Average"]=WK0_Total.mean(axis=1,skipna=True)


# In[81]:


Weekly["WK1_Average"]=WK1_Total.mean(axis=1,skipna=True)


# In[82]:


Weekly["WK2_Average"]=WK2_Total.mean(axis=1,skipna=True)


# In[83]:


Weekly["WK3_Average"]=WK3_Total.mean(axis=1,skipna=True)


# In[84]:


Weekly["WK4_Average"]=WK4_Total.mean(axis=1,skipna=True)


# In[85]:


Weekly["WK5_Average"]=WK5_Total.mean(axis=1,skipna=True)


# In[86]:


Weekly["WK6_Average"]=WK6_Total.mean(axis=1,skipna=True)


# In[87]:


Weekly["WK7_Average"]=WK7_Total.mean(axis=1,skipna=True)


# In[88]:


Total=Weekly[[ 'WK0_Recorded_Total', 'WK1_Recorded_Total', 'WK2_Recorded_Total',
       'WK3_Recorded_Total', 'WK4_Recorded_Total', 'WK5_Recorded_Total',
       'WK6_Recorded_Total', 'WK7_Recorded_Total']]
Weekly["All_WK_Average"]=Total.mean(axis=1,skipna=True)


# # Recorded weekly Percentage

# In[89]:


Weekly["Recorded_WK0_%"]=df[[ 'WK0_V1%', 'WK0_V2%']].mean(axis=1,skipna=True)


# In[90]:


Weekly["Recorded_WK1_%"]=df[[ 'WK1_V1%', 'WK1_V2%', 'WK1_V3%', 'WK1_V4%']].mean(axis=1,skipna=True)


# In[91]:


Weekly["Recorded_WK2_%"]=df[[ 'WK2_V1%', 'WK2_V2%', 'WK2_V3%', 'WK2_V4%', 'WK2_V5%']].mean(axis=1,skipna=True)


# In[92]:


Weekly["Recorded_WK3_%"]=df[[ 'WK3_V1%','WK3_V2%', 'WK3_V3%', 'WK3_V4%', 'WK3_V5%', 'WK3_V6%']].mean(axis=1,skipna=True)


# In[93]:


Weekly["Recorded_WK4_%"]=df[[ 'WK4_V1%',
       'WK4_V2%', 'WK4_V3%', 'WK4_V4%', 'WK4_V5%', 'WK4_V6%', 'WK4_V7%',
       'WK4_V8%', 'WK4_V9%',]].mean(axis=1,skipna=True)


# In[94]:


Weekly["Recorded_WK5_%"]=df[[ 'WK5_V1%', 'WK5_V2%', 'WK5_V3%']].mean(axis=1,skipna=True)


# In[95]:


Weekly["Recorded_WK6_%"]=df[['WK6_V1%',
       'WK6_V2%', 'WK6_V3%', 'WK6_V4%',]].mean(axis=1,skipna=True)


# In[96]:


Weekly["Recorded_WK7_%"]=df[[ 'WK7_V1%', 'WK7_V2%', 'WK7_V3%',
       'WK7_V4%', 'WK7_V5%',]].mean(axis=1,skipna=True)


# In[97]:


All_Week_percent=Weekly[['Recorded_WK0_%','Recorded_WK1_%','Recorded_WK2_%','Recorded_WK3_%','Recorded_WK4_%','Recorded_WK5_%','Recorded_WK6_%','Recorded_WK7_%']]


# In[98]:


Weekly["Recorded_All_Week%"]=All_Week_percent.mean(axis=1,skipna=True)


# # SUK Weekly Total Hours

# In[99]:


Weekly["WK0_SUK_Hours"]=data[['WK0_SUK_V','WK0_SUK_Live']].sum(axis=1)


# In[100]:


Weekly["WK1_SUK_Hours"]=data[['WK1_SUK_V','WK1_SUK_Live']].sum(axis=1)


# In[101]:


Weekly["WK2_SUK_Hours"]=data[['WK2_SUK_V','WK2_SUK_Live']].sum(axis=1)


# In[102]:


Weekly["WK3_SUK_Hours"]=data[['WK3_SUK_V','WK3_SUK_Live']].sum(axis=1)


# In[103]:


Weekly["WK4_SUK_Hours"]=data[['WK4_SUK_V','WK4_SUK_Live']].sum(axis=1)


# In[104]:


Weekly["WK5_SUK_Hours"]=data[['WK5_SUK_V','WK5_SUK_Live']].sum(axis=1)


# In[105]:


Weekly["WK6_SUK_Hours"]=data[['WK6_SUK_V','WK6_SUK_Live']].sum(axis=1)


# In[106]:


Weekly["WK7_SUK_Hours"]=data[['WK7_SUK_V','WK7_SUK_Live']].sum(axis=1)


# In[107]:


Weekly["SUK_Total_Hours"]=data[['WK0_SUK_V', 'WK1_SUK_V', 'WK2_SUK_V', 'WK3_SUK_V',
       'WK4_SUK_V', 'WK5_SUK_V', 'WK6_SUK_V', 'WK7_SUK_V', 'WK0_SUK_Live',
       'WK1_SUK_Live', 'WK2_SUK_Live', 'WK3_SUK_Live', 'WK4_SUK_Live',
       'WK5_SUK_Live','WK6_SUK_Live','WK7_SUK_Live']].sum(axis=1)


# # Max of SUK Live & SUK Recorded Hours for Weekly Average 

# In[108]:


Weekly["WK0_SUK_Average"]=data[["WK0_SUK_Live","WK0_SUK_V"]].max(axis=1)


# In[109]:


Weekly["WK1_SUK_Average"]=data[["WK1_SUK_Live","WK1_SUK_V"]].max(axis=1)


# In[110]:


Weekly["WK2_SUK_Average"]=data[["WK2_SUK_Live","WK2_SUK_V"]].max(axis=1)


# In[111]:


Weekly["WK3_SUK_Average"]=data[["WK3_SUK_Live","WK3_SUK_V"]].max(axis=1)


# In[112]:


Weekly["WK4_SUK_Average"]=data[["WK4_SUK_Live","WK4_SUK_V"]].max(axis=1)


# In[113]:


Weekly["WK5_SUK_Average"]=data[["WK5_SUK_Live","WK5_SUK_V"]].max(axis=1)


# In[114]:


Weekly["WK6_SUK_Average"]=data[["WK6_SUK_Live","WK6_SUK_V"]].max(axis=1)


# In[115]:


Weekly["WK7_SUK_Average"]=data[["WK7_SUK_Live","WK7_SUK_V"]].max(axis=1)


# In[116]:


Weekly["SUK_Average"]=Weekly[["WK0_SUK_Average","WK1_SUK_Average","WK2_SUK_Average","WK3_SUK_Average","WK4_SUK_Average","WK5_SUK_Average","WK6_SUK_Average","WK7_SUK_Average"]].mean(axis=1)


# # Weekly Max Live & Recorded Percentage average

# In[117]:


Weekly['WK0_SUK_%']=df[["WK0_SUK_Live%","WK0_SUK_V%"]].max(axis=1)


# In[118]:


Weekly['WK1_SUK_%']=df[["WK1_SUK_Live%","WK1_SUK_V%"]].max(axis=1)


# In[119]:


Weekly['WK2_SUK_%']=df[["WK2_SUK_Live%","WK2_SUK_V%"]].max(axis=1)


# In[120]:


Weekly['WK3_SUK_%']=df[["WK3_SUK_Live%","WK3_SUK_V%"]].max(axis=1)


# In[121]:


Weekly['WK4_SUK_%']=df[["WK4_SUK_Live%","WK4_SUK_V%"]].max(axis=1)


# In[122]:


Weekly['WK5_SUK_%']=df[["WK5_SUK_Live%","WK5_SUK_V%"]].max(axis=1)


# In[123]:


Weekly['WK6_SUK_%']=df[["WK6_SUK_Live%","WK6_SUK_V%"]].max(axis=1)


# In[124]:


Weekly['WK7_SUK_%']=df[["WK7_SUK_Live%","WK7_SUK_V%"]].max(axis=1)


# # SUK Average Percentage

# In[125]:


Weekly["SUK_Percentage"]=Weekly[[ 'WK0_SUK_%', 'WK1_SUK_%', 'WK2_SUK_%','WK3_SUK_%', 'WK4_SUK_%','WK5_SUK_%','WK6_SUK_%', 'WK7_SUK_%']].mean(axis=1)


# # Weekly Masterclass total hours

# In[126]:


Weekly["WK0_Master"]=data[['WK0_Master1','WK0_Master2']].sum(axis=1)


# In[127]:


Weekly["WK1_Master"]=data["WK1_Master"]


# In[128]:


Weekly["WK2_Master"]=data["WK2_Master"]


# In[129]:


Weekly["WK3_Master"]=data["WK3_Master"]


# In[130]:


Weekly["WK4_Master"]=data["WK4_Master"]


# In[131]:


Weekly["WK7_Master"]=data["WK7_Master"]


# In[132]:


Weekly["WK8_Master1"]=data["WK8_Master1"]


# In[133]:


Weekly["WK8_Master2"]=data["WK8_Master2"]


# In[134]:


Weekly['Total_Hours']=Weekly[["WK0_Master","WK1_Master","WK2_Master","WK3_Master","WK4_Master","WK7_Master","WK8_Master1","WK8_Master2"]].sum(axis=1)


# # Max Live & Recorded Hours for Weekly Master Average

# In[135]:


data["WK0_Master_Max"]=data[['WK0_Master1', 'WK0_Master2']].max(axis=1)


# # Weekly Master Average

# In[136]:


Weekly["WK0_Master_Average"]=data[['WK0_Master_Max']].mean(axis=1)


# In[137]:


Weekly["WK1_Master_Average"]=data[['WK1_Master']].mean(axis=1)


# In[138]:


Weekly["WK2_Master_Average"]=data[['WK2_Master']].mean(axis=1)


# In[139]:


Weekly["WK3_Master_Average"]=data[['WK3_Master']].mean(axis=1)


# In[140]:


Weekly["WK4_Master_Average"]=data[['WK4_Master']].mean(axis=1)


# In[141]:


Weekly["WK7_Master_Average"]=data[['WK7_Master']].mean(axis=1)


# In[142]:


Weekly["WK8_Master_Average"]=data[['WK8_Master1','WK8_Master2']].mean(axis=1)


# In[143]:


Weekly["Master_Average"]=data[['WK0_Master_Max', 'WK1_Master', 'WK2_Master',
       'WK3_Master', 'WK4_Master', 'WK7_Master','WK8_Master1','WK8_Master2']].mean(axis=1)


# # Percentage live Masterclass recorded max

# In[144]:


df['WK0_Max%']=df[['WK0_Master1%','WK0_Master2%']].max(axis=1)


# In[145]:


Weekly["WK0_Master%"]=df['WK0_Max%']


# In[146]:


Weekly["WK1_Master%"]=df['WK1_Master%']


# In[147]:


Weekly["WK2_Master%"]=df['WK2_Master%']


# In[148]:


Weekly["WK3_Master%"]=df['WK3_Master%']


# In[149]:


Weekly["WK4_Master%"]=df['WK4_Master%']


# In[150]:


Weekly["WK7_Master%"]=df['WK7_Master%']


# In[151]:


Weekly["WK8_Master%"]=df[['WK8_Master1%','WK8_Master2%']].mean(axis=1)


# # Masterclass Average Percent

# In[152]:


Weekly["Master_Percentage"]=df[[ 'WK0_Max%', 'WK1_Master%', 'WK2_Master%',
       'WK3_Master%', 'WK4_Master%', 'WK7_Master%','WK8_Master1%','WK8_Master2%']].mean(axis=1)


# In[153]:


Weekly=Weekly.round(2)


# In[154]:


# Exporting data to Output files
#Weekly.to_csv("C:\\Users\\Lenovo\\OneDrive - VigyanShaala\\02 Products  Initiatives-LAPTOP-D2TFS89H\\01 Kalpana\\05 Kalpana M&E\\00 DBMS 1.0\\Kalpana\\Incubator 1.0\\Kalpana Output\\Weekly_Attendence.csv",index=False)


# In[155]:


Weekly.to_csv(r"C:\Users\Lenovo\OneDrive - VigyanShaala\02 Products  Initiatives-LAPTOP-D2TFS89H\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Output\Weekly_Attendence.csv",mode='a',index=False)


# # Storing data to MySQL

# In[156]:


conn= msql.connect(host='localhost',user='root',password="VS@123",database="Kalpana",auth_plugin='mysql_native_password')


# In[157]:


cursor =conn.cursor()


# In[158]:


# Inserting new data on 09_Weekly_attendence
for i,row in Weekly.iterrows():
    cursor.execute("INSERT IGNORE INTO Kalpana.09_Weekly_attendence VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))


# In[159]:


#  Replacing previous records with new records
for i,row in Weekly.iterrows():
    cursor.execute("REPLACE INTO Kalpana.09_Weekly_attendence VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tuple(row))


# In[160]:


conn.commit()


# In[ ]:





# In[ ]:




