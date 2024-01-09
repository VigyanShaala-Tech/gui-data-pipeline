import pandas as pd
import numpy as np
import matplotlib.pyplot as py
import streamlit as st
import pymysql

# Create a title
st.title("Incubator and Weekly Attendance")

st.header("Incubator Results")

# Create a sidebar to put image
st.sidebar.image("assets/VS-logo.png")

# Connect to the MySQL database
connection = pymysql.connect(
    host="localhost",
    user="root",
    password="VS@123",
    db="kalpana",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

# Load data from the database into a DataFrame
with connection.cursor() as cursor:
    sql = "SELECT * FROM 08_incubator_and_attendence_monitoring"
    cursor.execute(sql)
    data = cursor.fetchall()
    df = pd.DataFrame(data)

# Close the database connection
connection.close()



# Load the DataFrame
#df = pd.read_csv(r"C:\Users\spjay\Desktop\VigyanShaala\OneDrive_1_2-10-2023\Kalpana Output\Incubator_and_attendence_monitoring.csv")

# Create a sidebar
st.sidebar.title("Monitoring Dashboard")

# Create a list of email options for the dropdown
email_options = df["Email"].unique().tolist()

# Selectbox for email
selected_email = st.sidebar.selectbox("Select Email", email_options)

# Match email with DataFrame
filtered_df = df[df["Email"] == selected_email]

# Display matched rows
st.subheader("Matched Rows:")
st.dataframe(filtered_df.style.highlight_max(axis=0))

# Choose all columns except "Email"
selected_column = st.selectbox("Select a column", [col for col in df.columns if col != 'Email'])

# Display the selected column and its comment column
st.subheader("Parameters:")
st.write(f"**{selected_column}:**")
st.dataframe(filtered_df[[selected_column]].style.highlight_max(axis=0))

#st.title("Weekly Results")
st.header("Weekly Results")

# Load the DataFrame
#ds = pd.read_csv(r"C:\Users\spjay\Desktop\VigyanShaala\OneDrive_1_2-10-2023\Kalpana Output\Weekly_Attendence.csv")

# Connect to the MySQL database
connection = pymysql.connect(
    host="localhost",
    user="root",
    password="VS@123",
    db="kalpana",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

# Load data from the database into a DataFrame
with connection.cursor() as cursor:
    sql = "SELECT * FROM 09_weekly_attendence"
    cursor.execute(sql)
    data = cursor.fetchall()
    ds = pd.DataFrame(data)

# Close the database connection
connection.close()

# Pass the selected email to the second code
filtered_ds = ds[ds["Email"] == selected_email]

# Choose a column from the filtered DataFrame
select_column = st.selectbox("Select a column", [col for col in ds.columns if col != 'Email'])

# Display the selected column
st.subheader("Parameters:")
st.write(f"**{select_column}:**")
st.dataframe(filtered_ds[[select_column]].style.highlight_max(axis=0))
