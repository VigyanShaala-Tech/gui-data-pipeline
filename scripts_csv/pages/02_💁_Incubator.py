# Import libraries

import pandas as pd
import numpy as np
import matplotlib.pyplot as py
import streamlit as st
import pandas as pd


# Create a title
st.title("Incubator and Weekly Attendance")

st.header("Incubator Results")

# Create a sidebar to put image
st.sidebar.image("assets/VS-logo.png")

# Load the DataFrame
df = pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives-LAPTOP-D2TFS89H\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Incubator_and_attendence_monitoring.csv")

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
ds = pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives-LAPTOP-D2TFS89H\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Weekly_Attendence.csv")

# Pass the selected email to the second code
filtered_ds = ds[ds["Email"] == selected_email]

# Choose a column from the filtered DataFrame
select_column = st.selectbox("Select a column", [col for col in ds.columns if col != 'Email'])

# Display the selected column
st.subheader("Parameters:")
st.write(f"**{select_column}:**")
st.dataframe(filtered_ds[[select_column]].style.highlight_max(axis=0))
