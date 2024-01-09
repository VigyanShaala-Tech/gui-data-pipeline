# Import libraries
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as py 
import streamlit as st
import pandas as pd




# Create a title for the web app
st.title("Column Comments")

# Create a sidebar to put image
st.sidebar.image("assets/VS-logo.png")

# Load the DataFrame
df = pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives-LAPTOP-D2TFS89H\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Assignment_Review.csv")

# Create a sidebar
st.sidebar.title("Comments Dashboard")

# Create a list of email options for the dropdown
email_options = df["Email"].unique().tolist()

# Selectbox for email
email = st.sidebar.selectbox("Select Email", email_options)

# Match email with DataFrame
filtered_df = df[df["Email"] == email]

# Display matched rows
#st.title("Comments Dashboard")
st.subheader("Matched Rows:")
st.dataframe(filtered_df.style.highlight_max(axis=0))



# Choose column starting with "WK"
selected_column = st.selectbox("Select a column", [col for col in df.columns if col.startswith("WK")])

# Get the next comment column
selected_comment_column = df.columns[df.columns.get_loc(selected_column) + 1]

# Display the selected column and its comment column
st.subheader("Selected Column and Comment:")
st.write(f"**{selected_column}:**")
st.dataframe(filtered_df[[selected_column]].style.highlight_max(axis=0))
st.write(f"**{selected_comment_column}:**")
st.dataframe(filtered_df[[selected_comment_column]].style.highlight_max(axis=0))



    
