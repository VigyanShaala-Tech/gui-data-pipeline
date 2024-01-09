# Import libraries
import pandas as pd 
import numpy as np 
import matplotlib.pyplot as py 
import streamlit as st
import pandas as pd

# Define the Power BI link
power_bi_link = "https://app.powerbi.com/"

expander_html = f"""
<details>
    <summary style="font-size: 18px; font-weight: bold;">Go to Power BI</summary>
    <a href="{power_bi_link}" target="_blank">
        <button style="font-size: 18px; padding: 10px 20px; background-color: #4CAF50; color: white; border: none; cursor: pointer; width: 100%;">Open Power BI Dashboard</button>
    </a>
</details>
"""

st.sidebar.markdown(expander_html, unsafe_allow_html=True)

# Create a title for the web app
st.title("Summary Overall Performance Monitoring")

# Create a sidebar to put image
st.sidebar.image("assets/VS-logo.png")

# Load the DataFrame
df = pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives-LAPTOP-D2TFS89H\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\summary_overall_performance_monitoring.csv")

# Create a sidebar
st.sidebar.title("Overall Performance Dashboard")

# Create a list of email options for the dropdown
email_options = df["Email"].unique().tolist()

# Selectbox for email
email = st.sidebar.selectbox("Select Email", email_options)

# Match email with DataFrame
filtered_df = df[df["Email"] == email]

# Display matched rows
#st.title("Monitoring Results")
st.subheader("Matched Rows:")
st.dataframe(filtered_df.style.highlight_max(axis=0))



# Choose column all columns
#selected_column = st.selectbox("Select a column", [col for col in df.columns ])
selected_column = st.selectbox("Select a column", [col for col in df.columns if col != 'Email'])


# Display the selected column and its comment column
st.subheader("Selected Column:")
st.write(f"**{selected_column}:**")
st.dataframe(filtered_df[[selected_column]].style.highlight_max(axis=0))


    
