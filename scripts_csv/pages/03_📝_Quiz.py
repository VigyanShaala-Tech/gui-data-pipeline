# Import libraries
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns

# Set page config
st.set_page_config(page_title="Quiz Information", page_icon=":pencil2:")

# Create a sidebar to put image
st.sidebar.image("assets/VS-logo.png")

# Load the DataFrame
df = pd.read_csv(r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives-LAPTOP-D2TFS89H\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output\Quiz_Information.csv")

# Create a sidebar
st.sidebar.title("Quiz Dashboard")

# Create a list of email options for the dropdown
email_options = df["Email"].unique().tolist()

# Selectbox for email
email = st.sidebar.selectbox("Select Email", email_options)

# Match email with DataFrame
filtered_df = df[df["Email"] == email]

# Display matched rows
st.title("Quiz Results")
st.subheader("Matched Rows:")
st.dataframe(filtered_df.style.highlight_max(axis=0))

# Choose column starting with "Marks"
selected_column = st.selectbox("Select a column", [col for col in df.columns if col.startswith("Marks")])

# Get the next comment column
selected_comment_column = df.columns[df.columns.get_loc('Overall_Average')]

# Display the selected column and its comment column
st.subheader("Selected Column:")
st.write(f"**{selected_column}:**")
st.dataframe(filtered_df[[selected_column]].style.highlight_max(axis=0))
st.write(f"**{selected_comment_column}:**")
st.dataframe(filtered_df[[selected_comment_column]].style.highlight_max(axis=0))

# Calculate rank based on Overall_Average
df['Rank'] = df['Overall_Average'].rank(ascending=False, method='min')

# Get the rank for the filtered email
rank = df[df['Email'] == email]['Rank'].values[0]

# Display the rank in a box with bold formatting
st.subheader("Rank")
st.markdown(f"The rank for the email {email} is **{rank}**")


# Plot a bar chart
st.subheader("Column Distribution")
column_names = [col for col in df.columns if col.startswith("Marks")]
column_values = filtered_df[column_names].values.flatten()
plt.figure(figsize=(10, 6))
sns.barplot(x=column_names, y=column_values)
plt.xlabel("Column Names")
plt.ylabel("Values")
plt.xticks(rotation=45)
st.pyplot(plt)







