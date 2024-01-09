# Import libraries
import subprocess
import streamlit as st


# Define the Power BI link
power_bi_link = "https://app.powerbi.com/"

expander_html = f"""
<details>
    <summary style="font-size: 35px; font-weight: bold;">Go to Power BI</summary>
    <a href="{power_bi_link}" target="_blank">
        <button style="font-size: 18px; padding: 10px 20px; background-color: #4CAF50; color: white; border: none; cursor: pointer; width: 100%;">Open Power BI Dashboard</button>
    </a>
</details>
"""

st.markdown(expander_html, unsafe_allow_html=True)



# Define the program options and their corresponding code files and paths in dict
programs = {
    "Incubator 1.0": {
        "files": [
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 1.0\Kalpana General Information.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 1.0\Incubator Attendence & Summary.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 1.0\Kalpana Weekly Attendence.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 1.0\Kalpana Quiz Review.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 1.0\Assignment.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 1.0\Summary_Overall_Performance_Monitoring.py"
        ],
        "output_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 1.0\Kalpana Output",
        "source_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 1.0\Kalpana Source File",
        "code_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 1.0\Kalpana Code"
    },
    "Incubator 2.0": {
        "files": [
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 2.0\Kalpana Genral INFO.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 2.0\Incubator Attendence & Summary.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 2.0\Kalpana Weekly Attendence.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 2.0\Kalpana Quiz Review.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 2.0\Assignment.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator 2.0\Summary_Overall_Performance_Monitoring.py"
        ],
        "output_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Output",
        "source_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Source File",
        "code_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator 2.0\Kalpana Code"
    },
   "Incubator New Remapping Code": {
        "files": [
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code\Kalpana Genral Information.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code\Incubator Attendence & Summary (RM).py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code\Kalpana Weekly Attendence.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code\Kalpana Quiz Review .py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code\Assignment.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code\Summary_Overall_Performance_Monitoring.py",
        ],

        "output_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output",
        "source_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File",
        "code_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Code"
    },
   "Incubator New Remapping Code (Without Notation)": {
        "files": [
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code (WN)\Kalpana Genral Information.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code (WN)\Incubator Attendence & Summary (WN).py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code (WN)\Kalpana Weekly Attendence.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code (WN)\Kalpana Quiz Review .py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code (WN)\Assignment.py",
            r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Data Pipeline GUI\Py Files\Incubator New Remapping Code (WN)\Summary_Overall_Performance_Monitoring.py",
        ],

        "output_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Output",
        "source_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Source File",
        "code_path": r"C:\Users\HP\OneDrive - VigyanShaala\02 Products  Initiatives\01 Kalpana\05 Kalpana M&E\00 DBMS 1.0\Kalpana\Incubator New Remapping Code\Kalpana Code"
    }         
}

# Create a title and description for the web app
st.title("Data Pipeline Runner")
st.write("This app allows you to run a data pipeline.")

# Create a dropdown to select the program
selected_program = st.selectbox("Select a program", list(programs.keys()))

# Create a button to start the pipeline
if st.button("Run Data Pipeline"):
    selected_files = programs[selected_program]["files"]
    for code_file in selected_files:
        st.write(f"Running {code_file}...")
        # Execute the code file using subprocess module
        subprocess.call(["python", code_file])
        st.write(f"{code_file} executed successfully.")

st.success("Data pipeline completed successfully!")

# Get the selected program's paths
output_path = programs[selected_program]["output_path"]
source_path = programs[selected_program]["source_path"]
code_path = programs[selected_program]["code_path"]

# Create buttons for Output, Source, and Code paths
col1, col2, col3 = st.columns(3)
if col1.button("Output Folder"):
    subprocess.Popen(f'explorer "{output_path}"')

if col2.button("Source Folder"):
    subprocess.Popen(f'explorer "{source_path}"')

if col3.button("Code Folder"):
    subprocess.Popen(f'explorer "{code_path}"')

