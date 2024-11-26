from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()
import streamlit as st
import pandas as pd
import openpyxl
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from PIL import Image
import webbrowser
#from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType,IntegerType
import numpy as np
import pandas as pd
import re
import pgeocode
import os
from datetime import datetime
import time
import json
import requests
import sqlite3


#spark = SparkSession.builder.appName('abc').getOrCreate()
# spark = SparkSession.builder \
#     .appName("StreamlitApp") \
#     .config("spark.driver.host", "localhost") \
#     .master("local[*]") \
#     .getOrCreate()

##### Validations ##################
def validations_change_multiple_stores(df):
    valid_business_units = {'Florida', 'Coastal Carolina', 'Southeast', 'Rocky Mountain', 'Gulf Coast', 
                            'West Coast', 'Midwest', 'Texas', 'South Atlantic', 'Grand Canyon', 
                            'Great Lakes', 'Heartland', 'Central Canada', 'Western Division', 'Eastern Canada'}
    expected_dtypes = {
        'Email': 'string',
        'What is the business unit of the store?': 'string',
        'What is the site number': 'int64',
        'Should the store be removed from localized pricing?': 'string',
        'What happened to the store?': 'string',
        'What is the address of the new store?': 'string',
        'What is the longitude and latitude of the new store? (e.g. 32.1231, -73.74939)': 'float'
    }
    error_messages = []
    #df = df[df['What happened to the store'] == 'A new store is opened (NTI)']
  # Check for nulls in each column
    nulls_per_column = df.isnull().sum()
    
    # Filter out columns that have no nulls
    # columns_with_nulls = nulls_per_column[nulls_per_column > 0]
    
    # if not columns_with_nulls.empty:
    #     for column, null_count in columns_with_nulls.items():
    #         error_messages.append({"Validation": "Null Check", 
    #                                 "Error": f"Column '{column}' has {null_count} null values"})
    business_unit_column = 'What is the business unit of the store?'  ## remove it 
    # Iterate over the business unit column and validate each value
    invalid_bu_mask = ~df[business_unit_column].isin(valid_business_units)

    # If any invalid values are found, collect error messages
    if invalid_bu_mask.any():
        invalid_bu_df = df[invalid_bu_mask][[business_unit_column]]
        for _, row in invalid_bu_df.iterrows():
            error_messages.append({"Validation": "Business Unit", 
                                    "Error": f"Invalid Business Unit: '{row[business_unit_column]}'"})
    column_to_check = 'What is the site number'  # Replace with your actual column namer #### QUESTION
    business_unit_column = 'What is the business unit of the store?'

    # Function to check if the value is a 7-digit number
    def is_seven_digit_number(value):
        if pd.isnull(value): 
            return False
        if isinstance(value, (int, float)): 
            return 1000000 <= value <= 9999999  
        if isinstance(value, str):  
            return value.isdigit() and len(value) == 7
        return False
    # Vectorized check for efficiency
    invalid_site_number_mask = ~df[column_to_check].apply(is_seven_digit_number)
    # If any invalid site numbers are found
    if invalid_site_number_mask.any():
        invalid_site_df = df[invalid_site_number_mask][[business_unit_column, column_to_check]]
        
        # Collect error messages for each invalid site number
        for _, row in invalid_site_df.iterrows():
            error_messages.append({"Validation": "Site Number", 
                                    "Error": f"Business Unit '{row[business_unit_column]}', Value '{row[column_to_check]}' (Not a 7-digit number)"})
    ## Vaidation for site id digits        
    column_to_check = 'What is the site number'  # Replace with your actual column name
    bu_column = 'What is the business unit of the store?'  # Replace with your actual BU column name

    # Function to check if a value is a 6-digit number
    def is_six_digit_number(value):
        if pd.isnull(value):
            return False
        if isinstance(value, (int, float)):
            return 100000 <= value <= 999999
        if isinstance(value, str):
            return value.isdigit() and len(value) == 6
        return False

    # Check if 'Central Canada' exists in the Business Unit column
    if 'Central Canada' in df[bu_column].values:
        
        # Filter rows where the Business Unit is 'Central Canada'
        df_central_canada = df[df[bu_column] == 'Central Canada']

        # Apply the function across the 'What is the site number' column for the filtered rows
        mask_invalid = ~df_central_canada[column_to_check].apply(is_six_digit_number)

        # If any invalid values are found, collect them
        if mask_invalid.any():
            # Filter the DataFrame to get rows with invalid site numbers in Central Canada
            invalid_bu_df = df_central_canada[mask_invalid][[bu_column, column_to_check]]
            
            # Collect error messages for each invalid site number
            for _, row in invalid_bu_df.iterrows():
                error_messages.append({
                    "Validation": "6-Digit Site Number (Central Canada)",
                    "Error": f"Business Unit: {row[bu_column]}, Value: {row[column_to_check]} (Not a 6-digit number)"
                })
    # 5. Validate 'Should the store be removed from localized pricing?'
    remove_from_lp = 'Should the store be removed from localized pricing?'
    business_unit_column = 'What is the business unit of the store?'
    
    # Set of valid values for 'Should the store be removed from localized pricing?'
    valid_remove_from_lp = {'Yes', 'No'}

    # Vectorized check for invalid values
    invalid_store_mask = ~df[remove_from_lp].isin(valid_remove_from_lp)

    # If any invalid values are found, collect error messages
    if invalid_store_mask.any():
        invalid_store_df = df[invalid_store_mask][[business_unit_column, remove_from_lp]]
        
        # Collect error messages for each invalid entry
        for _, row in invalid_store_df.iterrows():
            error_messages.append({"Validation": "Remove from Localized Pricing", 
                                    "Error": f"Business Unit: '{row[business_unit_column]}', Value: '{row[remove_from_lp]}' (Invalid Value)"})
    # 6. Validate 'What happened to the store?' column
    store_status = 'What happened to the store?'
    business_unit_column = 'What is the business unit of the store?'

    # Set of valid store statuses
    valid_store_status = {'The store is permanently closed', 'A new store is opened (NTI)'}

    # Vectorized check for invalid store statuses
    invalid_store_status_mask = ~df[store_status].isin(valid_store_status)

    # If any invalid values are found, collect error messages
    if invalid_store_status_mask.any():
        invalid_store_status_df = df[invalid_store_status_mask][[business_unit_column, store_status]]
        
        # Collect error messages for each invalid entry
        for _, row in invalid_store_status_df.iterrows():
            error_messages.append({"Validation": "Store Status", 
                                    "Error": f"Business Unit: '{row[business_unit_column]}', Store Status: '{row[store_status]}' (Invalid Store Status)"})
    return error_messages

def validations_cluster_mapping(df):
    valid_business_units = {'Florida', 'Coastal Carolina', 'Southeast', 'Rocky Mountain', 'Gulf Coast', 
                            'West Coast', 'Midwest', 'Texas', 'South Atlantic', 'Grand Canyon', 
                            'Great Lakes', 'Heartland', 'Central Canada', 'Western Division', 'Eastern Canada'}
    error_messages = []
    bu_column = 'business_unit'
    invalid_bu_mask = ~df[bu_column].isin(valid_business_units)

    # If any invalid values are found, collect error messages
    if invalid_bu_mask.any():
        invalid_bu_df = df[invalid_bu_mask][[bu_column]]
        for _, row in invalid_bu_df.iterrows():
            error_messages.append({"Validation": "Business Unit", 
                                    "Error": f"Invalid Business Unit: '{row[bu_column]}'"})
    def is_six_digit_number(value):
        if pd.isnull(value):
            return False
        if isinstance(value, (int, float)):
            return 100000 <= value <= 999999
        if isinstance(value, str):
            return value.isdigit() and len(value) == 6
        return False
    column_to_check = 'site_id'
    # Check if 'Central Canada' exists in the Business Unit column
    if 'Central Canada' in df[bu_column].values:
        
        # Filter rows where the Business Unit is 'Central Canada'
        df_central_canada = df[df[bu_column] == 'Central Canada']

        # Apply the function across the 'What is the site number' column for the filtered rows
        mask_invalid = ~df_central_canada[column_to_check].apply(is_six_digit_number)

        # If any invalid values are found, collect them
        if mask_invalid.any():
            # Filter the DataFrame to get rows with invalid site numbers in Central Canada
            invalid_bu_df = df_central_canada[mask_invalid][[bu_column, column_to_check]]
            
            # Collect error messages for each invalid site number
            for _, row in invalid_bu_df.iterrows():
                error_messages.append({
                    "Validation": "6-Digit Site Number (Central Canada)",
                    "Error": f"Business Unit: {row[bu_column]}, Value: {row[column_to_check]} (Not a 6-digit number)"
                })
    cluster_list = np.arange(1, 20 ,1)
    invalid_cluster_df = df[~df['cluster'].isin(cluster_list)]
    if invalid_cluster_df.shape[0] > 0:
        error_messages.append({"Validation" : "cluster value", 
                              "Error": "Invalid cluster valu entered"})
    return error_messages
############ END VALIDATIONS CODE #####################


## call databircks notebook ####

def call_update_clusters(DATABRICKS_INSTANCE, DATABRICKS_TOKEN, DATABRICKS_JOB_ID , blob_name_upload, source_container, dest_container, connection_string, choice , choice_dict):
    # DATABRICKS_INSTANCE = 'https://adb-8165306836189773.13.azuredatabricks.net'
    # DATABRICKS_TOKEN = 'dapi7fbdd0c908acb1e7309eb6531446ab25'
    # DATABRICKS_JOB_ID = '631136514737024'

    url = f"{DATABRICKS_INSTANCE}/api/2.0/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "job_id": DATABRICKS_JOB_ID,
       "notebook_params":  {"blob_name" : blob_name_upload,
                             "source_container": source_container,
                             "dest_container": dest_container ,
                             "connection_string" : connection_string,
                             "choice" : choice,
                             "choice_dict" : choice_dict}
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        st.write(f"Job triggered successfully: {response.json()}")
        return True
    else:
        st.write(f"Failed to trigger job: {response.status_code} - {response.text}")
        return False
########### END call databircks notebook #############################
########### UPDATE DELTA TABLE WITH TIMESTAMP AND EMAIL #################
def update_delta_table(df, file_name):
    DATABRICKS_INSTANCE = os.getenv('DATABRICKS_INSTANCE')
    # DATABRICKS_INSTANCE = "https://adb-8165306836189773.13.azuredatabricks.net/"  # e.g., https://adb-1234567890123456.7.azuredatabricks.net
    DATABRICKS_TOKEN = "dapi0e128d426fcd9a7925c999704a64b5fa"  # Replace with your Databricks token
    DELTA_TABLE_NAME = "default.blob_file_metadata"
    upload_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    email = df['email'].unique()[0]  ### change this 
    log_data = {
        "user_email": email,
        "file_upload_time": upload_time,
        "uploaded_file_name": file_name  
    }
    url = f"{DATABRICKS_INSTANCE}/api/2.0/sql/statements"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

    warehouse_id = "f30fd48eb9dab391"  # Your warehouse ID

    # Updated SQL query with new column names
    sql_query = f"""
    INSERT INTO {DELTA_TABLE_NAME} (filename, user, last_modified)
    VALUES ('{log_data['uploaded_file_name']}', '{log_data['user_email']}', '{log_data['file_upload_time']}')
    """
    payload = {
        "statement": sql_query,
        "warehouse_id": warehouse_id
    }
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()  # Raise an error for bad responses
        st.success("Data logged to Databricks successfully!")  # Success message
        return True
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to log data to Databricks: {e}")
        if 'response' in locals():
            st.error(f"Response content: {response.content.decode('utf-8')}")
        return False
     
def open_download_file(connection_string, container_name,  blob_name ):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    # Create a container if it doesn't exist
    container_client = blob_service_client.get_container_client(container_name)
    blob_container = container_client.get_blob_client(blob_name).download_blob()
    return BytesIO(blob_container.readall())

def upload_blob(connection_string, container_name,  blob_name_upload , df):
    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    # Get the container in which the file is to be stored
    container_client = blob_service_client.get_container_client(container_name)
    # Upload the xlsx file submiotted by BU
    try:
        container_client.upload_blob(name = blob_name_upload , data = df.to_csv(index = False), overwrite=True)
        st.write("file has been succesfully uploaded to the blob storage")
        var_run = True
    except Exception as e:
        var_run = False
    return var_run

def ask_confirmation(session_file_variable,blob_name_upload, DATABRICKS_INSTANCE, DATABRICKS_TOKEN, DATABRICKS_JOB_ID, source_container, dest_container, connection_string,choice, choice_dict ):
    if 'stage' not in st.session_state:
        st.session_state.stage = 0
    def set_stage(stage):
        st.session_state.stage = stage
    with st.container():
        st.warning("Are you sure you want to upload the file?")
        st.button("Yes", key = 'confirm', on_click = set_stage, args = (1,))
        st.button("No", key = 'confirm_deny', on_click = set_stage, args = (0,))
            #st.write(st.session_state.stage)
        if st.session_state.stage >  0:
                st.warning("are you sure you want to upload the file, changes cannot be undone")
                st.button("Yes", key = 'reconfirm', on_click = set_stage, args = (2,))
                st.button("No", key = 'reconfirm_deny' , on_click = set_stage , args = (0,))
                #st.write(st.session_state.stage)
                if st.session_state.stage > 1:
                        df = pd.read_excel(session_file_variable)
                        delta_table_bool= update_delta_table(df, session_file_variable.name)
                        if delta_table_bool:
                            st.write(df)
                            var_run = upload_blob(connection_string, container_name, blob_name_upload , df)
                            # output validation results
                            if var_run:
                                if choice == 1:
                                    error_messages = validations_change_multiple_stores(df)
                                elif choice == 2:
                                    error_messages = validations_cluster_mapping(df)
                                if bool(error_messages):
                                    update_info = False
                                    st.write("Errors found, please see the errors below, fix and reupload the file")  ##change ro reupload screen??
                                    st.write(error_messages)
                                else:
                                    update_info = True
                                    st.write("all validations succesfull, updating store information")
                                    update_info = call_update_clusters(DATABRICKS_INSTANCE, DATABRICKS_TOKEN, DATABRICKS_JOB_ID , blob_name_upload, source_container, dest_container, connection_string, choice , choice_dict )  ## updates cluster
                            else: 
                                st.error("error uploading file to blob storage, please contact code owner")
                        else:
                            st.error("error updating delta table, please contact code owner")
def ask_confirmation_one_store(DATABRICKS_INSTANCE, DATABRICKS_TOKEN, DATABRICKS_JOB_ID , choice_dict, blob_name_upload, source_container, dest_container, connection_string, choice  ):
    
    #if 'stage' not in st.session_state:
    st.session_state.stage = 0
    def set_stage(stage):
        st.session_state.stage = stage
    with st.container():
        st.warning("Are you sure you want to make changes to the store?")
        st.button("Yes", key = 'confirm_1_store', on_click = set_stage, args = (1,))
        st.button("No", key = 'confirm_deny_1_store', on_click = set_stage, args = (0,))
            #st.write(st.session_state.stage)
        if st.session_state.stage >  0:
                st.warning("Reconfirming, Are you sure you want to make changes to the store?, changes cannot be undone")
                st.button("Yes", key = 'reconfirm_1_store', on_click = set_stage, args = (2,))
                st.button("No", key = 'reconfirm_deny_1_store' , on_click = set_stage , args = (0,))
                #st.write(st.session_state.stage)
                if st.session_state.stage > 1:
                    df = pd.Dataframe(choice_dict)
                    delta_table_bool= update_delta_table(df, "changes requested for a single store")
                    if delta_table_bool:
                        
                        call_update_clusters(DATABRICKS_INSTANCE, DATABRICKS_TOKEN, DATABRICKS_JOB_ID , blob_name_upload, source_container, dest_container, connection_string, choice , choice_dict )
# Define Variables
logo_url = "https://logos-world.net/wp-content/uploads/2022/04/Circle-K-Logo.png"  
connection_string = "DefaultEndpointsProtocol=https;AccountName=rich0lucas0training;AccountKey=Q5eVH339D0ohgYYWNnMUzeEZqKsZK7MPEpoWjXDsH2IKgsHk5sxXkQZJtGZujaeOo0KOPkTILFdk+AStEQKO3A==;EndpointSuffix=core.windows.net"
container_name = source_container = "container-01-rl-training"  
dest_container = "container-01-rl-training-copy" 

st.image(logo_url, width=200) 
st.title("Welcome to Store-Cluster Mapping Update System")
st.write("Hi, What would you like to do today?")
choice = st.radio("please select one from the following" , ['upload changes for 1 store' , 'upload changes for more than 1 store','change cluster mapping', 'reclustering'] , index = None )
 
if choice == 'upload changes for more than 1 store':
    choice = 1
    blob_name_multiple_stores = "sample_input.xlsx"
    if st.button("click here to download the excel template") :
        blob_name_upload = 'demo_input_multiple_stores.csv'  ## change to a new file
        with open_download_file(connection_string, container_name, blob_name_multiple_stores) as file_xcl:
            btn = st.download_button(
            label="bu_sample_format",
            data=file_xcl,
            file_name="input_format.xlsx"
        )
        file_xcl.close()
            
    uploaded_file = st.file_uploader("once done editing please upload the file here")
    st.session_state['file'] = uploaded_file
    if st.session_state['file'] is not None:
       DATABRICKS_INSTANCE = 'https://adb-8165306836189773.13.azuredatabricks.net'
       DATABRICKS_TOKEN = 'dapi7fbdd0c908acb1e7309eb6531446ab25'
       DATABRICKS_JOB_ID = '631136514737024'
       blob_name_upload = 'demo_input.csv'
       ask_confirmation(st.session_state['file'], blob_name_upload, DATABRICKS_INSTANCE, DATABRICKS_TOKEN, DATABRICKS_JOB_ID,source_container, dest_container, connection_string, choice, choice_dict = '')
        
elif choice == 'upload changes for 1 store' :
    DATABRICKS_INSTANCE = 'https://adb-8165306836189773.13.azuredatabricks.net'
    DATABRICKS_TOKEN = 'dapi7fbdd0c908acb1e7309eb6531446ab25'
    DATABRICKS_JOB_ID = '631136514737024'
    blob_name_upload = "change_upload_1_store.csv"
    email  = st.text_input("enter your email", value = "")
    bu = st.selectbox("What is the business unit of the store?" , ('Florida', 'Coastal Carolina', 'Southeast', 'Rocky Mountain', 'Gulf Coast', 
                            'West Coast', 'Midwest', 'Texas', 'South Atlantic', 'Grand Canyon', 
                            'Great Lakes', 'Heartland', 'Central Canada', 'Western Division', 'Eastern Canada') , index = None)
    site_id = st.text_input("enter the site id" , value = "")
    close_status = st.selectbox("What happened to the store?" ,("The store is permanently closed" , "A new store is opened (NTI)") , index = None)

    choice_dict = { 'email' : [email] ,   'bu' : [bu] , 'What is the site number' : [site_id] , 'What happened to the store?' : [close_status]}
    st.write("once you have entered the details, click on the submit button to submit")
    if 'submit_info' not in st.session_state:
        st.session_state.stage_submit  = 0
    def set_stage_submit(stage_submit):
            st.session_state.stage_submit = stage_submit
    
    st.button(label = "Click here to submit store details" ,key = 'submit_info',  on_click = set_stage_submit, args = (1,))
    st.write(st.session_state.stage_submit)
    if st.session_state.stage_submit >0 :
        st.write("inside if loop")
        if 'stage' not in st.session_state:
            st.session_state.stage = 0
        def set_stage(stage):
            st.session_state.stage = stage
        
        st.warning("Are you sure you want to make changes to the store?")
        st.button("Yes", key = 'confirm', on_click = set_stage, args = (1,))
        st.button("No", key = 'confirm_deny', on_click = set_stage, args = (0,))
        # st.write(st.session_state.submit_info)
        if st.session_state.stage >  0:
                st.warning("Reconfirming, Are you sure you want to make changes to the store?, changes cannot be undone")
                st.button("Yes", key = 'reconfirm', on_click = set_stage, args = (2,))
                st.button("No", key = 'reconfirm_deny' , on_click = set_stage , args = (0,))
        #         #st.write(st.session_state.stage)
                if st.session_state.stage > 1:
                    st.write("inside if loop no 2" , st.session_state.stage)
                    df = pd.DataFrame(choice_dict)
                    delta_table_bool= update_delta_table(df, "changes requested for a single store")
                    if delta_table_bool:
                        upload_blob(connection_string, container_name,  blob_name_upload , df)
                        call_update_clusters(DATABRICKS_INSTANCE, DATABRICKS_TOKEN, DATABRICKS_JOB_ID , blob_name_upload, source_container, dest_container, connection_string, choice=2 ,choice_dict = ""  )
    

    
elif (choice == 'change cluster mapping') or (choice == 'reclustering'):
    if choice == 'change cluster mapping':
        if st.button("click here to download the excel template") :
            blob_name_upload_cluster = 'cluster_mapping.xlsx'
            blob_name = "sample_input_cluster_mapping.xlsx"
            with open_download_file(connection_string, container_name, blob_name_upload_cluster) as file_xcl:
                btn = st.download_button(
                label="Download file",
                data=file_xcl,
                file_name="cluster_mapping.xlsx"
            )
        uploaded_file = st.file_uploader("once done editing please upload the file here")
        blob_name_upload = 'change_cluster_mapping.csv'
    else:
        uploaded_file = st.file_uploader("**Please upload the reclustering file here**")
        blob_name_upload = 'reclustering.csv'

    st.session_state['file_cluster'] = uploaded_file
    if st.session_state['file_cluster'] is not None:
        DATABRICKS_INSTANCE = 'https://adb-8165306836189773.13.azuredatabricks.net/'
        DATABRICKS_TOKEN = 'dapi7fbdd0c908acb1e7309eb6531446ab25'
        DATABRICKS_JOB_ID = '385410668511049'
        
        ask_confirmation(st.session_state['file_cluster'], blob_name_upload, DATABRICKS_INSTANCE, DATABRICKS_TOKEN, DATABRICKS_JOB_ID,source_container, dest_container, connection_string, choice = 2, choice_dict = '')
        
# elif choice == 'reclustering':
#     uploaded_file_recluster = st.file_uploader("please upload the reclustering file here:")
#     st.session_state.recluster_file = uploaded_file_recluster
#     if st.session_state.recluster_file is not None:
#          df = pd.read_excel(st.session_state.recluster_file)
#          st.write(df)




        




