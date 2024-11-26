# Databricks notebook source
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# COMMAND ----------

# Load the Excel file -- 1st sheet #
file_path = '/dbfs/FileStore/Fizza/GSL/GSL_input_format__1_.xlsx'  
sheet_name = 'Store open closures'  

df = pd.read_excel(file_path, sheet_name=sheet_name)
print(df.dtypes)

# COMMAND ----------

error_messages = []

# COMMAND ----------

valid_business_units = {'Florida', 'Coastal Carolina', 'Southeast', 'Rocky Mountain', 'Gulf Coast', 
                            'West Coast', 'Midwest', 'Texas', 'South Atlantic', 'Grand Canyon', 
                            'Great Lakes', 'Heartland', 'Central Canada', 'Western Division', 'Eastern CANADA'}
expected_dtypes = {
    'Email': 'string',
    'What is the business unit of the store?': 'string',
    'What is the site number': 'int64',
    'Should the store be removed from localized pricing?': 'string',
    'What happened to the store?': 'string',
    'What is the address of the new store?': 'string',
    'What is the longitude and latitude of the new store? (e.g. 32.1231, -73.74939)': 'float'
}

def validations(df, valid_business_units):

    error_messages = []
  # Check for nulls in each column
    nulls_per_column = df.isnull().sum()
    
    # Filter out columns that have no nulls
    columns_with_nulls = nulls_per_column[nulls_per_column > 0]
    
    if not columns_with_nulls.empty:
        for column, null_count in columns_with_nulls.items():
            error_messages.append({"Validation": "Null Check", 
                                    "Error": f"Column '{column}' has {null_count} null values"})
    
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
    

    
  

# COMMAND ----------

# nulls_per_column = df.isnull().sum()
# print("Nulls per column:")
# print(nulls_per_column)

# COMMAND ----------

# 1. Validate Nulls in Columns
try:
    # Check for nulls in each column
    nulls_per_column = df.isnull().sum()
    
    # Filter out columns that have no nulls
    columns_with_nulls = nulls_per_column[nulls_per_column > 0]
    
    if not columns_with_nulls.empty:
        for column, null_count in columns_with_nulls.items():
            error_messages.append({"Validation": "Null Check", 
                                    "Error": f"Column '{column}' has {null_count} null values"})
except Exception as e:
    error_messages.append({"Validation": "Null Check", "Error": str(e)})

# COMMAND ----------

# 2. Validate Business Units
try:
    business_unit_column = 'What is the business unit of the store?'

    # Set of valid business units
    valid_business_units = {'Florida', 'Coastal Carolina', 'Southeast', 'Rocky Mountain', 'Gulf Coast', 
                            'West Coast', 'Midwest', 'Texas', 'South Atlantic', 'Grand Canyon', 
                            'Great Lakes', 'Heartland', 'Central Canada', 'Western Division', 'Eastern CANADA'}

    # Iterate over the business unit column and validate each value
    invalid_bu_mask = ~df[business_unit_column].isin(valid_business_units)

    # If any invalid values are found, collect error messages
    if invalid_bu_mask.any():
        invalid_bu_df = df[invalid_bu_mask][[business_unit_column]]
        for _, row in invalid_bu_df.iterrows():
            error_messages.append({"Validation": "Business Unit", 
                                    "Error": f"Invalid Business Unit: '{row[business_unit_column]}'"})

except Exception as e:
    error_messages.append({"Validation": "Business Unit", "Error": str(e)})

# COMMAND ----------

# Defininig a dictionary with expected data types
expected_dtypes = {
    'Email': 'string',
    'What is the business unit of the store?': 'string',
    'What is the site number': 'int64',
    'Should the store be removed from localized pricing?': 'string',
    'What happened to the store?': 'string',
    'What is the address of the new store?': 'string',
    'What is the longitude and latitude of the new store? (e.g. 32.1231, -73.74939)': 'float'
}

# COMMAND ----------

### Validating the data format for each column

for column in df.columns:
    actual_dtype = df[column].dtype
    expected_dtype = expected_dtypes.get(column, 'Not specified')  # Fetch expected type, if provided
    
    if expected_dtype != 'Not specified' and actual_dtype != expected_dtype:
        print(f"Column: {column}, has incorrect data type: {actual_dtype}, Expected Data Type: \033[91m{expected_dtype} (Incorrect)\033[0m")
   # else:
    #    print(f"Column: {column}, has correct data type: {actual_dtype}, Expected Data Type: {expected_dtype}")

# COMMAND ----------

# #### FOR ALL BU's #####
# # Specify the column to check for 7-digit numbers
# column_to_check = 'What is the site number'  # Replace with your actual column name

# # Function to check if the value is a 7-digit number
# def is_seven_digit_number(value):
#     if pd.isnull(value): 
#         return False
#     if isinstance(value, (int, float)): 
#         return 1000000 <= value <= 9999999  
#     if isinstance(value, str):  
#         return value.isdigit() and len(value) == 7
#     return False

# # Highlight rows where the value in the column is not a 7-digit number
# for index, value in df[column_to_check].iteritems():
#     if not is_seven_digit_number(value):
#         business_unit = df.loc[index, 'What is the business unit of the store?'] 
#         print(f"Business Unit: {business_unit}, Value: {value} (Not a 7-digit number)")

# COMMAND ----------

#### FOR ALL BU's #####
# 3. Validate 7-Digit Numbers in Site Number
try:
    column_to_check = 'What is the site number'  # Replace with your actual column name
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

except Exception as e:
    error_messages.append({"Validation": "Site Number", "Error": str(e)})

# COMMAND ----------

# ###### FOR CENTRAL CANADA BU SITES ONLY #######
# # Column to check for 6-digit numbers
# column_to_check = 'What is the site number'  # Replace with your actual column name
# bu_column = 'What is the business unit of the store?'  # Replace with your actual BU column name

# # Function to check if a value is a 6-digit number
# def is_six_digit_number(value):
#     if pd.isnull(value):
#         return False
#     if isinstance(value, (int, float)):
#         return 100000 <= value <= 999999
#     if isinstance(value, str):
#         return value.isdigit() and len(value) == 6
#     return False

# # Check if 'Central Canada' exists in the Business Unit column
# if 'Central Canada' in df[bu_column].values:
    
#     # Filter rows where the Business Unit is 'Central Canada'
#     df_central_canada = df[df[bu_column] == 'Central Canada']

#     # Apply the function across the 'What is the site number' column for the filtered rows
#     mask_invalid = ~df_central_canada[column_to_check].apply(is_six_digit_number)

#     # If any invalid values are found, raise an error and print them
#     if mask_invalid.any():
#         # Filter the DataFrame to get rows with invalid site numbers in Central Canada
#         invalid_bu_df = df_central_canada[mask_invalid][[bu_column, column_to_check]]
        
#         # Print each invalid BU and its corresponding site number
#         for _, row in invalid_bu_df.iterrows():
#             print(f"Business Unit: {row[bu_column]}, Value: {row[column_to_check]} (Not a 6-digit number)")
        
#         #raise ValueError("Some Central Canada Business Units have site numbers that are not valid 6-digit numbers.")
#     else:
#         print("All Central Canada Business Units have valid 6-digit numbers.")
        
# else:
#     print("No 'Central Canada' found in the Business Unit column.")

# COMMAND ----------

# 4. Validate 6-Digit Numbers for 'Central Canada' in Site Number
try:
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
        else:
            print("All Central Canada Business Units have valid 6-digit numbers.")
        
    else:
        print("No 'Central Canada' found in the Business Unit column.")
        
except Exception as e:
    error_messages.append({"Validation": "6-Digit Site Number (Central Canada)", "Error": str(e)})

# COMMAND ----------

error_messages

# COMMAND ----------

# ### Validate the BU column
# # Specify the Business Unit column
# business_unit_column = 'What is the business unit of the store?'

# # Example of valid BUs
# valid_business_units = {'Florida', 'Coastal Carolina', 'Southeast', 'Rocky Mountain', 'Gulf Coast', 'West Coast', 'Midwest', 'Texas', 'South Atlantic', 'Grand Canyon', 'Great Lakes', 'Heartland', 'Central Canada', 'Western Division', 'Eastern CANADA'}  

# # Validate the Business Unit column
# for index, bu_value in df[business_unit_column].iteritems():
#     if bu_value not in valid_business_units:
#         print(f"Error: Invalid Business Unit '{bu_value}'")

# COMMAND ----------

# ### Validation for removing the store column from lp ###
# remove_from_lp = 'Should the store be removed from localized pricing?'
# valid_remove_from_lp = {'Yes', 'No'}

# # Validate the Store column with exception handling
# for index, store_value in df[remove_from_lp].iteritems():
#     if store_value not in valid_remove_from_lp:
#         business_unit = df.loc[index, business_unit_column]  # Get the corresponding Business Unit
#         #raise ValueError(f"Invalid Store value '{store_value}' for Business Unit '{business_unit}'")
#         print(f"Error: Invalid Store value '{store_value}' for Business Unit '{business_unit}'")


# COMMAND ----------

# 5. Validate 'Should the store be removed from localized pricing?'
try:
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
except Exception as e:
    error_messages.append({"Validation": "Remove from Localized Pricing", "Error": str(e)})

# COMMAND ----------

# ### Validation for removing the store column from lp ###
# store_status = 'What happened to the store?'
# valid_store_status = {'The store is permanently closed', 'A new store is opened (NTI)'}

# # Validate the Store column with exception handling
# for index, store_value in df[store_status].iteritems():
#     if store_value not in valid_store_status:
#         business_unit = df.loc[index, business_unit_column]  # Get the corresponding Business Unit
#         #raise ValueError(f"Invalid Store value '{store_value}' for Business Unit '{business_unit}'")
#         print(f"Error: Invalid Store status '{store_value}' for Business Unit '{business_unit}'")

# COMMAND ----------

# 6. Validate 'What happened to the store?' column
try:
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

except Exception as e:
    error_messages.append({"Validation": "Store Status", "Error": str(e)})

# COMMAND ----------

if error_messages:
    error_df = pd.DataFrame(error_messages)
    print("Validation Errors:")
    print(error_df)
else:
    print("All validations passed successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC 2nd sheet validations

# COMMAND ----------

# Load the Excel file -- 2nd sheet #
file_path = '/dbfs/FileStore/Fizza/GSL/GSL_input_format__1_-1.xlsx'  
sheet_name = 'change existing cluster'  

df = pd.read_excel(file_path, sheet_name=sheet_name)
print(df.dtypes)

# COMMAND ----------

# nulls_per_column = df.isnull().sum()
# print("Nulls per column:")
# print(nulls_per_column)

# COMMAND ----------

# 2.1. Validate Nulls in Columns
try:
    # Check for nulls in each column
    nulls_per_column = df.isnull().sum()
    
    # Filter out columns that have no nulls
    columns_with_nulls = nulls_per_column[nulls_per_column > 0]
    
    if not columns_with_nulls.empty:
        for column, null_count in columns_with_nulls.items():
            error_messages.append({"Validation": "Null Check", 
                                    "Error": f"Column '{column}' has {null_count} null values"})
except Exception as e:
    error_messages.append({"Validation": "Null Check", "Error": str(e)})

# COMMAND ----------

#### FOR ALL BU's #####
# 2.2. Validate 7-Digit Numbers in Site Number
try:
    column_to_check = 'What is the site number'  # Replace with your actual column name
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

except Exception as e:
    error_messages.append({"Validation": "Site Number", "Error": str(e)})

# COMMAND ----------

# 2.3. Validate 6-Digit Numbers for 'Central Canada' in Site Number
try:
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
        else:
            print("All Central Canada Business Units have valid 6-digit numbers.")
        
    else:
        print("No 'Central Canada' found in the Business Unit column.")
        
except Exception as e:
    error_messages.append({"Validation": "6-Digit Site Number (Central Canada)", "Error": str(e)})

# COMMAND ----------

# 2.5. Validating the Cluster column
try:
    cluster_column = 'cluster'
    site_id_column = 'site_id'
    
    # Function to check if a value is a one-digit number (0-9)
    def is_one_digit(value):
        if pd.isnull(value):  # Skip null values
            return False
        if isinstance(value, (int, float)):  # For numerical values
            return 0 <= value <= 9 and value == int(value)
        if isinstance(value, str):  # For string representations of numbers
            return value.isdigit() and len(value) == 1
        return False

    # Vectorized check for invalid cluster values
    invalid_cluster_mask = ~df[cluster_column].apply(is_one_digit)

    # If any invalid values are found, collect error messages
    if invalid_cluster_mask.any():
        invalid_cluster_df = df[invalid_cluster_mask][[site_id_column, cluster_column]]
        
        # Collect error messages for each invalid entry
        for _, row in invalid_cluster_df.iterrows():
            error_messages.append({"Validation": "Cluster", 
                                    "Error": f"Site ID: '{row[site_id_column]}', Cluster: '{row[cluster_column]}' (Invalid Cluster Value)"})

except Exception as e:
    error_messages.append({"Validation": "Cluster", "Error": str(e)})

# COMMAND ----------

if error_messages:
    error_df = pd.DataFrame(error_messages)
    print("Validation Errors:")
    print(error_df)
else:
    print("All validations passed successfully.")

# COMMAND ----------

# def send_email(to_address, subject, body):
#     from_address = "s.bokhari@circlek.com"
#     msg = MIMEMultipart()
#     msg['From'] = from_address
#     msg['To'] = to_address
#     msg['Subject'] = subject

#     msg.attach(MIMEText(body, 'plain'))

#     # Setup the server (this example uses outlook SMTP server)
#     server = smtplib.SMTP('smtp.office365.com', 587)
#     server.starttls()
#     server.login(from_address, "Nothing@123")

#     # Send the email
#     server.sendmail(from_address, to_address, msg.as_string())
#     server.quit()


# COMMAND ----------

# if error_messages:
#     error_df = pd.DataFrame(error_messages)
#     print("Validation Errors:")
#     print(error_df)

#     # Get the email address from df_data
#     email_address = df['Email'].iloc[0]  # Assuming only one email per BU

#     # Send email with errors
#     email_body = f"The following validation errors were found:\n{error_df.to_string(index=False)}"
#     send_email(email_address, "Validation Errors Found", email_body)

# else:
#     print("All validations passed successfully.")

#     # Send success email
#     email_address = df['Email'].iloc[0]  # Assuming only one email per BU
#     send_email(email_address, "Validation Passed", "All validations passed successfully.")
