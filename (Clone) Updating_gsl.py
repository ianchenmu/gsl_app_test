# Databricks notebook source
!pip install haversine

# COMMAND ----------

jupyter nbconvert --to script your_notebook.ipynb

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType,IntegerType
import numpy as np

# COMMAND ----------

spark.conf.set("fs.azure.account.key.ckdreamteam.blob.core.windows.net", os.getenv('spark_secret '))

# COMMAND ----------

# import pandas as pd
# df = pd.read_excel('/dbfs/FileStore/Shipra/sample_input.xlsx')
# spark_df =spark.createDataFrame(df)
# display(spark_df)

# COMMAND ----------

# spark.conf.set("fs.azure.account.key.ckdreamteam.dfs.core.windows.net", "+vjVf+995ldI3ouglLV8T00f68oyP5YNt1Hhbt7nKGcXPqSme42TX5JgOvXzlrsWh/83Ky9Jb1et+AStvHbYXw==")
# spark_df.write.option("header", True).mode("overwrite").csv("abfss://gsl-files@ckdreamteam.dfs.core.windows.net/sample_input_2.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Reading the file from blob storage

# COMMAND ----------

blob_container = 'gsl-files'
storage_account_name = 'ckdreamteam'
filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/sample_input_2.csv"
gsl = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

# COMMAND ----------

# filePath

# COMMAND ----------

gsl_pd=gsl.toPandas()

# COMMAND ----------

display(gsl_pd)

# COMMAND ----------

!pip install pgeocode

# COMMAND ----------

import pandas as pd
import re
import pgeocode

# Load the CSV file into a DataFrame
df = gsl_pd
# Filter stores where 'Should the store be removed from localized pricing?' is 'No'
# and 'What happened to the store?' contains 'A new store is opened (NTI)'
filtered_stores = df[
    (df['Should the store be removed from localized pricing?'] == 'No') &
    (df['What happened to the store?'].str.contains('A new store is opened', case=False))
]

filtered_stores

# COMMAND ----------

# Function to geocode address using pgeocode based on ZIP code
def geocode_address_pgeocode(address, country='US'):
    """
    Geocode an address to obtain latitude and longitude using pgeocode based on the ZIP code.

    Parameters:
    - address (str): The full address of the store.
    - country (str): The country code (default is 'US').

    Returns:
    - tuple: (latitude, longitude) if successful, otherwise (None, None).
    """
    # Extract ZIP code using regex
    zip_search = re.search(r'\b\d{5}(?:-\d{4})?\b', address)
    if zip_search:
        zip_code = zip_search.group()
    else:
        print(f"ZIP code not found in address: {address}")
        return None, None

    # Initialize pgeocode for the specified country
    nomi = pgeocode.Nominatim(country)
    location = nomi.query_postal_code(zip_code)

    if pd.notnull(location.latitude) and pd.notnull(location.longitude):
        return (location.latitude, location.longitude)
    else:
        print(f"Geocoding failed for ZIP code: {zip_code}")
        return None, None

# Apply geocoding to the filtered addresses for multiple rows
# We will use a loop to process each row individually
coordinates = []
filtered_stores['coordinates'] = filtered_stores['What is the address of the new store?'].apply(lambda x: geocode_address_pgeocode(x))
filtered_stores['coordinates'] = filtered_stores[filtered_stores['coordinates'].notnull()]
filtered_stores['latitude'] = filtered_stores['coordinates'].apply(lambda x: x[0])
filtered_stores['longitude'] = filtered_stores['coordinates'].apply(lambda x: x[1])

filtered_stores.head()

# COMMAND ----------

filtered_stores.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Finding Nearest 5 sites

# COMMAND ----------

sites = """SELECT *  
                       FROM dl_localized_pricing_all_bu.site  
                              """
temp = sqlContext.sql(sites).toPandas()
display(temp)

# COMMAND ----------

# temp.head(5)

# COMMAND ----------

# Function to validate latitude and longitude
site_tbl = spark.sql(""" select gs.business_unit, gs.site_id as other_site_id, gs.cluster , st.gps_latitude , st.gps_longitude from circlek_db.grouped_store_list gs
    left join dl_localized_pricing_all_bu.site st
    on gs.site_id == st.site_number
    where gs.close_status = 'Open' """ ).toPandas()





def get_clusters(filtered_stores, site_tbl):
    def valid_coords(lat, lon):
      if lat is not None and lon is not None:
          if -90 <= lat <= 90 and -180 <= lon <= 180:
              return True
      return False
    site_clusters = pd.DataFrame()
    for index, row in filtered_stores.iterrows():

        site_tbl = site_tbl[site_tbl['business_unit'] == row['What is the business unit of the store?']]

        site_tbl['site_id'] = row['What is the site number']

        new_store_coords = row['coordinates']

        site_tbl['distance_miles'] = site_tbl.apply( lambda x: haversine(new_store_coords, (x['gps_latitude'], x['gps_longitude']), unit=Unit.MILES) if valid_coords(x['gps_latitude'], x['gps_longitude']) else None,
              axis=1)

        nearest_stores = site_tbl.nsmallest(5, 'distance_miles')

        site_clusters = pd.concat( [site_clusters, nearest_stores.groupby(['site_id','business_unit'])['cluster'].agg(lambda x: x.mode().iloc[0]).reset_index()], ignore_index= True)

    return site_clusters
  
site_clusters = get_clusters(filtered_stores, site_tbl)

# COMMAND ----------

required_columns = df_gsl.columns
site_clusters['close_status'] = 'Open'
site_clusters['active_status'] = 'Active'
site_clusters['exclude_from_lp_price'] = 'No'
for col in required_columns:
  if (col not in site_clusters.columns):
    site_clusters[col] = ''

site_clusters.head()

# COMMAND ----------

# Get final dataframe
df_gsl = spark.sql("SELECT * FROM circlek_db.grouped_store_list")

def get_final_gsl(site_clusters, df_gsl):
    required_columns = df_gsl.columns
    site_clusters['close_status'] = 'Open'
    site_clusters['active_status'] = 'Active'
    site_clusters['exclude_from_lp_price'] = 'No'
    for col in required_columns:
      if (col not in site_clusters.columns):
        site_clusters[col] = ''

    # Create a new DataFrame with the desired structure
    schema = StructType([
        StructField("business_unit", StringType(), True),
        StructField("site_id", IntegerType(), True),
        StructField("group", StringType(), True),
        StructField("cluster", StringType(), True),
        StructField("test_launch_date", StringType(), True),
        StructField("exclude_from_measurement", StringType(), True),
        StructField("exclude_from_lp_price", StringType(), True),
        StructField("active_status", StringType(), True),
        StructField("close_status", StringType(), True),
        StructField("lp_ext_phase", StringType(), True)])

    site_clusters = site_clusters[df_gsl.columns]
    # Create a new DataFrame with the correct number of columns
    site_clusters_spark = spark.createDataFrame(site_clusters.replace('', np.NaN), schema=schema)

    # Union the two DataFrames
    final_df = df_gsl.union(site_clusters_spark)

    return final_df
final_df = get_final_gsl(site_clusters, df_gsl)

# COMMAND ----------

temp_df.head()

# COMMAND ----------

import pandas as pd
from haversine import haversine, Unit

temp_df = temp

filtered_stores_df = filtered_stores

# Function to extract latitude and longitude from Coordinates column
def extract_coords(coord_str):
    # Ensure coord_str is a string
    coord_str = str(coord_str)
    if coord_str != '[null,null]':
        coords = eval(coord_str)
        return coords[0], coords[1]
    return None, None

# Function to validate latitude and longitude
def valid_coords(lat, lon):
    if lat is not None and lon is not None:
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return True
    return False
  
# Create a new DataFrame to store results
nearest_stores_results = []

# Iterate through each store in filtered_stores
for index, row in filtered_stores_df.iterrows():
    site_id = row['What is the site number']
    new_store_coords = extract_coords(row['coordinares'])

    if valid_coords(new_store_coords[0], new_store_coords[1]):
        # Calculate distances using Haversine formula
        temp_df['distance_miles'] = temp_df.apply(
            lambda x: haversine(new_store_coords, (x['gps_latitude'], x['gps_longitude']), unit=Unit.MILES) if valid_coords(x['gps_latitude'], x['gps_longitude']) else None,
            axis=1
        )

        # Filter out None distances
        temp_df_filtered = temp_df.dropna(subset=['distance_miles'])

        # Get 5 nearest stores
        nearest_stores = temp_df_filtered.nsmallest(5, 'distance_miles')
        
        # Append the results along with the site_id
        for _, store in nearest_stores.iterrows():
            nearest_stores_results.append({
                'site_id': site_id,
                'nearest_site_number': store['site_number_corporate'],
                'distance_miles': store['distance_miles']
            })

# Create a DataFrame for nearest stores results
nearest_stores_df = pd.DataFrame(nearest_stores_results)

# Display the nearest stores DataFrame
print(nearest_stores_df)

# COMMAND ----------

temp['site_number_corporate']=temp['site_number_corporate'].astype(int)
nearest_stores_df = nearest_stores_df.merge(temp[['site_number_corporate', 'division_desc']], right_on='site_number_corporate', left_on='site_id')
nearest_stores_df.display()

# COMMAND ----------

sites = """SELECT *  
                       FROM circlek_db.grouped_store_list   
                              """
gsl = sqlContext.sql(sites).toPandas()
display(gsl)

# COMMAND ----------

# Create DataFrame
nearest_stores_df = nearest_stores_df.merge(gsl[['site_id','cluster']], on='site_id')
nearest_stores_df.display()

# COMMAND ----------


# Group by 'site_id' and get the mode of 'cluster'
site_clusters = nearest_stores_df.groupby(['site_id','division_desc'])['cluster'].agg(lambda x: x.mode().iloc[0]).reset_index()

# Rename the columns for clarity
site_clusters.columns = ['site_id','business_unit' ,'cluster_mode']

# Display the result
print(site_clusters)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Saving temproray gsl table

# COMMAND ----------

# # Convert Pandas DataFrame to PySpark DataFrame
# temp_spark = spark.createDataFrame(gsl)

# # Save the PySpark DataFrame as a table in Databricks
# temp_spark.write.format("delta").mode("overwrite").saveAsTable("usr_shity.temproray_gsl_3")

# COMMAND ----------

# MAGIC %md
# MAGIC ## updating the gsl table

# COMMAND ----------

gsl.head(5)

# COMMAND ----------

sites = """SELECT *  
                       FROM usr_shity.temproray_gsl_3   
                              """
tmp_gsl = sqlContext.sql(sites).toPandas()
display(tmp_gsl)

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create a Spark session
spark = SparkSession.builder.appName("Update Table").getOrCreate()

# Assuming 'the_table' is the existing Spark DataFrame and 'site_clusters' is your Pandas DataFrame
# Convert the Pandas DataFrame to Spark DataFrame
site_clusters_spark = spark.createDataFrame(site_clusters)

# Create a new DataFrame with the desired structure
new_rows = site_clusters_spark.select(
    F.col("site_id"),
    F.col("business_unit"),
    F.col("cluster_mode").alias("cluster"),
    F.lit(None).alias("group"),
    F.lit(None).alias("test_launch_date"),
    F.lit(None).alias("exclude_from_measurement"),
    F.lit(None).alias("exclude_from_lp_price"),
    F.lit(None).alias("active_status"),
    F.lit(None).alias("close_status"),
    F.lit(None).alias("lp_ext_phase")
)

# Combine the existing table with the new rows
# Assuming tmp_gsl is a Pandas DataFrame, convert it to a Spark DataFrame
tmp_gsl_spark = spark.createDataFrame(tmp_gsl)

# Now you can use the union method
updated_table = tmp_gsl_spark.union(new_rows)

# Display the updated DataFrame
display(updated_table)


# COMMAND ----------

updated_table.select("site_id", lit(4703903).alias("literal_value")).display()

# COMMAND ----------


