# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType,IntegerType
import numpy as np
import pandas as pd
import re
import pgeocode

# COMMAND ----------

##with open_download_file(connection_string, container_name,, local_file_path)
       ## ADD VALIDATIONS ###
       #####  UPDATE TABLE CODE #######
    #    filtered_stores = pd.read_excel(uploaded_file)

    #    filtered_stores['coordinates'] = filtered_stores['What is the address of the new store?'].apply(lambda x: geocode_address_pgeocode(x))
    #    filtered_stores['coordinates'][filtered_stores['coordinates'].isna() == False]
    #    filtered_stores['latitude'] = filtered_stores['coordinates'].apply(lambda x: x[0])
    #    filtered_stores['longitude'] = filtered_stores['coordinates'].apply(lambda x: x[1])
    # #    try:
    #    site_tbl, df_gsl = read_tables()
    #    site_clusters = get_clusters(filtered_stores, site_tbl)
    #    update_gsl(site_clusters, df_gsl)
    #    webbrowser.open('http://google.com') ### add POWERBI dashboard here
    #    except: 
    #        st.write("error updating table, please ontact code owner")

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

filtered_stores['coordinates'] = filtered_stores['What is the address of the new store?'].apply(lambda x: geocode_address_pgeocode(x))[filtered_stores['coordinates'].notnull()]
filtered_stores['latitude'] = filtered_stores['coordinates'].apply(lambda x: x[0])
filtered_stores['longitude'] = filtered_stores['coordinates'].apply(lambda x: x[1])

# COMMAND ----------

# Function to validate latitude and longitude
def read_tables():
    site_tbl = spark.sql("""
        SELECT gs.business_unit, gs.site_id AS other_site_id, gs.cluster, st.gps_latitude, st.gps_longitude
        FROM circlek_db.grouped_store_list gs
        LEFT JOIN dl_localized_pricing_all_bu.site st ON gs.site_id = st.site_number
        WHERE gs.close_status = 'Open'
    """).toPandas()
    
    df_gsl = spark.sql("SELECT * FROM circlek_db.grouped_store_list")

    return site_tbl, df_gsl


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
        
        site_tbl['distance_miles'] = site_tbl.apply(
            lambda x: haversine(new_store_coords, (x['gps_latitude'], x['gps_longitude']), unit=Unit.MILES)
            if valid_coords(x['gps_latitude'], x['gps_longitude']) else None,
            axis=1
        )
        
        nearest_stores = site_tbl.nsmallest(5, 'distance_miles')
        
        site_clusters = pd.concat(
            [site_clusters, nearest_stores.groupby(['site_id', 'business_unit'])['cluster'].agg(lambda x: x.mode().iloc[0]).reset_index()],
            ignore_index=True
        )

    return site_clusters

#site_clusters = get_clusters(filtered_stores, site_tbl)

# COMMAND ----------

def update_gsl(site_clusters, df_gsl):
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

    final_df.write.mode("overwrite").saveAsTable("usr_shity.temproray_gsl_3")

    return final_df

#final_df = update_gsl(site_clusters, df_gsl)
