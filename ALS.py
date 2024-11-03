import time
import csv
from numpy import long
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
from hbase.rest_client import HBaseRESTClient
from hbase.admin import HBaseAdmin
from hbase.scan import Scan
from hbase.scan_filter_helper import build_single_column_value_filter
import os
from pyspark.sql.types import IntegerType, LongType 
import sys
from hbase.get import Get

# Configure PySpark
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = "notebook"
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_PYTHON'] = sys.executable

# Create a Spark session
spark = SparkSession.builder \
    .appName("HBase ALS Recommendation System 2") \
    .master("local[*]") \
    .getOrCreate()

client = HBaseRESTClient(['http://localhost:8080'])

def fetch_user_purchases1(table_name):
    purchases = []
    
    # Iterate over the range of users and products
    for user_id in range(100, 201):
        for product_id in range(100, 201):
            # Construct the key using the format 'user_XXX_product_XXX'
            row_key = f'user_{user_id}_product_{product_id}'
            
            # Create a GET request for the specific row
            get = Get(client)  # Encode the key to bytes as required by HBase
            
            # Execute the GET request
            result = get.get(table_name, row_key)
            
            if 'row' in result and len(result['row']) > 0:
                cells = result['row'][0]['cell']
                if len(cells) >= 2:
                    value_bytes_latest = cells[0]['$']  # Review cell
                    value_bytes_o = cells[1]['$']  # Quantity cell
                    value_bytes_q = cells[2]['$']
                    value_bytes_r = cells[3]['$']
                    
                    # Convert byte values to integers
                    value_int_r = int.from_bytes(value_bytes_r, byteorder='big') 
                    value_int_q = int.from_bytes(value_bytes_q, byteorder='big') 
                    value_int_o = int.from_bytes(value_bytes_o, byteorder='big')
                    value_int_latest = int.from_bytes(value_bytes_latest, byteorder='big') 
                
                # Append the result to purchases
                purchases.append((user_id, product_id, value_int_q, value_int_r, value_int_o, value_int_latest))
                
    
    return purchases

def run_recommendation_system():
    table_name = "user_purchases1"
    data = fetch_user_purchases1(table_name)
    print("fetched")
    columns = ["cid", "pid", "quantity", "highestreview", "orders", "latest"]
    user_purchases1_df = spark.createDataFrame(data, schema=columns)

    user_purchases1_df = user_purchases1_df.withColumn(
        "rating",
        F.when(user_purchases1_df.highestreview == 0, None).otherwise(user_purchases1_df.quantity * (user_purchases1_df.highestreview / user_purchases1_df.orders))
    )
    print("t1")
    user_purchases1_df = user_purchases1_df.filter(user_purchases1_df.rating.isNotNull())

    als_train_data = user_purchases1_df.select("cid", "pid", "rating")
    als_train_data = als_train_data \
        .withColumn("cid", F.col("cid").cast(IntegerType())) \
        .withColumn("pid", F.col("pid").cast(LongType()))
    print("t2")

    als = ALS(userCol="cid", itemCol="pid", ratingCol="rating", coldStartStrategy="drop", implicitPrefs=True, rank=10,maxIter=10, regParam=0.1, alpha=1.0)
    model = als.fit(als_train_data)
    print("t3")

    user_recommendations = model.recommendForAllUsers(20)

    # Get purchased items for each user, ensuring types are consistent
    purchased_items = user_purchases1_df.select("cid", "pid").rdd \
        .map(lambda x: (int(x[0]), long(x[1]))) \
        .groupByKey() \
        .mapValues(set) \
        .collectAsMap()

    filtered_recommendations = []
    for row in user_recommendations.collect():
        cid = row['cid']
        recommendations = row['recommendations']
        cid = int(cid)  # Explicitly cast cid to int
        purchased_set = purchased_items.get(cid, set())
        
        filtered_recommendations_for_user = [
            rec for rec in recommendations if int(rec['pid']) not in purchased_set
        ][:7] 

        filtered_recommendations.append((cid, filtered_recommendations_for_user))

    output_file = "user_recommendations.csv"
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["cid", "recommendations"])
        for cid, recommendations in filtered_recommendations:
            formatted_recommendations = [(rec['pid'], rec['rating']) for rec in recommendations]
            writer.writerow([cid, formatted_recommendations])



while True:
    run_recommendation_system()
    time.sleep(20)
    print("done")


