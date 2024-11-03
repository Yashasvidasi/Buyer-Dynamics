import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType
import requests
import json
import datetime
from hbase.rest_client import HBaseRESTClient # type: ignore
from hbase.admin import HBaseAdmin # type: ignore
import ast
from hbase.get import Get # type: ignore
from hbase.put import Put # type: ignore

def write_to_hbase_user_purchases1(cid, pid, quantity, review, GET, PUT):
    # Construct the row key based on user_id and product_id
    row_key = f"user_{cid}_product_{pid}"

    # Fetch existing data from HBase
    y = GET.get("user_purchases1", row_key)
    print("<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>",y)

    # Initialize default values for quantity and review
    value_int_q = 0
    value_int_r = 0
    value_int_o = 0
    value_int_latest = 0

    # Check if row exists and extract the existing values
    if 'row' in y and len(y['row']) > 0:
        cells = y['row'][0]['cell']
        # Ensure that the cells for quantity and review exist before accessing them
        if len(cells) >= 2:
            value_bytes_latest = cells[0]['$']  # Review cell
            value_bytes_o = cells[1]['$']  # Quantity cell
            value_bytes_q = cells[2]['$']
            value_bytes_r = cells[3]['$']
            
            # Convert byte values to integers
            value_int_r = int.from_bytes(value_bytes_r, byteorder='big') 
            value_int_q = int.from_bytes(value_bytes_q, byteorder='big') 
            value_int_o = int.from_bytes(value_bytes_o, byteorder='big') 
            

    # Update the quantity and review
    value_int_q += quantity
    value_int_r += review
    value_int_o +=1
    value_int_latest = review
    # Store the updated data in HBase
    PUT.put('user_purchases1', row_key, {
        "details:quantity": value_int_q, 
        "details:review": value_int_r,
        "details:orders":value_int_o,
        "details:latest":value_int_latest,

    })

def write_to_order(GET, PUT):
    row_key = "tt"
    y = GET.get("orders", row_key)
    q = 0
    if 'row' in y and len(y['row']) > 0:
        cells = y['row'][0]['cell']
        # Ensure that the cells for quantity and review exist before accessing them
        if len(cells) >= 1:
            value_bytes_r = cells[0]['$']  # Review cell
            q = int.from_bytes(value_bytes_r, byteorder='big') 
    PUT.put('orders', row_key, {"total:final":q+1})
            




def write_to_hbase_product_info1(pid, quantity_sold, current_time, GET, PUT):
    print(f"[DEBUG]: Writing to HBase for product {pid} at {current_time.minute}")
    # Construct the row key based on product_id
    row_key = f"product_{pid}"

    # Fetch existing data from HBase
    try:
        y = GET.get("product_info1", row_key)
        print(f"[DEBUG]: Retrieved data for row_key={row_key}: {y}")
    except Exception as e:
        print(f"[ERROR]: Failed to retrieve data for row_key={row_key}: {e}")
        return

    # Initialize default values for total sales and quantity sold
    total_sales = 0
    current_quantity_sold = 0

    # Check if row exists and extract the existing total sales and current minute data
    if 'row' in y and len(y['row']) > 0:
        cells = y['row'][0]['cell']
        print(f"[DEBUG]: Extracted cells: {cells}")
        # Extract the total sales
        for cell in cells:
            if cell['column'] == b'sales:total':
                total_sales_value = cell['$']
                total_sales = int.from_bytes(total_sales_value, byteorder='big')
                break

        # Extract quantity sold for the current minute
        minute_col = f"sales:{current_time.minute}".encode('utf-8')
        for cell in cells:
            if cell['column'] == minute_col:
                value_bytes_q = cell['$']
                current_quantity_sold = int.from_bytes(value_bytes_q, byteorder='big')
                break

    # Update total sales and quantity sold for the current minute
    total_sales += quantity_sold
    quantity_sold += current_quantity_sold

    print(f"[DEBUG]: Updating quantity_sold={quantity_sold}, total_sales={total_sales}, current_quantity={current_quantity_sold}")

    # Store the updated data in HBase
    try:
        PUT.put('product_info1', row_key, {
            f"sales:{current_time.minute}": quantity_sold,
            "sales:total": total_sales
        })
        print(f"[DEBUG]: Successfully updated HBase for row_key={row_key}")
    except Exception as e:
        print(f"[ERROR]: Failed to update HBase for row_key={row_key}: {e}")


    

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("KafkaStructuredStreamingApp") \
        .config("spark.sql.streaming.checkpointLocation", "/usr/demo/finalcps") \
        .config("spark.local.dir", "D:/worker/d1,D:/worker/d2") \
        .master("yarn") \
        .getOrCreate()

    # Kafka broker and topic
    broker = "localhost:9092"
    topic = "demo"

    #HBASE connector
    client = HBaseRESTClient(['http://localhost:8080'])
    admin = HBaseAdmin(client)

    get = Get(client)
    put = Put(client)

    kafkaDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", broker) \
        .option("subscribe", topic) \
        .load()

    # Define the schema for the expected data
    schema = StructType([
        StructField("cid", IntegerType()),
        StructField("pid", IntegerType()),
        StructField("invoicenum", IntegerType()),
        StructField("quantity", IntegerType()),
        StructField("review", IntegerType())
    ])

    # Parse the incoming Kafka data
    parsedDF = kafkaDF.selectExpr("CAST(value AS STRING)").select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")

    # Process the streaming data
    def process_batch(batch_df, batch_id):
        print(f"[DEBUG]: Processing batch {batch_id}")
        for row in batch_df.collect():
            cid = row.cid
            pid = row.pid
            quantity = row.quantity
            review = row.review
            print(f"[DEBUG]: Processing cid={cid}, pid={pid}, quantity={quantity}, review={review}")

            # Task 1: Update user purchases table
            write_to_hbase_user_purchases1(cid, pid, quantity, review, get, put)

            # Task 2: Update product info table
            current_time = datetime.datetime.now()
            write_to_hbase_product_info1(pid, quantity, current_time, get, put)

            write_to_order(get, put)


    # Write stream to HBase using foreachBatch
    query = parsedDF.writeStream \
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()
