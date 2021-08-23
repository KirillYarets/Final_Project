
import psycopg2
import requests
import os
import json
import pyspark.sql.functions as F
import logging

from datetime import datetime
from pyspark.sql.functions import trim, col
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from sqlalchemy import sql

################################## - Connection strings - ################################

pg_creds2 = \
    {
        'host': '192.168.43.150',
        'port': '5432',
        'database': 'dshop_bu',
        'user': 'pguser',
        'password': 'secret'
    }



gp_url = 'jdbc:postgresql://192.168.43.150:5433/postgres'
gp_proporties = {"user": "gpuser", "password": "secret"}

current_date = datetime.now().date()

client = InsecureClient(f'http://192.168.43.150:50070/', user='user')

url_in = 'http://robot-dreams-de-api.herokuapp.com/auth'
url_out = 'http://robot-dreams-de-api.herokuapp.com/out_of_stock'
username = 'rd_dreams'
password = 'djT6LasE'

spark = SparkSession.builder \
    .config('spark.driver.extraClassPath'
            , '/home/user/shared_folder/postgresql-42.2.23.jar') \
    .master('local') \
    .appName("Final") \
    .getOrCreate()

with psycopg2.connect(**pg_creds2) as pg_connection:
    cursor = pg_connection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
    # result = [x[0] for x in cursor.fetchall()]
    result = ['clients', 'store_types', 'products', 'aisles', 'departments', 'location_areas', 'stores', 'orders']
    #result = ['clients', 'orders', 'products', 'aisles', 'departments', 'location_areas', 'stores', 'store_types']

############################## -- Paths -- ##################################################

directory_bronze_api = os.path.join('/', f'4_Temp/Bronse/api_data/folder_{current_date}')
directory_bronze_base = f'4_Temp/Bronse/db_data/folder_{current_date}'

current_date = datetime.now().date()


############################## -- Functions -- ##############################################

#### -- API EXECUTIONS -- ###


def app_api():
    params = [f'{current_date}']

    try:
        for date in params:
            url_auth = url_in
            post_params = {"username": username, "password": password}
            response_post = requests.post(url_auth, json=post_params)

            key = response_post.json()
            params = {"date": date}
            url_token = url_out
            headers = {'Authorization': 'JWT ' + key['access_token']}

            logging.info(f"Writing table_{date} from {url_in} to Bronze")

            response_get = requests.get(url_token, params=params, headers=headers)
            data_r = response_get
            client.write(os.path.join(directory_bronze_api, date + '.json'), data=json.dumps(response_get.json(),))

            if (len(data_r) == 0):
                logging.info(f'Данных за дату {current_date} нет!')
            else:
                logging.info('Sucsess')

            logging.info('Sucsess')
            logging.basicConfig(level=logging.DEBUG)

    except requests.HTTPError as e:
        print('Error!')
        print(e)


### -- Upload data from database --###

def read_jdbc_shop_base():
    try:
        for params in result:
            logging.info(f"Writing table_{params} from {pg_creds2} to Bronze")

            with client.write(os.path.join('/', directory_bronze_base, params + '.csv')) as csv_file:
                cursor.copy_expert(f'COPY {params} TO STDOUT WITH HEADER CSV', csv_file)

    except ConnectionResetError:
        print("==> ConnectionResetError")

    except requests.HTTPError as e:
        print('Error!')
        print(e)

    except TimeoutError:
        print("==> Timeout")

    except Exception as t:
        print(t)



### -- Spark agregation -- ###


def spark_code_base():
    tables_spark = result
    try:
        logging.info(f"Writing table_{tables_spark[0]} from {pg_creds2} to Silver")

        df_base = spark.read.csv(
            f"webhdfs://192.168.43.150:50070/4_Temp/Bronse/db_data/folder_{current_date}/{tables_spark[0]}.csv"
            ,header = True)
        df_base = df_base.dropDuplicates()
        df_base = df_base.where(F.col('fullname').isNotNull())
        df_base = df_base.withColumn('fullname', trim('fullname'))
        df_base.write.parquet(f"/4_Temp/Silver/d_shop/{current_date}/{tables_spark[0].rsplit('.', 1)[0]}/file.parquet",
                              mode='overwrite')
    except requests.RequestException as e:
        print('Error!')
        print(e)

    ###########################


    try:

        logging.info(f"Writing table_{tables_spark[1]} from {pg_creds2} to Silver")

        df_base = spark.read.csv(
            f"webhdfs://192.168.43.150:50070/4_Temp/Bronse/db_data/folder_{current_date}/{tables_spark[1]}.csv"
            , header=True)
        df_base = df_base.dropDuplicates()
        df_base = df_base.where(F.col('type').isNotNull())
        df_base = df_base.withColumn('type', trim('type'))
        df_base.write.parquet(f"/4_Temp/Silver/d_shop/{current_date}/{tables_spark[1].rsplit('.', 1)[0]}/file.parquet",
                              mode='overwrite')

    except requests.RequestException as e:
        print('Error!')
        print(e)


    ##########################
    try:
        logging.info(f"Writing table_{tables_spark[2]} from {pg_creds2} to Silver")

        df_base = spark.read.csv(
            f"webhdfs://192.168.43.150:50070/4_Temp/Bronse/db_data/folder_{current_date}/{tables_spark[2]}.csv"
            ,header = True)
        df_base = df_base.dropDuplicates()
        df_base = df_base.where(F.col('product_name').isNotNull())
        df_base = df_base.withColumn('product_name', trim('product_name'))
        df_base.write.parquet(f"/4_Temp/Silver/d_shop/{current_date}/{tables_spark[2].rsplit('.', 1)[0]}/file.parquet",
                              mode='overwrite')

    except requests.RequestException as e:
        print('Error!')
        print(e)

    ##################################
    try:
        logging.info(f"Writing table_{tables_spark[3]} from {pg_creds2} to Silver")

        df_base = spark.read.csv(
            f"webhdfs://192.168.43.150:50070/4_Temp/Bronse/db_data/folder_{current_date}/{tables_spark[3]}.csv"
            ,header = True)
        df_base = df_base.dropDuplicates()
        df_base = df_base.where(F.col('aisle').isNotNull())
        df_base = df_base.withColumn('aisle', trim('aisle'))
        df_base.write.parquet(f"/4_Temp/Silver/d_shop/{current_date}/{tables_spark[3].rsplit('.', 1)[0]}/file.parquet",
                              mode='overwrite')

    except requests.RequestException as e:
        print('Error!')
        print(e)

    ####################################
    try:

        logging.info(f"Writing table_{tables_spark[4]} from {pg_creds2} to Silver")

        df_base = spark.read.csv(
            f"webhdfs://192.168.43.150:50070/4_Temp/Bronse/db_data/folder_{current_date}/{tables_spark[4]}.csv"
            ,header = True)
        df_base = df_base.dropDuplicates()
        df_base = df_base.where(F.col('department').isNotNull())
        df_base = df_base.withColumn('department', trim('department'))
        df_base.write.parquet(f"/4_Temp/Silver/d_shop/{current_date}/{tables_spark[4].rsplit('.', 1)[0]}/file.parquet",
                              mode='overwrite')

    except requests.RequestException as e:
        print('Error!')
        print(e)

    ####################################
    try:

        logging.info(f"Writing table_{tables_spark[5]} from {pg_creds2} to Silver")

        df_base = spark.read.csv(
            f"webhdfs://192.168.43.150:50070/4_Temp/Bronse/db_data/folder_{current_date}/{tables_spark[5]}.csv"
            ,header = True)
        df_base = df_base.dropDuplicates()
        df_base = df_base.where(F.col('area').isNotNull())
        df_base = df_base.withColumn('area', trim('area'))
        df_base.write.parquet(f"/4_Temp/Silver/d_shop/{current_date}/{tables_spark[5].rsplit('.', 1)[0]}/file.parquet",
                              mode='overwrite')

    except requests.RequestException as e:
        print('Error!')
        print(e)

    ####################################
    try:
        logging.info(f"Writing table_{tables_spark[6]} from {pg_creds2} to Silver")

        df_base = spark.read.csv(
            f"webhdfs://192.168.43.150:50070/4_Temp/Bronse/db_data/folder_{current_date}/{tables_spark[6]}.csv"
            ,header = True)
        df_base = df_base.dropDuplicates()
        df_base.write.parquet(f"/4_Temp/Silver/d_shop/{current_date}/{tables_spark[6].rsplit('.', 1)[0]}/file.parquet",
                              mode='overwrite')

    except requests.RequestException as e:
        print('Error!')
        print(e)

    ####################################
    try:
        logging.info(f"Writing table_{tables_spark[7]} from {pg_creds2} to Silver")

        df_base = spark.read.csv(
            f"webhdfs://192.168.43.150:50070/4_Temp/Bronse/db_data/folder_{current_date}/{tables_spark[7]}.csv"
            , header=True)
        df_base = df_base.dropDuplicates()
        df_base = df_base.where(F.col('product_id').isNotNull())
        df_base.write.parquet(f"/4_Temp/Silver/d_shop/{current_date}/{tables_spark[7].rsplit('.', 1)[0]}/file.parquet",
                              mode='overwrite')

    except requests.RequestException as e:
        print('Error!')
        print(e)


#
### -- Spark API Executions -- ###
params_spark2 = [f'{current_date}.json']


def spark_code_api():

    try:
        params_spark2 = [f'{current_date}.json']

        for tables_api in params_spark2:
            df_api = spark.read.json(
                f"webhdfs://192.168.43.150:50070/4_Temp/Bronse/api_data/folder_{current_date}/{tables_api}")
            df_api = df_api.dropDuplicates()
            df_api.write.parquet(f"/4_Temp/Silver/api/{current_date}/{tables_api.rsplit('.', 1)[0]}/file.parquet",
                                 mode='overwrite')

    except requests.RequestException as e:
        print('Error!')
        print(e)





### -- Insert data into database -- ###

def cr_tables():
    try:
            df_base = spark.read.parquet(
                f"webhdfs://192.168.43.150:50070/4_Temp/Silver/d_shop/{current_date}/aisles/file.parquet", header=True)

            df_base = df_base.withColumn("aisle_id", col("aisle_id").cast("int"))
            df_base = df_base.withColumn("aisle", col("aisle").cast("string"))

            df_base.write.jdbc(gp_url
                               , table=f"dim_aisles"
                               , properties=gp_proporties
                               , mode='overwrite')
    except requests.RequestException as e:
        print('Error!')
        print(e)


    try:
            df_base = spark.read.parquet(
                f"webhdfs://192.168.43.150:50070/4_Temp/Silver/d_shop/{current_date}/clients/file.parquet", header=True)

            df_base = df_base.withColumn("id", col("id").cast("int"))
            df_base = df_base.withColumn("fullname", col("fullname").cast("string"))
            df_base = df_base.withColumn("location_area_id", col("location_area_id").cast("int"))

            df_base.write.jdbc(gp_url
                               , table=f"dim_clients"
                               , properties=gp_proporties
                               , mode='overwrite')
    except requests.RequestException as e:
        print('Error!')
        print(e)

    try:
        df_base = spark.read.parquet(
            f"webhdfs://192.168.43.150:50070/4_Temp/Silver/d_shop/{current_date}/departments/file.parquet", header=True)

        df_base = df_base.withColumn("department_id", col("department_id").cast("int"))
        df_base = df_base.withColumn("department", col("department").cast("string"))

        df_base.write.jdbc(gp_url
                           , table=f"dim_departments"
                           , properties=gp_proporties
                           , mode='overwrite')
    except requests.RequestException as e:
        print('Error!')
        print(e)




    try:
        df_base = spark.read.parquet(
            f"webhdfs://192.168.43.150:50070/4_Temp/Silver/d_shop/{current_date}/location_areas/file.parquet", header=True)

        df_base = df_base.withColumn("area_id", col("area_id").cast("int"))
        df_base = df_base.withColumn("area", col("area").cast("string"))

        df_base.write.jdbc(gp_url
                           , table=f"dim_location_areas"
                           , properties=gp_proporties
                           , mode='overwrite')
    except requests.RequestException as e:
        print('Error!')
        print(e)



    try:
        df_base = spark.read.parquet(
            f"webhdfs://192.168.43.150:50070/4_Temp/Silver/d_shop/{current_date}/orders/file.parquet",
            header=True)

        df_base = df_base.withColumn("order_id", col("order_id").cast("int"))
        df_base = df_base.withColumn("product_id", col("product_id").cast("int"))
        df_base = df_base.withColumn("client_id", col("client_id").cast("int"))
        df_base = df_base.withColumn("store_id", col("store_id").cast("int"))
        df_base = df_base.withColumn("quantity", col("quantity").cast("int"))
        df_base = df_base.withColumn("order_date", col("order_date").cast("date"))

        df_base.write.jdbc(gp_url
                           , table=f"fct_orders"
                           , properties=gp_proporties
                           , mode='overwrite')
    except requests.RequestException as e:
        print('Error!')
        print(e)




    try:
        df_base = spark.read.parquet(
            f"webhdfs://192.168.43.150:50070/4_Temp/Silver/d_shop/{current_date}/products/file.parquet",
            header=True)

        df_base = df_base.withColumn("product_id", col("product_id").cast("int"))
        df_base = df_base.withColumn("product_name", col("product_name").cast("string"))
        df_base = df_base.withColumn("aisle_id", col("aisle_id").cast("int"))
        df_base = df_base.withColumn("department_id", col("department_id").cast("int"))


        df_base.write.jdbc(gp_url
                           , table=f"dim_products"
                           , properties=gp_proporties
                           , mode='overwrite')
    except requests.RequestException as e:
        print('Error!')
        print(e)



    try:
        df_base = spark.read.parquet(
            f"webhdfs://192.168.43.150:50070/4_Temp/Silver/d_shop/{current_date}/store_types/file.parquet",
            header=True)

        df_base = df_base.withColumn("store_type_id", col("store_type_id").cast("int"))
        df_base = df_base.withColumn("type", col("type").cast("string"))

        df_base.write.jdbc(gp_url
                           , table=f"dim_store_types"
                           , properties=gp_proporties
                           , mode='overwrite')
    except requests.RequestException as e:
        print('Error!')
        print(e)



    try:
        df_base = spark.read.parquet(
            f"webhdfs://192.168.43.150:50070/4_Temp/Silver/d_shop/{current_date}/stores/file.parquet",
            header=True)

        df_base = df_base.withColumn("store_id", col("store_id").cast("int"))
        df_base = df_base.withColumn("location_area_id", col("location_area_id").cast("int"))
        df_base = df_base.withColumn("store_type_id", col("store_type_id").cast("int"))

        df_base.write.jdbc(gp_url
                           , table=f"dim_stores"
                           , properties=gp_proporties
                           , mode='overwrite')
    except requests.RequestException as e:
        print('Error!')
        print(e)

    try:
        df_base = spark.read.parquet(
            f"webhdfs://192.168.43.150:50070/4_Temp/Silver/api/{current_date}/{current_date}/file.parquet", header=True)

        df_base = df_base.withColumn("product_id", col("product_id").cast("int"))
        df_base = df_base.withColumn("date", col("date").cast("date"))

        df_base.write.jdbc(gp_url
                           , table=f"fct_oos"
                           , properties=gp_proporties
                           , mode='overwrite')
    except requests.RequestException as e:
        print('Error!')
        print(e)

def cr_keys():
    try:

        pg_gren = psycopg2.connect(
            host='192.168.43.150',
            port='5433',
            database='postgres',
            user='gpuser',
            password='secret')

        cursor = pg_gren.cursor()
        query = "ALTER TABLE dim_aisles ADD CONSTRAINT dim_aisles_pk PRIMARY KEY (aisle_id) ; \
                 ALTER TABLE dim_products ADD CONSTRAINT products_pk PRIMARY KEY (product_id) ; \
                 ALTER TABLE dim_clients ADD CONSTRAINT clients_pkey PRIMARY KEY(id); \
                 ALTER TABLE dim_stores ADD CONSTRAINT stores_pkey PRIMARY KEY(store_id); \
                 ALTER TABLE dim_departments ADD CONSTRAINT departments_pk PRIMARY KEY(department_id); \
                 ALTER TABLE dim_location_areas ADD CONSTRAINT location_areas_pkey PRIMARY KEY(area_id); \
                 ALTER TABLE dim_store_types ADD CONSTRAINT store_types_pkey PRIMARY KEY(store_type_id); \
                 ALTER TABLE fct_orders ADD CONSTRAINT orders_clients_id_fk FOREIGN KEY(client_id) REFERENCES dim_clients(id); \
                 ALTER TABLE fct_orders ADD CONSTRAINT orders_products_id_fk FOREIGN KEY(product_id) REFERENCES dim_products(product_id); \
                 ALTER TABLE dim_products ADD CONSTRAINT products_department_id_fk FOREIGN KEY(department_id) REFERENCES dim_departments(department_id); \
                 ALTER TABLE dim_products ADD FOREIGN KEY (aisle_id) REFERENCES dim_aisles(aisle_id); \
                 ALTER TABLE fct_orders ADD CONSTRAINT stores_id_fk FOREIGN KEY(store_id) REFERENCES dim_stores(store_id);\
                 ALTER TABLE dim_stores ADD CONSTRAINT stores_location_areas_id_fk FOREIGN KEY(location_area_id) REFERENCES dim_location_areas(area_id); \
                 ALTER TABLE dim_stores ADD CONSTRAINT stores_types_store_id_fk FOREIGN KEY(store_type_id) REFERENCES dim_store_types(store_type_id); \
                 ALTER TABLE fct_oos ADD CONSTRAINT oos_product_id_fk FOREIGN KEY(product_id) REFERENCES dim_products(product_id); \
                 commit;"

        cursor.execute(query)
        pg_gren.commit()

    except requests.RequestException as e:
        print('Error!')
        print(e)


################# -- DAGS --#############################################


dag_api = DAG(
    dag_id='API',
    description='API DAG',
    start_date=datetime(2021, 7, 15, 14, 30),
    end_date=datetime(9999, 7, 15, 14, 30),
    schedule_interval='@daily'
)

dag_base = DAG(
    dag_id='DATA_BASE',
    description='BASE DAG',
    start_date=datetime(2021, 7, 15, 14, 30),
    end_date=datetime(9999, 7, 15, 14, 30),
    schedule_interval='@daily'
)

dag_spark_api = DAG(
    dag_id='SPARK_API',
    description='SPARK API DAG',
    start_date=datetime(2021, 7, 15, 14, 30),
    end_date=datetime(9999, 7, 15, 14, 30),
    schedule_interval='@daily'
)

dag_spark_base = DAG(
    dag_id='SPARK_BASE',
    description='SPARK BASE DAG',
    start_date=datetime(2021, 7, 15, 14, 30),
    end_date=datetime(9999, 7, 15, 14, 30),
    schedule_interval='@daily'
)

dag_grinplan_base = DAG(
    dag_id='GRINPLAN_BASE',
    description='SPARK BASE DAG',
    start_date=datetime(2021, 7, 15, 14, 30),
    end_date=datetime(9999, 7, 15, 14, 30),
    schedule_interval='@daily'
)


dag_keys = DAG(
    dag_id='Keys',
    description='SPARK BASE DAG',
    start_date=datetime(2021, 7, 15, 14, 30),
    end_date=datetime(9999, 7, 15, 14, 30),
    schedule_interval='@daily'
)



################## -- Tasks -- ##############################

task_api = PythonOperator(
    task_id='api_task',
    dag=dag_api,
    python_callable=app_api
)

task_base = PythonOperator(
    task_id='base_task',
    dag=dag_base,
    python_callable=read_jdbc_shop_base
)

task_spark_api = PythonOperator(
    task_id='Bronze_base_task',
    dag=dag_spark_api,
    python_callable=spark_code_api
)

task_spark_base = PythonOperator(
    task_id='Bronze_base_task',
    dag=dag_spark_base,
    python_callable=spark_code_base
)

tag_grinplan_base = PythonOperator(
    task_id='dag_grinplan_base',
    dag=dag_grinplan_base,
    python_callable=cr_tables
)

tag_keys = PythonOperator(
    task_id='dag_keys',
    dag=dag_keys,
    python_callable=cr_keys
)
