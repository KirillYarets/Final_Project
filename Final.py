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


################################## - Connection strings - ################################

pg_creds2 = \
    {
        'host': '192.168.0.110',
        'port': '5432',
        'database': 'dshop_bu',
        'user': 'pguser',
        'password': 'secret'
    }


gp_url= 'jdbc:postgresql://192.168.0.111:5433/postgres'
gp_proporties = {"user": "gpuser", "password": "secret"}

current_date = datetime.now().date()

client = InsecureClient(f'http://192.168.0.111:50070/', user='user')



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
    #result = [x[0] for x in cursor.fetchall()]
    result = ['clients','orders','products','aisles','departments','location_areas','stores','store_types']


############################## -- Paths -- ##################################################

directory_bronze_api = os.path.join('/', f'Data_lake/Bronse/api_data/folder_{current_date}')
directory_bronze_base = f'Data_lake/Bronse/db_data/folder_{current_date}'

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
            client.write(os.path.join(directory_bronze_api, date + '.json'), data=json.dumps(response_get.json()))

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

                    logging.info('Sucsess')
    except requests.HTTPError as e:
        print('Error!')
        print(e)


### -- Spark agregation -- ###


def spark_code_base():
    tables_spark = result

    logging.info(f"Writing table_{tables_spark[0]} from {pg_creds2} to Silver")

    df_base = spark.read.csv(
        f"webhdfs://192.168.0.110:50070/Data_lake/Bronse/db_data/folder_{current_date}/{tables_spark[0]}.csv")
    df_base = df_base.dropDuplicates()
    df_base = df_base.where(F.col('fulname').isNotNull())
    df_base = df_base.withColumn('fulname',trim('fulname'))
    df_base.write.csv(f"/Data_lake/Silver/d_shop/{current_date}/{tables_spark[0]}.csv",
                          mode='overwrite')

    logging.info('Sucsess')

    ###########################

    logging.info(f"Writing table_{tables_spark[1]} from {pg_creds2} to Silver")

    df_base = spark.read.csv(
        f"webhdfs://192.168.0.110:50070/Data_lake/Bronse/db_data/folder_{current_date}/{tables_spark[1]}.csv")
    df_base = df_base.dropDuplicates()
    df_base = df_base.where(F.col('product_id').isNotNull())
    df_base.write.csv(f"/Data_lake/Silver/d_shop/{current_date}/{tables_spark[1]}.csv",
                          mode='overwrite')

    logging.info('Sucsess')

    ##########################

    logging.info(f"Writing table_{tables_spark[2]} from {pg_creds2} to Silver")

    df_base = spark.read.csv(
        f"webhdfs://192.168.0.110:50070/Data_lake/Bronse/db_data/folder_{current_date}/{tables_spark[2]}.csv")
    df_base = df_base.dropDuplicates()
    df_base = df_base.where(F.col('product_name').isNotNull())
    df_base = df_base.withColumn('product_name', trim('product_name'))
    df_base.write.csv(f"/Data_lake/Silver/d_shop/{current_date}/{tables_spark[2]}.csv",
                          mode='overwrite')

    logging.info('Sucsess')

    ##################################

    logging.info(f"Writing table_{tables_spark[3]} from {pg_creds2} to Silver")

    df_base = spark.read.csv(
        f"webhdfs://192.168.0.110:50070/Data_lake/Bronse/db_data/folder_{current_date}/{tables_spark[3]}.csv")
    df_base = df_base.dropDuplicates()
    df_base = df_base.where(F.col('aisle').isNotNull())
    df_base = df_base.withColumn('aisle', trim('aisle'))
    df_base.write.csv(f"/Data_lake/Silver/d_shop/{current_date}/{tables_spark[3]}.csv",
                          mode='overwrite')

    logging.info('Sucsess')

    ####################################

    logging.info(f"Writing table_{tables_spark[4]} from {pg_creds2} to Silver")

    df_base = spark.read.csv(
        f"webhdfs://192.168.0.110:50070/Data_lake/Bronse/db_data/folder_{current_date}/{tables_spark[4]}.csv")
    df_base = df_base.dropDuplicates()
    df_base = df_base.where(F.col('department').isNotNull())
    df_base = df_base.withColumn('department', trim('department'))
    df_base.write.csv(f"/Data_lake/Silver/d_shop/{current_date}/{tables_spark[4]}.csv",
                          mode='overwrite')

    logging.info('Sucsess')

    ####################################

    logging.info(f"Writing table_{tables_spark[5]} from {pg_creds2} to Silver")

    df_base = spark.read.csv(
        f"webhdfs://192.168.0.110:50070/Data_lake/Bronse/db_data/folder_{current_date}/{tables_spark[5]}.csv")
    df_base = df_base.dropDuplicates()
    df_base = df_base.where(F.col('area').isNotNull())
    df_base = df_base.withColumn('area', trim('area'))
    df_base.write.csv(f"/Data_lake/Silver/d_shop/{current_date}/{tables_spark[5]}.csv",
                          mode='overwrite')

    logging.info('Sucsess')

    ####################################

    logging.info(f"Writing table_{tables_spark[6]} from {pg_creds2} to Silver")

    df_base = spark.read.csv(
        f"webhdfs://192.168.0.110:50070/Data_lake/Bronse/db_data/folder_{current_date}/{tables_spark[6]}.csv")
    df_base = df_base.dropDuplicates()
    df_base.write.csv(f"/Data_lake/Silver/d_shop/{current_date}/{tables_spark[6]}.csv",
                          mode='overwrite')

    logging.info('Sucsess')

    ####################################

    logging.info(f"Writing table_{tables_spark[7]} from {pg_creds2} to Silver")

    df_base = spark.read.csv(
        f"webhdfs://192.168.0.110:50070/Data_lake/Bronse/db_data/folder_{current_date}/{tables_spark[7]}.csv")
    df_base = df_base.dropDuplicates()
    df_base = df_base.where(F.col('type').isNotNull())
    df_base = df_base.withColumn('type', trim('type'))
    df_base.write.csv(f"/Data_lake/Silver/d_shop/{current_date}/{tables_spark[7]}.csv",
                          mode='overwrite')

    logging.info('Sucsess')

### -- Spark API Executions -- ###
params_spark2 = [f'{current_date}.json']

def spark_code_api():

    for tables_api in params_spark2:
        logging.info(f"Writing table_{tables_api} from {pg_creds2} to Silver")

        df_api = spark.read.json(
            f"webhdfs://192.168.0.110:50070/Data_lake/Bronse/api_data/folder_{current_date}/{tables_api}")
        df_api = df_api.dropDuplicates()
        df_api.write.csv(f"/Data_lake/Silver/api/{current_date}/{tables_api}.csv",
                             mode='overwrite')
        logging.info('Sucsess')

### -- Insert data into database -- ###

def cr_tables():

    for params_ins in result:

        df_base = spark.read.parquet(
            f"webhdfs://192.168.0.111:50070/Silver/d_shop/{current_date}/{params_ins}.csv", header=True)

        df_base.write.jdbc(gp_url
                     , table=f"dim_{params_ins}"
                     , properties=gp_proporties
                     , mode='overwrite')


    for params_ins_api in params_spark2:

        df_base = spark.read.parquet(
            f"webhdfs://192.168.0.111:50070/Silver/api/{current_date}/{params_ins_api}.csv", header=True)

        df_base.write.jdbc(gp_url
                     , table=f"dim_{params_ins_api}"
                     , properties=gp_proporties
                     , mode='overwrite')


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
    end_date=datetime(9998, 7, 15, 14, 30),
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
