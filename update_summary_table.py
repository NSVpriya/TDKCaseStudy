
import cx_Oracle
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("SummaryTableUpdate") \
    .getOrCreate()

dsn_tns = cx_Oracle.makedsn('Host Name', 'Port Number', service_name='Service Name')
con = cx_Oracle.connect('user', 'password', dsn_tns)
cur = con.cursor()

# Load processed data from Oracle DB
processed_data = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:host:1521:database") \
    .option("dbtable", "test") \
    .option("driver", "oracle.jdbc.driver.OracleDriver") \
    .load()

# Create a temporary view from processed data
processed_data.createOrReplaceTempView("processed_data")


def create_tables():
    table_name='user'
    cur.execute(f"SELECT count(*) FROM all_tables WHERE table_name = {table_name}")
    table_exists = cur.fetchone()[0]
    if not table_exists:
        cur.execute('''create table user (user_id int)''')
        print("User table created")

    table_name='user_request'
    cur.execute(f"SELECT count(*) FROM all_tables WHERE table_name = {table_name}")
    table_exists = cur.fetchone()[0]
    if not table_exists:
        cur.execute('''create table user_request (user_id int,request int)''')
        print("User Requests table created")

    table_name='success_request'
    cur.execute(f"SELECT count(*) FROM all_tables WHERE table_name = {table_name}")
    table_exists = cur.fetchone()[0]
    if not table_exists:
        cur.execute('''create table success_request (request int)''')
        print("Successful Request table created")

def insert_into_tables():
    cur.execute('''insert into user as select count(*) from(select distinct user_id from processed_data )''')
    cur.commit()
    print("Insert successfull for user table")
    cur.execute('''insert into user_request as select user_id,count(*) from processed_data group by user_id''')
    cur.commit()
    print("Insert successfull for User Requests table")
    cur.execute('''insert into success_request as select count(*) from processed_data where status_code=200 ''')
    cur.commit()
    print("Insert successfull for Successful Request table")


cur.close()

# Stop SparkSession
spark.stop()

def main():
    create_tables()
    insert_into_tables()
