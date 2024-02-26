import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col

#Connecting to the SparkSession
spark=SparkSession.builder.appName("TDKCaseStudy").getOrCreate()


today = datetime.date.today()

directory_path = "/home/valli/Downloads/TDK DE Logs Case Study/logs"
for filename in os.listdir(directory_path):
    file_path = os.path.join(directory_path, filename)
    if os.path.isfile(file_path):
        file_date = datetime.date.fromtimestamp(os.path.getmtime(file_path))  
        if file_date == today:
            print(f"{filename} is from today")
            files= [file for file in os.listdir(directory_path) if file.endswith('.log')]
            for file in files:
                file_path=os.path.join(directory_path,file)
                file_df=spark.read.text(file_path)
                split_file = file_df.select(split(file_df.value, " ").alias("_tmp")) \
                                   .select(
                                       col("_tmp")[0].alias("IP_ADDRESS"),
                                       col("_tmp")[1].alias("IDENTD"),
                                       col("_tmp")[2].alias("USER_ID"),
                                       col("_tmp")[3].alias("TIMESTAMP"),
                                       col("_tmp")[4].alias("PROTOCOL_VERSION"),
                                       col("_tmp")[5].alias("STATUS_CODE"),
                                       col("_tmp")[6].alias("OBJECT_SIZE"),
                                       col("_tmp")[7].alias("URL"),
                                       col("_tmp")[8].alias("BROWSER_NAME")
                                   )
                #split_file.show(split_file.count(),truncate=False)
                split_file.write.format('jdbc').options(
                    url='jdbc:oracle:thin:@192.168.11.100:1521:ORCL',
                    driver='oracle.jdbc.driver.OracleDriver',
                    dbtable='testschema.test',
                    user='testschema',
                    password='password').mode('append').save()

# Stop SparkSession
spark.stop()
