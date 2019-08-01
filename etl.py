
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.postgresql:postgresql:42.1.1 pyspark-shell'
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType


def create_spark_session():
    """ Scanning Spark Components """
    spark = SparkSession \
        .builder \
        .master("local") \
        .getOrCreate()
    return spark

def process_song_data(spark,songdata,url,properties):
    #process the songs table
    song_data_db = spark.read.jdbc(url=url,\
    table="songs", \
    properties=properties)
    song_data = songdata[['song_id','title','artist_id', 'year', 'duration']]

    song_data_db.registerTempTable("songsdb")
    song_data.registerTempTable("songsnew")
    song_insert = spark.sql("select n.* from songsnew n left outer join songsdb d on d.song_id = n.song_id where d.song_id is null")
    song_insert.write.jdbc(url=url, table='songs',mode='append',properties=properties)

    #process artist data
    artist_data = songdata[['artist_id','artist_name','artist_location', 'artist_latitude', 'artist_longitude']]
    
    artist_db = spark.read.jdbc(url=url,\
    table="artists", \
    properties=properties)
    
    artist_data.registerTempTable("artist")
    artist_db.registerTempTable("artist_db")
    print("insert data to artist")
    artist_insert = spark.sql("select distinct a.* from artist a left outer join artist_db d on a.artist_id= d.artist_id where d.artist_id is null")
    artist_insert.write.jdbc(url=url, table='artists',mode='append',properties=properties)
    
def process_log_data(spark,logdata,url,properties):
    
    logdata = logdata[logdata['page'] == 'NextSong']
    logdata.registerTempTable("log_data")

    #process time data
    time_data = spark.sql("select FROM_UNIXTIME(ts,'yyyy-MM-dd HH:mm:ss.sss') as start_time,hour(from_unixtime(ts/1000)) as hour,day(from_unixtime(ts/1000)) as day,weekofyear(from_unixtime(ts/1000))  as week,month(from_unixtime(ts/1000)) as month,year(from_unixtime(ts/1000)) as year,weekday(from_unixtime(ts/1000)) as weekday from log_data")
    
    time_db = spark.read.jdbc(url=url,\
    table="time", \
    properties=properties)
    time_db.registerTempTable("timedb")
    time_data.registerTempTable("time")
    time_insert = spark.sql("select distinct t.* from time t left outer join timedb d on d.start_time = t.start_time where d.hour is null")
    time_insert.write.jdbc(url=url, table='time',mode='append',properties=properties)
    
    #process user data
    
    user_df = logdata[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df= user_df.select(col("userId").alias("user_id").cast(IntegerType()),col("firstname").alias("first_name"),col("lastName").alias("last_name"),col("gender"),col("level"))
    user_db = spark.read.jdbc(url=url,\
    table="users", \
    properties=properties)
    user_df.registerTempTable("users")
    user_db.registerTempTable("users_db")
    user_insert = spark.sql("select distinct u.* from users u left outer join users_db d on u.user_id= d.user_id where d.user_id is null")
    user_insert.write.jdbc(url=url, table='users',mode='append',properties=properties)
    
    
def main():
    spark = create_spark_session()
    url = "jdbc:postgresql://127.0.0.1/etl"
    properties = {
        "driver": "org.postgresql.Driver",
        "user": "etl",
        "password": "etl"
                }
    logdata = spark.read.json("jsondata/log_data/*/*/*.json")
    songdata = spark.read.json("jsondata/song_data/*/*/*/*.json")
    process_song_data(spark,songdata,url,properties)
    process_log_data(spark,logdata,url,properties)
    
    

if __name__ == "__main__":
    main()