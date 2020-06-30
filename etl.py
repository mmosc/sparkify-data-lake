import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField,DecimalType as Dec, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','KEY')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','SECRET')


def create_spark_session():
    '''
    Creates and returns the spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data, aws=False):
    """
    Process song data by iterating over the .json files in
    the song_data folder in the input_data folder. 
    Creates songs and artists tables.
    If aws is True, writes the tables to the S3 bucket 
    give in the output_data
    
    key arg:
        - spark: the spark session
        - input_data: the path to the folder containing the song_data
        - output_data: the path to the S3 bucket where to write the tables
        - aws: to set to true when the script is executed on the cluster. 
            Set to False when executing locally for debugging
    """
    
    # Define the song schema before importing data
    songsSchema = R([
        StructField("num_songs",Int()),
        StructField("artist_id",Str()),
        StructField("artist_latitude",Dec()),
        StructField("artist_longitude",Dec()),
        StructField("artist_location",Str()),
        StructField("artist_name",Str()),
        StructField("song_id",Str()),
        StructField("title",Str()),
        StructField("duration",Dbl()),
        StructField("year",Int()),
])
    
    
    
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # read song data file
    print("Reading song_data from {}\n".format(song_data))
    df = spark.read.json(song_data,schema=songsSchema)
    
    # extract columns to create songs table
    print("Extracting columns to create the songs table...\n")
    df.createOrReplaceTempView("songs_data_table")
    songs_table = spark.sql('''
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM songs_data_table
    ''')
    print("done.")
    
    
    # print song table schema
    print("Songs table schema:\n")
    songs_table.printSchema()
    
    # write songs table to parquet files partitioned by year and artist
    if aws:
        print("Writing the songs table to parquet files partitioned by year and artist...\n")
        songs_table.write.parquet(output_data + "songs_table.parquet",
                                 partitionBy = ["year", "artist_id"],
                                 mode = "overwrite")
        print("done.")

    # extract columns to create artists table
    print("Extracting columns to create the artists table...\n")
    artists_table = spark.sql('''
        SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
        FROM songs_data_table
    ''')
    print("done.")
    
    # print artists table schema
    print("Artists table schema:\n")
    artists_table.printSchema()

    # write artists table to parquet files
    if aws:
        print("Writing the artists table to parquet files ...\n")
        artists_table.wtiteparquet(output_data + "artists_table.parquet",
                                  mode = "overwrite")
        print("done.")



def process_log_data(spark, input_data, output_data, aws=False):   
    """
    Process log and song data by iterating over the 
    - the .json files in the log_data folder
    - the .json files in the song_data folder. 
    
    Creates the users, times and songplays tables.
    If aws is True, writes the tables to the S3 bucket 
    give in the output_data
    
    key arg:
        - spark: the spark session
        - input_data: the path to the folder containing the song_data
        - output_data: the path to the S3 bucket where to write the tables
        - aws: to set to true when the script is executed on the cluster. 
            Set to False when executing locally for debugging
    """
    
    songsSchema = R([
        StructField("num_songs",Int()),
        StructField("artist_id",Str()),
        StructField("artist_latitude",Dec()),
        StructField("artist_longitude",Dec()),
        StructField("artist_location",Str()),
        StructField("artist_name",Str()),
        StructField("song_id",Str()),
        StructField("title",Str()),
        StructField("duration",Dbl()),
        StructField("year",Int()),
    ])
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    
    # read log data file
    print("Reading log_data from {}\n".format(log_data))
    df = spark.read.json(log_data)
    print("done.")
    
    # filter by actions for song plays
    print("Filter by actions for song plays...")
    df = df.filter(df.page=='NextSong')
    df.createOrReplaceTempView("logs_data_table")
    print("done.")
    
    # extract columns for users table   
    print("Extract columns for users table...")
    users_table = spark.sql('''
        SELECT DISTINCT userId as user_id, firstName as first_name, lastName as last_name, gender, level
        FROM logs_data_table
    ''')
    users_table = users_table.dropDuplicates(["user_id"])
    print("done.")

    
    # write users table to parquet files
    if aws: 
        print("Write users table to parquet files...")
        users_table.write.parquet(output_data + "users_table.parquet",
                                 mode = "overwrite") 
        print("done.")

    # create datetime column from original timestamp column 
    print("Create datetime column from original timestamp column...")
    get_datetime = udf(lambda time: datetime.fromtimestamp((time/1000.0)), Date())
    df = df.withColumn("date",get_datetime("ts")) 
    print("done.")
    
    
    # create timestamp column from original timestamp column 
    print("Create timestamp column from original timestamp column...")
    convert_ts = udf(lambda time: datetime.fromtimestamp((time/1000.0)), TimestampType())
    df = df.withColumn("ts",convert_ts("ts")) 
    print("done.")

    # extract columns to create time table 
    print("Extract columns to create time table...")
    df.createOrReplaceTempView("clean")
    time_table = spark.sql('''
        SELECT ts AS start_time, 
            date_format(date,'YYYY') AS year,
            date_format(date,'MM') AS month,
            date_format(date,'dd') AS day,
            date_format(date,'w') AS week,
            date_format(ts,'E') AS weekday,
            HOUR(ts) AS hour
        FROM clean
    ''').dropDuplicates(["start_time"])
    print("done.")
    
    # write time table to parquet files partitioned by year and month
    if aws: 
        print("Write time table to parquet files partitioned by year and month...")
        time_table.write.parquet(output_data + "songs_table.parquet",
                                 partitionBy = ["year", "month"],
                                 mode = "overwrite") 
        print("done.")

    # read in song data to use for songplays table 
    print("Read in song data to use for songplays table...")
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data, schema=songsSchema)
    song_df.createOrReplaceTempView("songs_data_table") 
    print("done.")

    # extract columns from joined song and log datasets to create songplays table  
    print("Extract columns from joined song and log datasets to create songplays table...")
    artists_table = spark.sql('''
        SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude
        FROM songs_data_table
    ''')
    artists_table.createOrReplaceTempView("artists") 
    print("done.")
    
     
    print("Extract columns to create songplays table...")
    songplays_table = spark.sql('''
        SELECT 
            l.ts AS start_time,
            l.userId AS user_id,
            l.level,
            s.song_id,
            a.artist_id,
            l.sessionId AS session_id,
            l.location,
            l.userAgent AS user_agent
        FROM clean AS l
        JOIN songs_data_table AS s 
        ON (l.song = s.title AND l.artist = s.artist_name)  
        JOIN artists AS a ON a.artist_id=s.artist_id
        LIMIT 5
    ''')
    print("done.")
    
    print("Create songplays_id...")
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    print("done.")

    # write songplays table to parquet files partitioned by year and month
    if aws: 
        print("Write songplays table to parquet files partitioned by year and month...")
        songplays_table.write.parquet(output_data + "songs_table.parquet",
                             partitionBy = ["year", "month"],
                             mode = "overwrite")
        print("done.")


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    input_data = "./data/"
    output_data = "s3a://sparkify-udacity/"
    
    process_song_data(spark, input_data, output_data, aws=False)    
    process_log_data(spark, input_data, output_data, aws=False)


if __name__ == "__main__":
    main()
