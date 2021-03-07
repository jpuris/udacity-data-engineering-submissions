import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import dayofmonth
from pyspark.sql.functions import dayofweek
from pyspark.sql.functions import hour
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import month
from pyspark.sql.functions import udf
from pyspark.sql.functions import weekofyear
from pyspark.sql.functions import year
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processes the song data with the provided SparkSession object.
    Input is read from input_data path and output is saved in output_data path.

    :param spark: pyspark.sql.SparkSession obj, Spark session
    :param input_data: S3, file or other path to input data
    :param output_data: S3, file or other path to output data
    :return: None
    """

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*'

    # read song data file
    df = spark.read.json(
        song_data, mode='PERMISSIVE',
        columnNameOfCorruptRecord='corrupt_record',
    ).drop_duplicates()

    # extract columns to create songs table
    df_songs = df.select(
        'song_id', 'title', 'artist_id',
        'year', 'duration',
    ).drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    df_songs.write.parquet(
        output_data + 'songs/',
        mode='overwrite', partitionBy=['year', 'artist_id'],
    )

    # extract columns to create artists table
    df_artists = df.select(
        'artist_id', 'artist_name', 'artist_location', 'artist_latitude',
        'artist_longitude',
    ).drop_duplicates()

    # write artists table to parquet files
    df_artists.write.parquet(output_data + 'artists/', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """Processes the log data with the provided SparkSession object.
    Input is read from input_data path and output is saved in output_data path.

    :param spark: pyspark.sql.SparkSession obj, Spark session
    :param input_data:
    :param output_data:
    :return:
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/')

    # read log data file
    df = spark.read.json(
        log_data, mode='PERMISSIVE',
        columnNameOfCorruptRecord='corrupt_record',
    ).drop_duplicates()

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    df_users = df.select(
        'userId', 'firstName', 'lastName',
        'gender', 'level',
    ).drop_duplicates()

    # write users table to parquet files
    df_users.write.parquet(
        os.path.join(
            output_data, 'users/',
        ), mode='overwrite',
    )

    # create timestamp column from original timestamp column
    get_timestamp = udf(
        lambda x: datetime.utcfromtimestamp(
            int(x) / 1000,
        ), TimestampType(),
    )
    df = df.withColumn('start_time', get_timestamp('ts'))

    # extract columns to create time table
    df_time = df.withColumn('hour', hour('start_time')) \
        .withColumn('day', dayofmonth('start_time')) \
        .withColumn('week', weekofyear('start_time')) \
        .withColumn('month', month('start_time')) \
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', dayofweek('start_time')) \
        .select('ts', 'start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').drop_duplicates()

    # write time table to parquet files partitioned by year and month
    df_time.write.parquet(
        os.path.join(
            output_data, 'time_table/',
        ), mode='overwrite', partitionBy=['year', 'month'],
    )

    # read in song data to use for songplays table
    song_df = spark.read \
        .format('parquet') \
        .option('basePath', os.path.join(output_data, 'songs/')) \
        .load(os.path.join(output_data, 'songs/*/*/'))

    # extract columns from joined song and log datasets to create songplays table
    df_songplays = df \
        .join(song_df, df.song == song_df.title, how='inner') \
        .select(
            monotonically_increasing_id().alias('songplay_id'), col(
                'start_time',
            ), col('userId').alias('user_id'),
            'level', 'song_id', 'artist_id', col(
                'sessionId',
            ).alias('session_id'), 'location',
            col('userAgent').alias('user_agent'),
        )

    df_songplays = df_songplays \
        .join(df_time, df_songplays.start_time == df_time.start_time, how='inner') \
        .select(
            'songplay_id', df_songplays.start_time, 'user_id', 'level', 'song_id', 'artist_id', 'session_id',
            'location', 'user_agent', 'year', 'month',
        ).drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    df_songplays.write.parquet(
        os.path.join(output_data, 'songplays/'), mode='overwrite',
        partitionBy=['year', 'month'],
    )


def main():
    """Processes Sparkify song and log data using Spark.
    Input is JSON song and log files and output is Parquet files.

    :return: None
    """
    spark = create_spark_session()

    # The directory paths for Sparkify input and output data
    input_data = 's3://udacity-data-engineer-nanodegree/project_3_data_lake/input_data/'
    output_data = 's3://udacity-data-engineer-nanodegree/project_3_data_lake/output_data/'

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == '__main__':
    main()
