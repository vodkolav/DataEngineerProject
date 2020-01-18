from pyspark.sql.types import StructType, StringType, MapType, StructField,\
                             BooleanType, DateType, NumericType, IntegerType,\
                             LongType, TimestampType, FloatType, ArrayType
import pyarrow as pa

topic = 'TweeterArchive'
partitionCol = "created_ym"
Keywords = 'Tesla'


hdfs_host = 'localhost'
hdfs_port = 8020 #9870
hive_port = 10000
hive_username = 'hdfs'
hive_password = 'naya'
hive_database = 'tesla'
hive_mode = 'CUSTOM'

hdfs_archive_path = '/tmp/project/archive'    
hdfs_archive_path

hdfs_hive_warehouse = '/user/hive/warehouse/' 
hdfs_hive_warehouse

hdfs_hive_tweets = f"{hdfs_hive_warehouse}/{hive_database}.db/tweets"
hdfs_hive_tweets

hdfs_hive_users = f"{hdfs_hive_warehouse}/{hive_database}.db/users"
hdfs_hive_users

hdfs_hive_users_staging = f"{hdfs_hive_warehouse}/{hive_database}.db/users_staging"
hdfs_hive_users_staging

#used in tweeter_producer
event_fields = [ 'id', 'text','created_at', 'geo', 'coordinates', 'place',
                 'quote_count', 'reply_count', 'retweet_count', 'favorite_count' ]

#used in Spark_consumer
tweet_keys =  event_fields + ['user_id', 'user_followers' ]

tweet_types = [LongType, StringType, TimestampType, StringType, StringType, StringType, 
               IntegerType, IntegerType, IntegerType, IntegerType, LongType, IntegerType]

# tweet_types  = [StringType]* 11 

#used in tweeter_producer
user_fields = ['id', 'name', 'screen_name','created_at', 'location', 'url',
                         'protected', 'verified', 'followers_count', 'friends_count',
                         'listed_count', 'favourites_count', 'statuses_count', 'withheld_in_countries']
#used in Spark_consumer
user_keys = user_fields + [partitionCol]

user_types = [LongType, StringType, StringType, TimestampType, StringType, StringType, 
              BooleanType, BooleanType, IntegerType, IntegerType, 
              IntegerType, IntegerType, IntegerType, StringType, StringType]

# user_types = [StringType]* 14





