import os,sys #for os.environment handling
import re

os.environ['PYSPARK_SUBMIT_ARGS'] = """--packages \
org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.2,\
org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 \
pyspark-shell"""


from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import json #for raw tweet parsing
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import Row, SparkSession
from pyspark import sql

from cassandra.cluster import Cluster

from textblob import TextBlob
from dateutil import parser


def quiet_logs(sparkcontext):
    logger = sparkcontext._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def map_raw_to_tuple(raw_data):
    """ Takes raw tweets and takes out the date/time, language, and text """
    json_parsed_data = json.loads(raw_data[1])
    # pprint.pprint(json_parsed_data)
    if 'created_at' in json_parsed_data:
        time_field = json_parsed_data['created_at']
        lang_field = json_parsed_data['lang']
        text_field = json_parsed_data['text']
        return time_field, lang_field, text_field
    else:
        """ This means we reached the limit data...
            twitter {'limit': {'timestamp_ms': 'xxxxxx', 'track': xx}} 
            """
        return None, None, None

def format_time(tweet_data, time_format = "%Y-%m-%d %H:%M:%S"):
    parsed_time = parser.parse(tweet_data[0])
    new_time_format = parsed_time.strftime(time_format)

    return (new_time_format, tweet_data[1], tweet_data[2])


def get_sanitation_function():
    """
    To get faster regex, we compile a pattern, and use that throughout our program.
    """
    regex_v2 = r"@([A-Za-z0-9_]+)|#([A-Za-z0-9_])+|http\S+|[^A-Z^a-z^ ^]+"
    regex_pattern = re.compile(regex_v2, flags=re.UNICODE)

    return lambda tweet: sanitize_tweets_fast(tweet, regex_pattern)

def sanitize_tweets_fast(tweet_data, regex_obj):
    #Possibility that this doesnt work, depending on how spark works with this shit
    """
    Removes links, hashtags, username tags, emojis, and punctuation.
    """
    tweet_sanitize = re.sub(' +|[\/\-\n\t]+',' ',tweet_data[2])
    tweet_sanitize = regex_obj.sub(r'', tweet_sanitize)
    return (tweet_data[0], tweet_data[1], tweet_sanitize.lower())



def calculate_sentiment(tweet_data):
    """
    Gets the sentiment by using the textblob api
    """
    tweet_sentiment = TextBlob(tweet_data[2]).sentiment
    return (tweet_data[0], (tweet_sentiment[0], tweet_sentiment[1])) #Set the score in the tuple

def average_sentiment(tweet_data):
    sentiments = tweet_data[1]

    average_values = tuple(map(lambda y: sum(y) / float(len(y)), zip(*sentiments)))
    return (tweet_data[0], average_values)

def saveDataToCassandra(x):
    cluster = Cluster()
    session = cluster.connect()
    session.set_keyspace("twitter_sentiment")

    session.execute(
        """
        INSERT INTO twitter_sentiment_table (createdat, sentiment_polarity, sentiment_subjectivity)
        VALUES (%(createdat)s, %(sentiment_polarity)s, %(sentiment_subjectivity)s)
        """,
        {'createdat': x[0], 'sentiment_polarity': x[1], 'sentiment_subjectivity': x[2]}
    )
    #cluster.shutdown()
    return x
                

def recieve_data(ip_address, port, is_socket=False):
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    sqlContext = sql.SQLContext(sc)
    quiet_logs(sc)
    ssc = StreamingContext(sc, 5) # 5 second batch interval
    conf_str = str(ip_address) + ":" + str(port)

    if is_socket:
        raw_tweets = ssc.socketTextStream(ip_address, port)
    else:
        raw_tweets = KafkaUtils.createDirectStream(
            ssc, topics=['twitterstream'], kafkaParams={"metadata.broker.list": conf_str})


    sanitation_function = get_sanitation_function()#Compiles the regex objects
    parsed_tweets = raw_tweets.map(map_raw_to_tuple).filter(lambda x: x[0] is not None)
    parsed_tweets = parsed_tweets.map(format_time)
    parsed_tweets = parsed_tweets.map(sanitation_function)
    parsed_tweets = parsed_tweets.map(calculate_sentiment)
    parsed_tweets = parsed_tweets.groupByKey().mapValues(list).map(average_sentiment)
    parsed_tweets = parsed_tweets.map(lambda x: (x[0], x[1][0], x[1][1]))

    parsed_tweets = parsed_tweets.map(saveDataToCassandra)
    parsed_tweets.pprint()

    ssc.start()			   # Start reading the stream
    ssc.awaitTermination() # Wait for the process to terminate
    


if __name__ == "__main__":
    ip_address = "localhost"  # Replace with your stream IP
    port = 9092  # Replace with your stream port


    cluster = Cluster()
    session = cluster.connect()

    key_space_name = "twitter_sentiment"
    session.execute("create keyspace if not exists {} with replication = {'class': SimpleStrategy, 'replication_factor': 1};".format(key_space_name))
    session.execute("create table if not exists {} (timestamp timestamp, sentiment_polarity float, sentiment_subjectivity float, primary key (timestamp));".format(key_space_name+'_table'))

    recieve_data(ip_address, port, is_socket=False)




##Nice functions

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

def convertToDataFrame(time, rdd):
    # Get the singleton instance of SparkSession
    spark = getSparkSessionInstance(rdd.context.getConf())

    # Convert RDD[String] to RDD[Row] to DataFrame
    rowRdd = rdd.map(lambda w: Row(word=w))
    dataFrame = spark.createDataFrame(rowRdd)
