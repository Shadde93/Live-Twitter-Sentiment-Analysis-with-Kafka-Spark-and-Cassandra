import os, configparser
import tweepy
import kafka, socket



class TweetsListenerSocket(tweepy.StreamListener):

    def __init__(self, host, port):
        super(TweetsListenerSocket, self).__init__()
        self.client_socket = self.create_socket(host, port)

    def create_socket(self, host, port):
        s = socket.socket()  # Create a socket object
        s.bind((host, port))  # Bind to the port

        print("Listening on port: %s" % str(port))
        s.listen(5)  # Now wait for client connection.
        c, addr = s.accept()  # Establish connection with client.
        print("Received request from: " + str(addr))

        return s

    def on_data(self, data):
        try:
            self.client_socket.send(data.encode('utf-8'))
            print(data)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


class TweetsListenerKafka(tweepy.StreamListener):

    def __init__(self, host, port):
        super(TweetsListenerKafka, self).__init__()
        self.producer = TweetsListenerKafka.create_producer(host, port)

    def create_producer(host, port):
        conf_str = str(host)+":"+str(port)
        print("creating kafka client "+conf_str+"...")
        kafka_client = kafka.SimpleClient(conf_str)
        print("creating kafka producer...")
        producer = kafka.SimpleProducer(kafka_client)  # batch_send_every_n = xxx
        print("kafka producer created!")
        return producer

    def on_data(self, data):
        try:
            self.producer.send_messages('twitterstream', data.encode('utf-8'))

            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def send_data(host, port, search_filter, is_socket=False):
    """
    Here we give users an option to use socket connections instead of kafka, just to test the difference.
    
    """
    # Read the credententials from 'twitter.txt' file
    config = configparser.ConfigParser()
    config.read('twitter.txt')
    consumer_key = config['DEFAULT']['consumer_key']
    consumer_secret = config['DEFAULT']['consumer_secret']
    access_token = config['DEFAULT']['access_token']
    access_secret = config['DEFAULT']['access_secret']
    print("configurations parsed and saved!")

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    if is_socket:
        tweet_listener = TweetsListenerSocket(host, port)
    else:
        tweet_listener = TweetsListenerKafka(host, port)

    print("TweetListener initialised!")

    twitter_stream = tweepy.Stream(auth, tweet_listener)
    twitter_stream.filter(languages=["en"], track=[search_filter])
    print("Twitter stream activated!")


if __name__ == "__main__":
    #set the topic_filter to get tweets corresponding to a topic
    topic_filter = '14th Amendment'
    send_data("localhost", 9092, topic_filter, is_socket=False)




