from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaConsumer
from time import sleep
from datetime import datetime
from random import randrange
from collections import OrderedDict 
import time, pickle

from A3 import dumpTweetsData, StoreTweets

Path = "./KafkaData/" # 10-min
MAX_SIZE = 10000
tweets = [OrderedDict(), OrderedDict(), OrderedDict(), OrderedDict()]
time_track = [0]*5
time_track[0] = 0; time_track[1] = 0; timerthirty = 0; timerforty = 0
file_count = [0]*4
tweet_time_counter = [0, 0]


if __name__ == "__main__":
	
	kafkaConsumer = KafkaConsumer('bdaa3', bootstrap_servers=['localhost:9092'], consumer_timeout_ms=10000)
	time_track[0] = time.time(); time_track[1] = time.time(); time_track[2] = time.time(); time_track[3] = time.time()
	time_track[4] = time.time()
	print("hi")
	for tweet in kafkaConsumer:
		uid = tweet.value.decode()
		# print(uid)
		
		tweet_time_counter[0] += 1
		current_time = time.time()
		time_track, file_count, tweets = dumpTweetsData(time_track, current_time, file_count, tweets)

		if current_time-time_track[4] > 120 and current_time - time_track[4] < 150:
			tweet_time_counter[1] += current_time-time_track[4]
			time_track[4] = current_time
			print("Tweets: {}, Time: {} min".format(tweet_time_counter[0], tweet_time_counter[1]/60))

		tweets = StoreTweets(tweets, uid)

