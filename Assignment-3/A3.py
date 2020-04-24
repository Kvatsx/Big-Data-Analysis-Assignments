#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'''
References-
1. http://adilmoujahid.com/posts/2014/07/twitter-analytics/
2. https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05
'''

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from time import sleep
from datetime import datetime
from random import randrange
from collections import OrderedDict 
import time, pickle

# access_token = "1168926103026753538-2SKmRCDc2vMM3HwSpDbHyV26lji6m3"
# access_token_secret = "YASKIXuO49K4P8UWdM1HTT68ylgx0rv8BggOknZF4gzAN"
# consumer_key = "Wjhu1vsNmLq5XqK2MYxOpoT0g"
# consumer_secret = "YwZHlGbtpYQOH8TLAStFMgWiL5Cn6hOChC6Ze759h2x5kYEawj"
access_token = "1185511879466078208-Edejt5qll7hjHlSzYOAFEoI4D6POEQ"
access_token_secret =  "srOyoLUTpKcXBiH26nv5wfJTIRKDul89B3wiHSOZEBlxk"
consumer_key =  "365Nf7GiaeSbWjCCxF7enLG1N"
consumer_secret =  "rkaXlWMgWqmqS3UDwz9R3XPEWFsPP1gqmgZdoP08dlXP2dcRoN"


Path = "./KafkaData/" # 10-min
MAX_SIZE = 10000
tweets = [OrderedDict(), OrderedDict(), OrderedDict(), OrderedDict()]
time_track = [0]*5
time_track[0] = 0; time_track[1] = 0; time_track[2] = 0; time_track[3] = 0
file_count = [0]*4
tweet_time_counter = [0, 0]

def getCurrentTime():
	now = datetime.now()
	current_time = now.strftime("%H:%M:%S")
	return current_time

def run():
	stdoutListner = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	stream = Stream(auth, stdoutListner)
	time_track[0] = time.time(); time_track[1] = time.time(); time_track[2] = time.time(); time_track[3] = time.time()
	time_track[4] = time.time()
	stream.filter(languages=["en"], track=['#Covid19', '#Coronavirus', '#Covid-19']) # , is_async=True

def handleTweet(temp, uid):
	index = randrange(MAX_SIZE)
	uid_to_replace = list(temp.keys())[index]
	temp.pop(uid_to_replace)
	temp[uid] = 1
	return temp


def dumpTweetsData(ttrack, current_time, file_count, tweets):
	if (file_count[0] < 15 and current_time - ttrack[0] > 600 and current_time - ttrack[0] < 660 ):
		ttrack[0] = current_time
		file_count[0]+=1
		pickle.dump(tweets[0], open(Path + '10-min/tweet_' + str(file_count[0]), 'wb'))
		tweets[0] = OrderedDict()

	if (file_count[1] < 15 and current_time - ttrack[1] > 1200 and current_time - ttrack[1] < 1260 ):
		ttrack[1] = current_time
		file_count[1]+=1
		pickle.dump(tweets[1], open(Path + '20-min/tweet_' + str(file_count[1]), 'wb'))
		tweets[1] = OrderedDict()

	if (file_count[2] < 15 and current_time - ttrack[2] > 1800 and current_time - ttrack[2] < 1860 ):
		ttrack[2] = current_time
		file_count[2]+=1
		pickle.dump(tweets[2], open(Path + '30-min/tweet_' + str(file_count[2]), 'wb'))
		tweets[2] = OrderedDict()

	if (file_count[3] < 15 and current_time - ttrack[3] > 2400 and current_time - ttrack[3] < 2460 ):
		ttrack[3] = current_time		
		file_count[3]+=1
		pickle.dump(tweets[3], open(Path + '40-min/tweet_' + str(file_count[3]), 'wb'))
		tweets[3] = OrderedDict()
	return ttrack, file_count, tweets

def StoreTweets(tweets, uid):
	# ---------------------
	if file_count[0] < 15:
		if len(tweets[0]) == MAX_SIZE:
			tweets[0] = handleTweet(tweets[0], uid)
		else:
			if uid in tweets[0]:
				tweets[0][uid] = tweets[0][uid] + 1
			else:
				tweets[0][uid] = 1
	# ---------------------
	if file_count[1] < 15:
		if len(tweets[1]) == MAX_SIZE:
			tweets[1] = handleTweet(tweets[1], uid)
		else:
			if uid in tweets[1]:
				tweets[1][uid] = tweets[1][uid] + 1
			else:
				tweets[1][uid] = 1
	# ---------------------
	if file_count[2] < 15:
		if len(tweets[2]) == MAX_SIZE:
			tweets[2] = handleTweet(tweets[2], uid)
		else:
			if uid in tweets[2]:
				tweets[2][uid] = tweets[2][uid] + 1
			else:
				tweets[2][uid] = 1
	# ---------------------
	if file_count[3] < 15:
		if len(tweets[3]) == MAX_SIZE:
			tweets[3] = handleTweet(tweets[3], uid)
		else:
			if uid in tweets[3]:
				tweets[3][uid] = tweets[3][uid] + 1
			else:
				tweets[3][uid] = 1
	return tweets


class StdOutListener(StreamListener):

	def on_status(self, post):
		global time_track, file_count, tweets
		tweet_time_counter[0] += 1
		current_time = time.time()
		time_track, file_count, tweets = dumpTweetsData(time_track, current_time, file_count, tweets)

		uid = post.user.id
		if current_time-time_track[4] > 120 and current_time - time_track[4] < 150:
			tweet_time_counter[1] += current_time-time_track[4]
			time_track[4] = current_time
			print("Tweets: {}, Time: {} min".format(tweet_time_counter[0], tweet_time_counter[1]/60))
		tweets = StoreTweets(tweets, uid)

	def on_error(self, status):
		print("[Error] Status: {}, Time: {}".format(status, getCurrentTime()))
		sleep(60)
		return True

if __name__ == "__main__":
	run()
	
