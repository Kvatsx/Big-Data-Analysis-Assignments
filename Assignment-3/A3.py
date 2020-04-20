#!/usr/bin/env python3
# -*- coding:utf-8 -*-

'''
References-
1. http://adilmoujahid.com/posts/2014/07/twitter-analytics/
2. https://towardsdatascience.com/getting-started-with-apache-kafka-in-python-604b3250aa05
'''

# from __future__ import absolute_import
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer


access_token = "1168926103026753538-2SKmRCDc2vMM3HwSpDbHyV26lji6m3"
access_token_secret = "YASKIXuO49K4P8UWdM1HTT68ylgx0rv8BggOknZF4gzAN"
consumer_key = "Wjhu1vsNmLq5XqK2MYxOpoT0g"
consumer_secret = "YwZHlGbtpYQOH8TLAStFMgWiL5Cn6hOChC6Ze759h2x5kYEawj"


class StdOutListener(StreamListener):

	# def on_data(self, data):
	# 	# producer.send("covid", data.encode('utf-8'))
	# 	# producer.flush()
	# 	# print(data)
	# 	return True

	def on_status(self, post):
		print(post.user.id)

	def on_error(self, status):
		print(status)

if __name__ == "__main__":
	# kafka = KafkaClient()
	producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
	stdoutListner = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	stream = Stream(auth, stdoutListner)
	stream.filter(languages=["en"], track=['#Covid', '#Coronavirus'])
