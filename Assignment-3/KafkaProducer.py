from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer

from A3 import getCurrentTime
from time import sleep

# access_token = "1168926103026753538-2SKmRCDc2vMM3HwSpDbHyV26lji6m3"
# access_token_secret = "YASKIXuO49K4P8UWdM1HTT68ylgx0rv8BggOknZF4gzAN"
# consumer_key = "Wjhu1vsNmLq5XqK2MYxOpoT0g"
# consumer_secret = "YwZHlGbtpYQOH8TLAStFMgWiL5Cn6hOChC6Ze759h2x5kYEawj"
access_token = "1185511879466078208-Edejt5qll7hjHlSzYOAFEoI4D6POEQ"
access_token_secret =  "srOyoLUTpKcXBiH26nv5wfJTIRKDul89B3wiHSOZEBlxk"
consumer_key =  "365Nf7GiaeSbWjCCxF7enLG1N"
consumer_secret =  "rkaXlWMgWqmqS3UDwz9R3XPEWFsPP1gqmgZdoP08dlXP2dcRoN"


class StdOutListener(StreamListener):

	def on_status(self, post):
		uid = bytes(str(post.user.id), encoding="utf-8")
		producer.send("bdaa3", uid)
		producer.flush()
		# print(post.user.id)

	def on_error(self, status):
		print("[Error] Status: {}, Time: {}".format(status, getCurrentTime()))
		sleep(60)
		return True

if __name__ == "__main__":

	producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
	stdoutListner = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	stream = Stream(auth, stdoutListner)
	stream.filter(languages=["en"], track=['#Covid19', '#Coronavirus', '#Covid-19']) # , is_async=True
