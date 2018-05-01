from twython import Twython
from twython import TwythonStreamer
from auth import *
from pprint import pprint
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pyodbc
import sys

analyser = SentimentIntensityAnalyzer()
conn = pyodbc.connect(r'DSN=adwDSN;UID=python;PWD=password')         
cur = conn.cursor()

tweets = []

class MyTwythonStreamer(TwythonStreamer):  
	def on_success(self, data):
		tweets = []
		if data['lang'] == 'en':
			tweets.append(data)
			print('received tweet #{}'.format(len(tweets)))

			for i in range(0,len(tweets)):
				created_at = str(tweets[i]['timestamp_ms'])
				did_work = 0
				try:
					full_text = str(tweets[i]['extended_tweet']['full_text'])
					sentiment = str(analyser.polarity_scores(tweets[i]['extended_tweet']['full_text']))
					did_work = 1
				except: 
					pass
				try:
					full_text = str(tweets[i]['quoted_status']['extended_tweet']['full_text'])
					sentiment = str(analyser.polarity_scores(tweets[i]['quoted_status']['extended_tweet']['full_text']))
					did_work = 1
				except:
					pass
				try:
					full_text = str(tweets[i]['retweeted_status']['extended_tweet']['full_text'])
					sentiment = str(analyser.polarity_scores(tweets[i]['retweeted_status']['extended_tweet']['full_text']))
					did_work = 1
				except:
					pass
				finally:
					if did_work == 0:
						full_text = str(tweets[i]['text'])
						sentiment = str(analyser.polarity_scores(tweets[i]['text']))

				name = str(tweets[i]['user']['name'])
				username = str(tweets[i]['user']['screen_name'])
				location = str(tweets[i]['user']['location'])
				user_desc = str(tweets[i]['user']['description'])
				follower_count = str(tweets[i]['user']['followers_count'])
				status_count = str(tweets[i]['user']['statuses_count'])
				tweet_source = str(tweets[i]['source'])
				insert_line = 'INSERT INTO Adventureworks2012.aa_tweets.tweets_immigrants (created_at,full_text,sentiment,name,username,location,user_desc,follower_count,status_count,tweet_source) VALUES (?,?,?,?,?,?,?,?,?,?)'
				try:
					cur.execute(insert_line,created_at,full_text,sentiment,name,username,location,user_desc,follower_count,status_count,tweet_source)
					cur.commit()
				except:
					pass

		if len(tweets) >= 10:
			self.disconnect()
	def on_error(self, status_code, data):
		print(status_code, data)
		self.disconnect()

stream = MyTwythonStreamer(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
stream.statuses.filter(track='immigrant')   #space='and'  #comma='or'
